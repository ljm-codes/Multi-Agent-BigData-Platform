"""
全局 ssh 连接池 —— 所有走 ssh 的工具共用这一份

**为什么全局且建了不关**：本集群的 ssh 握手实测 **10.6 秒**（OpenSSH 和 paramiko
一样慢，所以慢在服务端，多半是 sshd 的 UseDNS 在做反向解析；`UseDNS no` 能治本，
但要 root 改三台配置）。一轮诊断 5~10 次 ssh 调用，按次建连就是一两分钟纯握手。
所以：连上就一直留着，给后面所有工具复用。

入口可以先用 warm_up([...]) 把节点列表喂进来预热；没预热的 host 走懒建，
**建完同样不关**。服务端把连接断了的话，下一次调用会重连一次 —— 不然池里第一条
闲置连接会稳定失败一次，那很误导。

平台跑在非大数据节点上（Win 端），没有节点文件系统访问，所以：
  * 容器日志的**服务端过滤**（按级别 grep）只有这条路做得到 ——
    REST 那侧（NM containerlogs）只能整文件拉回来，而且返回的是 HTML 包壳；
  * dmesg / jstack 这类节点证据也只有这条路。
"""
import os
import socket
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

import paramiko

from SPO.tool_result import ERR_AUTH, ERR_REFUSED, ERR_TIMEOUT, ERR_UNKNOWN
from Tools.log_settings import LogSetting
from load_config.config import config

logger = LogSetting.create(__name__)

_ssh_conf = (config.get("ssh") or {})
SSH_USER = _ssh_conf.get("user", "hadoop")
SSH_PORT = int(_ssh_conf.get("port", 22))
SSH_TIMEOUT = int(_ssh_conf.get("timeout", 15))
# 密码从 .env 读（和 EMAIL_AUTH_CODE 一个路子）；留空则走密钥/agent 认证
SSH_PASSWORD = os.getenv("SSH_PASSWORD", "")

_pool: Dict[str, paramiko.SSHClient] = {}
# 每个连接一把锁：连接内的命令/SFTP 串行（gateway 本来也要求 ssh 类限并发），
# 同时避免多线程共用一个 paramiko 连接
_locks: Dict[str, threading.Lock] = {}
_pool_lock = threading.Lock()


@dataclass
class SshResult:
    """ok=False 表示**没执行成**；ok=True 但 rc!=0 表示执行了但命令自己失败 —— 两者别混"""
    ok: bool
    rc: int = -1
    stdout: str = ""
    stderr: str = ""
    error_kind: str = ""
    message: str = ""
    cost_ms: int = field(default=0)


def _endpoint(host: str, user: Optional[str] = None, port: Optional[int] = None,
              password: Optional[str] = None) -> Tuple[str, int, str]:
    """把「默认配置 + 调用方覆盖」合成一组连接参数"""
    return (str(user or SSH_USER), int(port or SSH_PORT),
            SSH_PASSWORD if password is None else str(password))


def _key(host: str, user: str, port: int) -> str:
    """池的键：同一台机器可能用不同账号/端口连（如 code.commit.ssh），不能只按 host"""
    return f"{user}@{host}:{port}"


def _lock_of(key: str) -> threading.Lock:
    with _pool_lock:
        return _locks.setdefault(key, threading.Lock())


def _connect(host: str, timeout: int, user: str, port: int,
             password: str) -> Tuple[Optional[paramiko.SSHClient], str, str]:
    """建连；失败返回 (None, 错误类别, 说明)"""
    client = paramiko.SSHClient()
    # 与测试脚手架的 -o StrictHostKeyChecking=no 等价：集群节点会重建，主机键留不住
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(
            hostname=host, port=port, username=user,
            password=password or None,
            timeout=timeout, banner_timeout=timeout, auth_timeout=timeout,
            look_for_keys=not password, allow_agent=not password,
        )
        # 创造 心跳
        client.get_transport().set_keepalive(timeout)
        return client, "", ""
    except paramiko.AuthenticationException as e:
        return None, ERR_AUTH, f"认证失败: {e}"
    except (socket.timeout, TimeoutError) as e:
        return None, ERR_TIMEOUT, f"建连超时({timeout}s): {e}"
    except (socket.gaierror, ConnectionRefusedError, OSError) as e:
        return None, ERR_REFUSED, f"连不上 {host}:{port}: {e}"
    except paramiko.SSHException as e:
        # "No authentication methods available" 也走这条（它是 SSHException 不是
        # AuthenticationException）—— 认证问题要归 auth，否则网关会当普通错误去重试
        if "auth" in str(e).lower():
            return None, ERR_AUTH, f"认证失败: {e}"
        return None, ERR_UNKNOWN, f"ssh 协议错误: {e}"
    except Exception:
        client.close()
        raise


def _alive(key: str) -> Optional[paramiko.SSHClient]:
    """取池里还活着的连接；断了的顺手丢掉"""
    with _pool_lock:
        client = _pool.get(key)
    if client is None:
        return None
    transport = client.get_transport()
    if transport is None or not transport.is_active():
        _drop(key)
        return None
    return client


def _drop(key: str) -> None:
    with _pool_lock:
        client = _pool.pop(key, None)
    if client is not None:
        try:
            client.close()
        except Exception:
            pass


def _ensure(host: str, timeout: int, *, user: Optional[str] = None,
            port: Optional[int] = None,
            password: Optional[str] = None) -> Tuple[Optional[paramiko.SSHClient], str, str]:
    """复用或新建；**新建的连接放进池里长期保留**"""
    user, port, password = _endpoint(host, user, port, password)
    key = _key(host, user, port)
    client = _alive(key)
    if client is not None:
        return client, "", ""
    client, kind, msg = _connect(host, timeout, user, port, password)
    if client is None:
        return None, kind, msg
    with _pool_lock:
        _pool[key] = client
    return client, "", ""


def warm_up(hosts: Iterable[str], *, timeout: int = SSH_TIMEOUT) -> Dict[str, str]:
    """
    提前建连（入口处调一次）—— 主机名是 YARN 动态报的，所以列表得从外面喂

    :return: {host: ""} 表示成功；失败的是 {host: 错误说明} —— **调用方要把它说出来**，
             静默失败会让后面每个 ssh 工具都各自超时一次，看起来像"集群有问题"
    """
    outcome: Dict[str, str] = {}
    for host in dict.fromkeys(h for h in hosts if h):
        user, port, password = _endpoint(host)
        key = _key(host, user, port)
        with _lock_of(key):
            client = _alive(key)
            if client is not None:
                outcome[host] = ""
                continue
            client, kind, msg = _connect(host, timeout, user, port, password)
            if client is None:
                logger.error(f"预热 ssh {host} 失败({kind}): {msg}")
                outcome[host] = f"{kind}: {msg}"
                continue
            with _pool_lock:
                _pool[key] = client
            outcome[host] = ""
        logger.info(f"ssh 预热完成: {host}")
    return outcome


def pool_status() -> List[dict]:
    """池子现状 —— 排查「明明连过为什么又慢」时看它"""
    with _pool_lock:
        items = list(_pool.items())
    out = []
    for key, client in items:
        transport = client.get_transport()
        out.append({"host": key, "active": bool(transport and transport.is_active())})
    return out


def close_all() -> None:
    """进程退出 / 配置变更时清池"""
    with _pool_lock:
        keys = list(_pool)
    for key in keys:
        _drop(key)


def _exec(client: paramiko.SSHClient, command: str, timeout: int) -> SshResult:
    # **必须用 export**，否则 写成 `LC_ALL=C cmd1; cmd2` 的话这个赋值只作用于 cmd1
    _, out, err = client.exec_command(f"export LC_ALL=C; {command}", timeout=timeout)
    stdout = out.read().decode("utf-8", errors="replace")
    stderr = err.read().decode("utf-8", errors="replace")
    return SshResult(ok=True, rc=out.channel.recv_exit_status(), stdout=stdout, stderr=stderr)


def run(host: str, command: str, *, timeout: int = SSH_TIMEOUT) -> SshResult:
    """
    在 host 上执行一条命令（复用池里的连接）

    :param host: 节点名（YARN 报的 assignedNodeId 去掉端口，如 "node1"）
    :param command: shell 命令；调用方自己负责转义参数
    :param timeout: 单次建连/执行的超时（秒）。一次调用最坏 = 建连 + 执行，各算各的
    """
    t0 = time.time()
    user, port, password = _endpoint(host)
    key = _key(host, user, port)
    with _lock_of(key):
        client, kind, msg = _ensure(host, timeout, user=user, port=port, password=password)
        if client is None:
            logger.error(f"ssh {host} 不可用({kind}): {msg}")
            return SshResult(ok=False, error_kind=kind, message=msg,
                             cost_ms=int((time.time() - t0) * 1000))
        try:
            result = _exec(client, command, timeout)
        except (socket.timeout, TimeoutError) as e:
            _drop(key)
            msg = f"命令执行超时({timeout}s): {e}"
            logger.error(f"ssh {host} {msg}")
            return SshResult(ok=False, error_kind=ERR_TIMEOUT, message=msg,
                             cost_ms=int((time.time() - t0) * 1000))
        except paramiko.SSHException as e:
            # 复用中的连接可能已被服务端断掉（空闲超时 / 重启）。丢掉重连一次
            _drop(key)
            fresh, kind, msg = _ensure(host, timeout, user=user, port=port, password=password)
            if fresh is None:
                return SshResult(ok=False, error_kind=kind, message=msg,
                                 cost_ms=int((time.time() - t0) * 1000))
            try:
                result = _exec(fresh, command, timeout)
            except paramiko.SSHException as e2:
                _drop(key)
                msg = f"重连后仍失败: {e2}"
                logger.error(f"ssh {host} {msg}")
                return SshResult(ok=False, error_kind=ERR_UNKNOWN, message=msg,
                                 cost_ms=int((time.time() - t0) * 1000))

        result.cost_ms = int((time.time() - t0) * 1000)
        return result


@contextmanager
def sftp(host: str, *, user: Optional[str] = None, port: Optional[int] = None,
         password: Optional[str] = None, timeout: int = SSH_TIMEOUT):
    """
    在池里的连接上开一个 SFTP 通道 —— **通道关，连接不关**

    同一端点的命令和 SFTP 共用一把锁，所以一个文件树传完前，别的 ssh 调用会等着；
    paramiko 的连接本来也不适合多线程并发。code.commit.ssh 可能是另一台/另一个账号，
    所以允许覆盖 user/port/password（密码默认 SSH_PASSWORD）。
    """
    user, port, password = _endpoint(host, user, port, password)
    key = _key(host, user, port)
    with _lock_of(key):
        client, kind, msg = _ensure(host, timeout, user=user, port=port, password=password)
        if client is None:
            raise RuntimeError(f"ssh {host} 不可用({kind}): {msg}")
        try:
            channel = client.open_sftp()
        except paramiko.SSHException:
            # 空闲连接可能已被服务端断掉，和 run() 一样重连一次
            _drop(key)
            client, kind, msg = _ensure(host, timeout, user=user, port=port, password=password)
            if client is None:
                raise RuntimeError(f"ssh {host} 不可用({kind}): {msg}")
            channel = client.open_sftp()
        try:
            yield channel
        finally:
            channel.close()


def ensure_remote_dirs(sftp: paramiko.SFTPClient, remote_dir: str) -> None:
    """逐级建远端目录（SFTP 没有 mkdir -p）"""
    if not remote_dir or remote_dir in ("/", "."):
        return
    current = "/" if remote_dir.startswith("/") else ""
    for part in remote_dir.strip("/").split("/"):
        if not part:
            continue
        current = f"{current.rstrip('/')}/{part}" if current else part
        try:
            sftp.stat(current)
        except OSError:
            sftp.mkdir(current)
