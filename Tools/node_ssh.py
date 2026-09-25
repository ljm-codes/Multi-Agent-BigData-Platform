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
# 每 host 一把锁：连接内的命令执行串行（gateway 本来也要求 ssh 类限并发），
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


def _lock_of(host: str) -> threading.Lock:
    with _pool_lock:
        return _locks.setdefault(host, threading.Lock())


def _connect(host: str, timeout: int) -> Tuple[Optional[paramiko.SSHClient], str, str]:
    """建连；失败返回 (None, 错误类别, 说明)"""
    client = paramiko.SSHClient()
    # 与测试脚手架的 -o StrictHostKeyChecking=no 等价：集群节点会重建，主机键留不住
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(
            hostname=host, port=SSH_PORT, username=SSH_USER,
            password=SSH_PASSWORD or None,
            timeout=timeout, banner_timeout=timeout, auth_timeout=timeout,
            look_for_keys=not SSH_PASSWORD, allow_agent=not SSH_PASSWORD,
        )
        return client, "", ""
    except paramiko.AuthenticationException as e:
        return None, ERR_AUTH, f"认证失败: {e}"
    except (socket.timeout, TimeoutError) as e:
        return None, ERR_TIMEOUT, f"建连超时({timeout}s): {e}"
    except (socket.gaierror, ConnectionRefusedError, OSError) as e:
        return None, ERR_REFUSED, f"连不上 {host}:{SSH_PORT}: {e}"
    except paramiko.SSHException as e:
        # "No authentication methods available" 也走这条（它是 SSHException 不是
        # AuthenticationException）—— 认证问题要归 auth，否则网关会当普通错误去重试
        if "auth" in str(e).lower():
            return None, ERR_AUTH, f"认证失败: {e}"
        return None, ERR_UNKNOWN, f"ssh 协议错误: {e}"
    except Exception:
        client.close()
        raise


def _alive(host: str) -> Optional[paramiko.SSHClient]:
    """取池里还活着的连接；断了的顺手丢掉"""
    with _pool_lock:
        client = _pool.get(host)
    if client is None:
        return None
    transport = client.get_transport()
    if transport is None or not transport.is_active():
        _drop(host)
        return None
    return client


def _drop(host: str) -> None:
    with _pool_lock:
        client = _pool.pop(host, None)
    if client is not None:
        try:
            client.close()
        except Exception:
            pass


def _ensure(host: str, timeout: int) -> Tuple[Optional[paramiko.SSHClient], str, str]:
    """复用或新建；**新建的连接放进池里长期保留**"""
    client = _alive(host)
    if client is not None:
        return client, "", ""
    client, kind, msg = _connect(host, timeout)
    if client is None:
        return None, kind, msg
    with _pool_lock:
        _pool[host] = client
    return client, "", ""


def warm_up(hosts: Iterable[str], *, timeout: int = SSH_TIMEOUT) -> Dict[str, str]:
    """
    提前建连（入口处调一次）—— 主机名是 YARN 动态报的，所以列表得从外面喂

    :return: {host: ""} 表示成功；失败的是 {host: 错误说明} —— **调用方要把它说出来**，
             静默失败会让后面每个 ssh 工具都各自超时一次，看起来像"集群有问题"
    """
    outcome: Dict[str, str] = {}
    for host in dict.fromkeys(h for h in hosts if h):
        with _lock_of(host):
            client = _alive(host)
            if client is not None:
                outcome[host] = ""
                continue
            client, kind, msg = _connect(host, timeout)
            if client is None:
                logger.error(f"预热 ssh {host} 失败({kind}): {msg}")
                outcome[host] = f"{kind}: {msg}"
                continue
            with _pool_lock:
                _pool[host] = client
            outcome[host] = ""
        logger.info(f"ssh 预热完成: {host}")
    return outcome


def pool_status() -> List[dict]:
    """池子现状 —— 排查「明明连过为什么又慢」时看它"""
    with _pool_lock:
        items = list(_pool.items())
    out = []
    for host, client in items:
        transport = client.get_transport()
        out.append({"host": host, "active": bool(transport and transport.is_active())})
    return out


def close_all() -> None:
    """进程退出 / 配置变更时清池"""
    with _pool_lock:
        hosts = list(_pool)
    for host in hosts:
        _drop(host)


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
    with _lock_of(host):
        client, kind, msg = _ensure(host, timeout)
        if client is None:
            logger.error(f"ssh {host} 不可用({kind}): {msg}")
            return SshResult(ok=False, error_kind=kind, message=msg,
                             cost_ms=int((time.time() - t0) * 1000))
        try:
            result = _exec(client, command, timeout)
        except (socket.timeout, TimeoutError) as e:
            _drop(host)
            msg = f"命令执行超时({timeout}s): {e}"
            logger.error(f"ssh {host} {msg}")
            return SshResult(ok=False, error_kind=ERR_TIMEOUT, message=msg,
                             cost_ms=int((time.time() - t0) * 1000))
        except paramiko.SSHException as e:
            # 复用中的连接可能已被服务端断掉（空闲超时 / 重启）。丢掉重连一次
            _drop(host)
            fresh, kind, msg = _ensure(host, timeout)
            if fresh is None:
                return SshResult(ok=False, error_kind=kind, message=msg,
                                 cost_ms=int((time.time() - t0) * 1000))
            try:
                result = _exec(fresh, command, timeout)
            except paramiko.SSHException as e2:
                _drop(host)
                msg = f"重连后仍失败: {e2}"
                logger.error(f"ssh {host} {msg}")
                return SshResult(ok=False, error_kind=ERR_UNKNOWN, message=msg,
                                 cost_ms=int((time.time() - t0) * 1000))

        result.cost_ms = int((time.time() - t0) * 1000)
        return result
