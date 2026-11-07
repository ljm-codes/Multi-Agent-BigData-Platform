"""
跨平台异步命令执行工具。

背景（Windows 的“死结”）：
    本项目用 psycopg 的异步连接（AsyncPostgresSaver / AsyncPostgresStore），
    psycopg 的异步等待依赖 loop.add_reader()/add_writer()，因此 Windows 上必须使用
    WindowsSelectorEventLoopPolicy（run.py 已设置）；
    但 SelectorEventLoop 不实现 asyncio 子进程，asyncio.create_subprocess_shell/exec
    会抛 NotImplementedError。

解决方式：
    不再使用 asyncio 子进程，而是用标准库 subprocess.Popen 起进程，由后台线程阻塞读取
    stdout，再通过 asyncio.Queue 把输出交回事件循环。这样：
    - 与事件循环类型无关（Selector / Proactor 都能跑）；
    - 保留流式输出，buffer 在命令执行期间可随时 snapshot()；
    - 协程被取消时能杀掉子进程（Windows 用 taskkill /T 杀整棵进程树）。
"""

import asyncio
import codecs
import os
import shlex
import shutil
import signal
import subprocess
import threading
from typing import Any

from Tools.log_settings import LogSetting
from load_config.config import ROOT_BASE_DIR_PATH

logger = LogSetting.create(__name__)
# 检查 mvn 是否在环境变量中（Linux 没有 where，用 shutil.which 跨平台）
_project_mvn = (
    ROOT_BASE_DIR_PATH / "Maven" / "mvn-3.9.9" / "bin"
    / ("mvn.cmd" if os.name == "nt" else "mvn")
)
is_use_project_mvn = False
if shutil.which("mvn") is None:
    logger.warning("mvn 不在环境变量中，尝试使用项目默认的mvn包")
    mvn_result = subprocess.run(
        [str(_project_mvn), "-v"],
        shell=(os.name == "nt"),
        capture_output=True,
        text=True
    )
    if mvn_result.returncode == 0:
        is_use_project_mvn = True
    else:
        logger.error(f"环境变量中没有将mvn放入PATH中，且项目的自带的mvn不见了。")
        raise Exception(f"环境变量中没有将mvn放入PATH中，且项目的自带的mvn不见了。")


def _split_command(command: str) -> list[str]:
    """
    按空白字符切分命令, 但保留引号内的内容。
    同时兼容 Windows 路径中的反斜杠 (不用 shlex.split 是为了避免反斜杠被当转义符吃掉)。
    支持单引号和双引号。
    """
    tokens: list[str] = []
    current: list[str] = []
    quote_char: str | None = None

    for ch in command:
        if quote_char is not None:
            if ch == quote_char:
                quote_char = None
            else:
                current.append(ch)
        else:
            if ch in ('"', "'"):
                quote_char = ch
            elif ch.isspace():
                if current:
                    tokens.append("".join(current))
                    current = []
            else:
                current.append(ch)

    if current:
        tokens.append("".join(current))

    return tokens


def run_mvn_command(command: str, path: str) -> tuple[int, str]:
    """
    执行 mvn 命令，会自动匹配 mvn
    """
    if not command or not command.strip():
        return -1, "命令不能为空"

    try:
        cmd_list = _split_command(command)
    except Exception as e:
        return -1, f"命令解析失败: {type(e).__name__}: {e}"
    if not cmd_list:
        return -1, "命令为空"

    cmd_list = [cmd for cmd in cmd_list if cmd != "mvn"]
    if is_use_project_mvn:
        command_list = [_project_mvn, *cmd_list]
    else:
        command_list = ["mvn", *cmd_list]
    try:
        res = subprocess.run(
            command_list,
            cwd=path,
            shell=(os.name == "nt"),
            capture_output=True,
            text=True,
            encoding="utf-8",  # 显式指定编码, 避免 Windows 默认 cp936
            errors="replace",  # 遇到无法解码的字节不抛异常
        )
        if res.returncode != 0:
            return res.returncode, (res.stderr or res.stdout or "").strip()  # mvn 若失败了，有些错误信息会在stdout中，所以这里需要判断
        return res.returncode, (res.stdout or "").strip()
    except FileNotFoundError as e:
        logger.error(f"命令未找到: {cmd_list[0]} ({e})")
        return -1, f"命令未找到: {cmd_list[0]} ({e})"
    except Exception as e:
        logger.error(f"执行 mvn 命令 {command} 失败, 错误信息{type(e).__name__}: {e}")
        return -1, f"执行 mvn 命令 {command} 失败, 错误信息{type(e).__name__}: {e}"


# ------------------------- 子进程执行（不依赖 asyncio 子进程） -------------------------

_READ_CHUNK_SIZE = 8192


async def _terminate_process(
        proc: subprocess.Popen,
        *,
        kill_tree: bool,
        kill_grace: float,
) -> None:
    """结束子进程；kill_tree=True 时尽量把整棵进程树一起结束。"""
    if proc.poll() is not None:
        return

    tree_killed = False
    try:
        if kill_tree and os.name == "nt":
            # taskkill 本身是阻塞命令，放到线程池执行；/T 表示连子进程一起杀
            result = await asyncio.to_thread(
                subprocess.run,
                ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                capture_output=True,
                encoding="utf-8",
                errors="replace",
                check=False,
            )
            tree_killed = result.returncode == 0
            if not tree_killed:
                logger.warning(
                    "taskkill 失败（returncode=%s）：%s；退化为只结束主进程",
                    result.returncode,
                    (result.stderr or result.stdout or "").strip(),
                )
        elif kill_tree:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                tree_killed = True
            except (ProcessLookupError, PermissionError):
                pass
    except OSError as e:
        logger.warning(f"结束进程树失败：{type(e).__name__}: {e}")

    if not tree_killed:
        try:
            proc.kill()
        except OSError:
            pass

    try:
        await asyncio.wait_for(asyncio.to_thread(proc.wait), timeout=kill_grace)
    except asyncio.TimeoutError:
        if kill_tree and os.name != "nt":
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                proc.kill()
        else:
            proc.kill()
        await asyncio.to_thread(proc.wait)


async def _stream_process(
        args: str | list[str],
        *,
        shell: bool,
        path: str,
        env: dict[str, str] | None = None,
        buffer: Any = None,
        encoding: str = "utf-8",
        kill_grace: float = 2.0,
        kill_tree: bool = True,
) -> dict[str, Any]:
    """用 subprocess.Popen 执行命令，输出流式写入 buffer（不依赖 asyncio 子进程）。"""
    popen_kwargs: dict[str, Any] = {
        "shell": shell,
        "cwd": path,
        "env": env,
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.STDOUT,
        "bufsize": 0,
    }
    if kill_tree:
        if os.name == "nt":
            popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            popen_kwargs["start_new_session"] = True

    proc = subprocess.Popen(args, **popen_kwargs)

    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[str | None] = asyncio.Queue()

    def _post(item: str | None) -> None:
        """从读取线程投递数据；事件循环已关闭时静默丢弃。"""
        try:
            loop.call_soon_threadsafe(queue.put_nowait, item)
        except RuntimeError:
            pass

    def _reader() -> None:
        # 增量解码：一个 UTF-8 汉字可能正好被 8192 字节切断
        decoder = codecs.getincrementaldecoder(encoding)(errors="replace")
        stdout = proc.stdout
        try:
            if stdout is not None:
                fd = stdout.fileno()
                while True:
                    chunk = os.read(fd, _READ_CHUNK_SIZE)
                    if not chunk:
                        break
                    text = decoder.decode(chunk)
                    if text:
                        _post(text)
                tail = decoder.decode(b"", final=True)
                if tail:
                    _post(tail)
        except (OSError, ValueError):
            # 进程被杀 / 管道被关闭，属于正常收尾
            pass
        except Exception:  # noqa: BLE001
            logger.exception("读取子进程输出失败")
        finally:
            _post(None)
            if stdout is not None:
                # 由读取线程自己关闭管道：主线程 close() 可能和阻塞中的 os.read 抢锁
                try:
                    stdout.close()
                except OSError:
                    pass

    reader = threading.Thread(
        target=_reader, name=f"cmd-reader-{proc.pid}", daemon=True
    )
    reader.start()

    parts: list[str] = []
    try:
        while True:
            text = await queue.get()
            if text is None:
                break
            parts.append(text)
            if buffer is None:
                logger.info(text)
            elif hasattr(buffer, "append"):
                buffer.append(text)
            else:
                buffer.write(text)

        returncode = await asyncio.to_thread(proc.wait)
        if buffer is not None and hasattr(buffer, "snapshot"):
            output = buffer.snapshot()
        else:
            output = "".join(parts)
        return {"returncode": returncode, "output": output or ""}
    except asyncio.CancelledError:
        await _terminate_process(proc, kill_tree=kill_tree, kill_grace=kill_grace)
        raise
    finally:
        if reader.is_alive():
            try:
                # 守护线程，最多等 1s，避免取消流程被残留子进程拖住
                await asyncio.to_thread(reader.join, 1.0)
            except asyncio.CancelledError:
                pass


async def arun_mvn_command(
        command: str,
        buffer: Any,
        *,
        path: str,
        kill_grace: float = 2.0
) -> dict[str, Any]:
    """异步执行 mvn 命令

    流式执行命令，输出实时写入 buffer；协程被取消时主动杀掉子进程。
    - buffer 需提供 append(text) 或 write(text)，并最好有 snapshot()。
    """
    if not command or not command.strip():
        return {"returncode": -1, "output": "命令不能为空"}

    try:
        cmd_list = _split_command(command)
    except Exception as e:
        return {"returncode": -1, "output": f"命令解析失败: {type(e).__name__}: {e}"}
    if not cmd_list:
        return {"returncode": -1, "output": "命令为空"}

    cmd_list = [cmd for cmd in cmd_list if cmd != "mvn"]
    if is_use_project_mvn:
        command_list = [str(_project_mvn), *cmd_list]
    else:
        command_list = ["mvn", *cmd_list]
    if not command_list:
        return {"returncode": -1, "output": "命令为空"}

    # shell=True 只接收一条命令字符串；Windows 用 list2cmdline，POSIX 用 shlex.join
    if os.name == "nt":
        shell_command = subprocess.list2cmdline(command_list)
    else:
        shell_command = shlex.join(command_list)
    return await _stream_process(
        shell_command,
        shell=True,
        path=path,
        buffer=buffer,
        kill_grace=kill_grace,
    )


async def arun_python_command(
        command: list[str],
        *,
        path: str,
        env: dict[str, str] | None = None,
        buffer: Any = None,
        kill_grace: float = 2.0,
        kill_tree: bool = True
) -> dict[str, Any]:
    """
    异步执行本地命令（参数列表，不经过 shell）
    流式输出写入 buffer；协程被取消时主动杀掉子进程（默认连子进程树一起杀）。

    :param command: 参数列表，如 [python, "-m", "pytest", ...]
    :param path: 工作目录
    :param env: 完整环境变量；None 表示继承当前进程
    :param buffer: 需提供 append(text) 或 write(text)，最好有 snapshot()
    :param kill_tree: 取消时是否杀整棵进程树（Windows 用 taskkill /T，POSIX 用 killpg）
    """
    if not command:
        return {"returncode": -1, "output": "命令不能为空"}

    try:
        return await _stream_process(
            [str(part) for part in command],
            shell=False,
            path=path,
            env=env,
            buffer=buffer,
            kill_grace=kill_grace,
            kill_tree=kill_tree,
        )
    except FileNotFoundError as e:
        return {"returncode": -1, "output": f"命令不存在: {e}"}


def run_java_command(command: str, path: str | None = None) -> tuple[int, str]:
    if not command or not command.strip():
        return -1, "命令不能为空"
    stripped = command.lstrip()
    if not (stripped.startswith("java") or stripped.startswith("jar")):
        return -1, f"必须是 java 命令或 jar 命令: {stripped}"
    try:
        cmd_list = _split_command(command)
    except Exception as e:
        return -1, f"命令解析失败: {type(e).__name__}: {e}"
    if not cmd_list:
        return -1, "命令为空"
    try:
        res = subprocess.run(
            cmd_list,
            cwd=path,
            shell=False,
            capture_output=True,
            text=True,
            encoding="utf-8",  # 显式指定编码, 避免 Windows 默认 cp936
            errors="replace",  # 遇到无法解码的字节不抛异常
        )
        if res.returncode != 0:
            return res.returncode, (res.stderr or res.stdout or "").strip()
        return res.returncode, (res.stdout or "").strip()
    except FileNotFoundError as e:
        logger.error(f"命令未找到: {cmd_list[0]} ({e})")
        return -1, f"命令未找到: {cmd_list[0]} ({e})"
    except Exception as e:
        logger.error(f"执行 java 命令 {command} 失败, 错误信息{type(e).__name__}: {e}")
        return -1, f"执行 java 命令 {command} 失败, 错误信息{type(e).__name__}: {e}"
