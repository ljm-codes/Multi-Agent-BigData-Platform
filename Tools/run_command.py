import asyncio
import os
import signal
import subprocess
from typing import Any

from Tools import LogSetting
from load_config.config import ROOT_BASE_DIR_PATH

logger = LogSetting.create(__name__)
# 检查 mvn 是否在环境变量中
result = subprocess.run("where mvn", shell=True, capture_output=True, text=True)
is_use_project_mvn = False
if result.returncode != 0:
    logger.warning(f"mvn 不在环境变量中: {result.stderr}，尝试使用项目默认的mvn包")
    mvn_result = subprocess.run(
        [ROOT_BASE_DIR_PATH / "Maven" / "mvn-3.9.9" / "bin" / "mvn.cmd", "-v"],
        shell=True,
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
        command_list = [ROOT_BASE_DIR_PATH / "Maven" / "mvn-3.9.9" / "bin" / "mvn.cmd", *cmd_list]
    else:
        command_list = ["mvn", *cmd_list]
    try:
        res = subprocess.run(
            command_list,
            cwd=path,
            shell=True,
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
        command_list = [ROOT_BASE_DIR_PATH / "Maven" / "mvn-3.9.9" / "bin" / "mvn.cmd", *cmd_list]
    else:
        command_list = ["mvn", *cmd_list]
    # create_subprocess_shell 只接收一条命令字符串；传 *command_list 会把第二个参数当成 stdin
    shell_command = " ".join(
        f'"{arg}"' if any(ch.isspace() for ch in str(arg)) else str(arg)
        for arg in command_list
    )
    proc = await asyncio.create_subprocess_shell(
        shell_command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=path,
    )
    assert proc is not None
    try:
        while True:
            chunk = await proc.stdout.read(8192)  # 8KB -> 辅助监控
            if not chunk:
                break
            text = chunk.decode(errors="replace")
            if buffer:
                if hasattr(buffer, "append"):
                    buffer.append(text)
                else:
                    buffer.write(text)
            else:
                logger.info(text)

        rc = await proc.wait()
        snap = buffer.snapshot() if buffer and hasattr(buffer, "snapshot") else None
        return {"returncode": rc, "output": snap or ""}
    except asyncio.CancelledError:
        # 当被 TaskSupervisor 取消时，必须杀死子进程
        if proc.returncode is None:
            proc.terminate()
            try:
                # 等待杀死
                await asyncio.wait_for(proc.wait(), timeout=kill_grace)
            except asyncio.TimeoutError:
                # 超时 -> 强制杀死
                proc.kill()
                await proc.wait()
        # 提供给 TaskSupervisor 感知
        raise


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

    spawn_kwargs: dict[str, Any] = {}
    if kill_tree:
        if os.name == "nt":
            spawn_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            spawn_kwargs["start_new_session"] = True

    try:
        proc = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=path,
            env=env,
            **spawn_kwargs,
        )
    except FileNotFoundError as e:
        return {"returncode": -1, "output": f"命令不存在: {e}"}

    assert proc is not None and proc.stdout is not None

    async def _kill() -> None:
        if proc.returncode is not None:
            return
        if kill_tree:
            if os.name == "nt":
                killer = await asyncio.create_subprocess_exec(
                    "taskkill", "/F", "/T", "/PID", str(proc.pid),
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                await killer.wait()
            else:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                except ProcessLookupError:
                    pass
        else:
            proc.terminate()

        try:
            await asyncio.wait_for(proc.wait(), timeout=kill_grace)
        except asyncio.TimeoutError:
            if kill_tree and os.name != "nt":
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass
            else:
                proc.kill()
            await proc.wait()

    try:
        while True:
            chunk = await proc.stdout.read(8192)
            if not chunk:
                break
            text = chunk.decode(errors="replace")
            if buffer:
                if hasattr(buffer, "append"):
                    buffer.append(text)
                else:
                    buffer.write(text)
            else:
                logger.info(text)

        rc = await proc.wait()
        snap = buffer.snapshot() if buffer and hasattr(buffer, "snapshot") else None
        return {"returncode": rc, "output": snap or ""}
    except asyncio.CancelledError:
        await _kill()
        raise


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
