import os
import platform
import shutil
import subprocess
from pathlib import Path

from fastapi import APIRouter
from pydantic import BaseModel, Field

from SPO.route_results import RouteResponse
from Tools import LogSetting

route = APIRouter()
logger = LogSetting.create(__name__)
IDE_CANDIDATES = {
    "code": {"name": "VS Code", "display": "Visual Studio Code"},
    "idea": {"name": "IntelliJ IDEA", "display": "IntelliJ IDEA"},
    "pycharm": {"name": "PyCharm", "display": "PyCharm"},
    "webstorm": {"name": "WebStorm", "display": "WebStorm"},
    "cursor": {"name": "Cursor", "display": "Cursor"},
    "subl": {"name": "Sublime Text", "display": "Sublime Text"},
    "notepad++": {"name": "Notepad++", "display": "Notepad++"},
}
JETBRAINS_IDES = {
    "idea", "pycharm", "webstorm", "goland", "clion",
    "phpstorm", "rubymine", "rider", "datagrip", "fleet",
}
VSCODE_LIKE = {"code", "cursor", "code-insiders", "windsurf"}
GUI_LINE_EDITORS = {"gedit", "kate", "mousepad"}
TERMINAL_EDITORS = {"nano", "vim", "vi", "emacs"}


class OpenFileInfo(BaseModel):
    file_path: str = Field(description="文件路径")
    ide_name: str = Field(default="code", description="IDE名称，默认VS Code")
    ide_path: str = Field(default="", description="IDE路径，默认空")
    start_idx: int = Field(default=1, description="打开文件的起始行索引，默认0")


def _scan_windows_registry():
    """查询 Windows 注册表 App Paths[reference:5]"""
    try:
        import winreg
    except ImportError:
        logger.error(f"当前环境非win环境，无法进行对win端的IDE扫描")
        return {}
    result = {}
    reg_paths = [
        r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths",
        r"SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\App Paths",
    ]
    for reg_path in reg_paths:
        try:
            key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, reg_path)
            for exe in IDE_CANDIDATES:
                try:
                    sub_key = winreg.OpenKey(key, f"{exe}.exe")
                    path, _ = winreg.QueryValueEx(sub_key, "")
                    result[exe] = {"path": path, "display": IDE_CANDIDATES[exe]["display"]}
                except FileNotFoundError:
                    continue
        except FileNotFoundError:
            continue
    return result


def _scan_windows_notepad():
    """通过 shutil 查找记事本路径"""
    notepad_path = shutil.which("notepad")
    if notepad_path:
        return {"notepad": {"path": notepad_path, "display": "notepad"}}
    return {}


def _scan_macos_apps():
    """扫描 macOS 的 .app 包"""
    result = {}
    app_dirs = [
        "/Applications",
        "/Applications/Setapp",
        str(Path.home() / "Applications"),
    ]
    # 常见 IDE 的 .app 名称映射
    mac_app_names = {
        "Visual Studio Code.app": "Visual Studio Code",
        "IntelliJ IDEA.app": "idea",
        "PyCharm.app": "pycharm",
        "WebStorm.app": "webstorm",
        "Cursor.app": "cursor",
        "Sublime Text.app": "subl",
    }
    for app_dir in app_dirs:
        if not os.path.isdir(app_dir):
            continue
        for app_name, exe in mac_app_names.items():
            app_path = os.path.join(app_dir, app_name)
            if os.path.exists(app_path):
                # 实际可执行文件通常在 Contents/MacOS/ 下
                exe_path = os.path.join(app_path, "Contents", "MacOS", exe)
                if os.path.exists(exe_path):
                    result[exe] = {"path": exe_path, "display": IDE_CANDIDATES[exe]["display"]}
    return result


def _scan_macos_notepad():
    textedit_path = "/System/Applications/TextEdit.app"
    if os.path.exists(textedit_path):
        return {"TextEdit": {"textpath": textedit_path, "display": "TextEdit"}}
    return {}


def _scan_linux_desktop():
    """扫描 Linux 的 .desktop 文件"""
    result = {}
    desktop_dirs = [
        "/usr/share/applications",
        str(Path.home() / ".local/share/applications"),
    ]
    # 通过 .desktop 文件的 Exec 字段匹配
    for desktop_dir in desktop_dirs:
        if not os.path.isdir(desktop_dir):
            continue
        for filename in os.listdir(desktop_dir):
            if not filename.endswith(".desktop"):
                continue
            filepath = os.path.join(desktop_dir, filename)
            with open(filepath, "r", errors="ignore") as f:
                content = f.read()
            for exe in IDE_CANDIDATES:
                # 简单的启发式：Exec 行包含 IDE 可执行名
                if f"Exec={exe}" in content or f"Exec=/usr/bin/{exe}" in content:
                    # 进一步解析 Exec 行获取完整路径
                    for line in content.splitlines():
                        if line.startswith("Exec="):
                            exec_cmd = line.split("=", 1)[1].strip().split()[0]
                            if shutil.which(exec_cmd):
                                result[exe] = {
                                    "path": shutil.which(exec_cmd),
                                    "display": IDE_CANDIDATES[exe]["display"],
                                }
    return result


def _scan_linux_notepad():
    editor = os.environ.get("EDITOR")
    if editor:
        return {"EDITOR": {"path": editor, "display": "EDITOR"}}
    else:
        for cmd in ["gedit", "nano", "vim", "vi"]:
            path = shutil.which(cmd)
            if path:
                return {f"{cmd}": {"path": path, "display": cmd}}
    return {}


# 发现IDE
def detect_ides():
    """返回 {可执行文件名: 完整路径} 的字典"""
    found = {}
    system = platform.system()

    # 1. 优先用 shutil.which（PATH 环境变量）—— 跨平台通用
    for exe, info in IDE_CANDIDATES.items():
        path = shutil.which(exe)
        if path:
            found[exe] = {"path": path, "display": info["display"]}

    # 2. Windows：补充查询注册表 App Paths
    if system == "Windows":
        found.update(_scan_windows_notepad())
        found.update(_scan_windows_registry())

    # 3. macOS：扫描 .app 包
    elif system == "Darwin":
        found.update(_scan_macos_notepad())
        found.update(_scan_macos_apps())

    # 4. Linux：扫描 .desktop 文件
    elif system == "Linux":
        found.update(_scan_linux_notepad())
        found.update(_scan_linux_desktop())

    return found


@route.get("/get_ides")
async def get_ides() -> RouteResponse:
    try:
        ides = detect_ides()
        return RouteResponse.ok(data=ides)
    except Exception as e:
        logger.error(f"获取IDE失败：{e}")
        return RouteResponse.error(msg=f"获取IDE失败：{e}")


def _build_argv(ide_name: str, ide_path: str, file_path: str, line: int):
    """返回 argv 列表；返回 None 表示需要走特殊逻辑（TextEdit / 兜底）。"""
    if ide_name in JETBRAINS_IDES:
        return [ide_path, "--line", str(line), file_path]
    if ide_name in VSCODE_LIKE:
        return [ide_path, "--goto", f"{file_path}:{line}"]
    if ide_name == "subl":
        return [ide_path, f"{file_path}:{line}"]
    if ide_name in ("notepad++", "notepadpp", "npp"):
        return [ide_path, "-n", str(line), file_path]
    if ide_name == "notepad":
        # Windows 系统记事本不支持跳行，只能打开
        return [ide_path, file_path]
    if ide_name in GUI_LINE_EDITORS or ide_name in TERMINAL_EDITORS:
        return [ide_path, f"+{line}", file_path]
    if ide_name in ("textedit", "textedit.app"):
        return None
    return None


def _popen_detached(argv):
    """不阻塞、不占终端、尽量脱离父进程。"""
    kwargs = dict(stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if platform.system() == "Windows":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True
    subprocess.Popen(argv, **kwargs)


def _open_with_default(file_path: str):
    system = platform.system()
    if system == "Windows":
        os.startfile(file_path)  # type: ignore[attr-defined]
    elif system == "Darwin":
        _popen_detached(["open", file_path])
    else:
        _popen_detached(["xdg-open", file_path])


@route.post("/open_file")
async def open_file(file_info: OpenFileInfo) -> RouteResponse:
    try:
        file_path = str(Path(file_info.file_path).expanduser().resolve())
        if not os.path.exists(file_path):
            return RouteResponse.error(msg=f"文件不存在：{file_path}")

        ide_name = (file_info.ide_name or "code").lower()
        ide_path = file_info.ide_path

        if not ide_path:
            ides = detect_ides()
            if ide_name not in ides:
                return RouteResponse.error(
                    msg=f"未找到 IDE：{ide_name}，请显式传入 ide_path"
                )
            ide_path = ides[ide_name]["path"]

        # 行号至少为 1
        line = max(int(file_info.start_idx or 1), 1)

        # 目录：直接用 IDE 打开项目，行号参数没有意义
        if os.path.isdir(file_path):
            _popen_detached([ide_path, file_path])
            return RouteResponse.ok(data={
                "opened": file_path, "line": line, "editor": ide_name, "is_dir": True,
            })

        # 构造命令
        argv = _build_argv(ide_name, ide_path, file_path, line)

        if argv is None:
            _open_with_default(file_path)
        else:
            _popen_detached(argv)

        return RouteResponse.ok(data={
            "opened": file_path, "line": line, "editor": ide_name
        })
    except Exception as e:
        logger.error(f"打开文件失败：{e}")
        return RouteResponse.error(msg=f"打开文件失败：{e}")
