"""
数据分析 Agent 工具

数据分析只面向 spark + python：分析代码写成 .py；工具负责写代码、查依赖库源码、
登记 spark-submit 参数，真正的打包/上传/提交由外部流程负责。
项目目录与数据开发侧同构 -> tmp_code/spark/{date}/{project_name}
"""
import ast
import json
import os
import shutil
import time
import tokenize
from pathlib import Path
from uuid import uuid4

from langchain_core.messages import ToolMessage
from langchain_core.tools import tool
from langgraph.prebuilt import ToolRuntime
from langgraph.types import Command

from Tools import LogSetting
from Tools.code_analysis import PyAnalyzer, find_definition, find_module_path
from Tools.registry import register_tool
from Tools.share_check_tool import redis_con
from load_config.config import config, ROOT_BASE_DIR_PATH

AGENT_NAME = "DAA"
# 数据分析只跑 spark/python，项目固定落在 spark 下，不跟随 context.app_type
APP_TYPE = "spark"
# 单次查看的行数上限：超了截断并说明，免得一次调用把上下文撑爆
MAX_VIEW_LINES = 400
# 单次列模块/类/函数的上限：同上，超了让 Agent 用 keyword 收窄
MAX_VIEW_SYMBOLS = 200
MAX_SYMBOL_CHARS = 12000
# 不允许写入的二进制后缀（只写文本代码/配置/数据）
_BINARY_SUFFIXES = {
    ".jar", ".class", ".exe", ".dll", ".so", ".pyd", ".pyc", ".pyo",
    ".zip", ".gz", ".tar", ".parquet", ".orc", ".avro", ".xlsx", ".xls",
    ".png", ".jpg", ".jpeg", ".gif", ".pdf", ".db", ".sqlite",
}
_CODE_DIR = "src/main/python"

logger = LogSetting.create(__name__)

daa_config = config.get("agent", {}).get(AGENT_NAME, {})
# 只查这个目录：pySpark/第三方库的源码都在 site-packages 里，标准库与项目源码不在范围内
_python_lib_dir = daa_config.get("python_lib_dir")
if _python_lib_dir:
    _p = Path(os.path.expanduser(_python_lib_dir))
    _PYTHON_LIB_DIR = _p if _p.is_dir() else None
else:
    _PYTHON_LIB_DIR = None
if _PYTHON_LIB_DIR is None:
    logger.warning(f"config.yaml 中 agent.{AGENT_NAME}.python_lib_dir 未配置或不是目录，源码查看工具将不可用")


def _project_path(runtime: ToolRuntime) -> Path | None:
    """项目根目录；还没初始化（state 里没有 project_name）时返回 None"""
    p_name = runtime.state.get("project_name")
    if not p_name:
        return None
    return ROOT_BASE_DIR_PATH / "tmp_code" / APP_TYPE / runtime.context.date / p_name


def _app_file(project_path: Path, file_path: str) -> Path | str:
    """
    把相对路径拼成项目内的绝对路径

    :return: 合法时返回绝对路径；非法（空 / 绝对路径 / 越界 / 二进制后缀）时返回错误说明
    """
    rel = Path(file_path.strip())
    if not file_path.strip() or rel.is_absolute() or rel.root or rel.drive:
        return f"file_path 只能是相对项目的路径，如：src/main/python/analysis.py（收到的是 {file_path!r}）"
    target = (project_path / rel).resolve()
    if not target.is_relative_to((project_path / _CODE_DIR).resolve()):
        return f"file_path 必须落在项目内的 {_CODE_DIR} 目录下（收到的是 {file_path}）"
    if target.suffix.lower() in _BINARY_SUFFIXES:
        return f"不允许写入二进制文件（{target.suffix}），本工具只写文本文件"
    return target


def submit_redis_key(project_name: str) -> str:
    """提交信息在 Redis 里的键：按 Agent 加前缀，避免与数据开发侧的项目名撞车"""
    return f"{AGENT_NAME}:{project_name}"


def _lib_roots() -> list[str] | str:
    """源码搜索根；不可用时返回错误说明"""
    if _PYTHON_LIB_DIR is None:
        return (f"未配置可用的 agent.{AGENT_NAME}.python_lib_dir（config.yaml），"
                f"源码查看工具无法工作")
    return [str(_PYTHON_LIB_DIR)]


def _locate_module(module: str, roots: list[str]) -> Path | str:
    """点分模块名 -> 源码文件路径；非法或找不到时返回错误说明"""
    m = (module or "").strip()
    if not m:
        return "module 不能为空，如：pyspark.sql.functions"
    if any(not seg.isascii() or not seg.isidentifier() for seg in m.split(".")):
        return f"module 必须是点分模块名（只含字母/数字/下划线，不能有 .. 或路径分隔符），收到的是 {module!r}"
    path = find_module_path(m, roots)
    if path is None:
        return (f"在 python_lib_dir 中找不到模块 {m}；"
                f"可以先用 list_python_lib_modules 查看有哪些顶层包/子模块")
    return Path(path)


def _load_module(module: str) -> tuple[list[str], Path, str] | str:
    """
    定位并读取模块源码

    :return: (搜索根, 文件路径, 源码)；失败时返回错误说明
    """
    roots = _lib_roots()
    if isinstance(roots, str):
        return roots
    path = _locate_module(module, roots)
    if isinstance(path, str):
        return path
    try:
        # tokenize.open 认编码声明；第三方库的源码不一定是纯 utf-8
        with tokenize.open(path) as f:
            source = f.read()
    except Exception as e:
        logger.error(f"读取模块 {module} 失败{type(e).__name__}: {e}")
        return f"读取模块 {module} 的源码失败{type(e).__name__}: {e}"
    return roots, path, source


def _make_analyzer(module: str, source: str) -> PyAnalyzer | str:
    try:
        return PyAnalyzer(source_code=source)
    except SyntaxError as e:
        return f"模块 {module} 的源码无法解析（第 {e.lineno} 行：{e.msg}），可能不是当前 Python 版本支持的语法"
    except Exception as e:
        logger.error(f"解析模块 {module} 失败{type(e).__name__}: {e}")
        return f"解析模块 {module} 的源码失败{type(e).__name__}: {e}"


def _node_start_line(node: ast.AST) -> int:
    """定义实际开始的行：函数/类带装饰器时，装饰器才是源码的第一行"""
    decos = getattr(node, "decorator_list", None) or []
    return min([node.lineno, *(d.lineno for d in decos)])


def _signature(node: ast.AST) -> str:
    """类/函数的签名，格式接近源码：class X(Base) / def f(a, b=1) / @x async def f()"""
    try:
        if isinstance(node, ast.ClassDef):
            bases = ", ".join(ast.unparse(b) for b in node.bases)
            sig = f"class {node.name}({bases})" if bases else f"class {node.name}"
        else:
            kw = "async def" if isinstance(node, ast.AsyncFunctionDef) else "def"
            sig = f"{kw} {node.name}({ast.unparse(node.args)})"
        decos = getattr(node, "decorator_list", None) or []
        if decos:
            sig = f"{' '.join('@' + ast.unparse(d) for d in decos)} {sig}"
        return sig
    except Exception:
        # ast.unparse 处理不了的节点，退回成只有名字，别让整个工具失败
        return getattr(node, "name", "<unknown>")


def _iter_children(base: Path, prefix: str) -> list[str]:
    """列目录下的包与 .py 模块；prefix 非空时返回点分全名"""
    names: list[str] = []
    for child in sorted(base.iterdir(), key=lambda p: p.name.lower()):
        if child.name == "__pycache__" or child.name.startswith("."):
            continue
        if child.is_dir():
            if (child / "__init__.py").is_file():
                names.append(f"{prefix}.{child.name}" if prefix else child.name)
        elif child.suffix == ".py" and child.name != "__init__.py":
            names.append(f"{prefix}.{child.stem}" if prefix else child.stem)
    return names


def _cap_by_chars(lines: list[str], max_chars: int = MAX_SYMBOL_CHARS) -> tuple[list[str], bool]:
    """按字符数截断；返回 (保留的行, 是否截断)"""
    kept: list[str] = []
    size = 0
    for line in lines:
        if size + len(line) + 1 > max_chars:
            return kept, True
        kept.append(line)
        size += len(line) + 1
    return kept, False


# 测试目录、缓存目录不参与打包/语法检查
_TEST_DIR_NAMES = {"__pycache__", "test", "tests", "__tests__"}


def analysis_business_files(project_path: Path) -> tuple[list[Path], list[Path]]:
    """
    扫描项目的业务代码目录 src/main/python

    :return: (python 文件列表, 非 python 文件列表)；测试目录、__pycache__、隐藏文件、
             .bak/.pyc/.pyo 都不参与
    """
    python_dir = project_path / _CODE_DIR
    py_files: list[Path] = []
    other_files: list[Path] = []
    if not python_dir.is_dir():
        return py_files, other_files
    for f in sorted(python_dir.rglob("*")):
        if not f.is_file():
            continue
        rel_parts = f.relative_to(python_dir).parts
        if any(p in _TEST_DIR_NAMES or p.startswith(".") for p in rel_parts[:-1]):
            continue
        if f.name.startswith(".") or f.name.endswith((".pyc", ".pyo", ".bak")):
            continue
        if f.suffix.lower() == ".py":
            py_files.append(f)
        else:
            other_files.append(f)
    return py_files, other_files


@register_tool(AGENT_NAME)
@tool
def initialize_project(runtime: ToolRuntime, project_name: str | None = None) -> Command | str:
    """
    初始化数据分析项目（只需调用一次，之后的写代码/改代码都靠它写回的项目名来定位）
    参数：
        project_name: 项目名称（可选；不填则用系统生成的名字）
    返回：
        成功时：把项目名写回状态，并返回"相对项目路径：src/main/python"
        失败时：返回失败原因（已清除掉半个项目目录）
    注意：
        - 数据分析项目只写 python 代码，不涉及 java / maven
        - 同名项目已存在时，系统会自动换个名字重建，不会覆盖已有项目
    """
    if runtime.state.get("is_repair", False):
        return "项目需要进行修改，不用进行初始化"
    uid = runtime.state.get("project_name")
    date = runtime.context.date
    p_name = project_name or uid or str(uuid4())
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / APP_TYPE / date / p_name
    if app_path.exists():
        p_name = f"{APP_TYPE}-{time.time()}-{uuid4()}-{p_name}"
        app_path = ROOT_BASE_DIR_PATH / "tmp_code" / APP_TYPE / date / p_name
    try:
        (app_path / "src" / "main" / "python").mkdir(parents=True)
        (app_path / "src" / "main" / "resources").mkdir(parents=True, exist_ok=True)
        (app_path / "src" / "tests").mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.error(f"初始化项目失败{type(e).__name__}: {e}")
        shutil.rmtree(app_path, ignore_errors=True)
        return f"初始化项目失败(已清除项目目录){type(e).__name__}: {e}"
    return Command(
        update={
            "messages": [
                ToolMessage(
                    content="项目初始化完成，相对项目路径为：src/main/python，之后的代码请在这里进行编写",
                    tool_call_id=runtime.tool_call_id,
                    id=f"tool-{uuid4()}",
                )
            ],
            "project_name": p_name,
        }
    )


@register_tool(AGENT_NAME)
@tool
def write_analysis_app_code(runtime: ToolRuntime, code: str, file_path: str) -> str:
    """
    把分析代码写入指定文件（文件与所在目录不存在时会自动创建）
    参数：
        code: 代码内容
        file_path: 相对项目的文件路径，如 src/main/python/analysis.py
    返回：
        写入结果
    注意：
        - 只能是相对项目的路径，不能是绝对路径，也不能用 .. 跳出项目
        - 只写文本文件：.py 之外的文本（如 .sql / .json / .csv）也可以写
    """
    project_path = _project_path(runtime)
    if project_path is None:
        return "项目未初始化，请先调用工具 initialize_project 进行初始化"
    tmp_code = code.strip()
    if not tmp_code:
        return "代码不能为空"
    app_file = _app_file(project_path, file_path)
    if isinstance(app_file, str):
        return app_file
    try:
        app_file.parent.mkdir(parents=True, exist_ok=True)
        app_file.write_text(code, encoding="utf-8")
        return f"文件 {file_path} 写入成功"
    except Exception as e:
        logger.error(f"写入文件 {file_path} 失败{type(e).__name__}: {e}")
        return f"写入文件 {file_path} 失败{type(e).__name__}: {e}"


@register_tool(AGENT_NAME)
@tool
def change_analysis_app_code(
        runtime: ToolRuntime, new_code: str,
        file_path: str, start_idx: int,
        end_idx: int | None = None
) -> str:
    """
    替换文件里指定行区间的代码
    参数：
        new_code: 新的代码内容
        file_path: 相对项目的文件路径，如 src/main/python/analysis.py
        start_idx: 起始行号（从 1 开始计数，1 表示首行）
        end_idx: 结束行号（从 1 开始计数，**开区间**；不填、或填成与 start_idx 相同的数，
                 都表示只替换 start_idx 这一行）
    返回：
        修改结果
    注意：
        - 区间是 [start_idx, end_idx)：end_idx 那一行本身不会被替换
        - 替换到文件最后一行时不会给它补换行（原本以换行结尾的文件，改完末行后可能不再以换行结尾）
        - 新代码的行数不要求和原区间一样，可以更长或更短
        - 不确定区间里是什么内容时，先用 view_analysis_app_code 看一眼
        - 改写失败会自动回滚
    """
    project_path = _project_path(runtime)
    if project_path is None:
        return "项目未初始化，请先调用工具 initialize_project 进行初始化"
    app_file = _app_file(project_path, file_path)
    if isinstance(app_file, str):
        return app_file
    if not app_file.exists():
        return (f"文件 {file_path} 不存在，请先用 write_analysis_app_code 创建"
                f"（路径要写成相对项目的，如 src/main/python/analysis.py）")
    if start_idx <= 0:
        logger.warning(f"Agent 填入了无效的起始行号 start_idx: {start_idx}，已自动调整为 1")
        start_idx = 1
    if end_idx is None or end_idx == start_idx:
        end_idx = start_idx + 1
    if start_idx > end_idx:
        return f"start_idx({start_idx}) 比 end_idx({end_idx}) 大，区间是 [start_idx, end_idx)，请检查行号"
    # 先备份，失败就回滚
    back_file = app_file.with_name(app_file.name + ".bak")
    shutil.copy2(app_file, back_file)
    try:
        with open(app_file, "r", encoding="utf-8", newline="") as f:
            lines = f.readlines()
        lines[start_idx - 1:end_idx - 1] = new_code.splitlines()
        # 后面还有行的都必须以换行结尾，否则会跟下一行粘在一起；只有文件最后一行可以不带
        for i, line in enumerate(lines[:-1]):
            if not line.endswith(("\n", "\r")):
                lines[i] = line + "\n"
        with open(app_file, "w", encoding="utf-8", newline="") as f:
            f.writelines(lines)
        os.remove(back_file)
        return f"文件 {file_path} 修改成功"
    except Exception as e:
        logger.error(f"修改文件 {file_path} 失败{type(e).__name__}: {e}")
        os.replace(back_file, app_file)
        return f"修改文件 {file_path} 失败(已回滚){type(e).__name__}: {e}"


@register_tool(AGENT_NAME)
@tool
def add_analysis_app_code(runtime: ToolRuntime, new_code: str, file_path: str, auto_wrap: bool = True) -> str:
    """
    把代码追加到文件末尾
    参数：
        new_code: 要追加的代码内容
        file_path: 相对项目的文件路径，如 src/main/python/analysis.py
        auto_wrap: 是否在新代码前面补一个换行符（默认 True）
    返回：
        追加结果
    注意：
        - 只追加、不覆盖原有内容，也不改写 new_code 本身
        - auto_wrap 为 True 时会在 new_code 前补一个 \n：原有内容末尾没有换行时，两边刚好各占一行；
          末尾已经有换行时，两者之间会多出一个空行（想要空行隔开就用这个开关）
        - auto_wrap 为 False 时不做任何换行处理，代码直接接在文件末尾（会跟原来最后一行粘在一起）
        - 追加失败会自动回滚
    """
    project_path = _project_path(runtime)
    if project_path is None:
        return "项目未初始化，请先调用工具 initialize_project 进行初始化"
    app_file = _app_file(project_path, file_path)
    if isinstance(app_file, str):
        return app_file
    if not app_file.exists():
        return (f"文件 {file_path} 不存在，请先用 write_analysis_app_code 创建"
                f"（路径要写成相对项目的，如 src/main/python/analysis.py）")
    back_file = app_file.with_name(app_file.name + ".bak")
    shutil.copy2(app_file, back_file)
    try:
        with open(app_file, "a+", encoding="utf-8", newline="") as f:
            f.write(f"{'\n' if auto_wrap else ''}{new_code}")
        os.remove(back_file)
        return f"文件 {file_path} 追加成功"
    except Exception as e:
        logger.error(f"追加文件 {file_path} 失败{type(e).__name__}: {e}")
        os.replace(back_file, app_file)
        return f"追加文件 {file_path} 失败(已回滚){type(e).__name__}: {e}"


@register_tool(AGENT_NAME)
@tool
def view_analysis_app_code(
        runtime: ToolRuntime, file_path: str,
        start_idx: int | None = None, end_idx: int | None = None
) -> str:
    """
    查看文件里指定行区间的代码（带行号）
    参数：
        file_path: 相对项目的文件路径，如 src/main/python/analysis.py
        start_idx: 起始行号（从 1 开始计数；不填表示从第 1 行开始）
        end_idx: 结束行号（从 1 开始计数，**开区间**；不填、或填成与 start_idx 相同的数，
                 都表示只看 start_idx 这一行；start_idx 也不填则看整个文件）
    返回：
        带行号的代码；单次最多显示 400 行，超了会截断并告诉你怎么看剩下的部分
    """
    project_path = _project_path(runtime)
    if project_path is None:
        return "项目未初始化，请先调用工具 initialize_project 进行初始化"
    app_file = _app_file(project_path, file_path)
    if isinstance(app_file, str):
        return app_file
    if not app_file.exists():
        return f"文件 {file_path} 不存在，请尝试相对项目的路径写法，如：src/main/python/analysis.py"
    try:
        with open(app_file, "r", encoding="utf-8", newline="") as f:
            lines = f.readlines()
    except Exception as e:
        logger.error(f"读取文件 {file_path} 失败{type(e).__name__}: {e}")
        return f"读取文件 {file_path} 失败{type(e).__name__}: {e}"
    total = len(lines)
    if total == 0:
        return f"文件 {file_path} 是空的"
    if start_idx is not None and start_idx <= 0:
        logger.warning(f"Agent 填入了无效的起始行号 start_idx: {start_idx}，已自动调整为 1")
        start_idx = 1
    if start_idx is None and end_idx is None:
        s, e = 1, total + 1  # 整文件
    else:
        s = 1 if start_idx is None else start_idx
        e = s + 1 if (end_idx is None or end_idx == s) else end_idx  # 不填、或填成同一个数 -> 只看这一行
    if e < s:
        return f"start_idx({s}) 比 end_idx({e}) 大，区间是 [start_idx, end_idx)，请检查行号"
    if s > total:
        return f"起始行 {s} 超出了文件范围（共 {total} 行）"
    e = min(e, total + 1)
    truncated = (e - s) > MAX_VIEW_LINES
    if truncated:
        e = s + MAX_VIEW_LINES
    code_lines = [f"{i}\t{lines[i - 1].rstrip()}" for i in range(s, e)]
    result = f"文件 {file_path} 第 [{s}, {e}) 行的代码如下：\n" + "\n".join(code_lines)
    if truncated:
        result += f"\n…（文件共 {total} 行，本次只显示到第 {e - 1} 行，剩下的用 start_idx / end_idx 继续看）"
    return result


@register_tool(AGENT_NAME)
@tool
def verify_analysis_app_code(runtime: ToolRuntime, file_path: str) -> str:
    """
    检查分析代码能否通过 python 编译（只做静态检查：不执行代码、不连集群、不产生任何文件）
    参数：
        file_path: 相对项目的文件路径，如 src/main/python/analysis.py（只支持 .py）
    返回：
        编译检查结果
    注意：
        - 检查强度等同于 python 自己加载源码时做的编译检查，能抓出：缩进/括号不配对、
          模块级 return、循环外 break、函数外 await、from __future__ 位置错、把关键字当变量名等
        - **抓不出**：导入了不存在的包、函数名拼错、DataFrame 操作写错这类只有真正跑起来才知道的问题，
          所以通过只代表"语法层面没问题"
        - 非 .py 文件不做验证（会明确说明没验证过，不会假装通过）
    """
    project_path = _project_path(runtime)
    if project_path is None:
        return "项目未初始化，请先调用工具 initialize_project 进行初始化"
    app_file = _app_file(project_path, file_path)
    if isinstance(app_file, str):
        return app_file
    if not app_file.exists():
        return f"文件 {file_path} 不存在，请尝试相对项目的路径写法，如：src/main/python/analysis.py"
    if app_file.suffix.lower() != ".py":
        return f"{app_file.suffix or '（无后缀）'} 不是 python 文件，本工具只验证 .py，这个文件没有被验证过"
    try:
        with tokenize.open(app_file) as f:
            source = f.read()
    except SyntaxError as e:
        return f"文件读取失败（编码声明有问题）：{e}"
    except Exception as e:
        logger.error(f"读取文件 {file_path} 失败{type(e).__name__}: {e}")
        return f"读取文件 {file_path} 失败{type(e).__name__}: {e}"
    try:
        # 不执行，只编译；用 compile 而不是 ast.parse —— 后者漏掉编译阶段的检查
        compile(source, str(app_file), "exec")
    except SyntaxError as e:
        line_text = (e.text or "").rstrip()
        location = f"第 {e.lineno} 行"
        if e.offset:
            location += f" 第 {e.offset} 列"
        return f"语法/编译错误：{location}：{e.msg}\n{' ' * 4}{line_text}"
    except (ValueError, MemoryError, RecursionError) as e:
        return f"编译失败{type(e).__name__}: {e}"
    return "语法/编译检查通过（只代表语法没问题；导入的包是否存在、逻辑对不对，需要真正运行才知道）"


@register_tool(AGENT_NAME)
@tool
def list_python_lib_modules(
        module: str | None = None,
        keyword: str | None = None,
        ignore_case: bool = True
) -> str:
    """
    列出 python 依赖库里的包/模块（只做文件系统探测，不导入、不执行）
    参数：
        module: 包名，如 pyspark.sql；不填则列出 python_lib_dir 下的顶层包/模块
        keyword: 模块名关键词（可选，只匹配名字最后一段，支持模糊匹配）
        ignore_case: 是否忽略大小写（默认 True）
    返回：
        模块名列表（多层包用点分全名）；超过 200 个时只显示前 200 个，并提示用 keyword 收窄
    注意：
        - 只查 python_lib_dir（第三方库）里的模块，不查标准库和项目源码
        - module 必须是包（目录）；单个 .py 模块没有子模块
    """
    roots = _lib_roots()
    if isinstance(roots, str):
        return roots
    if module:
        path = _locate_module(module, roots)
        if isinstance(path, str):
            return path
        if path.name != "__init__.py":
            return (f"{module} 是单个模块文件（{path}），没有子模块；"
                    f"请用 view_python_module_info(module={module!r}) 查看其中内容")
        base = path.parent
        prefix = module.strip()
        title = f"模块 {module} 下的子模块"
    else:
        base = _PYTHON_LIB_DIR
        prefix = ""
        title = f"python_lib_dir（{_PYTHON_LIB_DIR}）下的顶层包/模块"
    try:
        names = _iter_children(base, prefix)
    except Exception as e:
        logger.error(f"列出 {base} 下的模块失败{type(e).__name__}: {e}")
        return f"列出 {base} 下的模块失败{type(e).__name__}: {e}"
    if keyword:
        kw = keyword.lower() if ignore_case else keyword

        def _hit(name: str) -> bool:
            last = name.rsplit(".", 1)[-1]
            return kw in (last.lower() if ignore_case else last)

        names = [n for n in names if _hit(n)]
    if not names:
        hint = f"没有名称包含 {keyword!r} 的模块" if keyword else "没有找到任何模块"
        return f"{title}{hint}；可以换个关键词，或去掉 module 参数从顶层看起"
    total = len(names)
    shown = names[:MAX_VIEW_SYMBOLS]
    result = f"{title}共 {total} 个：\n" + "\n".join(shown)
    if total > len(shown):
        result += f"\n…（只显示前 {len(shown)} 个，用 keyword 收窄）"
    return result


@register_tool(AGENT_NAME)
@tool
def view_python_module_info(
        module: str,
        keyword: str | None = None,
        ignore_case: bool = True
) -> str:
    """
    查看 python 模块里有哪些类和函数（带签名与行号，不导入、不执行）
    参数：
        module: 点分模块名，如 pyspark.sql.functions
        keyword: 类/函数名关键词（可选，只匹配名字最后一段，支持模糊匹配）
        ignore_case: 是否忽略大小写（默认 True）
    返回：
        带缩进的类/函数签名列表；超过 200 个或 12000 字符时会截断并提示
    注意：
        - 只列模块内定义的类/函数（含私有、嵌套的），不列 import 进来的名字
        - 只看签名时用本工具；要看某个类/函数的完整源码用 view_python_api_code
    """
    got = _load_module(module)
    if isinstance(got, str):
        return got
    _, path, source = got
    analyzer = _make_analyzer(module, source)
    if isinstance(analyzer, str):
        return analyzer
    kw = keyword.lower() if (keyword and ignore_case) else keyword
    items: list[str] = []
    for qualname, node in analyzer.scopes.items():
        name = qualname.rsplit(".", 1)[-1]
        if kw:
            if ignore_case:
                if kw not in name.lower():
                    continue
            elif kw not in name:
                continue
        indent = "    " * qualname.count(".")
        items.append(f"{indent}{_signature(node)}  # L{node.lineno}")
    if not items:
        if path.name == "__init__.py" and not analyzer.roots:
            return (f"模块 {module} 的 __init__.py 里没有任何类/函数定义（通常只是转发导入）；"
                    f"可以先用 find_python_api_definition(module={module!r}, name=...) 定位名字，"
                    f"或用 list_python_lib_modules(module={module!r}) 查看子模块")
        top = " / ".join(n.name for n in analyzer.roots[:50]) or "（空）"
        return (f"模块 {module} 中没有名称匹配 {keyword!r} 的类/函数；"
                f"模块顶层的名字有：{top}")
    matched = len(items)
    all_total = len(analyzer.scopes)
    kept, truncated = _cap_by_chars(items)
    if len(kept) > MAX_VIEW_SYMBOLS:
        kept = kept[:MAX_VIEW_SYMBOLS]
        truncated = True
    scope_desc = f"模块共 {all_total} 个类/函数，匹配到 {matched} 个" if keyword else f"共 {all_total} 个类/函数"
    result = f"模块 {module}（文件：{path}，{scope_desc}）中的定义如下：\n" + "\n".join(kept)
    if truncated:
        result += (f"\n…（本次只显示前 {len(kept)} 个，共 {matched} 个；"
                   f"用 keyword 收窄，或直接 view_python_api_code 看具体某个定义）")
    if path.name == "__init__.py":
        result += f"\n（这是包的 __init__.py；子模块请用 list_python_lib_modules(module={module!r}) 查看）"
    return result


@register_tool(AGENT_NAME)
@tool
def view_python_api_code(
        module: str,
        qualname: str | None = None,
        start_idx: int | None = None,
        end_idx: int | None = None
) -> str:
    """
    查看 python 模块里某个类/函数的源码（不导入、不执行）
    参数：
        module: 点分模块名，如 pyspark.sql.functions
        qualname: 类/函数名，如 col / DataFrame.select；不填则查看整个模块文件
        start_idx: 起始行号（从 1 开始，**模块文件里的绝对行号**；不填表示从定义第一行开始）
        end_idx: 结束行号（从 1 开始，**开区间**；不填、或填成与 start_idx 相同的数，
                 都表示只看 start_idx 这一行）
    返回：
        带行号的源码；单次最多 400 行，超了会截断并告诉你怎么看剩下的部分
    注意：
        - qualname 只写模块内相对的名字，不用带模块前缀
        - 名字被 import 转发到别的模块时，会顺着转发找到真正定义的文件
        - 不确定有什么类/函数时，先用 view_python_module_info 看一眼
    """
    got = _load_module(module)
    if isinstance(got, str):
        return got
    roots, path, source = got
    lines = source.splitlines()
    total = len(lines)
    q = (qualname or "").strip()
    if q.startswith(module.strip() + "."):
        q = q[len(module.strip()) + 1:]

    if not q:
        # 整个模块文件：不解析 AST，语法不兼容当前解释器时也能看
        if total == 0:
            return f"模块文件 {path} 是空的"
        if start_idx is None and end_idx is None:
            s, e = 1, total + 1
        else:
            s = 1 if start_idx is None else start_idx
            if s <= 0:
                logger.warning(f"Agent 填入了无效的起始行号 start_idx: {s}，已自动调整为 1")
                s = 1
            if end_idx is None or end_idx == s:
                e = s + 1
            else:
                e = end_idx
            if e <= s:
                return f"start_idx({s}) 必须小于 end_idx({e})，区间是 [start_idx, end_idx)；文件共 {total} 行"
            if s > total:
                return f"起始行 {s} 超出了文件范围（共 {total} 行）"
            e = min(e, total + 1)
        truncated = (e - s) > MAX_VIEW_LINES
        if truncated:
            e = s + MAX_VIEW_LINES
        body = [f"{i}\t{lines[i - 1]}" for i in range(s, e)]
        result = f"模块 {module} 的源码（文件：{path}，共 {total} 行）第 [{s}, {e}) 行：\n" + "\n".join(body)
        if truncated:
            result += f"\n…（本次只显示到第 {e - 1} 行，剩下的用 start_idx / end_idx 继续看）"
        return result

    analyzer = _make_analyzer(module, source)
    if isinstance(analyzer, str):
        return analyzer
    node = analyzer.scopes.get(q)
    origin = ""
    found = None
    if node is None:
        # 可能只是转发：顺着 import 找到真正定义它的模块
        head = q.split(".", 1)[0]
        try:
            found = find_definition(head, module.strip(), roots)
        except Exception as e:
            logger.error(f"追踪 {module}.{head} 的定义失败{type(e).__name__}: {e}")
        if found and Path(found["file"]).resolve() != path.resolve():
            try:
                with tokenize.open(found["file"]) as f:
                    target_source = f.read()
                target_analyzer = _make_analyzer(found["module"], target_source)
            except Exception as err:
                logger.error(f"读取转发目标 {found['file']} 失败{type(err).__name__}: {err}")
                target_source, target_analyzer = None, None
            if target_source is not None and not isinstance(target_analyzer, str):
                target_node = target_analyzer.scopes.get(q)
                if target_node is not None:
                    path, source = Path(found["file"]), target_source
                    lines = target_source.splitlines()
                    analyzer, node = target_analyzer, target_node
                    origin = f"（由模块 {module} 转发到 {found['module']}）"
        if node is None:
            hint = analyzer.lookup_hint(q)
            msg = f"未在模块 {module} 中找到类/函数: {q}"
            if found:
                msg += f"；{head} 实际在 {found.get('module')}（{found.get('file')}）"
            if hint:
                msg += f"\n{hint}"
            msg += f"\n可以先用 view_python_module_info(module={module!r}) 查看模块里有哪些定义"
            return msg

    scope_start = _node_start_line(node)
    scope_end = node.end_lineno
    if start_idx is None and end_idx is None:
        s, e = scope_start, scope_end + 1
    else:
        if start_idx is None:
            s = scope_start
        else:
            s = start_idx
            if s <= 0:
                logger.warning(f"Agent 填入了无效的起始行号 start_idx: {s}，已自动调整为定义起始行 {scope_start}")
                s = scope_start
        if end_idx is None or end_idx == s:
            e = s + 1
        else:
            e = end_idx
        if e <= s:
            return f"start_idx({s}) 必须小于 end_idx({e})；{q} 的源码在第 {scope_start}-{scope_end} 行"
        if s < scope_start or s > scope_end:
            return f"start_idx({s}) 超出了 {q} 的源码范围（第 {scope_start}-{scope_end} 行）"
        e = min(e, scope_end + 1)
    truncated = (e - s) > MAX_VIEW_LINES
    if truncated:
        e = s + MAX_VIEW_LINES
    body = [f"{i}\t{lines[i - 1]}" for i in range(s, e)]
    result = f"模块 {module}{origin} 中 {q} 的源码（文件：{path}，第 {s}-{e - 1} 行）：\n" + "\n".join(body)
    if truncated:
        result += (f"\n…（{q} 的源码共 {scope_end - scope_start + 1} 行，本次只显示到第 {e - 1} 行；"
                   f"用 start_idx / end_idx 继续看）")
    return result


@register_tool(AGENT_NAME)
@tool
def find_python_api_definition(module: str, name: str) -> str:
    """
    在 python 模块里定位某个类/函数定义在哪个文件哪一行（不导入、不执行）
    参数：
        module: 起始模块名，如 pyspark.sql
        name: 单个类名/函数名，如 SparkSession（不要带点）
    返回：
        真正定义它的模块、文件、行号和定义行；名字被 import 转发时会跟到源头
    注意：
        - 只接受单段名字；要看某个类的方法请用 view_python_api_code(qualname="类.方法")
        - 只顺着该模块里的 import 转发查找，不会全局扫描所有包
    """
    roots = _lib_roots()
    if isinstance(roots, str):
        return roots
    path = _locate_module(module, roots)
    if isinstance(path, str):
        return path
    nm = (name or "").strip()
    if not nm or not nm.isidentifier():
        return f"name 必须是单个类名/函数名（只含字母/数字/下划线，不能带点），收到的是 {name!r}"
    try:
        found = find_definition(nm, module.strip(), roots)
    except Exception as e:
        logger.error(f"查找 {module}.{nm} 失败{type(e).__name__}: {e}")
        return f"查找 {module}.{nm} 失败{type(e).__name__}: {e}"
    if found is None:
        return (f"在模块 {module} 中没有找到 {nm} 的定义（已顺着 import 转发查找）；"
                f"可以先用 view_python_module_info(module={module!r}) 看看模块里有什么")
    if found.get("lineno") is None:
        return (f"{nm} 是一个子模块：{found['module']}（{found['file']}）；"
                f"用 view_python_module_info(module={found['module']!r}) 查看其中内容")
    return (f"{nm} 定义在模块 {found['module']}（文件：{found['file']}，第 {found['lineno']} 行）：\n"
            f"{found['text']}")


@register_tool(AGENT_NAME)
@tool
def register_submit_command(
        runtime: ToolRuntime,
        app_name: str,
        main_file: str,
        num_executors: int | None = None,
        executor_cores: int | None = None,
        executor_memory: str | None = None,
        driver_memory: str | None = None,
        files: list[str] | None = None,
        conf: dict[str, str] | None = None,
        app_params: dict[str, str] | None = None
) -> str:
    """
    注册 spark-submit 提交信息（数据分析任务最后必须调用该工具）
    参数：
        app_name: 应用名称（对应 spark-submit 的 --name）
        main_file: 主文件，相对项目的路径，如 src/main/python/analysis.py（必须是已存在的 .py）
        num_executors: 执行器数量（可选）
        executor_cores: 每个执行器的核数（可选）
        executor_memory: 每个执行器的内存，如 1g / 1024m（可选）
        driver_memory: 驱动内存，如 1g / 1024m（可选）
        files: 需要分发的非代码普通文件列表，相对项目路径（可选）
        conf: Spark 配置项，如 {"spark.executor.memoryOverhead": "512m"}（可选）
        app_params: 应用自己的参数，key 必须带 - 或 -- 前缀，如 {"--days": "7"}（可选）
    返回：
        注册结果（信息存在 Redis 的 DAA:项目名 下，供后续提交流程读取）
    注意：
        - 固定提交到 YARN、cluster 模式，不用填 master / deploy_mode
        - main_file 之外、src/main/python 下的文件会由提交端自动当作辅助代码文件处理
        - 并行度用 num_executors 表达，不再使用 flink 的 parallelism
        - 工具只登记信息，不会在这里真正打包、上传或提交
        - app_params 的 key 必须写成 --xxx 或 -x，否则无法与应用参数区分
    """
    project_path = _project_path(runtime)
    if project_path is None:
        return "项目未初始化，请先调用工具 initialize_project 进行初始化"
    app_name = (app_name or "").strip()
    if not app_name:
        return "app_name 不能为空"
    # app_name 会同时用作 HDFS 目录名和 cmd 文件名，不能带路径分隔符/控制字符
    if app_name in (".", "..") or "/" in app_name or "\\" in app_name or any(ord(ch) < 32 for ch in app_name):
        return "app_name 不能包含路径分隔符、控制字符，也不能是 . 或 .."
    main_path = _app_file(project_path, main_file)
    if isinstance(main_path, str):
        return main_path
    if main_path.suffix.lower() != ".py":
        return f"数据分析应用的主文件必须是 .py（收到的是 {main_file}）"
    if not main_path.exists():
        return f"主文件 {main_file} 不存在，请先用 write_analysis_app_code 写代码"

    if num_executors is not None and num_executors <= 0:
        return f"num_executors 必须大于 0，当前值为 {num_executors}"
    if executor_cores is not None and executor_cores <= 0:
        return f"executor_cores 必须大于 0，当前值为 {executor_cores}"
    for label, value in (("executor_memory", executor_memory), ("driver_memory", driver_memory)):
        if value is not None and not str(value).strip():
            return f"{label} 不能是空字符串"
    if files and any(not str(v).strip() for v in files):
        return "files 里不能有空路径"
    if conf:
        for k in conf:
            if not str(k).strip() or str(k).startswith("-"):
                return f"conf 的 key 直接写配置名（如 spark.executor.memoryOverhead），不要带 --conf 前缀；收到的是 {k!r}"
    if app_params:
        for k in app_params:
            if not str(k).startswith("-"):
                return f"app_params 的 key 必须带 - 或 -- 前缀（如 --days），收到的是 {k!r}"

    register_dict: dict = {"main_file": Path(main_file.strip()).as_posix()}
    if num_executors is not None:
        register_dict["num_executors"] = num_executors
    if executor_cores is not None:
        register_dict["executor_cores"] = executor_cores
    if executor_memory is not None:
        register_dict["executor_memory"] = str(executor_memory).strip()
    if driver_memory is not None:
        register_dict["driver_memory"] = str(driver_memory).strip()
    if files:
        register_dict["files"] = [Path(str(v).strip()).as_posix() for v in files]
    if conf:
        register_dict["conf"] = dict(conf)
    if app_params:
        register_dict["app_params"] = dict(app_params)

    p_name = runtime.state.get("project_name")
    key = submit_redis_key(p_name)
    try:
        redis_con.hset(key, "submit_command", json.dumps(register_dict, ensure_ascii=False))
        redis_con.hset(key, "app_name", app_name)
        redis_con.hset(key, "app_type", APP_TYPE)
        redis_con.hset(key, "date", runtime.context.date)
    except Exception as e:
        logger.error(f"注册提交命令失败{type(e).__name__}: {e}")
        return f"注册提交命令失败{type(e).__name__}: {e}"

    summary = [f"main_file={register_dict['main_file']}"]
    if num_executors is not None:
        summary.append(f"num_executors={num_executors}")
    if executor_cores is not None:
        summary.append(f"executor_cores={executor_cores}")
    if executor_memory is not None:
        summary.append(f"executor_memory={executor_memory}")
    if driver_memory is not None:
        summary.append(f"driver_memory={driver_memory}")
    return f"提交信息注册成功（Redis 键 {key}）：app_name={app_name}, {'; '.join(summary)}"
