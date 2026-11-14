import asyncio
import json
import os
import re
import shutil
import tempfile
import tokenize
import uuid
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Literal

from langchain_core.tools import tool
from langgraph.prebuilt import ToolRuntime
from langgraph.types import Command
from pydantic import BaseModel, Field

from Tasks.background_broker import background_broker, background_registry, background_thread_id
from Tasks.build_run_custom_tasks import TaskSupervisor
from Tasks.verification_registry import VerificationReporter
from Tools.log_settings import LogSetting
from Tools.code_analysis import JavaAnalyzer, PyAnalyzer
from Tools.registry import register_tool
from Tools.run_command import run_mvn_command, run_java_command, maven_java_release
from load_config.config import config, ROOT_BASE_DIR_PATH

AGENT_NAME = "CVA"
_ALIGN_MAVEN_JDK = bool((config.get("app") or {}).get("align_maven_jdk", True))
_MAX_CHARS = 64 * 1024  # 64KB
_CODE_DIRS = {
    "flink": {
        "test_code": ("src", "test", "java"),
        "business_code": ("src", "main", "java", "com", "fm")
    },
    "spark": {
        "test_code": ("src", "tests"),
        "business_code": ("src", "main", "python")
    }
}
_JAVAC_ERROR_LINE = re.compile(r":\d+:\s*(?:错误|error)", re.IGNORECASE)
logger = LogSetting.create(__name__)


class BusinessVerifyCommand(BaseModel):
    """Flink 单条业务验证命令。"""
    target_class: str = Field(description="全限定类名，如 com.fm.MyAppTest")
    target_method: str | None = Field(default=None, description="方法名；不填表示整个类")
    params: dict[str, str] = Field(default_factory=dict, description="mvn -D 参数")


class DeepVerifyCommand(BusinessVerifyCommand):
    """Flink 单条深层验证命令。"""


class SparkTestCommand(BaseModel):
    """Spark 单条验证命令。"""
    test_file: str = Field(description="相对项目的 pytest 文件，必须在 src/tests 下")
    test_name: str | None = Field(default=None, description="pytest 节点名；不填跑整个文件")
    master: str | None = Field(default=None, description="该测试的 Spark master，如 local[2]")


class CodeSnippet(BaseModel):
    """人工审核用的代码片段。"""
    file_path: str = Field(default="", description="代码片段所在相对项目文件路径")
    start_line: int = Field(default=0, description="片段起始行号")
    end_line: int = Field(default=0, description="片段结束行号")
    priority: Literal["high", "medium", "low"] = Field(default="medium", description="审查优先级")
    snippet: str = Field(default="", description="代码片段内容")
    note: str = Field(default="", description="代码片段的备注(可选)")


class CodeSummary(BaseModel):
    """run_deep 的人工审核摘要。"""
    data_result: str = Field(default="", description="数据验证结果，轻量、突出异常值")
    mermaid_flowgraph: str = Field(default="", description="Mermaid 语法的文字版流程图")
    snippets: list[CodeSnippet] = Field(default_factory=list, description="按优先级排序的代码片段")


def _as_dict(command: Any) -> dict:
    return command.model_dump() if isinstance(command, BaseModel) else command


def _code_file_path(
        app_path: Path, file_path: str,
        app_type: str, code_type: Literal["test_code", "business_code"] = "business_code"
) -> Path | str:
    code_dir = _CODE_DIRS.get(app_type, {}).get(code_type, None)
    if code_dir is None:
        return f"未知的应用类型: {app_type}, 或者未记录对应的 {code_type}"
    rel = Path(file_path.strip())
    if not file_path.strip() or rel.is_absolute() or rel.root or rel.drive:
        return f"file_path 只能是相对项目的路径，如：{'/'.join(code_dir)}/MyApp.java"
    target = (app_path / rel).resolve()
    if not target.is_relative_to((app_path / Path(*code_dir)).resolve()):
        return f"file_path 必须落在项目内的 {'/'.join(code_dir)} 目录下（收到的是 {file_path}）"
    return target


def _file_missing(file_path: Path, app_path: Path) -> str:
    """文件不存在时返回可自纠的提示，避免 JavaAnalyzer/PyAnalyzer 直接抛异常。"""
    try:
        rel = file_path.relative_to(app_path).as_posix()
    except ValueError:
        rel = str(file_path)
    return (f"文件不存在：{rel}。请先用 get_project_list 查看项目目录，"
            "确认 file_path 的包路径和文件名是否正确后再试。")


def _parse_command(command_list: list[dict]) -> list[str]:
    """把 Flink 业务命令展开成 mvn test 命令。"""
    commands: list[str] = []
    only_class: list[str] = []
    plain_methods: dict[str, list[str]] = {}
    for raw_command in command_list or []:
        command = _as_dict(raw_command)
        target_class = command.get("target_class")
        target_method = command.get("target_method")
        params = command.get("params")
        if not target_method:
            only_class.append(target_class)
        elif not params:
            plain_methods.setdefault(target_class, []).append(target_method)
        else:
            params_str = " ".join(f"-D{k}={v}" for k, v in params.items())
            commands.append(f"mvn test -Dtest={target_class}#{target_method} {params_str}")
    if only_class:
        commands.append(f"mvn test -Dtest={','.join(only_class)}")
    if plain_methods:
        combined = ",".join(
            f"{cls}#{'+'.join(methods)}" for cls, methods in plain_methods.items()
        )
        commands.append(f"mvn test -Dtest={combined}")
    return commands


def _parse_spark_command(command_list: list[dict]) -> list[dict]:
    """把 Spark 测试命令规范成 {test_file, test_name, master}。"""
    commands: list[dict] = []
    for raw_command in command_list or []:
        command = _as_dict(raw_command)
        test_file = (command.get("test_file") or "").strip().replace("\\", "/")
        if not test_file:
            continue
        commands.append({
            "test_file": test_file,
            "test_name": (command.get("test_name") or "").strip() or None,
            "master": (command.get("master") or "").strip() or None,
        })
    return commands


def _count_test_cases(commands: list[str]) -> int:
    """从 mvn -Dtest=... 估算测试方法数，避免进度条分母是命令数、分子是用例数。"""
    total = 0
    for command in commands or []:
        match = re.search(r"-Dtest=([^\s]+)", command)
        if not match:
            total += 1
            continue
        for spec in match.group(1).split(","):
            if "#" in spec:
                total += max(1, len(spec.split("#", 1)[1].split("+")))
            else:
                total += 1
    return max(total, 1)


def _count_spark_test_cases(app_path: Path, commands: list[dict]) -> int:
    """Spark：指定 test_name 算 1 个；否则数测试文件里的 test_* 函数。"""
    total = 0
    for command in commands or []:
        if command.get("test_name"):
            total += 1
            continue
        test_file = str(command.get("test_file") or "").strip()
        if not test_file:
            total += 1
            continue
        try:
            text = (app_path / test_file).read_text(encoding="utf-8", errors="replace")
            total += max(1, len(re.findall(r"^\s*def\s+test_\w+\s*\(", text, re.MULTILINE)))
        except Exception:  # noqa: BLE001
            total += 1
    return max(total, 1)


def _capped(payload: dict, big_key: str):
    """
    截取payload中的指定key，返回其对应的value
    """
    if len(json.dumps(payload, ensure_ascii=False)) > _MAX_CHARS:
        val = payload[big_key]
        keep = 200 if isinstance(val, list) else 4000
        payload[big_key] = val[:keep]
        payload["note"] = f"返回体过大已截断，{big_key} 只保留了前 {keep} 行；请收窄范围再取一次"
        return payload
    return payload


def _javac_classpath(app_path: Path) -> tuple[str, str]:
    cp_file = app_path / "target" / "classpath.txt"
    if not cp_file.exists():
        cp_file.parent.mkdir(parents=True, exist_ok=True)
        return_code, out = run_mvn_command(
            "mvn -q dependency:build-classpath -Dmdep.outputFile=target/classpath.txt",
            path=str(app_path)
        )
        if return_code != 0 or not cp_file.exists():
            return "", (out or "mvn 没有跑起来").strip()[:200]
    return cp_file.read_text(encoding="utf-8").strip(), ""


def _pick_javac_errors(out: str, keep_after: int = 4, max_chars: int = 2000) -> str:
    lines = out.splitlines()
    heads = [i for i, line in enumerate(lines) if _JAVAC_ERROR_LINE.search(line)]
    if not heads:  # 没匹配到（比如报的是选项错误）就不裁，宁可多给也别把真正的原因吞掉
        return out.strip()[:max_chars]
    kept, covered = [], -1
    for i in heads:
        if i <= covered:
            continue
        kept.extend(lines[i:i + keep_after])
        covered = i + keep_after - 1
    return "\n".join(kept).strip()[:max_chars]


# fromCollection(空集合) / fromElements() 在 Flink 里要么抛异常、要么根本测不出边界；
# 这类测试一旦写出来，surefire 只会给一句 Collection must not be empty，Agent 很难自己反应过来。
_EMPTY_SOURCE_PATTERNS = (
    re.compile(
        r"fromCollection\s*\(\s*(?:Collections\.emptyList\s*\(\s*\)"
        r"|new\s+ArrayList\s*<[^>]*>\s*\(\s*\)"
        r"|Arrays\.asList\s*\(\s*\)|List\.of\s*\(\s*\))"
    ),
    re.compile(r"fromElements\s*\(\s*\)"),
)


def _lint_test_code(app_type: str, code: str) -> list[str]:
    """检查测试代码的硬伤；只返回问题，不阻止写入（让模型立刻改）"""
    issues: list[str] = []
    if app_type == "flink":
        if not re.search(r"@Test\b", code):
            issues.append("没有任何 @Test 方法（禁止占位类/空测试）")
        if not re.search(r"\bclass\s+\w+", code):
            issues.append("没有 class 定义")
        for pattern in _EMPTY_SOURCE_PATTERNS:
            hit = pattern.search(code)
            if hit:
                issues.append(
                    f"Flink source 不能是空集合：{hit.group(0)}。"
                    f"空输出场景请用“非空输入 + 全部被过滤/非法”或直接测纯函数"
                )
                break
    elif app_type == "spark":
        if not re.search(r"^\s*def\s+test_\w+\s*\(", code, re.MULTILINE):
            issues.append("没有任何 test_* 测试函数（禁止占位/空测试）")
    return issues


def _verify_test_file(app_path: Path, app_type: str, p_file_path: Path) -> tuple[bool, str]:
    """对单个测试文件做语法/编译检查；返回 (是否通过, 给模型看的说明)"""
    if app_type == "spark":
        try:
            with tokenize.open(p_file_path) as f:
                source = f.read()
        except SyntaxError as e:
            return False, f"文件读取失败（编码声明有问题）：{e}"
        except Exception as e:  # noqa: BLE001
            logger.error(f"读取文件 {p_file_path} 失败{type(e).__name__}: {e}")
            return False, f"读取文件 {p_file_path} 失败{type(e).__name__}: {e}"
        try:
            # 不执行，只编译；ast.parse 会漏掉编译阶段的检查
            compile(source, str(p_file_path), "exec")
        except SyntaxError as e:
            line_text = (e.text or "").rstrip()
            location = f"第 {e.lineno} 行"
            if e.offset:
                location += f" 第 {e.offset} 列"
            return False, f"语法/编译错误：{location}：{e.msg}\n{' ' * 4}{line_text}"
        except (ValueError, MemoryError, RecursionError) as e:
            return False, f"编译失败{type(e).__name__}: {e}"
        return True, "语法/编译检查通过（语法没问题；导入的包和逻辑要真正跑测试才知道）"

    if app_type != "flink":
        return False, f"未知的应用类型: {app_type}"
    if p_file_path.suffix.lower() != ".java":
        return False, f"Flink 测试文件必须是 .java: {p_file_path.name}"
    classpath, why = _javac_classpath(app_path)
    # 绝不能把 javac 的产物写进 target/test-classes：
    # 本机 javac 可能是更高的 JDK，写出的 class 版本比 Maven 的 Java 8 新，
    # Maven 会误判“已是最新”而跳过重编译，surefire 直接在 forked JVM 里报
    # UnsupportedClassVersionError（B2c 实测）。编译检查产物只放临时目录。
    with tempfile.TemporaryDirectory(prefix="cva-testcheck-") as tmp_dir:
        out_dir = Path(tmp_dir)
        cp_parts = [str(app_path / "target" / "classes")]
        if classpath:
            cp_parts.insert(0, classpath)
        cp = os.pathsep.join(cp_parts)
        src_path = os.pathsep.join([
            str(app_path / "src" / "main" / "java"),
            str(app_path / "src" / "test" / "java"),
        ])
        rel = p_file_path.resolve().relative_to(app_path.resolve()).as_posix()
        base_cmd = (f'-encoding UTF-8 -d "{out_dir}" -sourcepath "{src_path}" '
                    f'-cp "{cp}" "{rel}"')
        release_version = maven_java_release(app_path) if _ALIGN_MAVEN_JDK else None
        release = f"--release {release_version} " if release_version else ""
        res_code, res_out = run_java_command(
            f'javac {release}{base_cmd}', path=str(app_path))
        if (_ALIGN_MAVEN_JDK and release_version == "8" and res_code != 0
                and "release" in res_out
                and ("invalid" in res_out.lower() or "无效" in res_out)):
            # 本机 javac 是 JDK 8 时不支持 --release
            res_code, res_out = run_java_command(
                f'javac -source 8 -target 8 {base_cmd}', path=str(app_path))
    if res_code != 0:
        if not classpath:
            # 没有依赖清单时，"程序包 xxx 不存在"是工具的盲区，不是代码的问题
            return False, (f"编译未通过，但本次没有加载依赖（{why}），"
                           f"像'程序包 xxx 不存在'这类错误不算代码问题：\n{_pick_javac_errors(res_out)}")
        return False, f"语法错误: {_pick_javac_errors(res_out)}"
    if not classpath:
        return True, "语法正确（本次未加载依赖，只确认了语法层面）"
    return True, "语法正确"


@register_tool(AGENT_NAME, priority=1)
@tool
def get_project_list(runtime: ToolRuntime) -> str:
    """
    获取该项目中所有文件的列表
    工具优先级：1
    """
    app_type = runtime.state.get("app_type", "flink")
    date = runtime.state.get("date")
    p_name = runtime.state.get("project_name")
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    if app_type == "flink":
        business_dir = _CODE_DIRS.get(app_type, {}).get("business_code", None)
        if not business_dir:
            return f"未知的应用类型: {app_type}, 或者未记录对应的目录"
        business_path = app_path / Path(*business_dir)
        if not business_path.exists():
            return f"业务代码目录不存在: {business_dir}"
        # 返回相对项目目录下的文件，资源文件与测试文件不算业务代码
        business_files = [str(f.relative_to(app_path)) for f in business_path.rglob('*') if f.is_file()]
        # 包括maven文件
        business_files.append("pom.xml")
        return f"该项目下的指定编写目录下有以下文件：\n{'\n'.join(business_files)}"
    elif app_type == "spark":
        spark_dir = _CODE_DIRS.get(app_type, {}).get("business_code", None)
        if not spark_dir:
            return f"未知的应用类型: {app_type}, 或者未记录对应的目录"
        business_path = app_path / Path(*spark_dir)
        if not business_path.exists():
            return f"业务代码目录不存在: {spark_dir}"
        # 返回相对项目目录下的文件，资源文件与测试文件不算业务代码
        business_files = [str(f.relative_to(app_path)) for f in business_path.rglob('*') if f.is_file()]
        return f"该项目下的指定编写目录下有以下文件：\n{'\n'.join(business_files)}"
    return f"未知的应用类型: {app_type}"


@register_tool(AGENT_NAME, priority=1)
@tool
def get_code_file_info(
        runtime: ToolRuntime,
        file_path: str,
        get_type: Literal["class_hierarchy", "vars"] = "class_hierarchy"
) -> str:
    """
    获取指定项目代码文件中的 变量、类、函数、方法等信息
    参数：
        file_path: 相对项目目录，如 src/main/java/com/fm/MyApp.java 或者 src/main/python/xxx.py
        get_type: 获取的类型，class_hierarchy（同时获取函数、类、方法信息） 或 vars（仅获取变量信息）
    注意：
        - 对于 class_hierarchy 中，对于java并没有函数信息，只有类与方法层级结构
        - 对于 vars 中，只能获取类中变量（java专属） 或者 全局变量，无法获取方法或者函数中变量
    返回：
        代码文件中的信息，如类与方法层级结构、变量列表等
    工具优先级：1
    """
    app_type = runtime.state.get("app_type", "flink")
    date = runtime.state.get("date")
    p_name = runtime.state.get("project_name")
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    file_path = _code_file_path(app_path, file_path, app_type)
    if isinstance(file_path, str):
        return file_path
    if not file_path.is_file():
        return _file_missing(file_path, app_path)
    if app_type == "flink":
        analyzer = JavaAnalyzer(str(file_path))
        if get_type == "class_hierarchy":
            return f"该项目中的类与方法层级结构为：\n{analyzer.class_hierarchy()}"
        elif get_type == "vars":
            return f"该项目中的变量为：\n{analyzer.vars()}"
        else:
            return f"未知的获取类型: {get_type}"
    elif app_type == "spark":
        py_analyzer = PyAnalyzer(str(file_path))
        if get_type == "class_hierarchy":
            return f"该项目中的类、方法、函数层级结构为：\n{py_analyzer.class_hierarchy()}"
        elif get_type == "vars":
            class_vars = py_analyzer.vars("类", "纯类内变量")
            global_vars = py_analyzer.vars("模块", "赋值")
            var_list = global_vars + class_vars
            return f"该项目中的变量为：\n{var_list}"
        else:
            return f"未知的获取类型: {get_type}"
    else:
        return f"未知的应用类型: {app_type}"


@register_tool(AGENT_NAME, priority=1)
@tool
def code_read(
        runtime: ToolRuntime,
        file_path: str,
        qualname: str,
        start_line: int | None = None,
        end_line: int | None = None
) -> str:
    """
    获取相对项目目录下指定文件中对应类、方法或函数的代码信息
    参数：
        file_path：相对项目目录，如 src/main/java/com/fm/MyApp.java 或者 src/main/python/xxx.py
        qualname：类、方法或函数的名称，如 com.fm.MyApp 或者 xxx
        start_line：开始行号(默认从1开始)，默认 None（可选）
        end_line：结束行号，默认 None（可选）
    注意：
        - start_line 与 end_line 均为可选参数，若不指定，则表达获取该类、方法或函数的所有代码
        - 当 数据的内容过多时，将会被拦截，并且返回一个带有 note 的提示
        - 与此同时建议一开始不用书写 start_line 与 end_line，而是先获取所有代码，因为你并不确定是否被拦截
    返回：
        相对项目目录下指定文件中对应类、方法或函数的代码信息 ->
            {"qualname", "start_line", "end_line", "code", "file_path", ("overloads" -> 当出现重载时，才会出现)}
            含义分别为：
                - qualname：类、方法或函数的名称
                - start_line：开始行号
                - end_line：结束行号
                - code：代码内容
                - file_path：文件路径
                - overloads：重载信息(python spark 中，该参数不存在)：
                    - signature：重载签名
                    - start_line：重载代码开始行号
                    - end_line：重载代码结束行号
    工具优先级：1
    """
    app_type = runtime.state.get("app_type", "flink")
    date = runtime.state.get("date")
    p_name = runtime.state.get("project_name")
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    p_file_path = _code_file_path(app_path, file_path, app_type)
    if isinstance(p_file_path, str):
        return p_file_path
    if not p_file_path.is_file():
        return _file_missing(p_file_path, app_path)
    if start_line is None and end_line is not None:
        return f"未指定开始行号，无法获取指定行号后的代码"
    if app_type == "flink":
        analyzer = JavaAnalyzer(str(p_file_path))
        got = analyzer.code_by_name(qualname, start_line, end_line)
        if not got:
            return f"未找到类、方法或函数: {qualname}"
        payload = {
            "qualname": got["qualname"], "start_line": got["lineno"],
            "end_line": got["end_lineno"], "code": got["code"],
            "file_path": file_path
        }
        if got.get("overloads"):
            payload["overloads"] = [
                {"signature": o["signature"], "start_line": o["lineno"], "end_line": o["end_lineno"]}
                for o in got["overloads"]]
        return f"相对项目目录：{file_path}中，对于{qualname}的代码信息为：\n{_capped(payload, 'code')}"
    elif app_type == "spark":
        py_analyzer = PyAnalyzer(str(p_file_path))
        got = py_analyzer.code_by_name(qualname)
        if not got:
            return f"未找到类、方法或函数: {qualname}"
        qualname_code = got.get("code")
        if start_line is not None:
            start_idx = got.get("lineno")
            got["lineno"] = start_line
            qualname_code_lines = qualname_code.splitlines()
            if end_line is None:
                end_line = start_line
            got["end_lineno"] = end_line
            end_idx = end_line - start_idx + 1
            start_idx = start_line - start_idx

            qualname_code_lines = qualname_code_lines[start_idx:end_idx]
            got["code"] = "\n".join(qualname_code_lines)

        return f"相对项目目录：{file_path}中，对于{qualname}的代码信息为：\n{_capped(got, 'code')}"
    return f"未知的应用类型: {app_type}"


@register_tool(AGENT_NAME, priority=1)
@tool
def code_file_grep(
        runtime: ToolRuntime,
        file_path: str,
        pattern: str,
        start_line: int | None = None,
        end_line: int | None = None,
        is_regex: bool = False,
        exclude_docstring: bool = False
) -> str:
    """
    对相对项目目录下指定文件中的代码进行 grep 操作
    参数：
        file_path：相对项目目录，如 src/main/java/com/fm/MyApp.java 或者 src/main/python/xxx.py
        pattern：要 grep的模式，如 com.fm.MyApp 或者 xxx
        start_line：开始行号（可选，不填从第 1 行开始）
        end_line：结束行号（可选，不填搜到文件末尾；只填 start_line 不填 end_line 时只看那一行）
        is_regex：是否为正则表达式，默认 False（可选）
        exclude_docstring：是否排除文档字符串，默认 False（可选）
    返回：
        相对项目目录下指定文件中指定的pattern的代码信息 ->
            {"file_path", "pattern", "matched", "hits"}
            含义分别为：
                - file_path：文件路径
                - pattern：要 grep的模式
                - matched：hits的总数
                - hits：匹配到的信息(list)
                    - lineno：起始行号
                    - text：匹配到的文本
                    - scope：匹配到的代码作用域
    工具优先级：1
    """
    app_type = runtime.state.get("app_type", "flink")
    date = runtime.state.get("date")
    p_name = runtime.state.get("project_name")
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    p_file_path = _code_file_path(app_path, file_path, app_type)
    if isinstance(p_file_path, str):
        return p_file_path
    if not p_file_path.is_file():
        return _file_missing(p_file_path, app_path)
    if app_type == "flink":
        analyzer = JavaAnalyzer(str(p_file_path))
    elif app_type == "spark":
        analyzer = PyAnalyzer(str(p_file_path))
    else:
        return f"未知的应用类型: {app_type}"
    hits = analyzer.grep(pattern, start_line, end_line, is_regex, exclude_docstring)
    payload = {"file_path": file_path, "pattern": pattern, "matched": len(hits), "hits": hits}
    return f"相对项目目录：{file_path}中，对于{pattern}的代码信息为：\n{_capped(payload, 'hits')}"


@register_tool(AGENT_NAME, priority=1)
@tool
def write_test_code(
        runtime: ToolRuntime,
        file_path: str,
        test_code: str
) -> str:
    """
    书写测试代码
    参数：
        file_path：相对项目目录，如 src/test/java/com/fm/MyAppTest.java 或者 src/tests/xxx_test.py
        test_code：测试代码
    返回：
        写入是否成功的结果
    注意：
        - Spark 测试代码必须放 src/tests/test_*.py，函数用 test_* 开头；
        - Flink 测试代码必须放 src/test/java，至少包含一个 @Test 方法；
        - Flink 的 fromCollection/fromElements 不能是空集合；空输出场景请用非空输入过滤或测纯函数；
        - 测试在本机用 pytest / javac 跑，用本地数据（master 读环境变量 SPARK_TEST_MASTER），
          不连 YARN/HDFS/网络，小数据、临时目录，结束 stop()；
        - 不要 conftest.py、第三方 pytest 插件，也不要依赖测试执行顺序；
        - 用原生 assert，一个测试只验证一个行为；DataFrame 结果先排序再断言；
        - 需要编译检查时显式调用 verify_test_code，不要在每次写入时重复编译。
    工具优先级：1
    """
    app_type = runtime.state.get("app_type", "flink")
    date = runtime.state.get("date")
    p_name = runtime.state.get("project_name")
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    p_file_path = _code_file_path(app_path, file_path, app_type, "test_code")
    if isinstance(p_file_path, str):
        return p_file_path
    tmp_code = test_code.strip()
    if not tmp_code:
        return "代码不能为空"
    try:
        p_file_path.parent.mkdir(parents=True, exist_ok=True)
        p_file_path.write_text(test_code, encoding='utf-8')
        return f"文件 {file_path} 写入成功"
    except Exception as e:
        logger.error(f"创建目录或者写入文件失败{type(e).__name__}: {e}")
        return f"创建目录或者写入文件失败{type(e).__name__}: {e}"


@register_tool(AGENT_NAME, priority=1)
@tool
def change_test_code(
        runtime: ToolRuntime, new_code: str,
        file_path: str, start_idx: int,
        end_idx: int | None = None
) -> str:
    """
    修改测试代码
    参数：
        new_code：新的测试代码
        file_path：相对项目目录，如 src/test/java/com/fm/MyAppTest.java 或者 src/tests/xxx_test.py
        start_idx：开始行号(默认从1开始)，默认 None
        end_idx：结束行号，默认 None（可选）
    注意：
        - 文件路径不能是绝对路径（因为系统会自动管理文件路径），只能是相对于 项目的路径，如：src/test/java/com/fm/MyAppTest.java 或者 src/tests/xxx_test.py
        - 其中的起始索引与结束索引表示一个区间，起始索引是闭区间, 结束索引是开区间。即：[start_idx, end_idx)
        - 同时，允许出现 new_code 的行数不等于原始代码的行数的情况。即，可比原始区间更长或更短。
        - 若 end_idx 为 None，则默认只替换 start_idx 行的代码
        - 若你不知道对应的区间的代码是怎么样的，你可以先尝试使用 view_test_interval_code 查看对应的代码
    返回：
        修改是否成功的结果
    工具优先级：1
    """
    app_type = runtime.state.get("app_type", "flink")
    date = runtime.state.get("date")
    p_name = runtime.state.get("project_name")
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    p_file_path = _code_file_path(app_path, file_path, app_type, "test_code")
    if isinstance(p_file_path, str):
        return p_file_path
    if not p_file_path.is_file():
        return _file_missing(p_file_path, app_path)
    if start_idx <= 0:
        logger.warning(f"Agent填入了无效的起始索引 start_idx: {start_idx}，已经自动调整为1")
        start_idx = 1
    if (end_idx is None) or (start_idx == end_idx):
        end_idx = start_idx + 1
    if start_idx > end_idx:
        logger.error("不允许出现 start_idx 大于 end_idx 的情况")
        return f"不允许出现 start_idx 大于 end_idx 的情况. start_idx: {start_idx}, end_idx: {end_idx}"
    # 进行备份修改
    back_file_path = p_file_path.with_suffix(".bak")
    shutil.copy2(p_file_path, back_file_path)
    try:
        with open(p_file_path, "r", encoding="utf-8") as f:
            old_lines = f.readlines()
        # 越界 end_idx 会静默截断/污染文件（B1 实测）；直接拒绝并提示看全文
        if end_idx > len(old_lines) + 1:
            os.remove(back_file_path)
            return (f"end_idx={end_idx} 超过文件总行数 {len(old_lines)}；"
                    f"请先用 view_test_interval_code 查看全文，或改用 write_test_code 覆写")
        # 替换代码进行转换为列表
        if isinstance(new_code, str):
            new_lines = new_code.splitlines()
        else:
            new_lines = list(new_code)
        old_lines[start_idx - 1:end_idx - 1] = new_lines
        for i, line in enumerate(old_lines[:-1]):  # 只有文件最后一行可以不带换行
            if not line.endswith(("\n", "\r")):
                old_lines[i] = line + "\n"

        with open(p_file_path, "w", encoding='utf-8', newline='') as f:
            f.writelines(old_lines)
        # 清除备份文件
        os.remove(back_file_path)
        return f"文件 {file_path} 修改成功（当前文件共 {len(old_lines)} 行）"
    except Exception as e:
        logger.error(f"修改文件 {file_path} 失败{type(e).__name__}: {e}")
        # 回滚
        shutil.copy2(back_file_path, p_file_path)
        return f"修改文件 {file_path} 失败{type(e).__name__}: {e}"


@register_tool(AGENT_NAME, priority=1)
@tool
def view_test_interval_code(
        runtime: ToolRuntime, file_path: str,
        start_idx: int, end_idx: int | None = None
) -> str:
    """
    查看测试代码文件指定区间的代码
    参数：
        file_path：相对项目目录，如 src/test/java/com/fm/MyAppTest.java 或者 src/tests/xxx_test.py
        start_idx：开始行号(默认从1开始)，默认 None
        end_idx：结束行号，默认 None（可选）
    返回：
        查看结果
    工具优先级：1
    """
    app_type = runtime.state.get("app_type", "flink")
    date = runtime.state.get("date")
    p_name = runtime.state.get("project_name")
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    p_file_path = _code_file_path(app_path, file_path, app_type, "test_code")
    if isinstance(p_file_path, str):
        return p_file_path
    if not p_file_path.exists():
        return f"文件 {file_path} 不存在"
    if start_idx <= 0:
        logger.warning(f"Agent填入了无效的起始索引 start_idx: {start_idx}，已经自动调整为1")
        start_idx = 1
    if start_idx > end_idx:
        logger.error("不允许出现 start_idx 大于 end_idx 的情况")
        return f"不允许出现 start_idx 大于 end_idx 的情况. start_idx: {start_idx}, end_idx: {end_idx}"
    try:
        print_list = []
        with open(p_file_path, "r", encoding='utf-8', newline='') as f:
            code = f.readlines()
        if start_idx > len(code):
            return f"start_idx={start_idx} 超过文件总行数 {len(code)}；文件共 {len(code)} 行"
        if end_idx is None:
            end_idx = len(code) + 1
        for i, line in enumerate(code, start=1):
            # 筛选出 [start_idx - 1:end_idx - 1] 区间的行
            if start_idx <= i < end_idx:
                print_list.append(f"{i}\t{line}")
        return (f"文件总行数：{len(code)}\n"
                f"区间[{start_idx}, {end_idx})的代码如下：\n{'\r'.join(print_list)}")
    except Exception as e:
        logger.error(f"查看文件 {file_path} 失败{type(e).__name__}: {e}")
        return f"查看文件 {file_path} 失败{type(e).__name__}: {e}"


@register_tool(AGENT_NAME, priority=1)
@tool
def add_test_code(
        runtime: ToolRuntime, file_path: str,
        new_code: str, auto_wrap: bool = True
) -> str:
    """
    在指定的测试代码文件后面进行追加文本
    参数：
        file_path：相对项目目录，如 src/test/java/com/fm/MyAppTest.java 或者 src/tests/xxx_test.py
        new_code：要追加的代码
        auto_wrap：是否自动添加换行符（默认值为True）（这里是指在 new_code 前面加上 \n 换行符）
    注意：
        - 系统会自动处理首行的换行符(防止出现首行在尾行开始)，无需手动添加。即：会在 new_code 前面加上 \n 换行符 -> \n{new_code}
        - 若 auto_wrap 为 False，则不会在 new_code 前面加上 \n 换行符
    返回：
        追加是否成功的结果
    工具优先级：1
    """
    app_type = runtime.state.get("app_type", "flink")
    date = runtime.state.get("date")
    p_name = runtime.state.get("project_name")
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    p_file_path = _code_file_path(app_path, file_path, app_type, "test_code")
    if isinstance(p_file_path, str):
        return p_file_path
    if not p_file_path.exists():
        return f"文件 {file_path} 不存在"
    tmp_code = new_code.strip()
    if not tmp_code:
        return "代码不能为空"
    # 进行备份
    back_file_path = p_file_path.with_suffix(".bak")
    shutil.copy2(p_file_path, back_file_path)
    try:
        with open(p_file_path, "a+", encoding='utf-8', newline='') as f:
            f.write(f"{'\n' if auto_wrap else ''}{new_code}")
        # 清除备份文件
        os.remove(back_file_path)
        with open(p_file_path, "r", encoding="utf-8") as f:
            total_lines = sum(1 for _ in f)
        return f"文件追加成功（当前文件共 {total_lines} 行）"
    except Exception as e:
        logger.error(f"追加文件 失败{type(e).__name__}: {e}")
        # 回滚
        os.replace(back_file_path, p_file_path)
        return f"追加文件 失败{type(e).__name__}: {e}"


@register_tool(AGENT_NAME, priority=1)
@tool
def verify_test_code(
        runtime: ToolRuntime,
        file_path: str
) -> str:
    """
    验证测试代码是否出现语法/编译错误（只编译，不执行测试）
    参数：
        file_path：相对项目目录，如 src/test/java/com/fm/MyAppTest.java 或者 src/tests/xxx_test.py
    注意：
        - 写/改测试的工具不会自动编译；本工具用于在跑测试前显式复查
        - 该工具不能检验业务代码的语法错误，业务代码由开发者负责
    返回：
        验证结果；未通过时给出 javac/compile 的错误位置和原文
    工具优先级：1
    """
    app_type = runtime.state.get("app_type", "flink")
    date = runtime.state.get("date")
    p_name = runtime.state.get("project_name")
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    p_file_path = _code_file_path(app_path, file_path, app_type, "test_code")
    if isinstance(p_file_path, str):
        return p_file_path
    if not p_file_path.exists():
        return f"文件 {file_path} 不存在"
    try:
        code = p_file_path.read_text(encoding="utf-8", errors="replace")
    except Exception as e:  # noqa: BLE001
        return f"读取文件 {file_path} 失败{type(e).__name__}: {e}"
    issues = _lint_test_code(app_type, code)
    if issues:
        return "验证结果：未通过（测试代码问题）\n- " + "\n- ".join(issues)
    ok, detail = _verify_test_file(app_path, app_type, p_file_path)
    return f"验证结果：{'通过' if ok else '未通过'}\n{detail}"


@register_tool(AGENT_NAME, priority=1)
@tool
def list_test_files(runtime: ToolRuntime) -> str:
    """
    列出项目测试目录下的测试文件、行数和测试数量
    参数：无
    返回：
        测试文件列表；多于一个时会提示“一个应用只保留一套测试文件”
    工具优先级：1
    """
    app_type = runtime.state.get("app_type", "flink")
    date = runtime.state.get("date")
    p_name = runtime.state.get("project_name")
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    code_dir = _CODE_DIRS.get(app_type, {}).get("test_code")
    if not code_dir:
        return f"未知的应用类型: {app_type}"
    test_dir = app_path / Path(*code_dir)
    if not test_dir.exists():
        return f"测试目录不存在: {'/'.join(code_dir)}"
    suffix = ".java" if app_type == "flink" else ".py"
    files = sorted(f for f in test_dir.rglob(f"*{suffix}") if f.is_file())
    if not files:
        return f"测试目录 {'/'.join(code_dir)} 下没有 {suffix} 文件"
    lines: list[str] = []
    for f in files:
        try:
            text = f.read_text(encoding="utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            text = ""
        if app_type == "flink":
            count = len(re.findall(r"@Test\b", text))
        else:
            count = len(re.findall(r"^\s*def\s+test_\w+\s*\(", text, re.MULTILINE))
        rel = f.relative_to(app_path).as_posix()
        lines.append(f"{rel}（{len(text.splitlines())} 行，{count} 个测试）")
    note = ""
    if len(files) > 1:
        note = ("\n注意：一个应用只保留一套测试文件；多余的旧测试文件会让编译/收集把无关失败带进来，"
                "请确认后用 delete_test_code 删除。")
    return "测试目录下有以下文件：\n" + "\n".join(lines) + note


@register_tool(AGENT_NAME, priority=1)
@tool
def view_pom_content(runtime: ToolRuntime) -> str:
    """
    系统自动识别该项目中是否含有pom.xml文件，并返回其内容
    参数： 无
    返回：
        pom.xml文件内容（如果有）
    工具优先级：1
    """
    app_type = runtime.state.get("app_type", "flink")
    if app_type != "flink":
        return "当前项目并不属于flink项目，没有pom.xml文件"
    date = runtime.state.get("date")
    p_name = runtime.state.get("project_name")
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    pom_file_path = app_path / "pom.xml"
    if not pom_file_path.exists():
        return "pom.xml文件不存在"
    try:
        return pom_file_path.read_text(encoding="utf-8", errors="replace")
    except Exception as e:  # noqa: BLE001
        logger.error(f"读取pom.xml文件失败{type(e).__name__}: {e}")
        return f"读取pom.xml文件失败{type(e).__name__}: {e}"


@register_tool(AGENT_NAME, priority=1)
@tool
def delete_test_code(runtime: ToolRuntime, file_path: str) -> str:
    """
    删除测试目录下的测试文件（只允许删除测试文件，不能删业务代码）
    参数：
        file_path：相对项目目录，如 src/test/java/com/fm/OldTest.java 或者 src/tests/test_old.py
    返回：
        删除结果
    工具优先级：1
    """
    app_type = runtime.state.get("app_type", "flink")
    date = runtime.state.get("date")
    p_name = runtime.state.get("project_name")
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    p_file_path = _code_file_path(app_path, file_path, app_type, "test_code")
    if isinstance(p_file_path, str):
        return p_file_path
    expect_suffix = ".java" if app_type == "flink" else ".py"
    if p_file_path.suffix.lower() != expect_suffix:
        return f"只能删除 {expect_suffix} 测试文件: {file_path}"
    if not p_file_path.exists():
        return f"文件 {file_path} 不存在"
    try:
        p_file_path.unlink()
        backup = p_file_path.with_suffix(".bak")
        if backup.exists():
            backup.unlink()
        return f"文件 {file_path} 已删除"
    except Exception as e:  # noqa: BLE001
        logger.error(f"删除文件 {file_path} 失败{type(e).__name__}: {e}")
        return f"删除文件 {file_path} 失败{type(e).__name__}: {e}"


def _jsonable(value: Any) -> Any:
    """把 TaskOutcome/路径等转成可 JSON 序列化的结构。"""
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


def _action_payload(update: dict[str, Any], content: str, parent: bool = False) -> str:
    """工具只返回数据，state 更新由工具网关解析后统一应用。"""
    return json.dumps(
        {"update": _jsonable(update), "content": content, "parent": parent},
        ensure_ascii=False, default=str,
    )


def _outcome_text(outcome: Any, limit: int = 20000) -> str:
    status = getattr(outcome, "status", None) or (outcome.get("status") if isinstance(outcome, dict) else None)
    result = getattr(outcome, "result", None)
    if result is None and isinstance(outcome, dict):
        result = outcome.get("result")
    text = json.dumps(_jsonable(result), ensure_ascii=False, default=str)
    return f"状态：{status or 'unknown'}；结果：{text[:limit]}"


@register_tool(AGENT_NAME, priority=8)
@tool
async def run_shallow(
        runtime: ToolRuntime,
        first_timeout: float = 300.0,
        flink_shallow_command: list[BusinessVerifyCommand] | None = None,
        spark_shallow_command: list[SparkTestCommand] | None = None,
) -> str:
    """
    执行浅层验证命令，返回真实测试结果
    工具优先级：8（先于 run_deep；同优先级并行）
    参数：
        first_timeout：浅层命令超时秒数，最大 300
        flink_shallow_command：Flink 浅层命令列表，每项 target_class/target_method/params
        spark_shallow_command：Spark 浅层命令列表，每项 test_file/test_name/master
    返回：
        JSON 字符串，含执行状态和测试结果；本轮 state 由系统更新
    """
    from agent.code_verify_agent import _verify_shallow_execute

    app_type = runtime.state.get("app_type")
    state = dict(runtime.state)
    if app_type == "flink":
        state["shallow_commands"] = _parse_command(flink_shallow_command or [])
    elif app_type == "spark":
        state["shallow_commands"] = _parse_spark_command(spark_shallow_command or [])
    else:
        return f"不支持的应用类型: {app_type}"
    state["shallow_timeout"] = min(float(first_timeout or 300), 300.0)
    try:
        command = await _verify_shallow_execute(state, runtime)
    except Exception as e:  # noqa: BLE001
        logger.exception("run_shallow 执行异常")
        update = {
            "shallow_result": {
                "task_id": f"shallow-error-{uuid.uuid4()}", "origin": {},
                "status": "error", "result": None, "partial": None, "snapshot": None,
                "message": "浅层执行异常", "ver_status": "shallow", "error": repr(e),
            },
            "status": "shallow",
        }
        return _action_payload(update, f"浅层执行异常：{type(e).__name__}: {e}")
    update = dict(command.update or {})
    parent = command.graph is not None
    return _action_payload(update, _outcome_text(update.get("shallow_result")), parent)


@register_tool(AGENT_NAME, priority=9)
@tool
async def run_deep(
        runtime: ToolRuntime,
        code_summary: CodeSummary,
        flink_deep_command: list[DeepVerifyCommand] | None = None,
        spark_deep_command: list[SparkTestCommand] | None = None,
        first_timeout: float = 300.0,
        hard_timeout: float = 600.0,
) -> str:
    """
    执行深层验证命令（人工确认通过后才会走到这里）
    工具优先级：9（最后执行；会先转人工确认）
    参数：
        code_summary：人工审核用的代码/数据摘要
        flink_deep_command：Flink 深层命令列表，每项 target_class/target_method/params
        spark_deep_command：Spark 深层命令列表，每项 test_file/test_name/master
        first_timeout：转后台前的等待秒数，最大 300
        hard_timeout：深层命令总超时秒数，最大 600
    返回：
        JSON 字符串，含执行状态和测试结果；本轮 state 由系统更新
    """
    app_type = runtime.state.get("app_type")
    state = dict(runtime.state)
    if app_type == "flink":
        state["deep_commands"] = _parse_command(flink_deep_command or [])
    elif app_type == "spark":
        state["deep_commands"] = _parse_spark_command(spark_deep_command or [])
    else:
        return f"不支持的应用类型: {app_type}"
    state["deep_timeout"] = min(float(hard_timeout or 600), 600.0)
    state["shallow_timeout"] = min(float(first_timeout or 300), 300.0)
    try:
        command = await _verify_deep_execute(state, runtime)
    except Exception as e:  # noqa: BLE001
        logger.exception("run_deep 执行异常")
        update = {
            "deep_result": {
                "task_id": f"deep-error-{uuid.uuid4()}", "origin": {},
                "status": "error", "result": None, "partial": None, "snapshot": None,
                "message": "深层执行异常", "ver_status": "deep", "error": repr(e),
            },
            "status": "deep",
        }
        return _action_payload(update, f"深层执行异常：{type(e).__name__}: {e}")
    update = dict(command.update or {})
    parent = command.graph is not None
    return _action_payload(update, _outcome_text(update.get("deep_result")), parent)


@register_tool(AGENT_NAME, priority=1)
@tool
async def check_background_progress(runtime: ToolRuntime, sleep_time: int = 5) -> str:
    """
    查看当前后台深层任务的进度；sleep_time 秒后再查，避免直接等后台回流
    工具优先级：1（可与业务工具并行）
    参数：
        sleep_time：等待秒数，0-120
    返回：
        JSON 字符串，含 status/summary/result/updated_at
    """
    from Tasks.background_broker import background_registry

    task_id = runtime.state.get("deep_task_id")
    await asyncio.sleep(max(0, min(int(sleep_time or 0), 120)))
    if not task_id:
        return "当前没有后台任务。"
    record = background_registry.get(str(task_id))
    if record is None:
        return f"未找到后台任务 {task_id}；可能已完成或已被清理。"
    return json.dumps({
        "task_id": record.task_id,
        "status": record.status,
        "summary": record.summary,
        "result": record.result,
        "updated_at": record.updated_at,
    }, ensure_ascii=False)


async def _verify_deep_execute(state: Any, runtime: Any) -> Command:
    """深层命令执行：由 run_deep 工具调用，结果交给工具网关写回 state。"""
    from agent.code_verify_agent import (
        TextBuffer, _run_with_progress, _run_flink_test_command,
        _run_spark_test_command, _test_files_signature, _route_error,
    )

    date = state.get("date")
    p_name = state.get("project_name")
    app_type = state.get("app_type")
    verify_id = state.get("id")
    usage_metadata = state.get("usage_metadata")
    response_metadata = state.get("response_metadata") or {}

    deep_timeout = state.get("deep_timeout") or 600
    shallow_timeout = state.get("shallow_timeout") or 300
    if shallow_timeout > 300:
        shallow_timeout = 300
    if deep_timeout < shallow_timeout:
        shallow_timeout = deep_timeout / 2

    if app_type == "flink":
        deep_commands = state.get("deep_commands", [])
        app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
        deep_signature = _test_files_signature(app_type, app_path)
        buf = TextBuffer()
        deep_task_id = f"deep-{uuid.uuid4()}"
        supervisor = TaskSupervisor(
            task_id=deep_task_id,
            origin={"graph": "_verify", "node": "run_deep",
                    "goto_node": "ver_chat", "thread_id": runtime.context.thread_id,
                    "bg_thread_id": background_thread_id(runtime.context.thread_id, deep_task_id)},
            first_timeout=shallow_timeout,
            action="handoff",
            second_timeout=deep_timeout,
            context={"buffer": buf},
            sink=background_broker,
            status="deep",
        )
        reporter = VerificationReporter(
            thread_id=runtime.context.thread_id,
            stage="deep",
            total=_count_test_cases(deep_commands),
            project_name=p_name or "",
            app_type=app_type,
            prior=state.get("shallow_metrics"),
        )
        reporter.start()
        final_result = await _run_with_progress(
            supervisor,
            _run_flink_test_command(
                deep_commands, path=app_path, buffer=buf, on_event=reporter.result,
            ),
            reporter, buf,
        )
        deep_metrics = reporter.finish(final_result)
        update_state = {
            "deep_result": final_result,
            "deep_metrics": deep_metrics,
            "deep_retry_num": state.get("deep_retry_num") or 0,
            "deep_test_signature": deep_signature,
            "status": "done" if final_result.status == "handed_off" else "deep",
        }
        if final_result.status != "completed":
            update_state["fail_num"] = (state.get("fail_num") or 0) + 1
        if final_result.status == "handed_off":
            background_registry.register(
                task_id=deep_task_id,
                user_id=runtime.context.user_id,
                main_thread_id=runtime.context.thread_id,
                status="running",
                summary=f"深度验证 {app_type}/{p_name}",
            )
        return Command(goto="ver_chat", update={
            **update_state,
            "deep_task_id": deep_task_id,
            "shallow_result": None,
        })

    if app_type == "spark":
        deep_commands = state.get("deep_commands", [])
        app_path = ROOT_BASE_DIR_PATH / "tmp_code" / "spark" / date / p_name
        deep_signature = _test_files_signature(app_type, app_path)
        buf = TextBuffer()
        deep_task_id = f"deep-{uuid.uuid4()}"
        supervisor = TaskSupervisor(
            task_id=deep_task_id,
            origin={"graph": "_verify", "node": "run_deep",
                    "goto_node": "ver_chat", "thread_id": runtime.context.thread_id,
                    "bg_thread_id": background_thread_id(runtime.context.thread_id, deep_task_id)},
            first_timeout=shallow_timeout,
            action="handoff",
            second_timeout=deep_timeout,
            context={"buffer": buf},
            sink=background_broker,
            status="deep",
        )
        reporter = VerificationReporter(
            thread_id=runtime.context.thread_id,
            stage="deep",
            total=_count_spark_test_cases(app_path, deep_commands),
            project_name=p_name or "",
            app_type=app_type,
            prior=state.get("shallow_metrics"),
        )
        reporter.start()
        final_result = await _run_with_progress(
            supervisor,
            _run_spark_test_command(
                deep_commands, path=app_path, buffer=buf, on_event=reporter.result,
            ),
            reporter, buf,
        )
        deep_metrics = reporter.finish(final_result)
        update_state = {
            "deep_result": final_result,
            "deep_metrics": deep_metrics,
            "deep_retry_num": state.get("deep_retry_num") or 0,
            "deep_test_signature": deep_signature,
            "status": "done" if final_result.status == "handed_off" else "deep",
        }
        if final_result.status != "completed":
            update_state["fail_num"] = (state.get("fail_num") or 0) + 1
        if final_result.status == "handed_off":
            background_registry.register(
                task_id=deep_task_id,
                user_id=runtime.context.user_id,
                main_thread_id=runtime.context.thread_id,
                status="running",
                summary=f"深度验证 {app_type}/{p_name}",
            )
        return Command(goto="ver_chat", update={
            **update_state,
            "deep_task_id": deep_task_id,
            "shallow_result": None,
        })

    return _route_error(
        state, runtime, f"不支持的应用类型: {app_type}", from_="run_deep",
        verify_id=verify_id, usage_metadata=usage_metadata,
        response_metadata=response_metadata,
    )
