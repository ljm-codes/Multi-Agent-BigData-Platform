import json
import os
import re
import shutil
import tokenize
from pathlib import Path
from typing import Literal

from langchain_core.tools import tool
from langgraph.prebuilt import ToolRuntime

from Tools.log_settings import LogSetting
from Tools.code_analysis import JavaAnalyzer, PyAnalyzer
from Tools.registry import register_tool
from Tools.run_command import run_mvn_command, run_java_command
from load_config.config import ROOT_BASE_DIR_PATH

AGENT_NAME = "CVA"
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


@register_tool(AGENT_NAME)
@tool
def get_project_list(runtime: ToolRuntime) -> str:
    """
    获取该项目中所有文件的列表
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
        business_files = [str(f.relative_to(app_path)) for f in business_path.iterdir() if f.is_file()]
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
        business_files = [str(f.relative_to(app_path)) for f in business_path.iterdir() if f.is_file()]
        return f"该项目下的指定编写目录下有以下文件：\n{'\n'.join(business_files)}"
    return f"未知的应用类型: {app_type}"


@register_tool(AGENT_NAME)
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
    """
    app_type = runtime.state.get("app_type", "flink")
    date = runtime.state.get("date")
    p_name = runtime.state.get("project_name")
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    file_path = _code_file_path(app_path, file_path, app_type)
    if isinstance(file_path, str):
        return file_path
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


@register_tool(AGENT_NAME)
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
    """
    app_type = runtime.state.get("app_type", "flink")
    date = runtime.state.get("date")
    p_name = runtime.state.get("project_name")
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    p_file_path = _code_file_path(app_path, file_path, app_type)
    if isinstance(p_file_path, str):
        return p_file_path
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


@register_tool(AGENT_NAME)
@tool
def code_file_grep(
        runtime: ToolRuntime,
        file_path: str,
        pattern: str,
        start_line: int = 1,
        end_line: int | None = None,
        is_regex: bool = False,
        exclude_docstring: bool = False
) -> str:
    """
    对相对项目目录下指定文件中的代码进行 grep 操作
    参数：
        file_path：相对项目目录，如 src/main/java/com/fm/MyApp.java 或者 src/main/python/xxx.py
        pattern：要 grep的模式，如 com.fm.MyApp 或者 xxx
        start_line：开始行号(默认从1开始)，默认 1（可选）
        end_line：结束行号，默认 None（可选）
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
    """
    app_type = runtime.state.get("app_type", "flink")
    date = runtime.state.get("date")
    p_name = runtime.state.get("project_name")
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    p_file_path = _code_file_path(app_path, file_path, app_type)
    if isinstance(p_file_path, str):
        return p_file_path
    if app_type == "flink":
        analyzer = JavaAnalyzer(str(p_file_path))
    elif app_type == "spark":
        analyzer = PyAnalyzer(str(p_file_path))
    else:
        return f"未知的应用类型: {app_type}"
    hits = analyzer.grep(pattern, start_line, end_line, is_regex, exclude_docstring)
    payload = {"file_path": file_path, "pattern": pattern, "matched": len(hits), "hits": hits}
    return f"相对项目目录：{file_path}中，对于{pattern}的代码信息为：\n{_capped(payload, 'hits')}"


@register_tool(AGENT_NAME)
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
        - 测试在本机用 pytest 跑，用本地 SparkSession（master 读环境变量 SPARK_TEST_MASTER），
          不连 YARN/HDFS/网络，小数据、临时目录，结束 stop()；
        - 不要 conftest.py、第三方 pytest 插件，也不要依赖测试执行顺序；
        - 用原生 assert，一个测试只验证一个行为；DataFrame 结果先排序再断言。
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


@register_tool(AGENT_NAME)
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
    """
    app_type = runtime.state.get("app_type", "flink")
    date = runtime.state.get("date")
    p_name = runtime.state.get("project_name")
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    p_file_path = _code_file_path(app_path, file_path, app_type, "test_code")
    if isinstance(p_file_path, str):
        return p_file_path
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
        return f"文件 {file_path} 修改成功"
    except Exception as e:
        logger.error(f"修改文件 {file_path} 失败{type(e).__name__}: {e}")
        # 回滚
        shutil.copy2(back_file_path, p_file_path)
        return f"修改文件 {file_path} 失败{type(e).__name__}: {e}"


@register_tool(AGENT_NAME)
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
        if end_idx is None:
            end_idx = len(code) + 1
        for i, line in enumerate(code, start=1):
            # 筛选出 [start_idx - 1:end_idx - 1] 区间的行
            if start_idx <= i < end_idx:
                print_list.append(f"{i}\t{line}")
        return f"区间[{start_idx}, {end_idx})的代码如下：\n{'\r'.join(print_list)}"
    except Exception as e:
        logger.error(f"查看文件 {file_path} 失败{type(e).__name__}: {e}")
        return f"查看文件 {file_path} 失败{type(e).__name__}: {e}"


@register_tool(AGENT_NAME)
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
        return f"文件追加成功"
    except Exception as e:
        logger.error(f"追加文件 失败{type(e).__name__}: {e}")
        # 回滚
        os.replace(back_file_path, p_file_path)
        return f"追加文件 失败{type(e).__name__}: {e}"


def verify_test_code(
        runtime: ToolRuntime,
        file_path: str
) -> str:
    """
    验证测试代码是否出现语法错误（仅仅进行语法检验，不会执行代码）
    参数：
        file_path：相对项目目录，如 src/test/java/com/fm/MyAppTest.java 或者 src/tests/xxx_test.py
    注意：
        - 该工具不能检验业务代码的语法错误，因为在此之前业务代码已经被检验过了
    返回：
        验证结果
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
    if app_type == "flink":
        classpath, why = _javac_classpath(app_path)
        classes_path = app_path / "target" / "classes"
        cp = f"{classes_path}{os.pathsep}{classpath}" if classpath else f"{classes_path}"
        res_code, res_out = run_java_command(
            f'javac -encoding UTF-8 -d "{classes_path}" -sourcepath src/test/java -cp "{cp}" {p_file_path.name}',
            path=f"{p_file_path.parent}"
        )
        if res_code != 0:
            if not classpath:
                # 没有依赖清单时，"程序包 xxx 不存在"是工具的盲区，不是代码的问题，别让 Agent 去改 import
                return (f"编译未通过，但本次没有加载依赖（{why}），"
                        f"像'程序包 xxx 不存在'这类错误不算代码问题：\n{_pick_javac_errors(res_out)}")
            return f"语法错误: {_pick_javac_errors(res_out)}"
        if not classpath:
            return f"语法正确（本次未加载依赖，只确认了语法层面）"
        return f"语法正确"
    elif app_type == "spark":
        try:
            with tokenize.open(p_file_path) as f:
                source = f.read()
        except SyntaxError as e:
            return f"文件读取失败（编码声明有问题）：{e}"
        except Exception as e:
            logger.error(f"读取文件 {file_path} 失败{type(e).__name__}: {e}")
            return f"读取文件 {file_path} 失败{type(e).__name__}: {e}"
        try:
            # 不执行，只编译；用 compile 而不是 ast.parse —— 后者漏掉编译阶段的检查
            compile(source, str(p_file_path), "exec")
        except SyntaxError as e:
            line_text = (e.text or "").rstrip()
            location = f"第 {e.lineno} 行"
            if e.offset:
                location += f" 第 {e.offset} 列"
            return f"语法/编译错误：{location}：{e.msg}\n{' ' * 4}{line_text}"
        except (ValueError, MemoryError, RecursionError) as e:
            return f"编译失败{type(e).__name__}: {e}"
        return "语法/编译检查通过（只代表语法没问题；导入的包是否存在、逻辑对不对，需要真正运行才知道）"
    else:
        return f"未知的应用类型: {app_type}"
