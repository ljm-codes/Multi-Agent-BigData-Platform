"""
代码质检Agent
需要额外编写一个子图
"""
import ast
import asyncio
import hashlib
import json
import os
import posixpath
import re
import shlex
import shutil
import tempfile
import threading
import time
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import cast, TypedDict, NotRequired, Literal, Any
from urllib.parse import quote, urlparse
from xml.etree import ElementTree as ET

from hdfs import InsecureClient
from sqlalchemy.dialects.postgresql import insert as pg_insert
from langchain.agents.middleware import ModelRequest, ModelResponse
from langchain.chat_models import init_chat_model
from langchain_core.messages import HumanMessage, SystemMessage, BaseMessage, AIMessage
from langgraph.constants import START
from langgraph.graph import StateGraph
from langgraph.prebuilt import ToolNode
from langgraph.runtime import Runtime
from langgraph.types import Command, Send, interrupt
from pydantic import BaseModel, Field

from SPO.main_agent_state_model import CodeVerifyAgentInputState, CodeVerifyAgentMainState, DevelopAgentMainState, \
    DevelopGraphErrorState, MainStateContext
from SPO.postgresql_model import AppFileIndex
from Tasks.background_broker import background_broker, background_registry, background_thread_id
from Tasks.verification_registry import VerificationReporter, verification_registry
from Tasks.build_run_custom_tasks import TaskSupervisor
from Tools import code_commit, node_ssh
from Tools.log_settings import LogSetting
from Tools.code_verify_agent_tools import AGENT_NAME as CVA
from Tools.data_analysis_agent_tools import analysis_business_files, submit_redis_key
from Tools.db import AsyncSessionLocal
from Tools.registry import get_tools
from Tools.run_command import arun_mvn_command, arun_python_command, run_mvn_command
from Tools.share_check_tool import redis_con, manager
from Tools.struct_analy import parse_route_message
from Tools.mermaid_check import check_and_repair_mermaid
from load_config.config import config, ROOT_BASE_DIR_PATH, get_model_params
from middleware.code_verify_agent_middleware import PerceptionBackground
from middleware.compose import chain_model_call_wrappers, ComposedModelResult, run_before_model, \
    chain_tool_call_wrappers
from middleware.tool_call_guard import repair_missing_tool_messages
from middleware.tool_trace_middleware import ToolTraceMiddleware

model_config = config.get("model")
_verify_model = init_chat_model(
    model=model_config.get("name"),
    **get_model_params(),
)
_hdfs_conf = (config.get("hadoop") or {}).get("hdfs") or {}
_yarn_conf = (config.get("hadoop") or {}).get("yarn") or {}
_hdfs_webhdfs_url = _hdfs_conf.get("webhdfs", {}).get("url", "node1:9870")
if "://" not in _hdfs_webhdfs_url:
    _hdfs_webhdfs_url = f"http://{_hdfs_webhdfs_url}"
_hdfs_webhdfs_user = _hdfs_conf.get("webhdfs", {}).get("user", "hadoop")
STARTUP_CMD_DIR = _yarn_conf.get("startup_cmd_dir", "/app")
_spark_submit_conf = _yarn_conf.get("spark", {}) or {}
SPARK_SUBMIT_BIN = _spark_submit_conf.get("submit_bin") or "/export/server/spark3/bin/spark-submit"
SPARK_PYTHON = _spark_submit_conf.get("python") or "/export/server/miniconda3/envs/spark_study/bin/python3"
SPARK_ARCHIVE = (_spark_submit_conf.get("archive") or "").strip()
SPARK_DIR = f"{STARTUP_CMD_DIR}/spark"
_agent_conf = config.get("agent") or {}
_daa_conf = _agent_conf.get("DAA") or {}
_spark_test_python = _daa_conf.get("test_python")
if not _spark_test_python:
    _lib = _daa_conf.get("python_lib_dir")
    if _lib:
        _spark_test_python = str(
            Path(_lib).resolve().parent.parent / ("python.exe" if os.name == "nt" else "python")
        )
    else:
        _spark_test_python = "python"
SPARK_TEST_PYTHON = _spark_test_python
SPARK_TEST_MASTER = (_daa_conf.get("spark_test_master") or "local[*]").strip()
PYTEST_XML_DIR = ROOT_BASE_DIR_PATH / "tmp" / "pytest"
logger = LogSetting.create(__name__)

with open(Path(__file__).parent / "prompt" / "code_verify_agent.md", "r", encoding="utf-8") as f:
    _verify_system_prompt = f.read()

# 集群开关：false 时禁用 HDFS/ssh 等集群依赖（HDFS 客户端置 None）
CONNECT_TO_CLUSTER = bool((config.get("app") or {}).get("connect_to_cluster", True))
_hdfs_client = None if not CONNECT_TO_CLUSTER else InsecureClient(_hdfs_webhdfs_url, user=_hdfs_webhdfs_user)

code_verify_agent_tools = get_tools(CVA)
verify_agent_middleware = [
    PerceptionBackground(background_broker),
    ToolTraceMiddleware("code_verify"),
]
verify_agent_chain = chain_model_call_wrappers(verify_agent_middleware)


class BusinessVerifyCommand(TypedDict):
    target_class: str
    # 模型经常把“可选”写成显式 null；允许 None，_parse_command 会按“整类”处理
    target_method: NotRequired[str | None]
    params: NotRequired[dict[str, str]]


class DeepVerifyCommand(BusinessVerifyCommand):
    pass


class SparkTestCommand(BaseModel):
    test_file: str = Field(
        description="相对项目的 pytest 文件，必须在 src/tests 下，如 src/tests/test_analysis.py；"
                    "文件名用 test_*.py，函数用 test_*，一个函数只验证一个行为"
    )
    test_name: str | None = Field(
        default=None,
        description="pytest 节点名/测试函数名，如 test_count；不填则跑整个文件"
    )
    master: str | None = Field(
        default=None,
        description="该测试的 Spark master，如 local[2]；不填用配置默认值"
    )


class CodeSnippet(BaseModel):
    file_path: str = Field(default="", description="代码片段所在相对项目文件路径，"
                                                   "如：src/main/java/com/example/MyClass.java "
                                                   "或 src/main/python/my_code.py")
    start_line: int = Field(default=0, description="片段起始行号")
    end_line: int = Field(default=0, description="片段结束行号")
    priority: Literal["high", "medium", "low"] = Field(default="medium", description="审查优先级")
    snippet: str = Field(default="", description="代码片段内容")
    note: str = Field(default="", description="代码片段的备注(可选)")


class CodeSummary(BaseModel):
    data_result: str = Field(default="", description="数据验证结果，轻量、突出异常值")
    mermaid_flowgraph: str = Field(
        default="",
        description="Mermaid 语法的文字版流程图；第一行 graph TD/flowchart TD；"
                    "一行只写一个节点/一条边；标签含空格、括号、加号时用双引号，"
                    "如 A[\"transformDataFrame S + lit(P)\"]；不要代码围栏",
    )
    snippets: list[CodeSnippet] = Field(default_factory=list, description="按优先级从高到低排序的代码片段")


class FlinkVerifyCommand(BaseModel):
    flink_shallow_command: list[BusinessVerifyCommand] = Field(
        default_factory=list,
        description="Flink 浅层(业务结果验证)命令列表，app_type=flink 时填这里。"
                    "每项填 target_class（必填）、target_method（可选）、params（可选）；"
                    "系统会拼成 mvn test -Dtest=... 执行。"
    )
    flink_deep_command: list[DeepVerifyCommand] = Field(
        default_factory=list,
        description="Flink 深层(检测代码是否抗造)命令列表，app_type=flink 时填这里；字段同浅层。"
    )


class SparkVerifyCommand(BaseModel):
    spark_shallow_command: list[SparkTestCommand] = Field(
        default_factory=list,
        description="Spark 浅层(业务结果验证) pytest 命令列表，app_type=spark 时填这里。"
                    "测试在本机用 3.8 + pyspark 执行，不提交集群；"
                    "测试代码放 src/tests，用本地 SparkSession，不连 YARN/HDFS/网络，"
                    "小数据、临时目录，测试结束 stop()。"
    )
    spark_deep_command: list[SparkTestCommand] = Field(
        default_factory=list,
        description="Spark 深层(检测代码是否抗造) pytest 命令列表，app_type=spark 时填这里；"
                    "字段同浅层；深浅由你填在哪个字段决定，不需要 pytest marker。"
    )


class VerifyAgentOutClass(BaseModel):
    goto: Literal["shallow", "deep", "end"] = Field(
        default="shallow", description="这个选项决定了系统下一步的操作。"
                                       "若为 shallow 则先执行浅层测试（验证结果会最终流向你）；"
                                       "若为 deep 则执行深层测试（验证结果会最终流向你）；"
                                       "若为 end 则结束流程，并且你能将你说的话写到end_chat中，交于上游进行处理"
    )
    flink_command: FlinkVerifyCommand = Field(
        default_factory=FlinkVerifyCommand,
        description="Flink 的浅/深测试命令；app_type=flink 时填这里，app_type=spark 时留空"
    )
    spark_command: SparkVerifyCommand = Field(
        default_factory=SparkVerifyCommand,
        description="Spark 的浅/深 pytest 命令；app_type=spark 时填这里，app_type=flink 时留空"
    )
    first_timeout: float = Field(
        default=300.0,
        description="浅层命令的绝对超时时间（秒）；在深层命令里表示转交后台的时间。"
                    "Flink/Spark 共用；不能超过 300，也不能超过 hard_timeout"
    )
    hard_timeout: float = Field(
        default=600.0,
        description="深层命令的总超时时间（秒）；Flink/Spark 共用；不能超过 600"
    )
    code_summary: CodeSummary = Field(default_factory=CodeSummary,
                                      description="代码质检结果，当 goto 为 deep 时生效，第一次没有结果时不用填写。"
                                                  "该字段的含义在于将前面的浅层测试结果与对应的数据进行汇总，方便人工进行观看。他会先进行人工审核再进行深度测试")
    end_chat: str = Field(
        default="",
        description="验证结束后，你想说些什么（不用提问是否需要提交，系统会自动处理，你只要说明情况就行了），"
                    "注意：当 goto 为 end 时生效。若本轮是异常收尾（无法写测试/命令或工具异常/无法继续），"
                    "必须以「【CVA 异常】」开头说明原因和证据，不要输出自由文本或把错误放 content。")


class TextBuffer:
    __slots__ = ("_parts", "_length")

    def __init__(self):
        self._parts: list[str] = []
        self._length: int = 0

    def append(self, text: str):
        self._parts.append(text)
        self._length += len(text)

    def snapshot(self) -> str:
        return "".join(self._parts)

    # 兼容 TaskSupervisor._extract_snapshot 的 fallback 探测
    @property
    def partial(self) -> str:
        return self.snapshot()


# ---------------  utils ---------------
def _short_model_text(message: Any, limit: int = 500) -> str:
    content = getattr(message, "content", message)
    if isinstance(content, str):
        text = content
    else:
        try:
            text = json.dumps(content, ensure_ascii=False, default=str)
        except Exception:
            text = str(content)
    return text[:limit].replace("\n", " ")


def _parse_verify_agent_message(ver_res: BaseMessage) -> VerifyAgentOutClass | None:
    return parse_route_message(ver_res, VerifyAgentOutClass)


def _repair_invalid_tool_calls(ver_res: AIMessage) -> AIMessage | None:
    """模型偶尔把 JSON 里的 true/false/null 写成 Python 的 True/False/None。

    LangChain 会把这些调用放进 invalid_tool_calls，tool_calls 为空。
    这里尽量按 Python 字面量解析参数，转成合法工具调用后继续跑。
    """
    invalid_calls = getattr(ver_res, "invalid_tool_calls", None) or []
    repaired_calls = []
    for call in invalid_calls:
        raw_args = call.get("args")
        if isinstance(raw_args, str):
            try:
                args = json.loads(raw_args)
            except Exception:  # noqa: BLE001
                try:
                    args = ast.literal_eval(raw_args)
                except Exception:  # noqa: BLE001
                    continue
        else:
            args = raw_args
        if not isinstance(args, dict):
            continue
        repaired_calls.append({
            "name": call.get("name"),
            "args": args,
            "id": call.get("id"),
            "type": "tool_call",
        })
    if not repaired_calls:
        return None
    return ver_res.model_copy(update={
        "tool_calls": [*(getattr(ver_res, "tool_calls", None) or []), *repaired_calls],
        "invalid_tool_calls": [],
    })


def _parse_command(command_list: list[dict]) -> list[str]:
    commands: list[str] = []  # 含参数的方法 -> 每个方法独立成命令
    only_class: list[str] = []  # 无方法 -> mvn test -Dtest={class}
    plain_methods: dict[str, list[str]] = {}  # 无参数的方法 -> 同类的多个方法聚合

    for command in command_list:
        target_class = command.get("target_class")
        target_method = command.get("target_method")
        params = command.get("params")

        if not target_method:
            # 无方法时 params 失效，表示该类的所有方法
            only_class.append(target_class)
        elif not params:
            # 无参数 -> 聚合到同类的多方法
            plain_methods.setdefault(target_class, []).append(target_method)
        else:
            # 有参数 -> 编译成逐个命令：mvn test -Dtest={class}#{method} -D{k}={v}
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


def _emit_progress(on_event: Any, payload: dict[str, Any]) -> None:
    """进度回调不能影响测试执行本身。"""
    if not on_event:
        return
    try:
        on_event(payload)
    except Exception:  # noqa: BLE001
        logger.exception("验证进度回调失败")


async def _run_with_progress(
        supervisor: Any,
        coro: Any,
        reporter: VerificationReporter,
        buffer: Any,
        interval: float = 5.0,
) -> Any:
    """跑 TaskSupervisor，同时每 interval 秒把 buffer 尾部作为心跳推给前端。"""
    task = asyncio.create_task(supervisor.run(coro))

    async def _heartbeat() -> None:
        while True:
            await asyncio.sleep(interval)
            reporter.heartbeat(buffer.snapshot() if buffer else "")

    heartbeat_task = asyncio.create_task(_heartbeat())
    try:
        return await task
    finally:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass


def _parse_surefire_xml(xml_path: Path) -> list[dict]:
    """把一个 TEST-*.xml 展开成逐条测试结果（名字/状态/失败信息）"""
    try:
        root = ET.parse(xml_path).getroot()
    except Exception as e:  # noqa: BLE001
        return [{"status": "error", "report": xml_path.name,
                 "message": f"解析 surefire XML 失败: {type(e).__name__}: {e}"}]
    results: list[dict] = []
    for case in root.iter("testcase"):
        status, message = "passed", ""
        for tag, st in (("failure", "failed"), ("error", "error"), ("skipped", "skipped")):
            node = case.find(tag)
            if node is not None:
                status = st
                message = (node.get("message") or node.text or "").strip()[:1000]
                break
        results.append({
            "name": case.get("name"),
            "classname": case.get("classname"),
            "status": status,
            "message": message,
            "time": case.get("time"),
            "report": xml_path.name,
        })
    return results


def _collect_surefire_results(app_path: Path, since: float) -> list[dict]:
    """只取本次命令之后更新的 surefire 报告，避免上一次命令的失败污染结果"""
    report_dir = app_path / "target" / "surefire-reports"
    if not report_dir.is_dir():
        return []
    results: list[dict] = []
    for xml_path in sorted(report_dir.glob("TEST-*.xml")):
        try:
            if xml_path.stat().st_mtime < since - 1.0:
                continue
        except OSError:
            continue
        results.extend(_parse_surefire_xml(xml_path))
    return results


_COMPILE_ERROR_AREA = re.compile(r"[\\/]src[\\/](test|main)[\\/]")


def _compile_error_area(output: str) -> str:
    """从 mvn 编译输出里判断错在 src/test 还是 src/main"""
    areas = {m.group(1) for m in _COMPILE_ERROR_AREA.finditer(output)}
    if "test" in areas and "main" not in areas:
        return "test"
    if "main" in areas and "test" not in areas:
        return "business"
    if areas == {"test", "main"}:
        return "test_and_business"
    return "unknown"


async def _flink_test_compile_gate(
        app_path: Path, buffer: Any, on_event: Any = None
) -> dict | None:
    """跑 mvn test 前先 test-compile：编译不过就别进测试，把错误直接交给 CVA"""
    command = "mvn -q test-compile"
    gate_buf = TextBuffer()
    res = await arun_mvn_command(command, buffer=gate_buf, path=str(app_path))
    output = gate_buf.snapshot()
    if buffer:
        buffer.append(output)
    returncode = res.get("returncode")
    if returncode == 0:
        return None
    area = _compile_error_area(output)
    if area == "business":
        message = "业务代码编译未通过（CVA 不能改业务代码，请把错误证据交给开发者/人工）"
    elif area == "test":
        message = "测试代码编译未通过（请修改测试代码后重跑）"
    else:
        message = "编译未通过（未能判断是 src/test 还是 src/main）"
    _emit_progress(on_event, {
        "name": command, "test_file": "mvn", "status": "failed",
        "message": f"{message}\n{output[-1000:]}",
    })
    return {
        "command": command,
        "stage": "test-compile",
        "returncode": res.get("returncode"),
        "compile_error_area": area,
        "message": message,
        "output": output[-3000:],
    }


async def _run_flink_test_command(
        commands: list[str], path: str, buffer: Any, on_event: Any = None
) -> list[dict]:
    """先做测试编译闸门，再逐条跑 mvn 命令；返回返回码、输出尾部和 surefire 逐条结果"""
    app_path = Path(path)
    results: list[dict] = []
    gate = await _flink_test_compile_gate(app_path, buffer, on_event)
    if gate is not None:
        return [gate]
    for command in commands:
        cmd_buf = TextBuffer()
        started = time.time()
        res = await arun_mvn_command(command, buffer=cmd_buf, path=path)
        output = cmd_buf.snapshot()
        if buffer:
            buffer.append(output)
        tests = _collect_surefire_results(app_path, started)
        returncode = res.get("returncode")
        failed = [t for t in tests if t.get("status") in ("failed", "error")]
        if tests:
            # 平铺成“一条测试一个结果”：VerificationReporter/build_stage_metrics 按条计数，
            # CVA 也能直接看到 class#method 的失败原文。
            for test in tests:
                test.setdefault("test_file", "mvn")
                test["command"] = command
                test["returncode"] = returncode
            results.extend(tests)
            summary = (f"{len(tests)} 个测试："
                       f"{len(tests) - len(failed)} 通过，{len(failed)} 失败/错误")
        else:
            results.append({
                "name": command,
                "test_file": "mvn",
                "command": command,
                "status": "passed" if returncode == 0 else "error",
                "returncode": returncode,
                "message": ("没有解析到 surefire 报告：检查 -Dtest 的类名/方法名是否真的存在"
                            if returncode == 0 else output[-2000:]),
                "output": output[-2000:],
            })
            summary = output[-1000:]
        detail = ""
        if failed:
            first = failed[0]
            detail = (f"{first.get('classname')}#{first.get('name')}: "
                      f"{(first.get('message') or '')[:300]}")
        _emit_progress(on_event, {
            "name": command,
            "test_file": "mvn",
            "status": "passed" if returncode == 0 and not failed else "failed",
            "message": detail or summary,
        })
    return results


def _parse_spark_command(command_list: list[dict]) -> list[dict]:
    commands: list[dict] = []
    for command in command_list or []:
        test_file = (command.get("test_file") or "").strip().replace("\\", "/")
        if not test_file:
            continue
        commands.append({
            "test_file": test_file,
            "test_name": (command.get("test_name") or "").strip() or None,
            "master": (command.get("master") or "").strip() or None,
        })
    return commands


def _pytest_xml_path(project_name: str, test_file: str) -> Path:
    out_dir = PYTEST_XML_DIR / project_name
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{Path(test_file).stem}-{uuid.uuid4().hex}.xml"


def _parse_pytest_xml(xml_path: Path) -> list[dict]:
    try:
        root = ET.parse(xml_path).getroot()
    except Exception as e:
        return [{"status": "error", "message": f"解析 pytest XML 失败: {type(e).__name__}: {e}"}]
    results: list[dict] = []
    for case in root.iter("testcase"):
        status, message = "passed", ""
        for tag, st in (("failure", "failed"), ("error", "error"), ("skipped", "skipped")):
            node = case.find(tag)
            if node is not None:
                status = st
                message = (node.get("message") or node.text or "").strip()[:1000]
                break
        results.append({
            "name": case.get("name"),
            "classname": case.get("classname"),
            "status": status,
            "message": message,
            "time": case.get("time"),
        })
    return results


def _spark_syntax_error(target: Path) -> str:
    """pytest 之前先做纯语法检查，错误位置比 pytest 的收集堆栈更直观"""
    try:
        source = target.read_text(encoding="utf-8")
    except Exception as e:  # noqa: BLE001
        return f"读取测试文件失败: {type(e).__name__}: {e}"
    try:
        compile(source, str(target), "exec")
    except SyntaxError as e:
        line_text = (e.text or "").rstrip()
        return f"第 {e.lineno} 行：{e.msg}\n{' ' * 4}{line_text}"
    except (ValueError, MemoryError, RecursionError) as e:
        return f"编译失败{type(e).__name__}: {e}"
    return ""


async def _spark_collect_gate(
        target: Path, project_path: Path, env: dict[str, str],
        buffer: Any, on_event: Any = None, test_file: str = "",
) -> dict | None:
    """pytest --collect-only：没有 test_* 或者 import 失败时，不要进入真正的测试执行"""
    command = [SPARK_TEST_PYTHON, "-m", "pytest", str(target), "--collect-only", "-q"]
    gate_buf = TextBuffer()
    try:
        res = await arun_python_command(
            command, path=str(project_path), env=env, buffer=gate_buf)
    except asyncio.CancelledError:
        raise
    except Exception as e:  # noqa: BLE001
        msg = f"启动 pytest --collect-only 失败: {type(e).__name__}: {e}"
        buffer.append(f"[spark-test] {msg}\n")
        return {"test_file": test_file, "status": "error", "stage": "collect", "message": msg}
    output = gate_buf.snapshot()
    if buffer:
        buffer.append(output)
    returncode = res.get("returncode")
    if returncode == 0:
        return None
    if returncode == 5:
        message = "测试文件里没有可收集的 test_* 用例（禁止占位/空测试）"
        status = "no_tests"
    else:
        message = f"pytest 收集失败(rc={returncode})：{output.strip()[-2000:]}"
        status = "error"
    _emit_progress(on_event, {
        "name": test_file, "test_file": test_file,
        "status": status, "message": message,
    })
    return {"test_file": test_file, "status": status, "stage": "collect",
            "returncode": returncode, "message": message}


async def _run_spark_test_command(
        commands: list[dict], path: str | Path, buffer: Any, on_event: Any = None
) -> list[dict]:
    """本机 pytest 执行器；返回按 testcase 展开的结果列表"""
    project_path = Path(path).resolve()
    project_name = project_path.name
    python_dir = (project_path / "src" / "main" / "python").resolve()
    tests_dir = (project_path / "src" / "tests").resolve()
    results: list[dict] = []

    for command in commands:
        test_file = (command.get("test_file") or "").strip()
        target = (project_path / test_file).resolve()
        if (not test_file or not target.is_relative_to(tests_dir)
                or not target.is_file() or target.suffix.lower() != ".py"):
            msg = f"非法测试文件（必须在 src/tests 下且是 .py）: {test_file}"
            buffer.append(f"[spark-test] {msg}\n")
            results.append({"test_file": test_file, "status": "error", "message": msg})
            _emit_progress(on_event, {
                "name": test_file, "test_file": test_file,
                "status": "error", "message": msg,
            })
            continue

        test_name = command.get("test_name")
        node = f"{target}::{test_name}" if test_name else str(target)
        xml_path = _pytest_xml_path(project_name, test_file)
        cmd = [SPARK_TEST_PYTHON, "-m", "pytest", node, "-q", "--junitxml", str(xml_path)]
        env = os.environ.copy()
        # 只保留业务代码目录：主环境的 PYTHONPATH 是 3.12 的包，不能混进 3.8 的 pytest
        env["PYTHONPATH"] = str(python_dir)
        env["PYSPARK_PYTHON"] = SPARK_TEST_PYTHON
        env["PYSPARK_DRIVER_PYTHON"] = SPARK_TEST_PYTHON
        env["SPARK_TEST_MASTER"] = command.get("master") or SPARK_TEST_MASTER

        syntax_error = _spark_syntax_error(target)
        if syntax_error:
            msg = f"测试代码语法错误：{syntax_error}"
            buffer.append(f"[spark-test] {msg}\n")
            results.append({"test_file": test_file, "status": "error",
                            "stage": "syntax", "message": msg})
            _emit_progress(on_event, {
                "name": test_name or test_file, "test_file": test_file,
                "status": "error", "message": msg,
            })
            continue
        gate = await _spark_collect_gate(
            target, project_path, env, buffer, on_event, test_file)
        if gate is not None:
            results.append(gate)
            continue
        buffer.append(f"[spark-test] 运行: {' '.join(cmd)}\n")

        try:
            proc_res = await arun_python_command(cmd, path=str(project_path), env=env, buffer=buffer)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            msg = f"启动 pytest 失败: {type(e).__name__}: {e}"
            buffer.append(f"[spark-test] {msg}\n")
            results.append({"test_file": test_file, "status": "error", "message": msg})
            _emit_progress(on_event, {
                "name": test_name or test_file, "test_file": test_file,
                "status": "error", "message": msg,
            })
            continue

        parsed = _parse_pytest_xml(xml_path)
        if not parsed:
            rc = proc_res.get("returncode")
            msg = ((proc_res.get("output") or "").strip()[-1000:]
                   or f"pytest 返回码 {rc}，没有生成 JUnit XML")
            parsed = [{"test_file": test_file,
                       "status": "no_tests" if rc == 5 else "error",
                       "message": msg}]
        for item in parsed:
            item.setdefault("test_file", test_file)
        results.extend(parsed)
        failed = sum(1 for item in parsed if item.get("status") in ("failed", "error"))
        _emit_progress(on_event, {
            "name": test_name or test_file,
            "test_file": test_file,
            "status": "failed" if failed else "passed",
            "message": f"{failed}/{len(parsed)} 个用例失败" if failed else "",
        })
        buffer.append(f"[spark-test] 完成 {test_file}: {len(parsed)} 条结果\n")

    return results


_CODE_COMMIT_CONF = ((config.get("code") or {}).get("commit") or {})
_COMMIT_TASKS: set[asyncio.Task] = set()


def _commit_source_conf() -> tuple[str, str]:
    """读取 code.commit，返回 (source_type, root)；缺配置时抛错"""
    source_type = str(_CODE_COMMIT_CONF.get("type") or "").strip().lower()
    if source_type not in ("hdfs", "local", "ssh", "git"):
        raise ValueError(f"code.commit.type 只能是 hdfs/local/ssh/git，当前是 {source_type!r}")
    root = str(_CODE_COMMIT_CONF.get("path") or "").strip()
    if not root:
        raise ValueError("code.commit.path 未配置")
    if source_type in ("local", "ssh") and "://" in root:
        raise ValueError(f"code.commit.type={source_type} 时 path 必须是目录，不能是 URI: {root!r}")
    if source_type == "hdfs" and "://" in root and not root.startswith("hdfs://"):
        raise ValueError(f"code.commit.type=hdfs 时 path 必须是 HDFS 路径: {root!r}")
    if source_type == "ssh" and not str((_CODE_COMMIT_CONF.get("ssh") or {}).get("ip") or "").strip():
        raise ValueError("code.commit.ssh.ip 未配置")
    if source_type == "git":
        if not str((_CODE_COMMIT_CONF.get("git") or {}).get("git_repo") or "").strip():
            raise ValueError("code.commit.git.git_repo 未配置")
        if not os.getenv("GIT_API", "").strip():
            raise ValueError("GIT_API 未配置")
    return source_type, root


def _commit_rel_dir(app_type: str, date: str, app_name: str) -> str:
    return f"{app_type}/{date}/{app_name}"


def _commit_public_dir(source_type: str, root: str, rel: str) -> str:
    """落库用的目录：hdfs 保留 URI，local 转绝对路径，ssh 保留远端路径"""
    if source_type == "hdfs":
        return f"{root.rstrip('/')}/{rel}"
    if source_type == "local":
        root_path = Path(root).expanduser()
        if not root_path.is_absolute():
            root_path = ROOT_BASE_DIR_PATH / root_path
        return str(root_path / Path(rel))
    if source_type == "ssh":
        return posixpath.join(root.rstrip("/"), rel)
    return f"/{rel.lstrip('/')}"


def _upload_commit_hdfs(root: str, rel: str, files: list[tuple[Path, str]]) -> str:
    if _hdfs_client is None:
        raise RuntimeError("CONNECT_TO_CLUSTER=false，HDFS 提交存储不可用")
    root_path = urlparse(root).path if "://" in root else root
    if not root_path:
        raise ValueError(f"无法从 code.commit.path 解析出 HDFS 路径: {root!r}")
    remote_dir = posixpath.join(root_path.rstrip("/"), rel)
    _hdfs_ensure_dir(remote_dir)
    for local, sub in files:
        dest = posixpath.join(remote_dir, sub)
        parent = posixpath.dirname(dest)
        if parent:
            _hdfs_ensure_dir(parent)
        _hdfs_client.upload(dest, str(local), overwrite=True)
    return f"{root.rstrip('/')}/{rel}"


def _upload_commit_local(root: str, rel: str, files: list[tuple[Path, str]]) -> str:
    root_path = Path(root).expanduser()
    if not root_path.is_absolute():
        root_path = ROOT_BASE_DIR_PATH / root_path
    target = root_path / Path(rel)
    target.mkdir(parents=True, exist_ok=True)
    for local, sub in files:
        dest = target / Path(sub)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local, dest)
    return str(target)


def _upload_commit_ssh(root: str, rel: str, files: list[tuple[Path, str]]) -> str:
    host, user, port, password = code_commit.ssh_endpoint()
    remote_dir = posixpath.join(root.rstrip("/"), rel)
    # 复用 node_ssh 的连接池：SFTP 通道用完就关，ssh 连接一直留着
    with node_ssh.sftp(host, user=user, port=port, password=password) as sftp:
        node_ssh.ensure_remote_dirs(sftp, remote_dir)
        for local, sub in files:
            dest = posixpath.join(remote_dir, sub)
            node_ssh.ensure_remote_dirs(sftp, posixpath.dirname(dest))
            sftp.put(str(local), dest)
    return remote_dir


def _upload_commit_git(root: str, rel: str, files: list[tuple[Path, str]]) -> str:
    return code_commit.upload(f"/{rel.lstrip('/')}", files, f"submit {rel}")


async def _save_app_file_idx(
        *, app_name: str, app_type: Literal["spark", "flink"],
        source_type: str, source_dir: str, commit_cmd: str,
) -> None:
    """upsert code.app_file_idx；更新时刷新 update_time"""
    now = datetime.now(timezone.utc)
    async with AsyncSessionLocal() as session:
        stmt = pg_insert(AppFileIndex).values(
            app_type=app_type.upper(),
            app_name=app_name,
            source_type=source_type,
            source_dir=source_dir,
            commit_cmd=commit_cmd,
            create_time=now,
            update_time=now,
        ).on_conflict_do_update(
            index_elements=["app_type", "app_name"],
            set_={
                "source_type": source_type,
                "source_dir": source_dir,
                "commit_cmd": commit_cmd,
                "update_time": now,
            },
        )
        await session.execute(stmt)
        await session.commit()


# 源码目录不传打包/编译产物：运维侧读的、改的都是源码
_SOURCE_SKIP_DIRS = {"target", "__pycache__", ".git", ".idea", ".venv", "venv",
                     "node_modules", "logs"}
_SOURCE_SKIP_SUFFIX = {".class", ".jar", ".pyc", ".pyo", ".log", ".bak", ".cmd"}


def _source_manifest(project_dir: Path) -> list[tuple[Path, str]]:
    """项目目录 -> [(本地文件, 项目内相对路径)]，跳过 target/__pycache__ 等产物"""
    root = Path(project_dir)
    files: list[tuple[Path, str]] = []
    for f in sorted(root.rglob("*")):
        if not f.is_file():
            continue
        rel = f.relative_to(root)
        if any(p in _SOURCE_SKIP_DIRS or p.startswith(".") for p in rel.parts[:-1]):
            continue
        if f.name.startswith(".") or f.name == "TIME":
            continue
        if f.suffix.lower() in _SOURCE_SKIP_SUFFIX:
            continue
        files.append((f, rel.as_posix()))
    return files


async def _submit_target_code(
        app_name: str, app_type: Literal["spark", "flink"],
        commit_cmd: str, project_dir: Path,
) -> None:
    """
    将项目源码传到 code.commit.path/{app_type}/{date}/{app_name}，
    并把定位信息 upsert 到 code.app_file_idx。

    上传和落库互不影响：上传失败也落库；落库失败不影响已经上传的文件。
    但 CONNECT_TO_CLUSTER=false 时不落库 —— 运维 Agent 不会启动，这条记录没有消费者。
    """
    create_time = datetime.now(timezone.utc)
    date = create_time.date().isoformat()
    try:
        source_type, root = _commit_source_conf()
    except Exception as exc:
        logger.error(f"读取 code.commit 配置失败，跳过代码提交登记: {exc}")
        return

    rel = _commit_rel_dir(app_type, date, app_name)
    source_dir = _commit_public_dir(source_type, root, rel)

    try:
        manifest = _source_manifest(Path(project_dir))
        if not manifest:
            logger.error(f"{project_dir} 里没有可上传的源码文件，跳过代码提交")
            return
        if source_type == "hdfs":
            source_dir = await asyncio.to_thread(_upload_commit_hdfs, root, rel, manifest)
        elif source_type == "local":
            source_dir = await asyncio.to_thread(_upload_commit_local, root, rel, manifest)
        elif source_type == "ssh":
            source_dir = await asyncio.to_thread(_upload_commit_ssh, root, rel, manifest)
        else:
            source_dir = await asyncio.to_thread(_upload_commit_git, root, rel, manifest)
    except Exception as exc:
        logger.error(f"提交源码到 {source_type} 失败（不影响落库）: {type(exc).__name__}: {exc}")

    if not CONNECT_TO_CLUSTER:
        logger.info("CONNECT_TO_CLUSTER=false，运维 Agent 不会运行，跳过 code.app_file_idx 落库")
        return

    try:
        await _save_app_file_idx(
            app_name=app_name, app_type=app_type,
            source_type=source_type, source_dir=source_dir, commit_cmd=commit_cmd,
        )
    except Exception as exc:
        logger.error(f"写入 code.app_file_idx 失败（不影响上传）: {type(exc).__name__}: {exc}")


def _schedule_submit_target_code(*args, **kwargs) -> None:
    """从同步打包函数里调度异步提交任务；没有事件循环时退到后台线程"""
    coro = _submit_target_code(*args, **kwargs)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is None:
        def _runner():
            try:
                asyncio.run(coro)
            except Exception as exc:
                logger.error(f"后台提交代码任务失败: {type(exc).__name__}: {exc}")

        threading.Thread(target=_runner, daemon=True).start()
        return

    task = loop.create_task(coro)
    _COMMIT_TASKS.add(task)

    def _done(t: asyncio.Task):
        _COMMIT_TASKS.discard(t)
        if not t.cancelled() and t.exception() is not None:
            logger.error(f"后台提交代码任务失败: {t.exception()}")

    task.add_done_callback(_done)


def _package_submit_f_command(app_path: str | Path, project_name: str, app_type: str = "flink") -> dict:
    # 打包：CVA 已经跑过测试；提交阶段跳过测试，避免测试代码问题阻塞应用打包
    rc, out = run_mvn_command("mvn clean package -Dmaven.test.skip=true", path=app_path)
    if rc != 0:
        return {"type": "error", "msg": f"mvn clean package 失败(rc={rc})：{out[:2000]}",
                "app_type": app_type}
    # 查找关于 jar 包的位置
    target_path = Path(app_path) / "target"
    jar_file = None
    for f in target_path.glob("*.jar"):
        if f.is_file():
            jar_file = f
            break
    if not jar_file:
        return {"type": "error", "msg": f"未找到 {target_path} 下的 jar 包", "app_type": app_type}

    # 提交 jar 包到指定的hdfs:///app/{app_type}/jar
    target_dir = f"/app/{app_type}/jar"
    cmd_dir = f"/app/{app_type}/cmd"
    prefix = "DDA" if app_type == "flink" else "DAA"
    # 获取注册表信息
    submit_command_str = redis_con.hget(f"{prefix}:{project_name}", "submit_command")
    app_name = redis_con.hget(f"{prefix}:{project_name}", "app_name")
    if isinstance(app_name, bytes):
        app_name = app_name.decode("utf-8")
    if not submit_command_str or not app_name:
        return {"type": "error", "msg": f"{project_name} 未注册提交命令或应用名", "app_type": app_type}
    if isinstance(submit_command_str, bytes):
        submit_command_str = submit_command_str.decode("utf-8")
    submit_command = json.loads(submit_command_str)
    # ynm 会覆盖 YARN 应用名；表、cmd 文件名和 job__restart 都按 YARN 名对齐
    yarn_app_name = str(submit_command.get("ynm") or app_name)
    if yarn_app_name in (".", "..") or "/" in yarn_app_name or "\\" in yarn_app_name:
        return {"type": "error", "msg": f"应用名不能作为目录名: {yarn_app_name!r}", "app_type": app_type}
    # Flink 1.18 run-application 只认 generic 选项 + -D；-yjm/-ytm/-yn/-ys 等短选项不适用
    command_list: list[str] = ["flink", "run-application", "-t", "yarn-application", "-d"]
    app_args: list[str] = []
    for k, v in submit_command.items():
        if k == "c":
            command_list += ["-c", str(v)]
        elif k == "p":
            command_list += ["-p", str(v)]
        elif k == "yjm":
            command_list += ["-D", f"jobmanager.memory.process.size={v}m"]
        elif k == "ytm":
            command_list += ["-D", f"taskmanager.memory.process.size={v}m"]
        elif k == "ys":
            command_list += ["-D", f"taskmanager.numberOfTaskSlots={v}"]
        elif k == "ynm":
            command_list += ["-D", f"yarn.application.name={v}"]
        elif k == "yn":
            # Application Mode 下 TM 数量由 slots/并行度决定，忽略
            continue
        elif k == "app_params":
            for p_k, p_v in (v or {}).items():
                app_args += [str(p_k), str(p_v)]
        else:
            command_list += ["-D", f"{k}={v}"]
    if not submit_command.get("ynm"):
        # 固定 YARN 应用名，保证 code.app_file_idx 的 app_name 与 RM 里的 name 一致
        command_list += ["-D", f"yarn.application.name={yarn_app_name}"]
    command_list += ["-D", f"yarn.provided.lib.dir=hdfs://{target_dir}"]
    command_list.append(f"hdfs://{target_dir}/{jar_file.name}")
    command_list += app_args
    complete_command = shlex.join(command_list)
    # 本地留一份 cmd，再按集群开关决定是否上传
    cmd_local = Path(app_path) / f"{yarn_app_name}.cmd"
    cmd_local.write_text(complete_command, encoding="utf-8")

    # 源码登记到 code.commit + code.app_file_idx（与集群提交互不影响）
    _schedule_submit_target_code(
        app_name=yarn_app_name, app_type="flink", commit_cmd=complete_command,
        project_dir=Path(app_path),
    )
    # 源码登记后，对Redis中的注册信息进行移除
    redis_con.delete(f"{prefix}:{project_name}")

    # 运行
    if not CONNECT_TO_CLUSTER:
        return {"type": "info",
                "msg": (f"集群被明令禁止启用，请手动将对应文件上传到集群并运行，可参考命令：{complete_command}"
                        f"\n项目在于：{app_path}"),
                "app_type": app_type}

    _hdfs_ensure_dir(target_dir)
    _hdfs_ensure_dir(cmd_dir)
    _hdfs_client.upload(target_dir, jar_file, overwrite=True)
    _hdfs_client.upload(cmd_dir, cmd_local, overwrite=True)

    host = urlparse(manager).hostname or ""
    res = node_ssh.run(host, complete_command, timeout=120)
    if not res.ok or (getattr(res, "rc", 0) or 0) != 0:
        return {"type": "error", "msg": f"执行命令 {complete_command} 失败, 错误信息{res.stderr or res.stdout or ""}",
                "app_type": app_type}
    return {"type": "info", "msg": f"执行命令 {complete_command} 成功, 输出信息{res.stdout or res.stderr or ""}",
            "app_type": app_type}


def _hdfs_uri(path: str) -> str:
    """HDFS 绝对路径 -> spark-submit 认识的形式（走默认 FS，不写 authority）"""
    return f"hdfs://{path}" if path.startswith("/") else f"hdfs:///{path}"


def _hdfs_ensure_dir(path: str) -> None:
    if _hdfs_client is None:  # 集群禁用：不触碰 HDFS
        return
    # status 存在时返回 dict（可能是空 dict），不存在时返回 None —— 不能用真假判断
    if _hdfs_client.status(path, strict=False) is None:
        _hdfs_client.makedirs(path)


def _hdfs_upload_file(target: str, local: str | Path) -> str:
    """上传单个文件到 HDFS 的完整目标路径，已存在时覆盖"""
    if _hdfs_client is None:  # 集群禁用：只返回约定路径，不真上传
        return target
    parent = target.rsplit("/", 1)[0]
    if parent:
        _hdfs_ensure_dir(parent)
    _hdfs_client.upload(target, str(local), overwrite=True)
    return target


def _spark_archive_conf() -> str | None:
    """预上传的 spark jars 归档；HDFS 上不存在就不加，免得 spark-submit 起不来"""
    if _hdfs_client is None or not SPARK_ARCHIVE:  # 集群禁用：不加载归档
        return None
    archive_path = urlparse(SPARK_ARCHIVE).path if "://" in SPARK_ARCHIVE else SPARK_ARCHIVE
    if not archive_path:
        return None
    try:
        if _hdfs_client.status(archive_path, strict=False) is not None:
            return f"spark.yarn.archive={SPARK_ARCHIVE}"
    except Exception as e:
        logger.warning(f"检查 spark.yarn.archive 失败，本次不加载: {type(e).__name__}: {e}")
    return None


def _package_submit_s_command(app_path: str | Path, project_name: str, app_type: str = "spark") -> dict:
    """
    打包并提交 python spark 应用

    - 资源目录：/app/spark/{app_name}（按应用名，运维 Agent 能直接对应 YARN 应用）
    - 命令目录：/app/spark/cmd/{app_name}.cmd
    - 辅助 .py：1 个直接传，多个打成 aux.zip，统一走 --py-files
    - 非 .py：上传后走 --files
    """
    try:
        key = submit_redis_key(project_name)
        submit_command_str = redis_con.hget(key, "submit_command")
        app_name = redis_con.hget(key, "app_name")
        if not submit_command_str or not app_name:
            return {"type": "error", "msg": f"{project_name} 未注册提交命令（Redis 键 {key}）",
                    "app_type": app_type}
        if isinstance(app_name, bytes):
            app_name = app_name.decode("utf-8")
        if app_name in (".", "..") or "/" in app_name or "\\" in app_name:
            return {"type": "error", "msg": f"应用名不能作为目录名: {app_name!r}", "app_type": app_type}
        submit_command = json.loads(submit_command_str)

        main_file = submit_command.get("main_file")
        if not main_file:
            return {"type": "error", "msg": "提交信息里缺少 main_file", "app_type": app_type}
        # 统一成 resolved 路径：Windows 下临时目录可能是 8.3 短名，
        # 而 analysis_business_files 返回的是传入前缀拼出来的路径，两边不统一会 relative_to 失败
        app_path = Path(app_path).resolve()
        python_dir = app_path / "src" / "main" / "python"
        main_path = (app_path / main_file).resolve()
        if not main_path.is_relative_to(python_dir) or not main_path.is_file():
            return {"type": "error", "msg": f"主文件不存在或不在 src/main/python 下: {main_file}",
                    "app_type": app_type}
        if main_path.suffix.lower() != ".py":
            return {"type": "error", "msg": f"主文件必须是 .py: {main_file}", "app_type": app_type}

        py_files, other_files = analysis_business_files(app_path)
        aux_py = [f for f in py_files if f.resolve() != main_path]

        resource_dir = f"{SPARK_DIR}/{app_name}"
        cmd_dir = f"{SPARK_DIR}/cmd"
        _hdfs_ensure_dir(resource_dir)
        _hdfs_ensure_dir(cmd_dir)

        # 主文件
        main_hdfs = _hdfs_upload_file(f"{resource_dir}/{main_path.name}", main_path)

        # 辅助 .py：1 个直接传，多个打成一个 zip
        py_files_arg: list[str] = []
        if len(aux_py) == 1:
            rel = aux_py[0].relative_to(python_dir).as_posix()
            aux_hdfs = _hdfs_upload_file(f"{resource_dir}/{rel}", aux_py[0])
            py_files_arg.append(_hdfs_uri(aux_hdfs))
        elif len(aux_py) > 1:
            with tempfile.TemporaryDirectory() as tmp:
                zip_path = Path(tmp) / "aux.zip"
                with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                    for f in aux_py:
                        zf.write(f, f.relative_to(python_dir).as_posix())
                aux_hdfs = _hdfs_upload_file(f"{resource_dir}/aux.zip", zip_path)
            py_files_arg.append(_hdfs_uri(aux_hdfs))

        # 非 .py：业务目录里发现的 + 注册时额外指定的 files
        files_arg: list[str] = []
        uploaded: set[Path] = set()
        for f in other_files:
            rel = f.relative_to(python_dir).as_posix()
            hdfs_path = _hdfs_upload_file(f"{resource_dir}/files/{rel}", f)
            files_arg.append(_hdfs_uri(hdfs_path))
            uploaded.add(f.resolve())
        for rel in submit_command.get("files") or []:
            extra = (app_path / rel).resolve()
            if not extra.is_relative_to(app_path.resolve()) or not extra.is_file():
                return {"type": "error", "msg": f"files 里的文件不存在或在项目外: {rel}",
                        "app_type": app_type}
            if extra.suffix.lower() == ".py":
                return {"type": "error", "msg": f"files 只用于非代码资源，.py 请放到 src/main/python: {rel}",
                        "app_type": app_type}
            if extra in uploaded:
                continue
            hdfs_path = _hdfs_upload_file(f"{resource_dir}/files/extra/{rel}", extra)
            files_arg.append(_hdfs_uri(hdfs_path))
            uploaded.add(extra)

        # 拼 spark-submit 命令
        cmd: list[str] = [
            SPARK_SUBMIT_BIN,
            "--master", "yarn",
            "--deploy-mode", "cluster",
            "--name", app_name,
            "--conf", "spark.yarn.submit.waitAppCompletion=false",
            "--conf", f"spark.yarn.appMasterEnv.PYSPARK_PYTHON={SPARK_PYTHON}",
            "--conf", f"spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON={SPARK_PYTHON}",
            "--conf", f"spark.executorEnv.PYSPARK_PYTHON={SPARK_PYTHON}",
        ]
        if archive_conf := _spark_archive_conf():
            cmd += ["--conf", archive_conf]
        for k, v in (submit_command.get("conf") or {}).items():
            cmd += ["--conf", f"{k}={v}"]
        if submit_command.get("driver_memory"):
            cmd += ["--driver-memory", str(submit_command["driver_memory"])]
        if submit_command.get("executor_memory"):
            cmd += ["--executor-memory", str(submit_command["executor_memory"])]
        if submit_command.get("num_executors") is not None:
            cmd += ["--num-executors", str(submit_command["num_executors"])]
        if submit_command.get("executor_cores") is not None:
            cmd += ["--executor-cores", str(submit_command["executor_cores"])]
        if py_files_arg:
            cmd += ["--py-files", ",".join(py_files_arg)]
        if files_arg:
            cmd += ["--files", ",".join(files_arg)]
        cmd.append(_hdfs_uri(main_hdfs))
        for k, v in (submit_command.get("app_params") or {}).items():
            cmd += [str(k), str(v)]
        complete_command = shlex.join(cmd)

        # 本地留一份 cmd，再上传到 /app/spark/cmd
        cmd_local = app_path / f"{app_name}.cmd"
        cmd_local.write_text(complete_command, encoding="utf-8")
        cmd_hdfs = _hdfs_upload_file(f"{cmd_dir}/{app_name}.cmd", cmd_local)

        # 源码登记到 code.commit + code.app_file_idx（与集群提交互不影响）
        _schedule_submit_target_code(
            app_name=app_name, app_type="spark", commit_cmd=complete_command,
            project_dir=app_path,
        )
        # 源码登记后，对Redis中的注册信息进行移除，避免占用资源
        redis_con.delete(key)

        if not CONNECT_TO_CLUSTER:
            return {"type": "info",
                    "msg": (f"集群被明令禁止启用，请手动将对应文件上传到集群，并运行，可参考命令：{complete_command}"
                            f"\n项目在于：{app_path}"),
                    "app_type": app_type}
        host = urlparse(manager).hostname or ""
        res = node_ssh.run(host, complete_command, timeout=60)
        # node_ssh 的 ok 只代表 ssh 执行完了；命令本身失败要看 rc
        if not res.ok or res.rc != 0:
            detail = res.message or (res.stderr or res.stdout or "").strip()
            return {"type": "error",
                    "msg": (f"执行 spark-submit 失败(ok={res.ok}, rc={res.rc}, {res.error_kind}): "
                            f"{detail}\n命令：{complete_command}"),
                    "app_type": app_type}
        return {"type": "info",
                "msg": (f"spark 应用已提交：app_name={app_name}\n"
                        f"资源目录：hdfs://{resource_dir}\n"
                        f"命令文件：hdfs://{cmd_hdfs}\n"
                        f"输出：{res.stdout or res.stderr}"),
                "app_type": app_type}
    except Exception as e:
        logger.error(f"提交 spark 应用失败{type(e).__name__}: {e}")
        return {"type": "error", "msg": f"提交 spark 应用失败{type(e).__name__}: {e}", "app_type": app_type}


def _splicing_doc(doc_info: dict) -> dict:
    # 主要为 CodeSummary
    doc_type = doc_info.get("type")
    app_type = doc_info.get("app_type")
    if doc_type is None or doc_type != "info":
        return {"type": "error", "msg": f"_splicing_doc函数传入了不该传入的类型：doc_type: {doc_type}，应为 info",
                "app_type": app_type}
    res_lines = []
    data_result = doc_info.get("data_result")
    raw_mermaid = doc_info.get("mermaid_flowgraph") or ""
    mermaid_flowgraph, mermaid_error = check_and_repair_mermaid(raw_mermaid)
    snippets = doc_info.get("snippets", [])

    # 代码片段里的 file_path 是项目相对路径；IDE 链接要补上真实项目根目录
    date = doc_info.get("date")
    project_name = doc_info.get("project_name")
    project_dir = None
    if date and project_name:
        project_dir = (
                ROOT_BASE_DIR_PATH / "tmp_code" / str(app_type) / str(date) / str(project_name)
        ).resolve()

    res_lines.append("## 数据结果\n")
    res_lines.append(data_result or "无" + "\n")
    res_lines.append("## 流程图\n")
    if mermaid_flowgraph:
        res_lines.append(f"```mermaid\n{mermaid_flowgraph}\n```\n")
    elif raw_mermaid:
        res_lines.append("流程图生成失败（已省略）\n")
    else:
        res_lines.append("无结果\n")
    if snippets:
        res_lines.append("## 核心代码片段（按优先级）\n")
        for snippet in snippets:
            file_path = (snippet.get("file_path") or "").strip()
            start_line = snippet.get("start_line")
            end_line = snippet.get("end_line")
            snippet_text = (snippet.get("snippet") or "").strip()

            if file_path:
                res_lines.append(f"### {file_path} : {start_line}-{end_line}\n")
            res_lines.append(f"**优先级**：{snippet.get('priority')}")
            # 模型没给片段内容（或只有空白）时不输出空 ``` 围栏
            if snippet_text:
                lang = "java" if app_type == "flink" else "python"
                res_lines.append(f"```{lang}\n{snippet_text}\n```\n")
            if note := snippet.get("note"):
                res_lines.append(f"> {note}\n")
            if file_path:
                if project_dir:
                    target = (project_dir / file_path).resolve()
                    target_url = quote(target.as_posix(), safe="/:")
                else:
                    # 缺 date/project_name 时保持相对路径，交给前端用 projectDir 拼
                    target_url = quote(file_path, safe="/:")
                # 路径里有空格（如 “pycharm study”），必须编码；# 只留给行号锚点
                res_lines.append(
                    f"[在 IDE 中查看](IDE://{target_url}"
                    f"#L{start_line}-L{end_line})\n")
    return {
        "type": "info",
        "msg": "\n".join(res_lines),
        "app_type": app_type,
        "mermaid_error": mermaid_error,
    }


# --------------- nodes ---------------
def _verify_input_handler(state: CodeVerifyAgentInputState) -> CodeVerifyAgentMainState:
    task_list = state.get("original_task_list") or []
    app_type = state.get("app_type")
    code_source = state.get("source")
    date = state.get("date")
    p_name = state.get("project_name")
    tasks_str = "\n".join(task_list)
    background_msg = state.get("background_msg")
    if background_msg:
        msg = background_msg
    else:
        msg = f"""
        代码编写者：{code_source}，已经完成了对应的{app_type}任务，原始的任务列表如下：
        {tasks_str}
        他最后的回复为：{state.get("source_msg")}
        """
    code_verify_agent_main_state = CodeVerifyAgentMainState(
        messages=[HumanMessage(content=msg)],
        date=date,
        project_name=p_name,
        app_type=app_type,
        source=code_source,
        original_task_list=state.get("original_task_list") or [],
        mermaid_retry_num=0,  # 每次质检只允许重修一次流程图
        is_submit=state.get("is_submit", False),
        is_ops=state.get("is_ops", False),
        status=state.get("status", "preliminary"),
    )
    if background_msg:
        # 后台回流是一次全新判断：清掉上一轮 handed_off 留下的结果和失败计数
        code_verify_agent_main_state["deep_result"] = None
        code_verify_agent_main_state["shallow_result"] = None
        code_verify_agent_main_state["fail_num"] = 0
    if repair_num := state.get("repair_num"):
        code_verify_agent_main_state["repair_num"] = repair_num
    if is_repair := state.get("is_repair"):
        code_verify_agent_main_state["is_repair"] = is_repair
    if deep_task_id := state.get("deep_task_id"):
        code_verify_agent_main_state["deep_task_id"] = deep_task_id
    return code_verify_agent_main_state


async def _verify_agent_chat(state: CodeVerifyAgentMainState, runtime: Runtime) -> list[Command]:
    verify_agent_update = await run_before_model(verify_agent_middleware, state, runtime)
    result_msg = []
    # 浅层结果在 ver_deep_exec 会被清空；只拼进当次 mags 的话，
    # deep 轮就再也看不到浅层测试已经通过，因此这里要同步持久化到 state.messages。
    persist_msg = []
    if sres := state.get("shallow_result"):
        # 正常结束
        if sres.status == "completed":
            content = f"所有命令执行完成，结果为：{json.dumps(sres.result)}"
        elif sres.status == "timeout":
            # 超时信息
            content = f"{sres.message}，终止之前返回的结果为：{json.dumps(sres.partial)}"
        else:
            # 剩余的就是错误了
            content = f"{sres.message}, 具体的错误为：{sres.error}"
        shallow_msg = AIMessage(
            content=content,
            name="shallow_command:result",
            id="shallow_command:result",
        )
        result_msg = [shallow_msg]
        persist_msg.append(shallow_msg)
    if dres := state.get("deep_result"):
        if dres.status in ("completed", "background_completed"):
            content = f"所有命令执行完成，结果为：{json.dumps(dres.result)}"
        elif dres.status == "handed_off":
            content = (f"{dres.message}\n在转交之前运行的结果为："
                       f"{json.dumps(dres.snapshot) if dres.snapshot else '无结果'}")
        elif dres.status in ("timeout", "background_timeout"):
            content = f"{dres.message}，终止之前返回的结果为：{json.dumps(dres.partial)}"
        else:
            content = f"{dres.message}, 具体的错误为：{dres.error}"
        result_msg = [AIMessage(content=content, name="deep_command:result")]

    verify_request = ModelRequest(
        model=_verify_model,
        messages=state.get("messages"),
        system_message=SystemMessage(content=_verify_system_prompt),
        tools=code_verify_agent_tools,
        state=state,  # type: ignore[arg-type]
        runtime=runtime,
    )

    async def verify_execute(ver_request: ModelRequest) -> ModelResponse:
        mags = ([ver_request.system_message]
                if ver_request.system_message else []) + ver_request.messages + result_msg
        # 工具被取消/异常时，历史里会留下没有 ToolMessage 的 tool_call；下一次模型调用会 400
        mags = [*mags, *repair_missing_tool_messages(mags)]
        tools = [VerifyAgentOutClass, *ver_request.tools]
        model_ = ver_request.model.bind_tools(tools)

        async def _invoke(messages: list[BaseMessage]):
            res = await model_.ainvoke(messages, config={"tags": ["agent:code_verify"]})
            if isinstance(res, AIMessage) and getattr(res, "invalid_tool_calls", None):
                repaired = _repair_invalid_tool_calls(res)
                if repaired is not None:
                    res = repaired
            return res

        ver_res = await _invoke(mags)
        ver_out = _parse_verify_agent_message(ver_res)
        # 模型偶尔把“无法继续”的自由文本塞进 content；只重试一次，并明确要求写到 end_chat。
        if ver_out is None and not (
                isinstance(ver_res, AIMessage) and ver_res.tool_calls):
            raw = _short_model_text(ver_res)
            retry_msg = HumanMessage(content=(
                f"上一次输出无法解析为 VerifyAgentOutClass。原始输出：{raw}\n"
                "请立即调用 VerifyAgentOutClass。若无法继续/发生异常，必须设置 goto='end'，"
                "并在 end_chat 中以『【CVA 异常】』开头说明原因；"
                "不要把错误信息写在自由文本 content 里。"
            ))
            ver_res = await _invoke([*mags, retry_msg])
            ver_out = _parse_verify_agent_message(ver_res)
        if ver_out is None:
            # 调用工具时
            if isinstance(ver_res, AIMessage) and ver_res.tool_calls:
                ver_response = ver_res.model_copy(
                    update={"additional_kwargs": {**ver_res.additional_kwargs}}
                )
                return ModelResponse(result=[ver_response])
            # 两次都解析不了：仍然走结构化 execute，goto=end + end_chat，
            # 让 dis_chat 看到“【CVA 异常】”，而不是把它当成 CVA 的验证结论。
            raw = _short_model_text(ver_res)
            fallback_execute = {
                "goto": "end",
                "end_chat": (f"【CVA 异常】结构化输出两次都无法解析；本轮不能视为验证通过。"
                             f"原始输出摘要：{raw}。请按异常处理并告知用户。"),
                "flink_command": {},
                "spark_command": {},
                "first_timeout": 300.0,
                "hard_timeout": 600.0,
                "code_summary": {
                    "data_result": "CVA 结构化输出解析失败",
                    "mermaid_flowgraph": "",
                    "snippets": [],
                },
                "cva_error": True,
            }
            return ModelResponse(result=[AIMessage(
                content="", additional_kwargs={"execute": fallback_execute})])
        execute_dict = ver_out.model_dump()
        out_ai_msg = AIMessage(
            content="",
            additional_kwargs={
                "execute": execute_dict
            }
        )
        return ModelResponse(result=[out_ai_msg])

    if verify_agent_chain is not None:
        verify_result = await verify_agent_chain(verify_request, verify_execute)
    else:
        verify_result = ComposedModelResult(model_response=await verify_execute(verify_request))

    commands: list[Command] = [Command(update={
        "messages": [*persist_msg, *verify_result.model_response.result],
        **verify_agent_update,
    })]
    commands.extend(verify_result.commands)
    return commands


def _route_error(state: CodeVerifyAgentMainState, runtime: Runtime, desc: str,
                 from_: str = "ver_chat") -> Command:
    return Command(
        graph=Command.PARENT,
        goto="work_end",
        update={
            "type": "error",
            "desc": desc,
            "from_": from_,
            "thread_id": runtime.context.thread_id,
            "app_type": state.get("app_type"),
            "date": state.get("date"),
            "project_name": state.get("project_name"),
        }
    )


def _route_error_send(state: CodeVerifyAgentMainState, desc: str, from_: str = "ver_chat") -> Send:
    """条件边不能返回 Command；错误统一交给 ver_error 节点回传父图"""
    return Send("ver_error", {**state, "desc": desc, "from_": from_})


def _verify_error_node(state: dict, runtime: Runtime) -> Command:
    """ver_error 节点：把错误以 Command(graph=PARENT) 交回主图"""
    desc = state.get("desc") or "代码质检失败"
    # 浅层连错被强制终止时，把最后一次失败原因一起交给 work_end
    if shallow := state.get("shallow_result"):
        detail = (getattr(shallow, "message", "") or getattr(shallow, "error", "")
                  or getattr(shallow, "partial", "") or getattr(shallow, "result", ""))
        if detail:
            desc = f"{desc}；浅层最后一次结果：{detail}"
    return Command(
        graph=Command.PARENT,
        goto="work_end",
        update={
            "type": "error",
            "desc": desc,
            "from_": state.get("from_") or "ver_chat",
            "thread_id": runtime.context.thread_id,
            "app_type": state.get("app_type"),
            "date": state.get("date"),
            "project_name": state.get("project_name"),
        },
    )


def _verify_warn_node(state: dict) -> Command:
    """把「状态不匹配」的提示写回消息后重新问模型，不在节点间乱跳"""
    return Command(
        goto="ver_chat",
        update={
            "status": state.get("status", "preliminary"),
            "messages": state.get("messages", []),
        }
    )


def _deep_test_file_problem(app_type: str, app_path: Path, deep_command: list[dict]) -> str:
    """深层命令必须指向已经写好、确实存在的测试类/文件"""
    if not deep_command:
        return ("深层命令为空：请先写好深层测试文件，再把 target_class/target_method "
                "（Flink）或 test_file（Spark）填进 deep_command。")
    problems: list[str] = []
    if app_type == "flink":
        for command in deep_command:
            target_class = (command.get("target_class") or "").strip()
            if not target_class:
                problems.append("flink_deep_command 里有 target_class 为空")
                continue
            rel = "src/test/java/" + target_class.replace(".", "/") + ".java"
            test_path = app_path / rel
            if not test_path.is_file():
                problems.append(f"找不到深层测试类 {target_class}（期望 {rel}）")
                continue
            if "@Test" not in test_path.read_text(encoding="utf-8", errors="replace"):
                problems.append(f"深层测试类 {target_class} 没有 @Test 方法")
    elif app_type == "spark":
        tests_dir = (app_path / "src" / "tests").resolve()
        for command in deep_command:
            test_file = (command.get("test_file") or "").strip().replace("\\", "/")
            test_path = (app_path / test_file).resolve() if test_file else None
            if (test_path is None or not test_path.is_relative_to(tests_dir)
                    or not test_path.is_file()):
                problems.append(f"找不到深层测试文件 {test_file or '(空)'}")
                continue
            if not re.search(r"^\s*def\s+test_\w+\s*\(",
                             test_path.read_text(encoding="utf-8", errors="replace"),
                             re.MULTILINE):
                problems.append(f"深层测试文件 {test_file} 没有 test_* 函数")
    return "；".join(problems)


def _test_files_signature(app_type: str, app_path: Path) -> str:
    """测试目录内容哈希；用来判断深层失败后测试是否真的改过"""
    if app_type == "flink":
        test_dir = app_path / "src" / "test" / "java"
        pattern = "*.java"
    elif app_type == "spark":
        test_dir = app_path / "src" / "tests"
        pattern = "*.py"
    else:
        return ""
    if not test_dir.is_dir():
        return ""
    digest = hashlib.sha256()
    try:
        files = sorted(test_dir.rglob(pattern))
    except OSError:
        return ""
    for file in files:
        if not file.is_file():
            continue
        try:
            digest.update(file.relative_to(app_path).as_posix().encode("utf-8"))
            digest.update(b"\0")
            digest.update(file.read_bytes())
            digest.update(b"\0")
        except OSError:
            continue
    return digest.hexdigest()


def _deep_has_failures(deep: Any) -> bool:
    """上一次深层是否有失败/异常结论；后台仍在跑时不能重试"""
    if deep is None:
        return False
    if isinstance(deep, dict):
        deep_status = deep.get("status")
        deep_result = deep.get("result")
    else:
        deep_status = getattr(deep, "status", None)
        deep_result = getattr(deep, "result", None)
    if deep_status in ("handed_off", "running"):
        return False
    if deep_status in ("timeout", "background_timeout", "error", "background_error"):
        return True
    if deep_status not in ("completed", "background_completed"):
        return True
    items = deep_result if isinstance(deep_result, list) else []
    if not items:
        return True
    for item in items:
        if not isinstance(item, dict):
            return True
        if item.get("stage") in ("test-compile", "collect", "syntax"):
            return True
        if item.get("status") in ("failed", "error", "no_tests"):
            return True
    return False


def _verify_msg_dispatch(state: CodeVerifyAgentMainState, runtime: Runtime) -> Send | Literal["tools"]:
    """中转站"""
    last_ai_msg = None
    for msg in reversed(state["messages"]):
        if isinstance(msg, AIMessage):
            last_ai_msg = msg
            break

    app_type = state.get("app_type")
    date = state.get("date")
    p_name = state.get("project_name")

    if last_ai_msg is None:
        return _route_error_send(state, "无法处理结果返回的消息")
    if last_ai_msg.tool_calls:
        return "tools"
    error_info = last_ai_msg.additional_kwargs.get("error")  # DevelopGraphErrorState
    if error_info:
        return Send(
            "ver_error",
            {**state, "desc": error_info.get("desc"), "from_": error_info.get("from_")},
        )
    execute_info = last_ai_msg.additional_kwargs.get("execute")
    if not isinstance(execute_info, dict):
        return _route_error_send(state, "无法解析执行信息（execute 不是字典）")
    flink_command = execute_info.get("flink_command") or {}
    spark_command = execute_info.get("spark_command") or {}
    first_timeout = execute_info.get("first_timeout") or 300
    hard_timeout = execute_info.get("hard_timeout") or 600
    goto = execute_info.get("goto")

    # CVA 自己解析失败时状态可能还是 preliminary/shallow；不能走 ver_warn 死循环，
    # 必须直接把“【CVA 异常】...”作为 end_chat 交给 ver_end -> dis_chat。
    if execute_info.get("cva_error") or (
            goto == "end" and str(execute_info.get("end_chat") or "").startswith("【CVA 异常】")
    ):
        return Send(
            "ver_end",
            {
                "verify_end_msg": execute_info.get("end_chat"),
                "from_": "ver_dispatch",
                "date": date,
                "project_name": p_name,
                "app_type": app_type,
            },
        )

    deep_result = state.get("deep_result")
    deep_status = (deep_result.get("status") if isinstance(deep_result, dict)
                   else getattr(deep_result, "status", None))
    status = state.get("status", "preliminary")
    if deep_status == "handed_off":
        # 深层还在后台跑：本轮只允许收尾汇报，重跑/提前提交一律拦下
        if goto == "end":
            return Send(
                "ver_end",
                {
                    "verify_end_msg": execute_info.get("end_chat"),
                    "from_": "ver_dispatch",
                    "date": date,
                    "project_name": p_name,
                    "app_type": app_type,
                },
            )
        return Send(
            "ver_warn",
            {
                "status": status,
                "messages": [HumanMessage(
                    content="深层测试已转后台运行，本轮只能用 goto=end 汇报情况，不要重跑测试或提交。")],
            },
        )

    # 浅层失败后允许改测试重跑，所以 shallow 阶段也接受 shallow
    if goto == "shallow" and status in ("preliminary", "shallow"):
        if fail_num := state.get("fail_num"):
            # 浅层连续失败超过 2 次就强制终止，直接交父图 work_end，不再转人工
            if fail_num > 2:
                return _route_error_send(
                    state, f"浅层测试连续失败 {fail_num} 次，超过系统上限，已强制终止")
        if app_type == "flink":
            shallow_commands = _parse_command(flink_command.get("flink_shallow_command") or [])
        elif app_type == "spark":
            shallow_commands = _parse_spark_command(spark_command.get("spark_shallow_command") or [])
        else:
            return _route_error_send(state, f"不支持的应用类型: {app_type}")
        return Send(
            "ver_com_exec",
            {
                **state,
                "shallow_commands": shallow_commands,
                "shallow_timeout": first_timeout,
            },
        )
    elif goto == "deep" and status == "shallow":
        if app_type == "flink":
            deep_raw = flink_command.get("flink_deep_command") or []
            deep_commands = _parse_command(deep_raw)
        elif app_type == "spark":
            deep_raw = spark_command.get("spark_deep_command") or []
            deep_commands = _parse_spark_command(deep_raw)
        else:
            return _route_error_send(state, f"不支持的应用类型: {app_type}")
        app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
        deep_problem = _deep_test_file_problem(app_type, app_path, deep_raw)
        if deep_problem:
            return Send(
                "ver_warn",
                {
                    "status": status,
                    "messages": [HumanMessage(content=(
                        f"深层命令未执行：{deep_problem}\n"
                        "请先写好对应的测试，或修正 deep_command 后重新调用 "
                        "VerifyAgentOutClass(goto='deep')。deep 命令会立即执行。"
                    ))],
                },
            )
        code_summary = {
            "type": "info",
            **(execute_info.get("code_summary") or {}),
            "app_type": app_type,
            "source": state.get("source"),
            "original_task_list": state.get("original_task_list") or [],
            "deep_commands": deep_commands,
            "shallow_timeout": first_timeout,
            "deep_timeout": hard_timeout,
            "date": date,
            "project_name": p_name,
            "mermaid_retry_num": state.get("mermaid_retry_num") or 0,
        }
        if repair_num := state.get("repair_num"):
            code_summary["repair_num"] = repair_num
        return Send(
            "ver_manual",
            code_summary
        )

    elif goto == "deep" and status == "deep":
        # 深层失败后的受控重试：必须改过测试文件、最多 1 次；不跳步、不回退。
        # 重试直接进 ver_deep_exec，不经过 ver_manual：人工确认是独立节点，
        # 只在第一次 shallow→deep 时出现；重试是修测试代码，不是人的决策。
        retry_num = state.get("deep_retry_num") or 0
        if retry_num >= 1:
            return Send(
                "ver_warn",
                {
                    "status": status,
                    "messages": [HumanMessage(content=(
                        "深层重试次数已用尽（最多 1 次）。请调用 VerifyAgentOutClass(goto='end') "
                        "如实汇报失败原因，不要再重跑。"
                    ))],
                },
            )
        if not _deep_has_failures(state.get("deep_result")):
            return Send(
                "ver_warn",
                {
                    "status": status,
                    "messages": [HumanMessage(content=(
                        "上一次深层没有失败用例（或仍在后台运行），不需要重跑；请 goto='end'。"
                    ))],
                },
            )
        app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
        current_sig = _test_files_signature(app_type, app_path)
        prev_sig = state.get("deep_test_signature")
        if not prev_sig or not current_sig or current_sig == prev_sig:
            return Send(
                "ver_warn",
                {
                    "status": status,
                    "messages": [HumanMessage(content=(
                        "测试文件没有变化，不能原样重跑 deep。请先根据 surefire 失败原文"
                        "修改失败用例，再只带着失败方法调用 VerifyAgentOutClass(goto='deep')；"
                        "如果确认是业务代码问题，直接 goto='end' 交人工。"
                    ))],
                },
            )
        if app_type == "flink":
            deep_raw = flink_command.get("flink_deep_command") or []
            deep_commands = _parse_command(deep_raw)
        elif app_type == "spark":
            deep_raw = spark_command.get("spark_deep_command") or []
            deep_commands = _parse_spark_command(deep_raw)
        else:
            return _route_error_send(state, f"不支持的应用类型: {app_type}")
        deep_problem = _deep_test_file_problem(app_type, app_path, deep_raw)
        if deep_problem:
            return Send(
                "ver_warn",
                {
                    "status": status,
                    "messages": [HumanMessage(content=(
                        f"深层重试命令未执行：{deep_problem}\n"
                        "请修正 deep_command 后重新调用 VerifyAgentOutClass(goto='deep')。"
                    ))],
                },
            )
        return Send(
            "ver_deep_exec",
            {
                **state,
                "deep_commands": deep_commands,
                "deep_timeout": hard_timeout,
                "shallow_timeout": first_timeout,
                "deep_retry_num": retry_num + 1,
                "deep_test_signature": current_sig,
                "shallow_result": None,
                "deep_result": None,
            },
        )

    elif goto == "end" and status == "deep":
        is_submit = state.get("is_submit", False)
        submit_info = {
            "verify_end_msg": execute_info.get("end_chat"),
            "date": state.get("date"),
            "project_name": state.get("project_name"),
            "app_type": app_type,
        }
        if is_submit:
            return Send(
                "ver_submit",
                submit_info
            )
        else:
            submit_info["from_"] = "ver_dispatch"
            return Send(
                "ver_end",
                submit_info
            )
    else:
        target_goto = "shallow"
        if status == "shallow":
            target_goto = "deep"
        elif status in ("deep", "done"):
            target_goto = "end"
        return Send(
            "ver_warn",
            {
                "status": status,
                "messages": [HumanMessage(content=f"当前状态: {status}，请先调用{target_goto}")],
            }
        )


async def _verify_shallow_execute(state: CodeVerifyAgentMainState, runtime: Runtime) -> Command:
    """命令执行"""
    date = state.get("date")
    p_name = state.get("project_name")
    app_type = state.get("app_type")
    buf = TextBuffer()
    shallow_timeout = state.get("shallow_timeout") or 300
    if shallow_timeout > 300:
        shallow_timeout = 300

    if app_type == "flink":
        flink_shallow_test_commands = state.get("shallow_commands", [])

        app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
        supervisor = TaskSupervisor(
            task_id=f"shallow-{uuid.uuid4()}",
            origin={"graph": "_verify", "node": "ver_com_exec",
                    "goto_node": "ver_chat", "thread_id": runtime.context.thread_id},
            first_timeout=shallow_timeout,
            action="cancel",
            context={"buffer": buf}
        )
        reporter = VerificationReporter(
            thread_id=runtime.context.thread_id,
            stage="shallow",
            total=len(flink_shallow_test_commands),
            project_name=p_name or "",
            app_type=app_type,
        )
        reporter.start()
        final_result = await _run_with_progress(
            supervisor,
            _run_flink_test_command(
                flink_shallow_test_commands,
                path=app_path,
                buffer=buf,
                on_event=reporter.result,
            ),
            reporter,
            buf,
        )
        shallow_metrics = reporter.finish(final_result)
        update_state = {
            "shallow_result": final_result,
            "shallow_metrics": shallow_metrics,
            "status": "shallow",
        }
        if final_result.status != "completed":
            if fail_num := state.get("fail_num"):
                update_state["fail_num"] = fail_num + 1
            else:
                update_state["fail_num"] = 1
        return Command(
            goto="ver_chat",
            update=update_state
        )

    elif app_type == "spark":
        spark_commands = state.get("shallow_commands", [])
        app_path = ROOT_BASE_DIR_PATH / "tmp_code" / "spark" / date / p_name
        supervisor = TaskSupervisor(
            task_id=f"shallow-{uuid.uuid4()}",
            origin={"graph": "_verify", "node": "ver_com_exec",
                    "goto_node": "ver_chat", "thread_id": runtime.context.thread_id},
            first_timeout=shallow_timeout,
            action="cancel",
            context={"buffer": buf}
        )
        reporter = VerificationReporter(
            thread_id=runtime.context.thread_id,
            stage="shallow",
            total=len(spark_commands),
            project_name=p_name or "",
            app_type=app_type,
        )
        reporter.start()
        final_result = await _run_with_progress(
            supervisor,
            _run_spark_test_command(
                spark_commands,
                path=app_path,
                buffer=buf,
                on_event=reporter.result,
            ),
            reporter,
            buf,
        )
        shallow_metrics = reporter.finish(final_result)
        update_state = {
            "shallow_result": final_result,
            "shallow_metrics": shallow_metrics,
            "status": "shallow",
        }
        if final_result.status != "completed":
            update_state["fail_num"] = (state.get("fail_num") or 0) + 1
        return Command(
            goto="ver_chat",
            update=update_state
        )

    return _route_error(state, runtime, f"不支持的应用类型: {app_type}", from_="ver_com_exec")


def _verify_manual(code_summary: dict, runtime: Runtime) -> Command:
    to_manual_dict = _splicing_doc(code_summary)
    mermaid_error = to_manual_dict.pop("mermaid_error", "")
    retry_num = code_summary.get("mermaid_retry_num") or 0
    if mermaid_error and retry_num < 1:
        # 流程图修不好时先回模型重修一次；错误信息必须一并带给模型
        logger.warning(f"Mermaid 校验失败，回退重修一次：{mermaid_error}")
        return Command(
            goto="ver_chat",
            update={
                "messages": [HumanMessage(content=(
                    "上一次输出的 mermaid_flowgraph 未通过 Mermaid 语法校验，请修正后重新调用 "
                    "VerifyAgentOutClass（goto=deep），其它字段保持不变。\n"
                    f"校验错误：{mermaid_error}\n"
                    f"原始流程图：\n{code_summary.get('mermaid_flowgraph') or ''}"
                ))],
                "mermaid_retry_num": retry_num + 1,
            },
        )
    date = code_summary.get("date")
    p_name = code_summary.get("project_name")
    app_type = code_summary.get("app_type")
    # 人工必须回传：{"pass": true/false, "msg": 人工建议，当pass为false时，填写", "force_terminate": true/false}
    manual_answer = interrupt(to_manual_dict)
    if not isinstance(manual_answer, dict):
        return Command(
            graph=Command.PARENT,
            goto="work_end",
            update={
                "type": "error",
                "desc": "人工回复格式错误，传入的必须是字典格式",
                "from_": "ver_manual",
                "thread_id": runtime.context.thread_id,
                "date": date,
                "project_name": p_name,
                "app_type": app_type,
            }
        )
    if manual_answer.get("force_terminate"):
        return Command(
            graph=Command.PARENT,
            goto="work_end",
            update={
                "type": "info",
                "desc": "人工强制终止任务",
                "from_": "ver_manual",
                "thread_id": runtime.context.thread_id,
                "date": date,
                "project_name": p_name,
                "app_type": app_type,
            }
        )
    is_pass = manual_answer.get("pass")
    if not isinstance(is_pass, bool):
        return Command(
            graph=Command.PARENT,
            goto="work_end",
            update={
                "type": "error",
                "desc": "pass 必须是布尔值 true/false",
                "from_": "ver_manual",
                "thread_id": runtime.context.thread_id,
                "date": date,
                "project_name": p_name,
                "app_type": app_type,
            }
        )
    if is_pass:
        deep_commands = code_summary.get("deep_commands", [])
        deep_timeout = code_summary.get("deep_timeout", 600)
        shallow_timeout = code_summary.get("shallow_timeout", 300)
        return Command(
            goto="ver_deep_exec",
            update={
                "deep_commands": deep_commands,
                "deep_timeout": deep_timeout,
                "shallow_timeout": shallow_timeout,
                "status": "deep",
                "fail_num": 0,  # 由于到达了这，说明浅层已经完成或者失败，并涉及人工，则重置失败次数
            }
        )
    if not is_pass and manual_answer.get("msg"):
        # 谁写的谁改：数据分析的产物不能打回数据开发 Agent
        goto = "data_analysis" if code_summary.get("source") == "data_analysis_agent" else "data_develop"
        original_task_list = code_summary.get("original_task_list") or []
        return Command(
            graph=Command.PARENT,
            goto=goto,
            update={
                "human_suggestion": manual_answer.get("msg"),
                "date": date,
                "project_name": p_name,
                "app_type": app_type,
                "original_task_list": original_task_list,
                "task_list": original_task_list,
                "task_num": len(original_task_list),
                "repair_num": 1 if not code_summary.get("repair_num") else code_summary.get("repair_num") + 1,
                "is_repair": True,
            }
        )
    else:
        return Command(
            graph=Command.PARENT,
            goto="work_end",
            update={
                "type": "error",
                "desc": "填写错误，或者当pass为false时，没有填写建议",
                "from_": "ver_manual",
                "thread_id": runtime.context.thread_id,
                "date": date,
                "project_name": p_name,
                "app_type": app_type,
            }
        )


async def _verify_deep_execute(state: CodeVerifyAgentMainState, runtime: Runtime) -> Command:
    date = state.get("date")
    p_name = state.get("project_name")
    app_type = state.get("app_type")
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
            origin={"graph": "_verify", "node": "ver_deep_exec",
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
            total=len(deep_commands),
            project_name=p_name or "",
            app_type=app_type,
            prior=state.get("shallow_metrics"),
        )
        reporter.start()
        final_result = await _run_with_progress(
            supervisor,
            _run_flink_test_command(
                deep_commands,
                path=app_path,
                buffer=buf,
                on_event=reporter.result,
            ),
            reporter,
            buf,
        )
        deep_metrics = reporter.finish(final_result)
        update_state = {
            "deep_result": final_result,
            "deep_metrics": deep_metrics,
            "deep_retry_num": state.get("deep_retry_num") or 0,
            "deep_test_signature": deep_signature,
            # 转后台时深层还没有结论：本轮只能收尾，等后台回流再 end
            "status": "done" if final_result.status == "handed_off" else "deep",
        }
        if final_result.status != "completed":
            if fail_num := state.get("fail_num"):
                update_state["fail_num"] = fail_num + 1
            else:
                update_state["fail_num"] = 1
        if final_result.status == "handed_off":
            background_registry.register(
                task_id=deep_task_id,
                user_id=runtime.context.user_id,
                main_thread_id=runtime.context.thread_id,
                status="running",
                summary=f"深度验证 {app_type}/{p_name}",
            )
        return Command(
            goto="ver_chat",
            update={
                **update_state,
                "deep_task_id": deep_task_id,
                "shallow_result": None,  # 清空状态，否则被干扰
            }
        )

    elif app_type == "spark":
        deep_commands = state.get("deep_commands", [])
        app_path = ROOT_BASE_DIR_PATH / "tmp_code" / "spark" / date / p_name
        deep_signature = _test_files_signature(app_type, app_path)
        buf = TextBuffer()
        deep_task_id = f"deep-{uuid.uuid4()}"
        supervisor = TaskSupervisor(
            task_id=deep_task_id,
            origin={"graph": "_verify", "node": "ver_deep_exec",
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
            total=len(deep_commands),
            project_name=p_name or "",
            app_type=app_type,
            prior=state.get("shallow_metrics"),
        )
        reporter.start()
        final_result = await _run_with_progress(
            supervisor,
            _run_spark_test_command(
                deep_commands,
                path=app_path,
                buffer=buf,
                on_event=reporter.result,
            ),
            reporter,
            buf,
        )
        deep_metrics = reporter.finish(final_result)
        update_state = {
            "deep_result": final_result,
            "deep_metrics": deep_metrics,
            "deep_retry_num": state.get("deep_retry_num") or 0,
            "deep_test_signature": deep_signature,
            # 转后台时深层还没有结论：本轮只能收尾，等后台回流再 end
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
        return Command(
            goto="ver_chat",
            update={
                **update_state,
                "deep_task_id": deep_task_id,
                "shallow_result": None,  # 清空状态，否则被干扰
            }
        )

    return _route_error(state, runtime, f"不支持的应用类型: {app_type}", from_="ver_deep_exec")


async def _verify_submit(state: dict, runtime: Runtime) -> Command:
    verify_end_msg = state.get("verify_end_msg")
    date = state.get("date")
    # 上游 Send 里两种写法都兼容：老版本叫 p_name，新版本叫 project_name
    p_name = state.get("project_name") or state.get("p_name")
    app_type = state.get("app_type")
    if not date or not p_name or not app_type:
        return Command(
            graph=Command.PARENT,
            goto="work_end",
            update={
                "type": "error",
                "desc": f"提交信息不完整: date={date!r}, project_name={p_name!r}, app_type={app_type!r}",
                "from_": "ver_submit",
                "thread_id": runtime.context.thread_id,
                "date": date,
                "project_name": p_name,
                "app_type": app_type,
            }
        )
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / p_name
    answer = interrupt("请再次确认是否需要自动提交应用到真实环境")  # 人工回传 "yes"/"no" "yes" -> 自动提交 "no" -> 手动提交
    answer_norm = answer.strip().lower() if isinstance(answer, str) else ""
    if answer_norm in {"no", "n"}:
        return Command(
            goto="ver_end",
            update={
                "type": "info",
                "submit_desc": "当再次确认时，人工表示拒绝自动提交，采用手动提交任务",
                "from_": "ver_submit",
                "thread_id": runtime.context.thread_id,
                "date": date,
                "project_name": p_name,
                "app_type": app_type,
                "verify_end_msg": verify_end_msg,
            }
        )
    if answer_norm not in {"yes", "y"}:
        return Command(
            graph=Command.PARENT,
            goto="work_end",
            update={
                "type": "error",
                "desc": f"提交确认只能回传 yes 或 no，收到的是 {answer!r}",
                "from_": "ver_submit",
                "thread_id": runtime.context.thread_id,
                "date": date,
                "project_name": p_name,
                "app_type": app_type,
            }
        )
    try:
        if app_type == "flink":
            # 当前 flink 为 java代码进行编写，因此上传 jar 包到对应的集群即可
            # 当前未对 flink 进行打包，因此需要先打包
            # 打包完后，通过redis注册表注册的命令参数上传jar包，并运行对应的命令
            res = _package_submit_f_command(app_path, p_name, app_type)
        elif app_type == "spark":
            # spark 侧代码在 src/main/python，辅助文件由提交端自动发现/打包
            res = _package_submit_s_command(app_path, p_name, app_type)
        else:
            res = {"type": "error", "msg": f"不支持类型：{app_type}", "app_type": app_type}
    except Exception as e:
        logger.error(f"提交应用失败{type(e).__name__}: {e}")
        res = {"type": "error", "msg": f"提交应用失败{type(e).__name__}: {e}", "app_type": app_type}
    return Command(
        goto="ver_end",
        update={
            "type": res.get("type"),
            "submit_desc": res.get("msg"),
            "from_": "ver_submit",
            "thread_id": runtime.context.thread_id,
            "date": date,
            "project_name": p_name,
            "app_type": app_type,
            "verify_end_msg": verify_end_msg,
        }
    )


def _verify_end(state: dict, runtime: Runtime) -> Command:
    """这里结束验证，将信息传到上游"""
    verify_end_msg = state.get("verify_end_msg", "")
    submit_desc = state.get("submit_desc")
    date = state.get("date")
    p_name = state.get("project_name")
    app_type = state.get("app_type")
    from_ = state.get("from_")
    submit_type = state.get("type")
    if submit_desc:
        submit_msg = f"对于提交返回的结果：提交后返回的类型为{submit_type}, 该类型的详细描述为：{submit_desc}"
    else:
        submit_msg = "作业没有进行提交"
    out_msg = (f"验证完毕，该项目的目录落于{ROOT_BASE_DIR_PATH / 'tmp_code' / app_type / date / p_name} 中。"
               f"\n对于验证Agent它表示：{verify_end_msg}"
               f"\n{submit_msg}")

    verification = verification_registry.get_report(runtime.context.thread_id)
    if verification is None:
        shallow = state.get("shallow_metrics")
        deep = state.get("deep_metrics")
        if shallow or deep:
            verification = {
                "thread_id": runtime.context.thread_id,
                "status": "completed" if deep and deep.get("status") in (
                    "completed", "background_completed") else "running",
                "passed": all(
                    stage.get("failed", 0) == 0
                    for stage in (shallow, deep) if isinstance(stage, dict)
                ),
                "shallow": shallow,
                "deep": deep,
            }

    if state.get("is_ops"):
        # 运维转交：结果直接交父图 work_end，由 dev_handover 写回 AgentRun
        return Command(
            graph=Command.PARENT,
            goto="work_end",
            update={
                "type": "error" if submit_desc and submit_type == "error" else "info",
                "desc": out_msg,
                "from_": from_ or "ver_end",
                "thread_id": runtime.context.thread_id,
                "app_type": app_type,
                "date": date,
                "project_name": p_name,
                **({"verification": verification} if verification else {}),
            },
        )
    return Command(
        graph=Command.PARENT,
        goto="dis_chat",
        update={
            "messages": [AIMessage(content=out_msg, name=f"ver:{from_}")],
            **({"verification": verification} if verification else {}),
        }
    )


# --------------- build graph ---------------
_verify_builder = StateGraph(
    cast("StateT", CodeVerifyAgentMainState),
    context_schema=cast("ContextT", MainStateContext),
    input_schema=cast("InputT", CodeVerifyAgentInputState),
    output_schema=cast("OutputT", DevelopAgentMainState),
)
_verify_builder.add_node("ver_handler", cast("StateNode", _verify_input_handler))
_verify_builder.add_node("ver_chat", cast("StateNode", _verify_agent_chat))
_verify_builder.add_node("ver_error", cast("StateNode", _verify_error_node))
_verify_builder.add_node("ver_warn", cast("StateNode", _verify_warn_node))
_verify_builder.add_node("ver_com_exec", cast("StateNode", _verify_shallow_execute))
_verify_builder.add_node("ver_manual", cast("StateNode", _verify_manual))
_verify_builder.add_node("ver_deep_exec", cast("StateNode", _verify_deep_execute))
_verify_builder.add_node("ver_submit", cast("StateNode", _verify_submit))
_verify_builder.add_node("tools", ToolNode(
    tools=code_verify_agent_tools,
    awrap_tool_call=chain_tool_call_wrappers(verify_agent_middleware)
))
_verify_builder.add_node("ver_end", cast("StateNode", _verify_end))

_verify_builder.add_edge(START, "ver_handler")
_verify_builder.add_edge("ver_handler", "ver_chat")
_verify_builder.add_conditional_edges(
    "ver_chat",
    _verify_msg_dispatch,
    ["ver_com_exec", "ver_manual", "ver_submit", "ver_deep_exec",
     "tools", "ver_error", "ver_warn", "ver_end"]
)
_verify_builder.add_edge("tools", "ver_chat")
verify_agent = _verify_builder.compile()
