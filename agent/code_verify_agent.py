"""
代码质检Agent
需要额外编写一个子图
"""
import asyncio
import json
import os
import shlex
import tempfile
import uuid
import zipfile
from dataclasses import asdict
from pathlib import Path
from typing import cast, TypedDict, NotRequired, Literal, Any
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

from hdfs import InsecureClient
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
from Tasks.background_broker import background_broker, background_registry, background_thread_id
from Tasks.build_run_custom_tasks import TaskSupervisor
from Tools import LogSetting, node_ssh
from Tools.code_verify_agent_tools import AGENT_NAME as CVA
from Tools.data_analysis_agent_tools import analysis_business_files, submit_redis_key
from Tools.registry import get_tools
from Tools.run_command import arun_mvn_command, arun_python_command, run_mvn_command
from Tools.share_check_tool import redis_con, manager
from Tools.struct_analy import parse_route_message
from load_config.config import config, ROOT_BASE_DIR_PATH, get_model_params
from middleware.code_verify_agent_middleware import PerceptionBackground
from middleware.compose import chain_model_call_wrappers, ComposedModelResult, run_before_model, \
    chain_tool_call_wrappers

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

_hdfs_client = InsecureClient(_hdfs_webhdfs_url, user=_hdfs_webhdfs_user)

code_verify_agent_tools = get_tools(CVA)
verify_agent_middleware = [PerceptionBackground(background_broker)]
verify_agent_chain = chain_model_call_wrappers(verify_agent_middleware)


class BusinessVerifyCommand(TypedDict):
    target_class: str
    target_method: NotRequired[str]
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
    mermaid_flowgraph: str = Field(default="", description="Mermaid 语法的文字版流程图")
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
    goto: Literal["execute", "user", "submit"] = Field(
        default="execute", description="这个选项决定了系统下一步的操作。"
                                       "若为 execute 则先执行浅层测试（验证结果会最终流向你），"
                                       "若为 user 则进行人工检验，若为 submit 则提交代码到真实环境（注意：非测试环境）"
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
                                      description="代码质检结果，当 goto 为 user 时生效，第一次没有结果时不用填写")
    submit_chat: str = Field(
        default="",
        description="提交代码到真实环境需要给人工说些什么（不用提问是否需要提交，系统会自动处理，你只要说明情况就行了），注意：当 goto 为 submit 时生效")


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
def _parse_verify_agent_message(ver_res: BaseMessage) -> VerifyAgentOutClass | None:
    return parse_route_message(ver_res, VerifyAgentOutClass)


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


async def _run_flink_test_command(commands: list[str], path: str, buffer: Any) -> list[dict]:
    """逐条跑 mvn 命令，返回每条命令的返回码和输出尾部（原来只写 buffer、不返回结果）"""
    results: list[dict] = []
    for command in commands:
        cmd_buf = TextBuffer()
        res = await arun_mvn_command(command, buffer=cmd_buf, path=path)
        output = cmd_buf.snapshot()
        if buffer:
            buffer.append(output)
        results.append({
            "command": command,
            "returncode": res.get("returncode"),
            "output": output[-2000:],
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


async def _run_spark_test_command(commands: list[dict], path: str | Path, buffer: Any) -> list[dict]:
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
        buffer.append(f"[spark-test] 运行: {' '.join(cmd)}\n")

        try:
            proc_res = await arun_python_command(cmd, path=str(project_path), env=env, buffer=buffer)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            msg = f"启动 pytest 失败: {type(e).__name__}: {e}"
            buffer.append(f"[spark-test] {msg}\n")
            results.append({"test_file": test_file, "status": "error", "message": msg})
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
        buffer.append(f"[spark-test] 完成 {test_file}: {len(parsed)} 条结果\n")

    return results


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
    _hdfs_ensure_dir(target_dir)
    _hdfs_ensure_dir(cmd_dir)
    # 上传 jar 包到 hdfs
    _hdfs_client.upload(target_dir, jar_file, overwrite=True)
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
    command_list += ["-D", f"yarn.provided.lib.dir=hdfs://{target_dir}"]
    command_list.append(f"hdfs://{target_dir}/{jar_file.name}")
    command_list += app_args
    complete_command = shlex.join(command_list)
    with open(app_path / f"{app_name}.cmd", "w") as f:
        f.write(complete_command)
    # 上传至 hdfs
    _hdfs_client.upload(cmd_dir, app_path / f"{app_name}.cmd", overwrite=True)
    # 运行
    from urllib.parse import urlparse
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
    # status 存在时返回 dict（可能是空 dict），不存在时返回 None —— 不能用真假判断
    if _hdfs_client.status(path, strict=False) is None:
        _hdfs_client.makedirs(path)


def _hdfs_upload_file(target: str, local: str | Path) -> str:
    """上传单个文件到 HDFS 的完整目标路径，已存在时覆盖"""
    parent = target.rsplit("/", 1)[0]
    if parent:
        _hdfs_ensure_dir(parent)
    _hdfs_client.upload(target, str(local), overwrite=True)
    return target


def _spark_archive_conf() -> str | None:
    """预上传的 spark jars 归档；HDFS 上不存在就不加，免得 spark-submit 起不来"""
    if not SPARK_ARCHIVE:
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
    mermaid_flowgraph = doc_info.get("mermaid_flowgraph")
    snippets = doc_info.get("snippets", [])

    res_lines.append("## 数据结果\n")
    res_lines.append(data_result or "无" + "\n")
    res_lines.append("## 流程图\n")
    res_lines.append(f"```mermaid\n{mermaid_flowgraph}\n```\n" if mermaid_flowgraph else "无结果\n")
    if snippets:
        res_lines.append("## 核心代码片段（按优先级）\n")
        for snippet in snippets:
            res_lines.append(
                f"### {snippet.get('file_path')} : {snippet.get('start_line')}-{snippet.get('end_line')}\n")
            res_lines.append(f"**优先级**：{snippet.get('priority')}")
            res_lines.append(f"```{'java' if app_type == 'flink' else 'python'}\n{snippet.get('snippet')}\n```\n")
            if note := snippet.get("note"):
                res_lines.append(f"> {note}\n")
            res_lines.append(
                f"[在 IDE 中查看](IDE://{snippet.get('file_path')}"
                f"#L{snippet.get('start_line')}-L{snippet.get('end_line')})\n")
    return {"type": "info", "msg": "\n".join(res_lines), "app_type": app_type}


def _error_dict_to_str(error_dict: dict) -> dict:
    # 主要为 TaskOutcome
    task_type = error_dict.get("type")
    app_type = error_dict.get("app_type")
    if task_type is None or task_type != "error":
        return {"type": "error", "msg": f"_error_dict_to_str函数传入了不该传入的类型：task_type: {task_type}，应为 error",
                "app_type": app_type}
    msg = f"""任务在连续出现了2次错误被强制终止，现在由您确认一遍：
    {error_dict.get('message') or error_dict.get('error') or '未解析'}
    在强制中断前，任务的运行结果为：\n{error_dict.get('partial', '无')}
    请你手动确认该测试是否通过，并进行手动测试。之后若对业务结果不满意，则可以提交对应说明去进行整改。
    """
    return {"type": "error", "msg": msg, "app_type": app_type}


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
    if sres := state.get("shallow_result"):
        # 正常结束
        if sres.status == "completed":
            result_msg = [AIMessage(
                content=f"所有命令执行完成，结果为：{json.dumps(sres.result)}",
                name="shallow_command:result"
            )]
        elif sres.status == "timeout":
            # 超时信息
            result_msg = [AIMessage(
                content=f"{sres.message}，终止之前返回的结果为：{json.dumps(sres.partial)}",
                name="shallow_command:result"
            )]
        else:
            # 剩余的就是错误了
            result_msg = [AIMessage(
                content=f"{sres.message}, 具体的错误为：{sres.error}",
                name="shallow_command:result"
            )]
    if dres := state.get("deep_result"):
        if dres.status == "completed":
            result_msg = [AIMessage(
                content=f"所有命令执行完成，结果为：{json.dumps(dres.result)}",
                name="deep_command:result"
            )]
        elif dres.status == "handed_off":
            result_msg = [AIMessage(
                content=f"{dres.message}\n在转交之前运行的结果为：{json.dumps(dres.snapshot) if dres.snapshot else '无结果'}",
                name="deep_command:result"
            )]
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
        tools = [VerifyAgentOutClass, *ver_request.tools]
        model_ = ver_request.model.bind_tools(tools)

        ver_res = await model_.ainvoke(mags)
        ver_out = _parse_verify_agent_message(ver_res)
        if ver_out is None:
            # 调用工具时
            if isinstance(ver_res, AIMessage) and ver_res.tool_calls:
                ver_response = ver_res.model_copy(
                    update={"additional_kwargs": {**ver_res.additional_kwargs}}
                )
                return ModelResponse(result=[ver_response])
            fallback = AIMessage(
                content="",
                additional_kwargs={
                    "error": DevelopGraphErrorState(
                        from_="verify_agent",
                        thread_id=runtime.context.thread_id,
                        desc=f"无法处理结果返回的消息：{ver_res}",
                        goto_="work_end",
                        original_task_list=[],
                    )
                },
            )
            return ModelResponse(result=[fallback])
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

    commands: list[Command] = [Command(update={"messages": verify_result.model_response.result, **verify_agent_update})]
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
    return Command(
        graph=Command.PARENT,
        goto="work_end",
        update={
            "type": "error",
            "desc": state.get("desc") or "代码质检失败",
            "from_": state.get("from_") or "ver_chat",
            "thread_id": runtime.context.thread_id,
            "app_type": state.get("app_type"),
            "date": state.get("date"),
            "project_name": state.get("project_name"),
        },
    )


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

    # 深层已经 completed 时禁止再跑 execute/user（否则会重复浅层/深层和人工确认）。
    # 只允许 submit；submit_chat 缺失时给默认说明。
    deep_result = state.get("deep_result")
    deep_status = (deep_result.get("status") if isinstance(deep_result, dict)
                   else getattr(deep_result, "status", None))
    if deep_status == "completed" and goto != "submit":
        return Send(
            "ver_submit",
            {
                "submit_msg": execute_info.get("submit_chat") or "深层测试已完成，请确认是否提交。",
                "date": date,
                "project_name": p_name,
                "app_type": app_type,
            },
        )

    if goto == "execute":
        if app_type == "flink":
            if fail_num := state.get("fail_num"):
                # 只能失败2次
                if fail_num > 2:
                    # 不行只能交给人工
                    shallow_result = state.get("shallow_result")
                    if hasattr(shallow_result, "__dataclass_fields__"):
                        shallow_result = asdict(shallow_result)
                    test_code_dict = {
                        "type": "error",
                        **(shallow_result or {}),
                        "app_type": app_type,
                        "source": state.get("source"),
                        "original_task_list": state.get("original_task_list") or [],
                        "date": date,
                        "project_name": p_name,
                    }
                    return Send(
                        "ver_manual",
                        test_code_dict
                    )
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

    if goto == "user":
        if app_type == "flink":
            deep_commands = _parse_command(flink_command.get("flink_deep_command") or [])
        elif app_type == "spark":
            deep_commands = _parse_spark_command(spark_command.get("spark_deep_command") or [])
        else:
            return _route_error_send(state, f"不支持的应用类型: {app_type}")
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
        }
        if repair_num := state.get("repair_num"):
            code_summary["repair_num"] = repair_num
        return Send(
            "ver_manual",
            code_summary
        )

    if goto == "submit":
        submit_info = {
            "submit_msg": execute_info.get("submit_chat"),
            "date": state.get("date"),
            "project_name": state.get("project_name"),
            "app_type": app_type,
        }
        return Send(
            "ver_submit",
            submit_info
        )
    return _route_error_send(state, f"无法识别的 goto: {goto!r}")


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
        final_result = await supervisor.run(
            coro=_run_flink_test_command(
                flink_shallow_test_commands,
                path=app_path,
                buffer=buf
            )
        )
        update_state = {"shallow_result": final_result}
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
        final_result = await supervisor.run(
            coro=_run_spark_test_command(
                spark_commands,
                path=app_path,
                buffer=buf
            )
        )
        update_state = {"shallow_result": final_result}
        if final_result.status != "completed":
            update_state["fail_num"] = (state.get("fail_num") or 0) + 1
        return Command(
            goto="ver_chat",
            update=update_state
        )

    return _route_error(state, runtime, f"不支持的应用类型: {app_type}", from_="ver_com_exec")


def _verify_manual(code_summary: dict, runtime: Runtime) -> Command:
    code_res_type = code_summary.get("type")
    if code_res_type == "error":
        to_manual_dict = _error_dict_to_str(code_summary)
    else:
        to_manual_dict = _splicing_doc(code_summary)
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
        )
        final_result = await supervisor.run(
            coro=_run_flink_test_command(
                deep_commands,
                path=app_path,
                buffer=buf
            )
        )
        update_state = {"deep_result": final_result}
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
                "shallow_result": None  # 清空状态，否则被干扰
            }
        )

    elif app_type == "spark":
        deep_commands = state.get("deep_commands", [])
        app_path = ROOT_BASE_DIR_PATH / "tmp_code" / "spark" / date / p_name
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
        )
        final_result = await supervisor.run(
            coro=_run_spark_test_command(
                deep_commands,
                path=app_path,
                buffer=buf
            )
        )
        update_state = {"deep_result": final_result}
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
                "shallow_result": None  # 清空状态，否则被干扰
            }
        )

    return _route_error(state, runtime, f"不支持的应用类型: {app_type}", from_="ver_deep_exec")


async def _verify_submit(state: dict, runtime: Runtime) -> Command:
    submit_msg = state.get("submit_msg")
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
    answer = interrupt(submit_msg)  # 人工回传 "yes"/"no" "yes" -> 自动提交 "no" -> 手动提交
    answer_norm = answer.strip().lower() if isinstance(answer, str) else ""
    if answer_norm in {"no", "n"}:
        return Command(
            graph=Command.PARENT,
            goto="work_end",
            update={
                "type": "info",
                "desc": "人工拒绝自动提交，采用手动提交任务",
                "from_": "ver_submit",
                "thread_id": runtime.context.thread_id,
                "date": date,
                "project_name": p_name,
                "app_type": app_type,
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
        graph=Command.PARENT,
        goto="work_end",
        update={
            "type": res.get("type"),
            "desc": res.get("msg"),
            "from_": "ver_submit",
            "thread_id": runtime.context.thread_id,
            "date": date,
            "project_name": p_name,
            "app_type": app_type,
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
_verify_builder.add_node("ver_com_exec", cast("StateNode", _verify_shallow_execute))
_verify_builder.add_node("ver_manual", cast("StateNode", _verify_manual))
_verify_builder.add_node("ver_deep_exec", cast("StateNode", _verify_deep_execute))
_verify_builder.add_node("ver_submit", cast("StateNode", _verify_submit))
_verify_builder.add_node("tools", ToolNode(
    tools=code_verify_agent_tools,
    awrap_tool_call=chain_tool_call_wrappers(verify_agent_middleware)
))

_verify_builder.add_edge(START, "ver_handler")
_verify_builder.add_edge("ver_handler", "ver_chat")
_verify_builder.add_conditional_edges(
    "ver_chat",
    _verify_msg_dispatch,
    ["ver_com_exec", "ver_manual", "ver_submit", "tools", "ver_error"]
)
_verify_builder.add_edge("tools", "ver_chat")
verify_agent = _verify_builder.compile()
