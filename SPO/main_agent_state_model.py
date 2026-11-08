from dataclasses import dataclass
from datetime import datetime
from typing import TypedDict, NotRequired, Literal, Any

from langgraph.graph import MessagesState

OutcomeStatus = Literal[
    "completed",
    "timeout",
    "handed_off",
    "background_completed",
    "background_timeout",
    "error",
    "background_error",
]


@dataclass(slots=True)
class TaskOutcome:
    task_id: str
    origin: dict[str, Any]
    status: OutcomeStatus
    result: Any = None  # 正常完成时的完整结果
    partial: Any = None  # 后台超时中断时的已产出内容
    snapshot: Any = None  # 场景二第一阶段 handed_off 时的进度快照
    message: str = ""
    ver_status: Literal["preliminary", "shallow", "deep", "done"] = "preliminary"
    error: str | None = None


def content_text(content: Any) -> str:
    """把消息 content 归一化成字符串：兼容 str 和 content-block list。"""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and item.get("type") in (None, "text"):
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
        return "".join(parts)
    if content is None:
        return ""
    return str(content)


def agent_label(from_: str | None) -> str:
    """把链路里的 from_ 归一成 UI 上展示的 Agent 名。"""
    if not from_:
        return "dis_chat"
    if from_ in {"code_verify", "verify_agent"} or from_.startswith("ver_"):
        return "code_verify"
    if from_ in {"data_analysis", "data_analysis_agent"}:
        return "data_analysis"
    if from_ in {"data_develop", "data_develop_agent"}:
        return "data_develop"
    if from_ in {"dis_chat", "work_end", "handler", "distribute_task_to_agent"}:
        return "dis_chat"
    return from_


# 大数据开发Agent的状态模型
class OpsHandoverState(TypedDict):
    """运维转交开发时带过来的上下文（不经过 dis_chat 直接进开发节点）"""
    app_id: str
    app_type: Literal["spark", "flink"]
    app_name: str          # 运维拉取目录里的原始应用名
    project_name: str      # 搬进 tmp_code 后的目录名（重名时带前缀）
    date: str              # tmp_code 下使用的当前日期
    source_path: str       # 原来的 tmp/target/... 路径
    session_id: str        # 运维会话 id，开发 thread_id = f"ops:{session_id}"
    error_describe: str
    suggest: str
    task_list: list[str]


class DevelopAgentInputState(TypedDict):
    input_msg: str
    # 后台续跑时由感知层带入，用于中间件按任务取结果
    deep_task_id: NotRequired[str]
    is_background: NotRequired[bool]
    is_ops: NotRequired[bool]
    ops_handover: NotRequired[OpsHandoverState]
    source: NotRequired[str]
    # 后台回流时带回来的验证阶段，CVA 据此决定 goto 是否放行
    status: NotRequired[Literal["preliminary", "shallow", "deep", "done"]]


class AgentDevelopInputState(TypedDict):
    is_repair: bool
    task_list: NotRequired[list[str]]
    task_num: NotRequired[int]
    app_type: Literal["spark", "flink"]
    date: NotRequired[str]
    project_name: NotRequired[str]
    error_msg: NotRequired[str]
    human_suggestion: NotRequired[str]
    repair_num: NotRequired[int]
    is_submit: NotRequired[bool]
    is_ops: NotRequired[bool]
    ops_handover: NotRequired[OpsHandoverState]


class AllocateErrorInfo(TypedDict):
    from_: str
    desc: str


class DevelopAgentMainState(MessagesState):
    # assistant: NotRequired[dict]
    error_info: NotRequired[AllocateErrorInfo]
    # CVA 通过父图 Command(goto=..., update=...) 回传的字段；不声明成 channel 会被丢掉
    type: NotRequired[Literal["info", "warn", "error"]]
    desc: NotRequired[str]
    from_: NotRequired[str]
    thread_id: NotRequired[str]
    deep_task_id: NotRequired[str]
    is_background: NotRequired[bool]
    is_ops: NotRequired[bool]
    ops_handover: NotRequired[OpsHandoverState]
    source: NotRequired[str]
    app_type: NotRequired[Literal["spark", "flink"]]
    date: NotRequired[str]
    project_name: NotRequired[str]
    task_list: NotRequired[list[str]]
    task_num: NotRequired[int]
    original_task_list: NotRequired[list[str]]
    repair_num: NotRequired[int]
    is_repair: NotRequired[bool]
    human_suggestion: NotRequired[str]
    is_submit: NotRequired[bool]
    # 验证阶段的结构化报告；由 ver_end 回传，work_end 写进 metadata
    verification: NotRequired[dict[str, Any]]
    # 验证阶段；后台回流时从 CodeVerify 的 TaskOutcome.ver_status
    status: NotRequired[Literal["preliminary", "shallow", "deep", "done"]]
    # work_end 的产出；event streaming v3 的 stream.output() 读的是 state values
    output_msg: NotRequired[str]
    out_time: NotRequired[datetime]
    metadata: NotRequired[dict[str, Any]]


class DevelopAgentOutputState(TypedDict):
    output_msg: str
    out_time: datetime
    metadata: NotRequired[dict[str, Any]]


# 共用的上下文模型
# 必须是 dataclass/BaseModel：LangGraph 对 TypedDict 会把 context 传成 dict，
# 而节点/工具都按 runtime.context.thread_id 这种属性方式访问
@dataclass(slots=True)
class MainStateContext:
    thread_id: str
    user_id: str
    app_type: Literal["spark", "flink"] | None = None
    date: str | None = None


class DevelopGraphErrorState(TypedDict):
    from_: str
    thread_id: str
    desc: str
    goto_: str
    original_task_list: list[str]


# 代码质检Agent的状态模型
class CodeVerifyAgentInputState(TypedDict):
    source: str
    source_msg: str
    app_type: Literal["spark", "flink"]
    project_name: str
    date: str
    original_task_list: list[str]
    repair_num: NotRequired[int]
    is_repair: NotRequired[bool]
    deep_task_id: NotRequired[str]
    background_msg: NotRequired[str]
    is_submit: NotRequired[bool]
    is_ops: NotRequired[bool]
    status: NotRequired[Literal["preliminary", "shallow", "deep", "done"]]


class CodeVerifyAgentMainState(MessagesState):
    date: str
    app_type: str
    project_name: str
    status: Literal["preliminary", "shallow", "deep", "done"]  # 当前验证Agent处于哪个阶段
    source: NotRequired[str]
    deep_task_id: NotRequired[str]
    original_task_list: NotRequired[list[str]]
    # Flink 是 mvn 命令字符串；Spark 是 {test_file, test_name, master} 字典
    shallow_commands: NotRequired[list[Any]]
    shallow_timeout: NotRequired[float]
    deep_commands: NotRequired[list[Any]]
    deep_timeout: NotRequired[float]
    shallow_result: NotRequired[TaskOutcome]
    deep_result: NotRequired[TaskOutcome]
    # 结构化阶段指标；shallow_metrics 在进入 deep 后仍保留，供最终报告/前端卡片使用
    shallow_metrics: NotRequired[dict[str, Any]]
    deep_metrics: NotRequired[dict[str, Any]]
    # 深层失败后的受控重试：最多 1 次，且必须改过测试文件
    deep_retry_num: NotRequired[int]
    deep_test_signature: NotRequired[str]
    fail_num: NotRequired[int]
    repair_num: NotRequired[int]
    is_repair: NotRequired[bool]
    mermaid_retry_num: NotRequired[int]
    is_submit: NotRequired[bool]
    is_ops: NotRequired[bool]
    verify_end_msg: NotRequired[str]
    # ver_submit 回传给 ver_end 的字段；
    type: NotRequired[Literal["info", "warn", "error"]]
    submit_desc: NotRequired[str]
    from_: NotRequired[str]
    thread_id: NotRequired[str]


# 结束状态
class WorkEndState(TypedDict):
    type: Literal["info", "warn", "error"]
    desc: str
    from_: str
    thread_id: str
    app_type: NotRequired[Literal["spark", "flink"]]
    date: NotRequired[str]
    project_name: NotRequired[str]
    # dispatch 在同一轮判断出的线程动作；new_task 时由调用层开新 thread
    thread_action: NotRequired[Literal["continue", "new_task"]]
    thread_title: NotRequired[str]
    request_text: NotRequired[str]
    verification: NotRequired[Any]
