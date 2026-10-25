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
    error: str | None = None


# 大数据开发Agent的状态模型
class DevelopAgentInputState(TypedDict):
    input_msg: str
    # 后台续跑时由感知层带入，用于中间件按任务取结果
    deep_task_id: NotRequired[str]
    is_background: NotRequired[bool]
    source: NotRequired[str]


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


class CodeVerifyAgentMainState(MessagesState):
    date: str
    app_type: str
    project_name: str
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
    fail_num: NotRequired[int]
    repair_num: NotRequired[int]
    is_repair: NotRequired[bool]


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
