from typing import TypedDict, Literal, NotRequired, List, Dict

from langgraph.graph import MessagesState
from pydantic import BaseModel


class OperationsAndReconcileContext(BaseModel):
    app_id: str
    session_id: str


class OperationsAndReconcileMainState(MessagesState):
    # 类型 -> 用于描述 是否发生了错误
    type: NotRequired[Literal["error"]]
    # 错误ID 列表
    error_id_list: NotRequired[List[int]]
    # 记录本次对话的错误描述文本 -> 当 type 为 error 时 才会有值
    error_desc_text: NotRequired[str]
    # 剩余下来需要执行的 工具
    remaining_tool_calls: NotRequired[List[Dict]]
    # 在网关执行时 临时添加的 AI 消息 -> 到时候将会在转 Agent 节点时，进行去除
    temp_ai_ids: NotRequired[List[str]]


class OperationsAndReconcileCheckInfo(TypedDict):
    # 错误ID
    id: int
    # 错误描述 -> 包括检查者本身的错误 与 检查者检查到的错误
    error_describe: str
    # 检查类型
    check_type: str
    # 错误类型 -> error / warning
    error_type: str
    # 重复次数
    repeat_count: int


class OperationsAndReconcileInputState(TypedDict):
    # 错误信息列表
    error_list: list[OperationsAndReconcileCheckInfo]
    # 是否为集群
    is_cluster: bool
    # 应用ID
    app_id: NotRequired[str]
    # 应用类型
    app_type: NotRequired[Literal["SPARK", "FLINK", "CLUSTER"]]


class OperationsAndReconcileOutputState(TypedDict):
    type: Literal["error", "info"]
    out_msg: str


class ErrorInfo(TypedDict):
    type: Literal["error"]
    error_from: str
    error_info: NotRequired[Dict]
    error_desc_text: NotRequired[str]
