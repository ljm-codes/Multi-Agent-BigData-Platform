from typing import TypedDict, Literal, NotRequired

from langgraph.graph import MessagesState
from pydantic import BaseModel


class OperationsAndReconcileContext(BaseModel):
    app_id: str


class OperationsAndReconcileMainState(MessagesState):
    # 类型 -> 用于描述 是否发生了错误
    type: NotRequired[Literal["error"]]
    # 记录本次对话的错误描述文本 -> 当 type 为 error 时 才会有值
    error_desc_text: NotRequired[str]


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
    out_msg: str
