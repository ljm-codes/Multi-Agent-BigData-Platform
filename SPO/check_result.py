from typing import TypedDict, List, Optional, Dict, Any, NotRequired

from pydantic import BaseModel, Field


class TaskMetrics(TypedDict):
    task_id: int
    status: str
    records_read: float
    jvm_gc_time: float


class StageMetrics(TypedDict):
    stage_id: int
    status: str
    tasks: List[TaskMetrics]


class ExecutorMetrics(TypedDict):
    executor_id: str
    host: str
    stdout_log: str
    stderr_log: str


class UsersSchedulerLeafQueuePartInfo(TypedDict):
    username: str
    num_pending_applications: int


class CapacitySchedulerLeafQueuePartInfo(BaseModel):
    """
    叶子队列部分信息(非全部)
    """
    queue_name: str
    state: str
    num_pending_applications: int
    absolute_max_capacity: float
    absolute_used_capacity: float
    max_active_applications: int
    num_active_applications: int
    users: Optional[List[UsersSchedulerLeafQueuePartInfo]] = None


class FlinkCheckpointAbnormalInfo(TypedDict):
    """
    checkpoint 异常信息, 同时覆盖耗时超 P95 与 checkpoint 失败两种情况
    """
    app_id: str
    job_id: str
    type: str
    describe: str
    status: str
    duration_P95: NotRequired[float]
    duration: NotRequired[int]
    error_msg: NotRequired[str]


class StageAbnormalInfo(TypedDict):
    """
    Spark 阶段或任务粒度的异常信息, 覆盖网络超时与其他异常情况
    """
    stage_id: int
    status: str
    describe: str
    failure_reason: NotRequired[str]
    wait_ratio: NotRequired[float]
    task_id: NotRequired[int]
    error_msg: NotRequired[str]
    # stage 会重试, 同 stage_id 可能有多次 attempt。这里记下证据来自哪一次:
    # attempt_id > 0 且该 stage 之后重试成功, 说明这是**已经自愈**的失败 ——
    # 仍然是值得看的劣化信号, 但和"当前正在失败"不是一回事。
    attempt_id: NotRequired[int]


class FlinkDataSkewInfo(TypedDict):
    """
    Flink 数据倾斜信息
    """
    app_id: str
    job_id: str
    vertices_id: str
    type: str
    describe: str
    max_records: NotRequired[float]
    median_records: NotRequired[float]
    skew_ratio: NotRequired[float]


class FlinkTaskManagerOOMInfo(TypedDict):
    """
    Flink TaskManager 内存水位信息, 只给出水位与日志入口, 是否真的 OOM 交由 Agent 判断
    字段说明:
        log_path: 日志文件清单的 URL。GET 返回 JSON, 列出 taskmanager.log、taskmanager.err、
                  prelaunch.* 等; 取具体文件内容再拼 /logs/{文件名}。
                  OOM 的直接证据通常在 taskmanager.err
        path:     TaskManager 的 REST 详情入口, 含 metrics / hardware / memoryConfiguration /
                  allocatedSlots
    注: 两者都基于该应用的 Flink REST 基址(经 YARN 代理)拼出, 用 TaskManager 的 container id
        定位。因此既不依赖 JobManager 落在哪台机器, 也不要求调用方能直连 TaskManager 所在节点
    """
    app_id: str
    taskmanager_id: str
    heap_used: int
    heap_max: int
    heap_usage_ratio: float
    describe: str
    log_path: NotRequired[str]
    path: NotRequired[str]


class FlinkJobHangInfo(TypedDict):
    """
    Flink 子任务挂起信息
    """
    app_id: str
    job_id: str
    vertices_id: str
    type: str
    describe: str
    status: NotRequired[str]
    duration: NotRequired[int]


class FlinkOtherAbnormalInfo(TypedDict):
    """
    Flink 其它异常信息, 覆盖类/依赖冲突、外部数据源、认证/权限、数据格式四类
    """
    app_id: str
    job_id: str
    type: str
    describe: str
    exception_name: NotRequired[str]
    task_name: NotRequired[str]
    taskmanager_id: NotRequired[str]
    timestamp: NotRequired[int]
    stacktrace: NotRequired[str]


class FlinkBackpressureInfo(TypedDict):
    """
    Flink 背压与网络异常信息, 同时承载子任务背压与异常历史中的网络问题
    字段说明:
        背压条目: subtask_id / ratio / busy_ratio / idle_ratio
        网络条目: exception_name / task_name / stacktrace
    """
    app_id: str
    job_id: str
    vertices_id: str
    type: str
    describe: str
    subtask_id: NotRequired[int]
    ratio: NotRequired[float]
    busy_ratio: NotRequired[float]
    idle_ratio: NotRequired[float]
    exception_name: NotRequired[str]
    task_name: NotRequired[str]
    stacktrace: NotRequired[str]


class FlinkWatermarkInfo(TypedDict):
    id: NotRequired[str]
    value: NotRequired[float]
    type: str
    subtask_id: NotRequired[str]
    late_records_dropped: NotRequired[int]
    records_out: NotRequired[int]
    ratio: NotRequired[float]


class FlinkWatermarksAbnormalInfo(TypedDict):
    app_id: str
    job_id: str
    type: str
    vertices_id: str
    watermarks_infos: List[FlinkWatermarkInfo]


class CheckResult(BaseModel):
    type: str = Field(default="", description="检查类型")
    describe: str = Field(description="错误描述")
    data: Optional[Dict[str, Any] | List[Dict[str, Any]]] = Field(default=None, description="相关数据")


class CheckResultQueue(BaseModel):
    pass
