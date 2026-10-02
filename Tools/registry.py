"""
注册表

一、checker
    分三组, (入参和数据粒度):
      * CLUSTER: 读 RM 级的整集群数据(节点健康/JMX/队列), **不接受 app_id**,
        每轮只跑一次。若按应用跑, 同一条磁盘异常会被报 N 次。
      * SPARK / FLINK: 按 app_id 取该应用的 REST, 每个应用各一份。

    登记格式: (检查项名称, 模块路径, 函数名)。检查项名称同时充当结果的 check_type。
    新增 checker 时记得登记 —— 漏登记的表现是"这条检查从来不跑", 和"一切正常"同形。

二、Agent 工具调用
"""
from typing import Callable, List, Tuple, Literal

from langchain_core.runnables import Runnable
from langchain_core.tools import BaseTool

from Tools.share_check_tool import is_flink_app, is_spark_app

_TOOL_REGISTRY: dict[str, list[BaseTool | Callable[[Callable | Runnable], BaseTool]]] = {}
_TOOL_SEQUENCE: dict[str: dict[int, list[str]]] = {}  # Agent_name -> 级别 -> 工具名

ALL_CHECKERS: dict[str, List[Tuple[str, str, str]]] = {
    "CLUSTER": [
        ("磁盘", "Tools.check_disk_abnormal", "check_disk"),
        ("文件句柄", "Tools.check_file_handle_abnormal", "check_file_handle"),
        ("队列资源", "Tools.check_resource_abnormal", "check_resource_insufficient"),
    ],
    "SPARK": [
        ("数据倾斜", "Tools.spark.check_spark_data_skew", "check_spark_data_skew"),
        ("OOM", "Tools.spark.check_spark_OOM", "check_OOM"),
        ("网络超时", "Tools.spark.check_spark_network_timeout", "network_timeout_check"),
        ("其他异常", "Tools.spark.check_other_spark_abnormal", "check_other"),
        ("任务超时", "Tools.check_job_timeout", "check_spark_job_timeout"),
    ],
    "FLINK": [
        ("检查点异常", "Tools.flink.check_flink_checkpoint", "check_checkpoint"),
        ("数据倾斜", "Tools.flink.check_flink_data_skew", "check_flink_data_skew"),
        ("任务挂起", "Tools.flink.check_flink_job_hang", "check_flink_job_hang"),
        ("TaskManager OOM 水位", "Tools.flink.check_flink_oom", "check_flink_oom"),
        ("水位线异常", "Tools.flink.check_flink_watermarks", "check_watermarks"),
        ("背压与网络", "Tools.flink.check_flink_backpressure", "check_flink_backpressure"),
        ("其他异常", "Tools.flink.check_flink_other_abnormal", "check_flink_other_abnormal"),
    ]
}


def checkers_for(app: dict):
    """
    按 YARN 报的 applicationType 选该应用对应的清单

    :return: 清单(list); **未知类型返回 None** —— 调用方必须显式处理,
             不能默认当成 spark(那会让 Flink / 新类型的应用静默地一条检查都不跑)
    """
    if is_flink_app(app):
        return ALL_CHECKERS["FLINK"], "FLINK"
    if is_spark_app(app):
        return ALL_CHECKERS["SPARK"], "SPARK"
    return None, ""


def register_tool(agent_name: str, priority: int = -1):
    """
    agent_name: 需要用该工具的 Agent 名称
    priority: 工具优先级 -> 越接近 1 越优先 -> 不支持 优先级为 0
            -> 若你不设置时 将会采用默认的 -1 ，该值会将优先级降为最低
            -> 当你设置了 0 时 ，将会退化为 默认的 -1
    当然 工具 并不局限于 唯一的优先级
    """

    def register(func):
        _TOOL_REGISTRY.setdefault(agent_name, []).append(func)
        func_name = func.name if hasattr(func, 'name') else func.__name__
        level = priority if priority > 0 else -1
        _TOOL_SEQUENCE.setdefault(agent_name, {}).setdefault(level, []).append(func_name)
        return func

    return register


def register_checker(check_name: str, check_group: Literal["SPARK", "FLINK", "CLUSTER"]):
    """
    自定义 检测脚本 注册器 -> 当原本的检测脚本不够全面时，你可以通过这个来添加到 注册表中
    check_name -> 检测的内容 或者 主题
    check_group -> 该检测的是属于 SPARK/FLINK/CLUSTER 维度
    """
    def register(func):
        ALL_CHECKERS.setdefault(check_group, []).append((check_name, func.__module__, func.__name__))
        return func

    return register


def get_tools(agent_name: str):
    if agent_name not in _TOOL_REGISTRY:
        # 判断是否出现 空注册表 「这个 Agent 就是没有工具」
        raise KeyError(f"没有为 {agent_name!r} 注册任何工具 —— 通常是忘了 import "
                       f"对应的工具模块（如 Tools.operations_agent_tools）")
    return _TOOL_REGISTRY[agent_name].copy()


def get_priority_tools(agent_name: str):
    if agent_name not in _TOOL_SEQUENCE:
        raise KeyError(f"没有为 {agent_name!r} 注册任何工具 —— 通常是忘了 import "
                       f"对应的工具模块（如 Tools.operations_agent_tools）")

    return _TOOL_SEQUENCE[agent_name].copy()
