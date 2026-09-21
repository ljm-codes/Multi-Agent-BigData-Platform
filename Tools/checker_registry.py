"""
checker 清单 —— 哪个应用类型该跑哪些检查

分三组, (入参和数据粒度):
  * CLUSTER_CHECKERS: 读 RM 级的整集群数据(节点健康/JMX/队列), **不接受 app_id**,
    每轮只跑一次。若按应用跑, 同一条磁盘异常会被报 N 次。
  * SPARK_CHECKERS / FLINK_CHECKERS: 按 app_id 取该应用的 REST, 每个应用各一份。

登记格式: (检查项名称, 模块路径, 函数名)。检查项名称同时充当结果的 check_type。

新增 checker 时记得登记 —— 漏登记的表现是"这条检查从来不跑", 和"一切正常"同形。
"""
from Tools.share_check_tool import is_flink_app, is_spark_app

CLUSTER_CHECKERS = [
    ("磁盘", "Tools.check_disk_abnormal", "check_disk"),
    ("文件句柄", "Tools.check_file_handle_abnormal", "check_file_handle"),
    ("队列资源", "Tools.check_resource_abnormal", "check_resource_insufficient"),
]

SPARK_CHECKERS = [
    ("数据倾斜", "Tools.spark.check_spark_data_skew", "check_spark_data_skew"),
    ("OOM", "Tools.spark.check_spark_OOM", "check_OOM"),
    ("网络超时", "Tools.spark.check_spark_network_timeout", "network_timeout_check"),
    ("其他异常", "Tools.spark.check_other_spark_abnormal", "check_other"),
    # 任务超时靠 redis 里前后两次的任务数快照判定 —— 每轮只调一次, 靠**跨轮**比较,
    # 同一轮里连调两次是判不出来的(第一次只写快照)
    ("任务超时", "Tools.check_job_timeout", "check_spark_job_timeout"),
]

FLINK_CHECKERS = [
    ("检查点异常", "Tools.flink.check_flink_checkpoint", "check_checkpoint"),
    ("数据倾斜", "Tools.flink.check_flink_data_skew", "check_flink_data_skew"),
    ("任务挂起", "Tools.flink.check_flink_job_hang", "check_flink_job_hang"),
    ("TaskManager OOM 水位", "Tools.flink.check_flink_oom", "check_flink_oom"),
    ("水位线异常", "Tools.flink.check_flink_watermarks", "check_watermarks"),
    ("背压与网络", "Tools.flink.check_flink_backpressure", "check_flink_backpressure"),
    ("其他异常", "Tools.flink.check_flink_other_abnormal", "check_flink_other_abnormal"),
]


def checkers_for(app: dict):
    """
    按 YARN 报的 applicationType 选该应用对应的清单

    :return: 清单(list); **未知类型返回 None** —— 调用方必须显式处理,
             不能默认当成 spark(那会让 Flink / 新类型的应用静默地一条检查都不跑)
    """
    if is_flink_app(app):
        return FLINK_CHECKERS, "FLINK"
    if is_spark_app(app):
        return SPARK_CHECKERS, "SPARK"
    return None, ""
