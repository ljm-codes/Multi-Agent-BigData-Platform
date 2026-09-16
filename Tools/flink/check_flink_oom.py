import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from SPO.check_result import CheckResult, FlinkTaskManagerOOMInfo
from Tools.log_settings import LogSetting
from Tools.share_check_tool import (get_all_running_apps, get_flink_rest_base,
                                    get_running_flink_job_ids, is_flink_app,
                                    request_get, poll_interval)

logger = LogSetting.create(__name__)

# TaskManager 堆内存使用率告警阈值, 与 Spark 侧保持一致
HEAP_USAGE_WARNING_RATIO = 0.9


def get_all_taskmanagers(base: str) -> list:
    """
    获取全部 TaskManager 信息
    :param base: 该应用的 Flink REST 基址
    :return: TaskManager 信息列表
    """
    # /taskmanagers -> TaskManagersInfo, 返回 taskmanagers 数组
    taskmanagers_info = request_get(f"{base}/taskmanagers")
    if not taskmanagers_info:
        return []
    return taskmanagers_info.get("taskmanagers", [])


def get_taskmanager_detail(base: str, taskmanager_id: str) -> dict:
    """
    获取单个 TaskManager 的详细信息
    :param base: 该应用的 Flink REST 基址
    :param taskmanager_id: TaskManager ID
    :return: TaskManager 详细信息
    """
    # /taskmanagers/:taskmanagerid -> TaskManagerDetailsInfo
    taskmanager_detail = request_get(f"{base}/taskmanagers/{taskmanager_id}")
    if not taskmanager_detail:
        return {}
    return taskmanager_detail


def check_taskmanager_oom(base: str, taskmanager_info: dict, job_ids: set, app_id: str) -> list:
    """
    检查单个 TaskManager 的堆内存使用情况
    :param base: 该应用的 Flink REST 基址
    :param taskmanager_info: TaskManager 信息
    :param job_ids: 本次正在检查的工作 ID 集合
    :param app_id: 应用 ID
    :return: 内存水位异常信息列表
    """
    taskmanager_id = taskmanager_info.get("id", "")
    if not taskmanager_id:
        return []
    taskmanager_detail = get_taskmanager_detail(base, taskmanager_id)
    if not taskmanager_detail:
        return []
    # 只关心本应用正在使用该 TaskManager 的情况
    allocated_job_ids = {
        slot_info.get("jobId", "") for slot_info in taskmanager_detail.get("allocatedSlots", [])
        if slot_info.get("jobId", "")
    }
    if not allocated_job_ids & job_ids:
        return []
    metrics = taskmanager_detail.get("metrics", {})
    heap_used = metrics.get("heapUsed", 0)
    heap_max = metrics.get("heapMax", 0)
    if not heap_max:
        return []
    heap_usage_ratio = heap_used / heap_max
    if heap_usage_ratio < HEAP_USAGE_WARNING_RATIO:
        return []
    # 原来这两个字段都取 taskmanager_info["path"], 而它的实际值是
    # "pekko.tcp://flink@node2:39972/user/rpc/taskmanager_0" —— 一条 RPC 地址, 既不是
    # 日志入口也不是 REST 地址, 对调用方没有用处(且 JMX 未开启, jmxPort 恒为 -1)。
    # 改用 Flink REST 自己的路径, 由 JobManager 提供, 因此不要求调用方能直连 TaskManager 所在节点。
    taskmanager_url = f"{base}/taskmanagers/{taskmanager_id}"
    return [FlinkTaskManagerOOMInfo(
        app_id=app_id,
        taskmanager_id=taskmanager_id,
        heap_used=heap_used,
        heap_max=heap_max,
        heap_usage_ratio=heap_usage_ratio,
        describe=f"TaskManager {taskmanager_id} 堆内存使用率为 {heap_usage_ratio:.2%}",
        # GET 返回该 TM 的日志文件清单(taskmanager.log / taskmanager.err / prelaunch.* 等),
        # 再拼 /logs/{文件名} 即可取到内容。OOM 的直接证据通常在 taskmanager.err
        log_path=f"{taskmanager_url}/logs",
        # 该 TM 的 REST 详情入口: metrics / hardware / memoryConfiguration / allocatedSlots
        path=taskmanager_url
    )]


def check_flink_oom(app_id: str):
    """
    检查 Flink 应用所在 TaskManager 的内存水位
    """
    base = get_flink_rest_base(app_id)
    jobs_list = get_running_flink_job_ids(app_id)
    if not jobs_list:
        return None
    taskmanagers_info = get_all_taskmanagers(base)
    if not taskmanagers_info:
        return None
    memory_error_list = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(check_taskmanager_oom, base, taskmanager_info, set(jobs_list), app_id): taskmanager_info
            for taskmanager_info in taskmanagers_info
        }
        for future in as_completed(futures):
            try:
                taskmanager_error_list = future.result()
            except Exception as e:
                logger.error(f"检查 TaskManager 内存水位失败: {e}")
                continue
            if taskmanager_error_list:
                memory_error_list.extend(taskmanager_error_list)
    if not memory_error_list:
        return None
    return CheckResult(
        type="warning",
        describe="TaskManager 堆内存水位过高, 存在 OOM 风险, 请检查对应日志确认",
        data=memory_error_list
    )


def check_flink_abnormal_situation():
    """
    检查运行中 Flink 应用的内存水位情况
    """
    running_apps = get_all_running_apps()
    if not running_apps:
        return None
    for running_app in running_apps:
        app_id = running_app.get("id", "")
        if not app_id:
            continue
        if not is_flink_app(running_app):
            continue
        logger.info(f"正在检测ID为 {app_id} 的应用内存水位情况")
        error_result = check_flink_oom(app_id)
        if error_result:
            return error_result
    return None


if __name__ == "__main__":
    while True:
        error_result = check_flink_abnormal_situation()
        if error_result:
            print(f"!!!检测到 OOM 风险!!!: {error_result}")
            break
        time.sleep(poll_interval)
