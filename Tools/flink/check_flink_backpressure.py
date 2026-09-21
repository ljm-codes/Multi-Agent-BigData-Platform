import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from SPO.check_result import CheckResult, FlinkBackpressureInfo
from Tools.log_settings import LogSetting
from Tools.share_check_tool import (get_all_running_apps, get_flink_rest_base,
                                    get_running_flink_job_ids, is_flink_app,
                                    request_get, poll_interval)

logger = LogSetting.create(__name__)

# 网络问题关键词, 与 Spark 侧的正则保持一致的判定范围
NETWORK_PATTERN = re.compile(
    r'(Connection refused|Connection timed out|connect timed out|Broken pipe|'
    r'SocketTimeoutException|ClosedChannelException|NoRouteToHostException|'
    r'Failed to connect|Remote transport closed|Remote connection closed|'
    r'ConnectException|UnknownHostException)',
    re.IGNORECASE
)


def check_vertex_backpressure(base: str, job_id: str, vertex_info: dict, app_id: str) -> list:
    """
    检查单个顶点下各子任务的背压情况
    :param base: 该应用的 Flink REST 基址
    :param job_id: 工作 ID
    :param vertex_info: 顶点信息
    :param app_id: 应用 ID
    :return: 背压异常信息列表
    """
    vertex_id = vertex_info.get("id", "")
    if not vertex_id:
        return []
    # /jobs/:jobid/vertices/:vertexid/backpressure -> JobVertexBackPressureInfo
    # 顶点级的 backpressureLevel 是所有子任务的平均, 个别子任务严重背压时会被平均掉,
    # 因此这里逐子任务判断
    backpressure_info = request_get(f"{base}/jobs/{job_id}/vertices/{vertex_id}/backpressure")
    if not backpressure_info:
        return []
    backpressure_error_list = []
    for subtask_info in backpressure_info.get("subtasks", []):
        if subtask_info.get("backpressureLevel", "") != "high":
            continue
        subtask_id = subtask_info.get("subtask", 0)
        # ratio 即背压占比, busyRatio 为忙碌占比
        backpressure_ratio = subtask_info.get("ratio", 0.0)
        backpressure_error_list.append(FlinkBackpressureInfo(
            app_id=app_id,
            job_id=job_id,
            vertices_id=vertex_id,
            type="warning",
            describe=f"子任务 {subtask_id} 背压等级为 high, 背压占比 {backpressure_ratio:.2%}",
            subtask_id=subtask_id,
            ratio=backpressure_ratio,
            busy_ratio=subtask_info.get("busyRatio", 0.0),
            idle_ratio=subtask_info.get("idleRatio", 0.0)
        ))
    return backpressure_error_list


def check_job_exception_network(base: str, job_id: str, app_id: str) -> list:
    """
    从工作异常历史中检查网络问题
    :param base: 该应用的 Flink REST 基址
    :param job_id: 工作 ID
    :param app_id: 应用 ID
    :return: 网络异常信息列表
    """
    # /jobs/:jobid/exceptions -> JobExceptionsInfoWithHistory
    exceptions_info = request_get(f"{base}/jobs/{job_id}/exceptions")
    if not exceptions_info:
        return []
    entries = exceptions_info.get("exceptionHistory", {}).get("entries", [])
    if not entries:
        return []
    network_error_list = []
    for entry in entries:
        # 根异常与并发异常都可能携带网络问题信息
        exception_infos = [entry] + entry.get("concurrentExceptions", [])
        for exception_info in exception_infos:
            stacktrace = exception_info.get("stacktrace", "")
            exception_name = exception_info.get("exceptionName", "")
            if not stacktrace and not exception_name:
                continue
            if not NETWORK_PATTERN.search(stacktrace) and not NETWORK_PATTERN.search(exception_name):
                continue
            network_error_list.append(FlinkBackpressureInfo(
                app_id=app_id,
                job_id=job_id,
                vertices_id="",
                type="error",
                describe=f"异常 {exception_name} 中发现网络问题",
                exception_name=exception_name,
                task_name=exception_info.get("taskName", ""),
                stacktrace=stacktrace
            ))
    return network_error_list


def check_flink_backpressure(app_id: str, n_jobs: int = 10):
    """
    检查 Flink 应用的背压与网络异常情况
    """
    base = get_flink_rest_base(app_id)
    jobs_list = get_running_flink_job_ids(app_id)
    if not jobs_list:
        return None
    backpressure_error_list = []
    for job_id in jobs_list:
        # 网络异常来自异常历史, 不需要顶点信息
        backpressure_error_list.extend(check_job_exception_network(base, job_id, app_id))
        job_info = request_get(f"{base}/jobs/{job_id}")
        if not job_info:
            continue
        vertices_infos = job_info.get("vertices", [])
        if not vertices_infos:
            continue
        with ThreadPoolExecutor(max_workers=n_jobs) as executor:
            futures = {
                executor.submit(check_vertex_backpressure, base, job_id, vertex_info, app_id): vertex_info
                for vertex_info in vertices_infos
            }
            for future in as_completed(futures):
                try:
                    vertex_backpressure_infos = future.result()
                except Exception as e:
                    logger.error(f"检查工作 {job_id} 的顶点背压情况失败: {e}")
                    continue
                if vertex_backpressure_infos:
                    backpressure_error_list.extend(vertex_backpressure_infos)
    if not backpressure_error_list:
        return None
    return CheckResult(
        type="error",
        describe="存在背压或网络异常",
        check_time=datetime.now(timezone.utc),
        data=backpressure_error_list
    )


def check_flink_abnormal_situation(n_jobs: int = 10):
    """
    检查运行中 Flink 应用的背压与网络情况
    """
    # tests 用 生产环境请忽略
    running_apps = get_all_running_apps()
    if not running_apps:
        return None
    for running_app in running_apps:
        app_id = running_app.get("id", "")
        if not app_id:
            continue
        if not is_flink_app(running_app):
            continue
        logger.info(f"正在检测ID为 {app_id} 的应用背压与网络情况")
        error_result = check_flink_backpressure(app_id, n_jobs)
        if error_result:
            return error_result
    return None


if __name__ == "__main__":
    while True:
        error_result = check_flink_abnormal_situation()
        if error_result:
            print(f"!!!检测到背压或网络异常!!!: {error_result}")
            break
        time.sleep(poll_interval)
