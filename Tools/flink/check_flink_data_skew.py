import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import numpy as np

from SPO.check_result import CheckResult, FlinkDataSkewInfo
from Tools.log_settings import LogSetting
from Tools.share_check_tool import (get_all_running_apps, get_flink_rest_base,
                                    get_running_flink_job_ids, is_flink_app,
                                    request_get, poll_interval)

logger = LogSetting.create(__name__)

# 数据倾斜比阈值, 与 Spark 侧保持一致
SKEW_WARNING_RATIO = 2.0
SKEW_ERROR_RATIO = 5.0


def get_vertex_subtask_records(base: str, job_id: str, vertex_info: dict) -> list:
    """
    获取顶点下每个子任务的输入记录数

    用 numRecordsIn 原因:
      numRecordsOut 不可靠 —— map 与 sink 融合在同一 task 时, 无前缀的
        numRecordsOut 落在链尾的 sink 上, 而 sink 没有输出, 恒为 0;
        numRecordsIn 则是 keyBy 之后每个子任务实际分到多少条, 正是倾斜的信号。

    语义与 Spark 侧对齐: 那边读的是 shuffleReadMetrics.recordsRead(上一次 shuffle 读入量),
    都是"上游写完、下游分到多少"。
    :param base: 该应用的 Flink REST 基址
    :param job_id: 工作 ID
    :param vertex_info: 顶点信息
    :return: [{"subtask": 子任务号, "subtask_id": 子任务 ID, "records": 记录数}, ...]
    """
    vertices_id = vertex_info.get("id", "")
    if not vertices_id:
        return []
    # /jobs/:jobid/vertices/:vertexid -> 返回 subtasks 数组
    subtask_json = request_get(f"{base}/jobs/{job_id}/vertices/{vertices_id}")
    if not subtask_json:
        return []
    subtask_infos = subtask_json.get("subtasks", [])
    if not subtask_infos:
        return []
    subtask_records = []
    for subtask_info in subtask_infos:
        subtask_id = subtask_info.get("subtask", 0)
        # /jobs/:jobid/vertices/:vertexid/subtasks/:subtaskindex/metrics
        # 返回 list -> [{"id": 指标, "value": 指标值}, ...]
        subtask_metrics = request_get(
            f"{base}/jobs/{job_id}/vertices/{vertices_id}/subtasks/{subtask_id}/metrics",
            params={"get": "numRecordsIn"}
        )
        if not subtask_metrics:
            continue
        records = 0
        for metric in subtask_metrics:
            if metric.get("id", "") == "numRecordsIn":
                records = int(metric.get("value", 0))
                break
        subtask_records.append({
            "subtask": subtask_id,
            "subtask_id": subtask_info.get("id", ""),
            "records": records
        })
    return subtask_records


def check_vertex_data_skew(base: str, job_id: str, vertex_info: dict, app_id: str) -> list:
    """
    检查单个顶点是否存在数据倾斜
    :param base: 该应用的 Flink REST 基址
    :param job_id: 工作 ID
    :param vertex_info: 顶点信息
    :param app_id: 应用 ID
    :return: 数据倾斜异常信息列表
    """
    subtask_records = get_vertex_subtask_records(base, job_id, vertex_info)
    if not subtask_records:
        return []
    records = [subtask_info.get("records", 0) for subtask_info in subtask_records]
    if not records:
        return []
    median_records = np.median(records)
    max_records = max(records)
    # 中位数为 0 或最大值为 0 时, 视为大部分子任务没有输出, 不参与倾斜判断
    if not median_records or not max_records:
        return []
    skew_ratio = max_records / median_records
    if skew_ratio < SKEW_WARNING_RATIO:
        return []
    if skew_ratio > SKEW_ERROR_RATIO:
        skew_type = "error"
        describe = f"出现数据倾斜, 顶点倾斜比为 {skew_ratio:.2f}"
    else:
        skew_type = "warning"
        describe = f"疑似出现倾斜, 顶点倾斜比为 {skew_ratio:.2f}"
    return [FlinkDataSkewInfo(
        app_id=app_id,
        job_id=job_id,
        vertices_id=vertex_info.get("id", ""),
        type=skew_type,
        describe=describe,
        max_records=max_records,
        median_records=median_records,
        skew_ratio=skew_ratio
    )]


def check_flink_data_skew(app_id: str, n_jobs: int = 10):
    """
    检查 Flink 应用的数据倾斜情况
    """
    base = get_flink_rest_base(app_id)
    jobs_list = get_running_flink_job_ids(app_id)
    if not jobs_list:
        return None
    skew_error_list = []
    for job_id in jobs_list:
        job_info = request_get(f"{base}/jobs/{job_id}")
        if not job_info:
            continue
        vertices_infos = job_info.get("vertices", [])
        if not vertices_infos:
            continue
        with ThreadPoolExecutor(max_workers=n_jobs) as executor:
            futures = {
                executor.submit(check_vertex_data_skew, base, job_id, vertex_info, app_id): vertex_info
                for vertex_info in vertices_infos
            }
            for future in as_completed(futures):
                try:
                    vertex_skew_infos = future.result()
                except Exception as e:
                    logger.error(f"检查工作 {job_id} 的顶点数据倾斜情况失败: {e}")
                    continue
                if vertex_skew_infos:
                    skew_error_list.extend(vertex_skew_infos)
    if not skew_error_list:
        return None
    return CheckResult(
        type="error",
        describe="存在数据倾斜",
        check_time=datetime.now(timezone.utc),
        data=skew_error_list
    )


def check_flink_abnormal_situation(n_jobs: int = 10):
    """
    检查运行中 Flink 应用的数据倾斜情况
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
        logger.info(f"正在检测ID为 {app_id} 的应用数据倾斜情况")
        error_result = check_flink_data_skew(app_id, n_jobs)
        if error_result:
            return error_result
    return None


if __name__ == "__main__":
    while True:
        error_result = check_flink_abnormal_situation()
        if error_result:
            print(f"!!!检测到数据倾斜问题!!!: {error_result}")
            break
        time.sleep(poll_interval)
