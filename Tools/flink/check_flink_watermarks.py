import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from SPO.check_result import CheckResult, FlinkWatermarkInfo, FlinkWatermarksAbnormalInfo
from Tools.log_settings import LogSetting
from Tools.share_check_tool import (get_all_running_apps, get_flink_rest_base,
                                    get_running_flink_job_ids, is_flink_app,
                                    request_get, redis_con, poll_interval)

logger = LogSetting.create(__name__)
LONG_MIN_VALUE = -2 ** 63  # 等价于 Java 的 Long.MIN_VALUE, 水位线未初始化时 Flink 会返回该值


def to_watermark_num(watermarks_value) -> Optional[int]:
    """
    把水位线值转成数字

    Flink REST 返回的 value 是**字符串**(形如 "-9223372036854775808"), 直接参与比较和
    算术会出三类问题, 原先三处子检查因此全部失效:
      - == LONG_MIN_VALUE 恒为 False  -> "未初始化"永不命中
      - now_time - value 抛 TypeError  -> "过度超前"直接崩
      - int > str 抛 TypeError, int == str 恒为 False -> "回溯"崩, "停滞"计数永不增长
    :return: 转换后的整数, 无法转换时返回 None
    """
    try:
        return int(watermarks_value)
    except (TypeError, ValueError):
        return None


def check_watermarks_uninitialized(watermarks_infos: list) -> list:
    """
    检查水位线是否未初始化
    """
    uninitialized_watermarks = []
    for watermarks_info in watermarks_infos:
        watermarks_id = watermarks_info.get("id", "")
        watermarks_value = to_watermark_num(watermarks_info.get("value", ""))
        if watermarks_value is None:
            continue
        if watermarks_value == LONG_MIN_VALUE:
            uninitialized_watermarks.append(FlinkWatermarkInfo(
                id=watermarks_id,
                value=watermarks_value,
                type="uninitialized"
            ))
    return uninitialized_watermarks


def check_watermarks_stalled_backtrack(job_id: str, watermarks_infos: list) -> list:
    """
    检查水位线是否停滞或回溯
    """
    stalled_watermarks = []
    for watermarks_info in watermarks_infos:
        watermarks_id = watermarks_info.get("id", "")
        watermarks_value = to_watermark_num(watermarks_info.get("value", ""))
        if watermarks_value is None:
            continue
        watermarks_value_redis = redis_con.hget(f"watermarks:{job_id}", watermarks_id)
        if not watermarks_value_redis:
            redis_con.hset(f"watermarks:{job_id}", watermarks_id, json.dumps({
                "watermarks": watermarks_value,
                "times": 1
            }))
            continue
        watermarks_value_json = json.loads(watermarks_value_redis)
        # Redis 中存的是序列化后的字符串, 比较前需要还原成数字
        watermarks_value_redis_num = int(watermarks_value_json.get("watermarks", 0))
        if watermarks_value_json.get("times", 0) >= 3:
            stalled_watermarks.append(FlinkWatermarkInfo(
                id=watermarks_id,
                value=watermarks_value,
                type="stalled"
            ))
            continue
        if watermarks_value_redis_num > watermarks_value:
            stalled_watermarks.append(FlinkWatermarkInfo(
                id=watermarks_id,
                value=watermarks_value,
                type="backtrack"
            ))
            continue
        if watermarks_value_redis_num == watermarks_value:
            watermarks_value_json["times"] = watermarks_value_json.get("times", 0) + 1
            redis_con.hset(f"watermarks:{job_id}", watermarks_id, json.dumps(watermarks_value_json))
    return stalled_watermarks


def check_watermarks_overdue(watermarks_infos: list, app_id: str) -> list:
    maximum_latency = redis_con.get(f"latency:{app_id}")  # 获取注册表中应用的允许最大延迟
    if not maximum_latency:
        return []
    maximum_latency = float(maximum_latency)
    overdue_watermarks = []
    for watermarks_info in watermarks_infos:
        watermarks_id = watermarks_info.get("id", "")
        watermarks_value = to_watermark_num(watermarks_info.get("value", ""))
        if watermarks_value is None:
            continue
        now_time = time.time() * 1000  # 水位线为毫秒级时间戳, 统一单位后再比较
        # 硬编码 1.5 倍允许最大延迟，判断是否过度超前
        if now_time - watermarks_value > maximum_latency * 1.5:
            overdue_watermarks.append(FlinkWatermarkInfo(
                id=watermarks_id,
                value=watermarks_value,
                type="overdue"
            ))
    return overdue_watermarks


def check_watermarks_late_data_ratio(base: str, job_id: str, vertices_id: str) -> list:
    """
    检查水位线是否迟到数据比例异常
    :param base: 该应用的 Flink REST 基址
    :param job_id: 工作 ID
    :param vertices_id: 顶点 ID
    """
    # /jobs/:job_id/vertices/:vertex_id
    # 获取顶点的 subtask_index 列表
    subtask_json = request_get(f"{base}/jobs/{job_id}/vertices/{vertices_id}")
    if not subtask_json:
        return []
    subtask_ids = [subtask_info.get("subtask", 0) for subtask_info in subtask_json.get("subtasks", []) if
                   subtask_info.get("subtask", 0)]
    if not subtask_ids:
        return []
    # /jobs/:jobid/vertices/:vertex_id/subtasks/:subtask_index/metrics
    # 获取每个子任务的 numLateRecordsDropped、numRecordsOut 指标
    late_data_error_list = []
    for subtask_id in subtask_ids:
        subtask_metrics = request_get(
            f"{base}/jobs/{job_id}/vertices/{vertices_id}/subtasks/{subtask_id}/metrics",
            params={
                "get": "numLateRecordsDropped,numRecordsOut"
            }
        )  # 返回的是一个list -> [{"id": 指标, "value": 指标值}, ...]
        if not subtask_metrics:
            continue

        late_records_dropped = 0
        records_out = 0
        for metric in subtask_metrics:
            metric_id = metric.get("id", "")
            metric_value = metric.get("value", "")
            if metric_id not in ("numLateRecordsDropped", "numRecordsOut"):
                continue
            if metric_id == "numLateRecordsDropped":
                late_records_dropped = int(metric_value)
            if metric_id == "numRecordsOut":
                records_out = int(metric_value)

        if records_out == 0:
            continue
        if late_records_dropped == 0:
            late_data_ratio = 0.0
        else:
            late_data_ratio = late_records_dropped / records_out
        if late_data_ratio > 0.9:
            late_data_error_list.append(FlinkWatermarkInfo(
                type="late_data_abnormal",
                late_records_dropped=late_records_dropped,
                records_out=records_out,
                ratio=late_data_ratio
            ))
    return late_data_error_list


def check_watermarks_by_vertices(base: str, job_id: str, vertices_id: str, app_id: str) -> list | None:
    """
    根据作业ID和顶点ID检查水位线
    :param base: 该应用的 Flink REST 基址
    :param job_id: 工作 ID
    :param vertices_id: 顶点 ID
    :param app_id: 应用 ID
    """
    # watermarks_infos -> [{"id": id, "value": value}, {...}, ...]
    watermarks_infos = request_get(f"{base}/jobs/{job_id}/vertices/{vertices_id}/watermarks")
    if not watermarks_infos:
        return []
    # 水位线未初始化
    if uninitialized_info := check_watermarks_uninitialized(watermarks_infos):
        return uninitialized_info

    # 水位线停滞或回溯
    if stalled_info := check_watermarks_stalled_backtrack(job_id, watermarks_infos):
        return stalled_info

    # 水位线过度超前
    if overdue_info := check_watermarks_overdue(watermarks_infos, app_id):
        return overdue_info

    # 迟到数据比例异常
    if late_data_info := check_watermarks_late_data_ratio(base, job_id, vertices_id):
        return late_data_info

    return None


def check_watermarks(app_id: str):
    """
    检查应用的水位线异常
    """
    base = get_flink_rest_base(app_id)
    jobs_list = get_running_flink_job_ids(app_id)
    if not jobs_list:
        return None
    watermarks_error_list = []
    for job_id in jobs_list:
        job_info = request_get(f"{base}/jobs/{job_id}")
        if not job_info:
            continue
        vertices_infos = job_info.get("vertices", [])
        if not vertices_infos:
            continue
        vertices_ids = [vertices_info.get("id", "") for vertices_info in vertices_infos if vertices_info.get("id", "")]
        if not vertices_ids:
            continue
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(check_watermarks_by_vertices, base, job_id, vertices_id, app_id): vertices_id
                for vertices_id in vertices_ids
            }
            for future in as_completed(futures):
                vertices_id = futures[future]
                try:
                    watermarks_infos = future.result()
                except Exception as e:
                    # 其余 checker 都有这层保护, 唯独这里原先没有:
                    # 单个顶点出错会直接掀翻整个 check_watermarks
                    logger.error(f"检查工作 {job_id} 顶点 {vertices_id} 的水位线失败: {e}")
                    continue
                if not watermarks_infos:
                    continue
                watermarks_error_list.append(FlinkWatermarksAbnormalInfo(
                    app_id=app_id,
                    job_id=job_id,
                    type="error",
                    vertices_id=vertices_id,
                    watermarks_infos=watermarks_infos
                ))
    if not watermarks_error_list:
        return None
    return CheckResult(
        type="watermarks_abnormal",
        describe="水位线异常",
        data=watermarks_error_list
    )


def check_flink_abnormal_situation():
    """
    检查运行中 Flink 应用的水位线情况
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
        logger.info(f"正在检测ID为 {app_id} 的应用水位线情况")
        error_result = check_watermarks(app_id)
        if error_result:
            return error_result
    return None


if __name__ == "__main__":
    while True:
        error_result = check_flink_abnormal_situation()
        if error_result:
            print(f"!!!检测到水位线异常!!!: {error_result}")
            break
        time.sleep(poll_interval)
