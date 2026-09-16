"""
检查 Flink 子任务是否挂起

照搬的spark逻辑「status 与 duration 连续 N 次不变」, 实测**永远无法命中**:
duration 是子任务的存活时长(now - start-time), RUNNING 状态下必然单调递增,
所以「两个都不变」这个条件对真正卡住的子任务恒不成立。
用「源停止推进」造出真实停滞场景后连调 5 次, 一次都没报出来。

现在按两类信号分别判定:
  1. 子任务处于中间态(CREATED/SCHEDULED/DEPLOYING/RECONCILING)且长时间不动
     —— 这是**拿不到资源在排队**, 属于资源饥饿, 不是挂起, 单独描述
  2. 子任务 RUNNING 但**记录数不再增长**
     —— 这才是"卡住": 作业状态还是 RUNNING, 数据却不再流动
"""
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from SPO.check_result import CheckResult, FlinkJobHangInfo
from Tools.log_settings import LogSetting
from Tools.share_check_tool import (get_all_running_apps, get_flink_rest_base,
                                    get_running_flink_job_ids, is_flink_app,
                                    request_get, redis_con, poll_interval)

logger = LogSetting.create(__name__)


# 连续多少次轮询没有进展, 判定为挂起
HANG_TIMES_THRESHOLD = 3
# 子任务处于这些状态属于中间态, 长时间停留即异常
HANG_STATUSES = {"CREATED", "SCHEDULED", "DEPLOYING", "RECONCILING"}
# 两次采样至少间隔这么久, 才把"没变化"计入无进展次数

# 刷新间隔, 连续两次会读到同一个缓存值 —— 作业明明在正常流动却看着像冻结, 造成误报。
HANG_MIN_SAMPLE_INTERVAL = poll_interval


def get_subtask_progress(base: str, job_id: str, vertex_id: str, subtask_id: int) -> dict:
    """
    取子任务的进展计数

    用累积计数(numRecordsIn/numRecordsOut)而非瞬时速率: 「计数没动过」是无歧义的,
    而 PerSecond 类指标在低速率作业上本来就会频繁为 0。

    注意链式算子下无前缀的指标落在链尾: 例如 map 与 sink 融合时, 该顶点的
    numRecordsOut 恒为 0(汇没有输出), 而此时 numRecordsIn 正常增长。
    因此下面按「两个计数都没变」判定 —— 任一在增长就不算挂起, 足以规避这个偏差。
    :return: {"in": 计数, "out": 计数}, 取不到时返回 {}
    """
    metrics = request_get(
        f"{base}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics",
        params={"get": "numRecordsIn,numRecordsOut"}
    )
    if not metrics:
        return {}
    progress = {"in": 0, "out": 0}
    for metric in metrics:
        metric_id = metric.get("id", "")
        if metric_id == "numRecordsIn":
            progress["in"] = int(metric.get("value", 0))
        elif metric_id == "numRecordsOut":
            progress["out"] = int(metric.get("value", 0))
    return progress


def check_subtask_hang(base: str, job_id: str, vertex_id: str,
                       subtask_info: dict, app_id: str) -> list:
    """
    检查单个子任务是否挂起
    :param base: 该应用的 Flink REST 基址
    :param job_id: 工作 ID
    :param vertex_id: 顶点 ID
    :param subtask_info: 子任务信息
    :param app_id: 应用 ID
    :return: 挂起异常信息列表
    """
    subtask_id = subtask_info.get("subtask", 0)
    status = subtask_info.get("status", "")
    duration = subtask_info.get("duration", 0)

    # ---------- 中间态: 长时间停留在调度阶段 (资源饥饿, 不是数据层面的挂起) ----------
    if status in HANG_STATUSES:
        describes = _track_and_report(
            f"job_hang:{job_id}:{vertex_id}:{subtask_id}",
            {"status": status, "duration": duration},
            lambda times: f"子任务 {subtask_id} 持续停留在 {status} 状态, 已连续 {times} 次 (疑似拿不到资源)"
        )

    # ---------- RUNNING: 记录数不再增长才是"卡住" ----------
    elif status == "RUNNING":
        progress = get_subtask_progress(base, job_id, vertex_id, subtask_id)
        # 从未有过任何流量 -> 是空闲不是挂起, 不参与判定
        if not progress or (progress["in"] == 0 and progress["out"] == 0):
            return []
        describes = _track_and_report(
            f"job_progress:{job_id}:{vertex_id}:{subtask_id}",
            progress,
            lambda times: (
                f"子任务 {subtask_id} 数据已连续 {times} 次未流动"
                f" (numRecordsIn={progress['in']}, numRecordsOut={progress['out']}), 疑似挂起"
            )
        )

    else:
        return []

    return [
        FlinkJobHangInfo(
            app_id=app_id,
            job_id=job_id,
            vertices_id=vertex_id,
            type="warning",
            describe=describe,
            status=status,
            duration=duration
        )
        for describe in describes
    ]


def _track_and_report(redis_key: str, snapshot: dict, describe) -> list:
    """
    把本次快照与 Redis 中上一次比较, 连续 HANG_TIMES_THRESHOLD 次不变则返回描述

    "没变化"只有在距上次采样足够久(HANG_MIN_SAMPLE_INTERVAL)时才计入次数,
    否则会把 JobManager 的指标缓存误当成停滞。
    :param redis_key: 记录键
    :param snapshot: 本次快照(状态或进展计数)
    :param describe: 达到阈值时生成描述的函数, 入参为连续次数
    :return: 命中时返回 [describe 的结果], 否则返回空列表
    """
    now = time.time()
    stored = redis_con.get(redis_key)
    if not stored:
        redis_con.set(redis_key, json.dumps({**snapshot, "times": 1, "ts": now}))
        return []
    try:
        stored_json = json.loads(stored)
    except ValueError:
        redis_con.delete(redis_key)
        return []

    # 有变化 -> 说明还在推进, 重新计数(不受采样间隔限制, 免得漏掉进展)
    if any(stored_json.get(k) != v for k, v in snapshot.items()):
        redis_con.set(redis_key, json.dumps({**snapshot, "times": 1, "ts": now}))
        return []

    # 没变化, 但距上次采样太近 -> 可能读到的是同一份缓存, 不计数
    if now - stored_json.get("ts", 0) < HANG_MIN_SAMPLE_INTERVAL:
        return []

    times = stored_json.get("times", 0) + 1
    redis_con.set(redis_key, json.dumps({**snapshot, "times": times, "ts": now}))
    if times < HANG_TIMES_THRESHOLD:
        return []
    return [describe(times)]


def check_vertex_hang(base: str, job_id: str, vertex_info: dict, app_id: str) -> list:
    """
    检查顶点下所有子任务是否挂起
    :param base: 该应用的 Flink REST 基址
    :param job_id: 工作 ID
    :param vertex_info: 顶点信息
    :param app_id: 应用 ID
    :return: 挂起异常信息列表
    """
    vertex_id = vertex_info.get("id", "")
    if not vertex_id:
        return []
    # /jobs/:jobid/vertices/:vertexid -> 返回 subtasks 数组
    subtask_json = request_get(f"{base}/jobs/{job_id}/vertices/{vertex_id}")
    if not subtask_json:
        return []
    subtask_infos = subtask_json.get("subtasks", [])
    if not subtask_infos:
        return []
    hang_error_list = []
    for subtask_info in subtask_infos:
        subtask_hang_infos = check_subtask_hang(base, job_id, vertex_id, subtask_info, app_id)
        if subtask_hang_infos:
            hang_error_list.extend(subtask_hang_infos)
    return hang_error_list


def check_flink_job_hang(app_id: str):
    """
    检查 Flink 应用的工作是否挂起
    """
    base = get_flink_rest_base(app_id)
    jobs_list = get_running_flink_job_ids(app_id)
    if not jobs_list:
        return None
    hang_error_list = []
    for job_id in jobs_list:
        job_info = request_get(f"{base}/jobs/{job_id}")
        if not job_info:
            continue
        vertices_infos = job_info.get("vertices", [])
        if not vertices_infos:
            continue
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(check_vertex_hang, base, job_id, vertex_info, app_id): vertex_info
                for vertex_info in vertices_infos
            }
            for future in as_completed(futures):
                try:
                    vertex_hang_infos = future.result()
                except Exception as e:
                    logger.error(f"检查工作 {job_id} 的子任务挂起情况失败: {e}")
                    continue
                if vertex_hang_infos:
                    hang_error_list.extend(vertex_hang_infos)
    if not hang_error_list:
        return None
    return CheckResult(
        type="warning",
        describe="存在疑似挂起的子任务",
        data=hang_error_list
    )


def check_flink_abnormal_situation():
    """
    检查运行中 Flink 应用的挂起情况
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
        logger.info(f"正在检测ID为 {app_id} 的应用挂起情况")
        error_result = check_flink_job_hang(app_id)
        if error_result:
            return error_result
    return None


if __name__ == "__main__":
    while True:
        error_result = check_flink_abnormal_situation()
        if error_result:
            print(f"!!!检测到任务挂起!!!: {error_result}")
            break
        time.sleep(poll_interval)
