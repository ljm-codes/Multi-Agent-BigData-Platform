from concurrent.futures import ThreadPoolExecutor, as_completed

from SPO.check_result import CheckResult, FlinkCheckpointAbnormalInfo
from Tools.share_check_tool import (get_all_running_apps, get_flink_rest_base,
                                    get_running_flink_job_ids, is_flink_app, request_get)
from Tools.log_settings import LogSetting

logger = LogSetting.create(__name__)


def get_job_checkpoint_info(base: str, job_id: str):
    """
    获取flink job的信息
    :param base: 该应用的 Flink REST 基址
    :param job_id: 工作 ID
    """
    job_info = request_get(f"{base}/jobs/{job_id}/checkpoints")
    if not job_info:
        return None
    return job_info


def check_checkpoint(app_id: str):
    """
    检查flink应用的checkpoint的异常情况
    """
    base = get_flink_rest_base(app_id)
    jobs_list = get_running_flink_job_ids(app_id)
    if not jobs_list:
        return None
    checkpoint_abnormal_list = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(get_job_checkpoint_info, base, job_id): job_id
            for job_id in jobs_list
        }
        for future in as_completed(futures):
            job_id = futures[future]
            try:
                job_info = future.result()
            except Exception as e:
                logger.error(f"获取工作 {job_id} 的检查点信息失败: {e}")
                continue
            if not job_info:
                continue
            latest_info = job_info.get("latest", {})
            failed_info = latest_info.get("failed", {})
            # 正在运行的检查点不在 latest 中, 只在 history 中
            in_progress_infos = [
                history_info for history_info in job_info.get("history", [])
                if history_info.get("status", "") == "IN_PROGRESS"
            ]
            duration_P95 = float(job_info.get("summary", {}).get("end_to_end_duration", {}).get("p95", 0))
            if duration_P95:
                for in_progress_info in in_progress_infos:
                    if in_progress_info.get("end_to_end_duration", 0) > duration_P95 * 1.5:
                        checkpoint_abnormal_list.append(FlinkCheckpointAbnormalInfo(
                            app_id=app_id,
                            job_id=job_id,
                            type="warning",
                            describe=f"正在运行的应用{app_id}中工作{job_id}超过历史P95档位时间的1.5倍",
                            status=in_progress_info.get("status", ""),
                            duration_P95=duration_P95,
                            duration=in_progress_info.get("end_to_end_duration", 0),
                        ))

            # 正常
            if not failed_info or not failed_info.get("failure_message", ""):
                continue

            # 剩下的就是异常情况
            checkpoint_abnormal_list.append(FlinkCheckpointAbnormalInfo(
                app_id=app_id,
                job_id=job_id,
                type="error",
                describe=f"正在运行的应用{app_id}中工作{job_id}异常",
                status=failed_info.get("status", ""),
                error_msg=failed_info.get("failure_message", ""),
            ))

    if not checkpoint_abnormal_list:
        return None
    return CheckResult(
        type="checkpoint_abnormal",
        describe="检查点异常",
        data=checkpoint_abnormal_list,
    )


def check_flink_abnormal_situation():
    """
    检查运行中 Flink 应用的检查点情况
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
        logger.info(f"正在检测ID为 {app_id} 的应用检查点情况")
        error_result = check_checkpoint(app_id)
        if error_result:
            return error_result
    return None
