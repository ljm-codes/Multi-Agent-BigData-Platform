import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from SPO.check_result import CheckResult, StageAbnormalInfo
from Tools.log_settings import LogSetting
from Tools.share_check_tool import get_stage_summary, request_get, proxy

logger = LogSetting.create(__name__)


def network_timeout_check(app_id: str, n_jobs: int = 10):
    app_jobs = request_get(f"{proxy}/proxy/{app_id}/api/v1/applications/{app_id}/jobs")
    if not app_jobs:
        return None
    app_run_job = app_jobs[0]
    stage_ids = app_run_job.get("stageIds", [])
    network_error_json_list = []
    # Connection refused 不是超时。原先放进来导致同一个「数据源端口不通」的故障
    # 被这个 checker 和 check_other 同时上报。
    # 注意 shuffle 服务不可用的场景仍然覆盖得到 —— 那条路径抛的是 FetchFailedException。
    network_pattern = re.compile(
        r'(FetchFailedException|Connection timed out|connect timed out|'
        r'Broken pipe|SocketTimeoutException|'
        r'ClosedChannelException|NoRouteToHostException|Failed to connect|'
        r'Remote transport closed|Remote connection closed)',
        re.IGNORECASE
    )
    with ThreadPoolExecutor(max_workers=n_jobs) as executor:
        futures = {
            executor.submit(get_stage_summary, stage_id, app_id): stage_id
            for stage_id in stage_ids
        }
        for future in as_completed(futures):
            try:
                stage_id = futures[future]
                latest_infos, failed_infos = future.result()

                if failed_infos:
                    failure_reason = failed_infos.get("failureReason", "")
                    if network_pattern.search(failure_reason):
                        network_error_json_list.append(StageAbnormalInfo(
                            stage_id=stage_id,
                            status=failed_infos.get("status", "FAILED"),
                            describe="阶段因网络问题失败",
                            failure_reason=failure_reason,
                            attempt_id=failed_infos.get("attemptId", 0),
                        ))
                if latest_infos and latest_infos.get("status") != "FAILED":
                    status = latest_infos.get("status", "")
                    wait_time = latest_infos.get("shuffleFetchWaitTime", 0)
                    executor_run_time = latest_infos.get("executorRunTime", 0)
                    if wait_time == 0:
                        continue
                    wait_ratio = wait_time / executor_run_time if executor_run_time else 0.0
                    if wait_ratio > 0.3:
                        network_error_json_list.append(StageAbnormalInfo(
                            stage_id=stage_id,
                            status=status,
                            describe="阶段 shuffle 拉取等待占比过高",
                            wait_ratio=wait_ratio,
                            attempt_id=latest_infos.get("attemptId", 0),
                        ))
            except Exception as e:
                logger.error(f"检测阶段 {stage_id} 网络超时失败: {e}")
                continue

    if not network_error_json_list:
        return None
    return CheckResult(
        type="error",
        describe="存在网络超时问题",
        data=network_error_json_list,
        check_time=datetime.now(timezone.utc),
    )
