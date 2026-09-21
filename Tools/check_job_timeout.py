import json
import time
from datetime import datetime, timezone
from typing import List, Optional
from zoneinfo import ZoneInfo

from SPO.check_result import CheckResult
from Tools.share_check_tool import request_get, proxy, max_job_timeout, redis_con


def check_tasks_num_is_run(
        app_id: str,
        job_id: str,
        *,
        max_task: int,
        active_tasks: int,
        completed_tasks: int,
        success_jobs: Optional[List[int]] = None,
):
    """
    检查历史job是否存在任务数量不变的情况
    """
    job_task_info = redis_con.hget(app_id, job_id)
    # 若没有 success_jobs 那么说明应用刚开始，则对应的数值必定没有记录
    if not job_task_info or not success_jobs:
        redis_con.hset(app_id, job_id, json.dumps({
            "max_task": max_task,
            "active_tasks": active_tasks,
            "completed_tasks": completed_tasks,
        }))
        return False

    if job_id in success_jobs:
        # 成功，删除记录
        redis_con.hdel(app_id, job_id)
        return False
    job_task_json = json.loads(job_task_info)
    if (job_task_json.get("completed_tasks") == completed_tasks
            and job_task_json.get("active_tasks") == active_tasks):
        return True
    # 不一样，更新
    redis_con.hset(app_id, job_id, json.dumps({
        "max_task": max_task,
        "active_tasks": active_tasks,
        "completed_tasks": completed_tasks,
    }))
    return False


def check_spark_job_timeout(app_id: str):
    """
    检查正在运行的应用中的job是否超时
    """
    running_jobs = request_get(f"{proxy}/proxy/{app_id}/api/v1/applications/{app_id}/jobs",
                               params={"status": "RUNNING"})
    success_jobs = request_get(f"{proxy}/proxy/{app_id}/api/v1/applications/{app_id}/jobs",
                               params={"status": "SUCCEEDED"})

    # 请求失败时 request_get 返回 None, 直接推导会抛 TypeError
    success_jobs = [job.get("jobId", "") for job in (success_jobs or [])]

    if not running_jobs:
        return None

    for job in running_jobs:
        if job.get("status") != "RUNNING":
            continue
        job_id = job.get("jobId", "")
        submit_time = job.get("submissionTime", "")
        max_task = job.get("numTasks", 0)
        active_tasks = job.get("numActiveTasks", 0)
        completed_tasks = job.get("numCompletedTasks", 0)

        # 转时间
        submit_time = datetime.strptime(submit_time, "%Y-%m-%dT%H:%M:%S.%f%Z")
        dt_gmt = submit_time.replace(tzinfo=ZoneInfo("GMT"))

        submit_time_shanghai_naive = dt_gmt.astimezone(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None)

        # 转时间戳
        submit_timestamp = submit_time_shanghai_naive.timestamp()

        if check_tasks_num_is_run(
                app_id=app_id,
                job_id=job_id,
                max_task=max_task,
                active_tasks=active_tasks,
                completed_tasks=completed_tasks,
                success_jobs=success_jobs,
        ) and time.time() - submit_timestamp > max_job_timeout:
            return CheckResult(
                type="warning",
                describe=f"应用{app_id} 中job {job_id} 超时 {max_job_timeout} 秒",
                check_time=datetime.now(timezone.utc),
                data={
                    "app_id": app_id,
                    "job_id": job_id,
                    "submit_time": submit_timestamp,
                    "max_task": max_task,
                    "active_tasks": active_tasks,
                    "completed_tasks": completed_tasks,
                }
            )
