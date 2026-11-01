import json
import time
from datetime import datetime, timezone

from SPO.check_result import (CapacitySchedulerLeafQueuePartInfo,
                              FairSchedulerLeafQueuePartInfo,
                              UsersSchedulerLeafQueuePartInfo, CheckResult)

from Tools.share_check_tool import rm, redis_con, yarn_conf
from Tools.yarn_queue import (CAPACITY, FAIR, leaf_queues, scheduler_root,
                              scheduler_type_of)

SAFE_IGNORE_AVAILABLE_MB = 5 * 1024
PENDING_PERSIST_SECONDS = 60 * 5  # 5分钟内
SINGLE_CONTAINER_MIN_MB = 1024  # 1GB


def _capacity_queue_info(queue: dict) -> CapacitySchedulerLeafQueuePartInfo:
    user_part_list = []
    users_list = (queue.get("users") or {}).get("user", []) or []
    for user in users_list:
        user_part_list.append(UsersSchedulerLeafQueuePartInfo(
            username=user.get("username", ""),
            num_pending_applications=user.get("numPendingApplications", 0)
        ))
    if len(user_part_list) > 1:
        user_part_list.sort(key=lambda x: x.num_pending_applications, reverse=True)
    return CapacitySchedulerLeafQueuePartInfo(
        queue_name=queue.get("queueName", ""),
        state=queue.get("state", ""),
        num_pending_applications=queue.get("numPendingApplications", 0),
        absolute_max_capacity=queue.get("absoluteMaxCapacity", 0.0),
        absolute_used_capacity=queue.get("absoluteUsedCapacity", 0.0),
        max_active_applications=queue.get("maxActiveApplications", 0),
        num_active_applications=queue.get("numActiveApplications", 0),
        users=user_part_list
    )


def _fair_queue_info(queue: dict) -> FairSchedulerLeafQueuePartInfo:
    return FairSchedulerLeafQueuePartInfo(
        queue_name=queue.get("queueName", ""),
        state=queue.get("state"),
        num_pending_apps=int(queue.get("numPendingApps") or 0),
        num_active_apps=int(queue.get("numActiveApps") or 0),
        max_apps=int(queue.get("maxApps") or 0),
        pending_containers=int(queue.get("pendingContainers") or 0),
        allocated_containers=int(queue.get("allocatedContainers") or 0),
        used_memory_mb=int((queue.get("usedResources") or {}).get("memory") or 0),
        max_memory_mb=int((queue.get("maxResources") or {}).get("memory") or 0),
    )


def get_all_queue_info(scheduler_info: dict, scheduler_list: list,
                       scheduler_type: str = ""):
    """
    解析叶子队列

    :param scheduler_type: capacity / fair; 留空(或 auto)时按 schedulerInfo.type 判断
    """
    kind = str(scheduler_type or "").strip().lower()
    if kind not in (CAPACITY, FAIR):
        kind = scheduler_type_of(scheduler_info)
    # 公平调度器 0 应用时 root 就是唯一队列, 要兜底; 容量调度器的根不能当叶子
    leaves = leaf_queues(scheduler_root(scheduler_info),
                         root_fallback=(kind == FAIR))
    for queue in leaves:
        if kind == FAIR:
            scheduler_list.append(_fair_queue_info(queue))
        else:
            scheduler_list.append(_capacity_queue_info(queue))
    return scheduler_list or None


def get_specified_field(field: str):
    redis_result = redis_con.get("cluster-metrics")
    if not redis_result:
        return None
    redis_result = json.loads(redis_result)
    return redis_result.get(field)


def _update_cache(ts, pending, reserved):
    # 原为 getdel, 该命令需 Redis >= 6.2; 此处无并发写, get + delete 等价
    redis_result = redis_con.get("cluster-metrics")
    redis_con.delete("cluster-metrics")
    if not redis_result:
        redis_con.set("cluster-metrics", json.dumps({
            "last_check_ts": ts,
            "last_pending": pending,
            "last_reserved": reserved}
        ))
        return
    redis_result = json.loads(redis_result)
    redis_result["last_check_ts"] = ts
    redis_result["last_pending"] = pending
    redis_result["last_reserved"] = reserved
    redis_con.set("cluster-metrics", json.dumps(redis_result))


def _reset_cache():
    redis_con.delete("cluster-metrics")


def check_dynamic_containers(metrics: dict):
    errors = []
    cluster = metrics.get("clusterMetrics", {})
    containers_pending = cluster.get("containersPending", 0)
    reserved_mb = cluster.get("reservedMB", 0)
    available_mb = cluster.get("availableMB", 0)
    current_ts = time.time()
    if reserved_mb > 0 and containers_pending > 0:
        errors.append({
            "describe": f"资源碎片阻塞：预留内存 {reserved_mb}MB 且存在 {containers_pending} 个待分配容器",
            "reservedMB": reserved_mb,
            "containersPending": containers_pending,
        })
        # 碎片已触发告警，无需再走持续时间判断，但保留缓存更新
        _update_cache(current_ts, containers_pending, reserved_mb)
        return errors
    # 若当前 pending = 0，重置缓存
    if containers_pending == 0:
        _reset_cache()
        return errors

    # 当前 pending > 0，检查持续时间
    last_ts = get_specified_field("last_check_ts")
    last_pending = get_specified_field("last_pending")
    # 缓存缺失时 get_specified_field 返回 None；只判 == 0 会走到 current_ts - None
    if last_ts is None or last_ts == 0:
        # 首次检测到 pending > 0，记录时间，不报警
        _update_cache(current_ts, containers_pending, reserved_mb)
        return errors
    # 计算持续时长
    duration = current_ts - last_ts

    if available_mb > SAFE_IGNORE_AVAILABLE_MB or duration < 5:
        # 忽略，但更新缓存（保持计数）
        _update_cache(current_ts, containers_pending, reserved_mb)
        return errors

    if duration >= PENDING_PERSIST_SECONDS and available_mb < SINGLE_CONTAINER_MIN_MB:
        errors.append({
            "describe": f"真实资源阻塞：containersPending={containers_pending} 持续 {duration:.1f} 秒，可用内存仅 {available_mb}MB",
            "containersPending": containers_pending,
            "duration": duration,
            "availableMB": available_mb,
        })
        # 报警后重置缓存，防止重复报警
        _reset_cache()
    else:
        # 未达到报警条件，只更新缓存
        _update_cache(current_ts, containers_pending, reserved_mb)
    return errors


def _configured_scheduler_type(scheduler_info: dict) -> str:
    """hadoop.yarn.scheduler_type 优先; 配 auto/认不出时按 RM 报的类型判断"""
    configured = str(yarn_conf("scheduler_type", "auto") or "auto").strip().lower()
    if configured in (CAPACITY, FAIR):
        return configured
    return scheduler_type_of(scheduler_info)


def _capacity_queue_errors(queue_info) -> list:
    errors = []
    if queue_info.num_pending_applications <= 0:
        return errors
    if queue_info.absolute_used_capacity >= queue_info.absolute_max_capacity:
        user_desc = ""
        if queue_info.users and queue_info.users[0].num_pending_applications > 0:
            user_desc = f"，用户{queue_info.users[0].username}已用尽弹性资源上限"
        errors.append({
            "describe": f"队列{queue_info.queue_name}已用尽弹性资源上限{user_desc}",
            "data": queue_info.model_dump()
        })
    if queue_info.num_active_applications >= queue_info.max_active_applications:
        user_desc = ""
        if queue_info.users and queue_info.users[0].num_pending_applications > 0:
            user_desc = f"，用户{queue_info.users[0].username}已用尽弹性资源上限"
        errors.append({
            "describe": f"队列{queue_info.queue_name}队列达到应用并发上限，新应用无法启动{user_desc}",
            "data": queue_info.model_dump()
        })
    return errors


def _fair_queue_errors(queue_info) -> list:
    errors = []
    if (0 < queue_info.max_memory_mb <= queue_info.used_memory_mb
            and queue_info.pending_containers > 0):
        errors.append({
            "describe": f"队列{queue_info.queue_name}已用尽资源上限，"
                        f"仍有{queue_info.pending_containers}个容器待分配",
            "data": queue_info.model_dump()
        })
    if 0 < queue_info.max_apps <= queue_info.num_active_apps:
        errors.append({
            "describe": f"队列{queue_info.queue_name}达到应用并发上限，新应用无法启动",
            "data": queue_info.model_dump()
        })
    return errors


def check_resource_insufficient():
    schedulers_info = rm.cluster_scheduler().data
    if not schedulers_info:
        return None
    # 类型由 hadoop.yarn.scheduler_type 选择; auto 时按 schedulerInfo.type 判断
    scheduler_info = schedulers_info.get("scheduler", {}).get("schedulerInfo", {})
    if not scheduler_info:
        return None
    scheduler_type = _configured_scheduler_type(scheduler_info)
    scheduler_list = []
    all_queue_info = get_all_queue_info(scheduler_info, scheduler_list, scheduler_type)

    if not all_queue_info:
        # 无叶子队列时返回 None, 统一成空列表后继续:
        # 下面的容器指标检查与队列无关, 不能在这里提前返回
        all_queue_info = []
    queue_error_list = []
    for queue_info in all_queue_info:
        if isinstance(queue_info, FairSchedulerLeafQueuePartInfo):
            queue_error_list.extend(_fair_queue_errors(queue_info))
        else:
            queue_error_list.extend(_capacity_queue_errors(queue_info))
    metrics_resp = rm.cluster_metrics()
    if metrics_resp and metrics_resp.data:
        metrics_errors = check_dynamic_containers(metrics_resp.data)
        queue_error_list.extend(metrics_errors)
    if queue_error_list:
        return CheckResult(
            type="error",
            describe="资源不足",
            check_time=datetime.now(timezone.utc),
            data=queue_error_list
        )


if __name__ == '__main__':
    # test
    check_resource_insufficient()
