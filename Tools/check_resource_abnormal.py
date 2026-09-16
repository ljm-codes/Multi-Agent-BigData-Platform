import json
import time

from SPO.check_result import CapacitySchedulerLeafQueuePartInfo, UsersSchedulerLeafQueuePartInfo, CheckResult

from Tools.share_check_tool import rm, redis_con

SAFE_IGNORE_AVAILABLE_MB = 5 * 1024
PENDING_PERSIST_SECONDS = 60 * 5  # 5分钟内
SINGLE_CONTAINER_MIN_MB = 1024  # 1GB


def get_all_queue_info(scheduler_info: dict, scheduler_list: list):
    # 迭代获取叶子队列信息
    queues = (scheduler_info.get("queues") or {}).get("queue", [])
    if not queues:
        return None
    for queue in queues:
        get_all_queue_info(queue, scheduler_list)
        if queue.get("type", "") != "capacitySchedulerLeafQueueInfo":
            continue
        # print(queue)
        user_part_list = []
        users_list = (queue.get("users") or {}).get("user", []) or []
        for user in users_list:
            user_part_list.append(UsersSchedulerLeafQueuePartInfo(
                username=user.get("username", ""),
                num_pending_applications=user.get("numPendingApplications", 0)
            ))
        if len(user_part_list) > 1:
            user_part_list.sort(key=lambda x: x.num_pending_applications, reverse=True)
        scheduler_list.append(CapacitySchedulerLeafQueuePartInfo(
            queue_name=queue.get("queueName", ""),
            state=queue.get("state", ""),
            num_pending_applications=queue.get("numPendingApplications", 0),
            absolute_max_capacity=queue.get("absoluteMaxCapacity", 0.0),
            absolute_used_capacity=queue.get("absoluteUsedCapacity", 0.0),
            max_active_applications=queue.get("maxActiveApplications", 0),
            num_active_applications=queue.get("numActiveApplications", 0),
            users=user_part_list
        ))
    return scheduler_list


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
        errors.append(CheckResult(
            type="error",
            describe=f"资源碎片阻塞：预留内存 {reserved_mb}MB 且存在 {containers_pending} 个待分配容器",
            data={"reservedMB": reserved_mb, "containersPending": containers_pending}
        ))
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
    if last_ts == 0:
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
        errors.append(CheckResult(
            type="error",
            describe=f"真实资源阻塞：containersPending={containers_pending} 持续 {duration:.1f} 秒，可用内存仅 {available_mb}MB",
            data={"containersPending": containers_pending, "duration": duration, "availableMB": available_mb}
        ))
        # 报警后重置缓存，防止重复报警
        _reset_cache()
    else:
        # 未达到报警条件，只更新缓存
        _update_cache(current_ts, containers_pending, reserved_mb)
    return errors


def check_resource_insufficient():
    schedulers_info = rm.cluster_scheduler().data
    if not schedulers_info:
        return None

    scheduler_info = schedulers_info.get("scheduler", {}).get("schedulerInfo", {})
    if not scheduler_info:
        return None
    scheduler_list = []
    all_queue_info = get_all_queue_info(scheduler_info, scheduler_list)

    if not all_queue_info:
        # 无叶子队列时返回 None, 统一成空列表后继续:
        # 下面的容器指标检查与队列无关, 不能在这里提前返回
        all_queue_info = []
    queue_error_list = []
    for queue_info in all_queue_info:
        if queue_info.num_pending_applications <= 0:
            continue
        if queue_info.absolute_used_capacity >= queue_info.absolute_max_capacity:
            user_desc = ""
            if queue_info.users and queue_info.users[0].num_pending_applications > 0:
                user_desc = f"，用户{queue_info.users[0].username}已用尽弹性资源上限"
            queue_error_list.append(CheckResult(
                type="error",
                describe=f"队列{queue_info.queue_name}已用尽弹性资源上限{user_desc}",
                data=queue_info.model_dump()
            ))
        if queue_info.num_active_applications >= queue_info.max_active_applications:
            user_desc = ""
            if queue_info.users and queue_info.users[0].num_pending_applications > 0:
                user_desc = f"，用户{queue_info.users[0].username}已用尽弹性资源上限"
            queue_error_list.append(CheckResult(
                type="error",
                describe=f"队列{queue_info.queue_name}队列达到应用并发上限，新应用无法启动{user_desc}",
                data=queue_info.model_dump()
            ))
    metrics_resp = rm.cluster_metrics()
    if metrics_resp and metrics_resp.data:
        metrics_errors = check_dynamic_containers(metrics_resp.data)
        queue_error_list.extend(metrics_errors)

    return queue_error_list


if __name__ == '__main__':
    # test
    check_resource_insufficient()
