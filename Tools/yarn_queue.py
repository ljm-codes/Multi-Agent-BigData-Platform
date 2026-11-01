"""
YARN 调度器队列树的解析 —— 容量调度器与公平调度器都认

两种调度器的 JSON 形状不同, 这里只负责「找根 + 摊平叶子 + 判断类型」,
字段取值由调用方按类型决定。实测来源: Hadoop 3.4.1

  * 容量调度器: 根就是 schedulerInfo, 子节点在 queues.queue,
    叶子 type=capacitySchedulerLeafQueueInfo
  * 公平调度器: 根在 schedulerInfo.rootQueue, 子节点在 childQueues.queue
    (dict 包一层), 叶子 type=fairSchedulerLeafQueueInfo;
    没有子队列时 childQueues 整个字段缺失 (getChildQueues 空集合返回 null)
"""

CAPACITY = "capacity"
FAIR = "fair"


def scheduler_root(info: dict) -> dict:
    """容量调度器的根就是 schedulerInfo, 公平调度器的根在 rootQueue 里"""
    return (info or {}).get("rootQueue") or info or {}


def scheduler_type_of(info: dict) -> str:
    """按 schedulerInfo.type 判断; 认不出来返回空串(不猜)"""
    kind = str((info or {}).get("type", "")).lower()
    if "capacityscheduler" in kind:
        return CAPACITY
    if "fairscheduler" in kind:
        return FAIR
    return ""


def _children(node: dict) -> list:
    node = node or {}
    return ((((node.get("queues") or {}).get("queue"))
             or ((node.get("childQueues") or {}).get("queue"))
             or []))


def leaf_queues(node: dict, *, root_fallback: bool = True) -> list:
    """
    递归摊平出叶子队列 —— 判据是节点自己的 type, 不靠层数

    :param root_fallback: True 时, "没有 type、没有子节点、但有 queueName" 的节点
        当叶子兜底 —— 公平调度器 0 应用时 root 就是这样, 不兜底会整棵树丢掉;
        False 时直接丢弃 —— 容量调度器的根没有子队列时应返回空, 不能把根当叶子
    """
    if str((node or {}).get("type", "")).endswith("LeafQueueInfo"):
        return [node]
    kids = _children(node)
    if not kids:
        if root_fallback and (node or {}).get("queueName"):
            return [node]
        return []
    out = []
    for child in kids:
        out.extend(leaf_queues(child, root_fallback=root_fallback))
    return out
