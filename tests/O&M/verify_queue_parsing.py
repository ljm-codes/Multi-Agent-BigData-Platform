"""
验证队列解析: 容量调度器(原有) + 公平调度器(新增), 离线可跑

fixture: tests/test_data/scheduler_3.4.1_leaf.json
  —— Hadoop 3.4.1 实测快照: 提交 MR pi 后动态建出的 root.hadoop 叶子

用法: python "tests/O&M/verify_queue_parsing.py"
"""
import json
import pathlib
import sys

# 导入 Tools.* 之前给 yarn_api_client 打桩, 保证离线可跑
import yarn_api_client
import yarn_api_client.resource_manager as _rm_mod


class _OfflineRM:
    def __init__(self, *args, **kwargs):
        pass


yarn_api_client.ResourceManager = _OfflineRM
_rm_mod.ResourceManager = _OfflineRM

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from SPO.check_result import (CapacitySchedulerLeafQueuePartInfo,
                              FairSchedulerLeafQueuePartInfo)
import Tools.check_resource_abnormal as res
from Tools.yarn_queue import (CAPACITY, FAIR, leaf_queues, scheduler_root,
                              scheduler_type_of)

PASS, FAIL = "✅", "❌"
_results = []


def check(name: str, ok: bool, detail=""):
    _results.append((name, ok))
    print(f"  {PASS if ok else FAIL} {name}")
    if detail:
        print(f"      {detail}")


FIXTURE = PROJECT_ROOT / "tests" / "test_data" / "scheduler_3.4.1_leaf.json"

CAPACITY_SAMPLE = {
    "type": "capacityScheduler",
    "queueName": "root",
    "queues": {"queue": [
        {
            "type": "capacitySchedulerLeafQueueInfo",
            "queueName": "root.a",
            "state": "RUNNING",
            "numPendingApplications": 1,
            "absoluteMaxCapacity": 50.0,
            "absoluteUsedCapacity": 50.0,
            "maxActiveApplications": 1,
            "numActiveApplications": 1,
            "users": {"user": [
                {"username": "u1", "numPendingApplications": 1},
            ]},
        },
    ]},
}

EMPTY_CAPACITY = {"type": "capacityScheduler", "queueName": "root",
                  "queues": {"queue": []}}

FAIR_ROOT_ONLY = {
    "type": "fairScheduler",
    "rootQueue": {
        "queueName": "root",
        "usedResources": {"memory": 0},
        "maxResources": {"memory": 6144},
        "pendingContainers": 0,
        "allocatedContainers": 0,
        "maxApps": 2147483647,
    },
}


def t1_fair_fixture():
    print("\n[1] 公平调度器真实快照 —— root.hadoop 叶子")
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    si = raw["scheduler"]["schedulerInfo"]

    check("schedulerInfo.type 识别为 fair", scheduler_type_of(si) == FAIR)
    check("叶子摊平",
          [q.get("queueName") for q in leaf_queues(scheduler_root(si))]
          == ["root.hadoop"])

    out = res.get_all_queue_info(si, [], FAIR)
    check("显式 fair 解析出 1 条", bool(out) and len(out) == 1)
    q = out[0]
    check("字段名/取值", (
        isinstance(q, FairSchedulerLeafQueuePartInfo)
        and q.queue_name == "root.hadoop"
        and q.state is None
        and q.num_pending_apps == 0
        and q.num_active_apps == 0
        and q.pending_containers == 0
        and q.allocated_containers == 0
        and q.used_memory_mb == 0
        and q.max_memory_mb == 6144
    ), q.model_dump())


def t2_auto_and_config():
    print("\n[2] auto 检测 / 配置选择")
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    si = raw["scheduler"]["schedulerInfo"]

    check("留空时按 schedulerInfo.type 自动识别",
          len(res.get_all_queue_info(si, []) or []) == 1)

    original = res.yarn_conf
    try:
        res.yarn_conf = lambda k, d: "capacity" if k == "scheduler_type" else d
        check("scheduler_type=capacity 优先于自动检测",
              res._configured_scheduler_type(si) == CAPACITY)

        res.yarn_conf = lambda k, d: "auto"
        check("scheduler_type=auto 回落到自动检测",
              res._configured_scheduler_type(si) == FAIR)
    finally:
        res.yarn_conf = original


def t3_capacity_preserved():
    print("\n[3] 容量调度器 —— 原字段和告警文本保持不变")
    out = res.get_all_queue_info(CAPACITY_SAMPLE, [], CAPACITY)
    check("仍解析出 1 条", bool(out) and len(out) == 1)
    q = out[0]
    check("仍是 CapacitySchedulerLeafQueuePartInfo",
          isinstance(q, CapacitySchedulerLeafQueuePartInfo))
    check("原字段", (
        q.queue_name == "root.a"
        and q.state == "RUNNING"
        and q.num_pending_applications == 1
        and q.absolute_used_capacity == 50.0
        and q.num_active_applications == 1
        and q.users[0].username == "u1"
    ), q.model_dump())

    errs = res._capacity_queue_errors(q)
    check("资源上限 + 并发上限各一条", len(errs) == 2)
    check("告警文本不变", (
        errs[0]["describe"] == "队列root.a已用尽弹性资源上限，用户u1已用尽弹性资源上限"
        and errs[1]["describe"] == "队列root.a队列达到应用并发上限，新应用无法启动，用户u1已用尽弹性资源上限"
    ), [e["describe"] for e in errs])

    check("容量调度器根无子队列 -> None(不把根当叶子)",
          res.get_all_queue_info(EMPTY_CAPACITY, [], CAPACITY) is None)


def t4_fair_root_fallback():
    print("\n[4] 公平调度器 0 应用 —— root 兜底")
    out = res.get_all_queue_info(FAIR_ROOT_ONLY, [], FAIR)
    check("解出 root", bool(out) and len(out) == 1
          and out[0].queue_name == "root", out[0].model_dump() if out else None)
    check("root 没有应用数字段时给 0",
          out[0].num_pending_apps == 0 and out[0].num_active_apps == 0)


def t5_fair_errors():
    print("\n[5] 公平调度器告警判据")
    q = FairSchedulerLeafQueuePartInfo(
        queue_name="root.sample", pending_containers=3,
        used_memory_mb=6144, max_memory_mb=6144,
        num_active_apps=10, max_apps=10,
    )
    errs = res._fair_queue_errors(q)
    check("资源上限 + 并发上限各一条", len(errs) == 2,
          [e["describe"] for e in errs])
    check("文本", "仍有3个容器待分配" in errs[0]["describe"]
          and "应用并发上限" in errs[1]["describe"])

    q2 = FairSchedulerLeafQueuePartInfo(
        queue_name="root.hadoop", pending_containers=0,
        used_memory_mb=0, max_memory_mb=6144,
        num_active_apps=1000, max_apps=2147483647,
    )
    check("maxApps 极大时不误报并发上限", res._fair_queue_errors(q2) == [])


class _Resp:
    def __init__(self, data):
        self.data = data


class _FakeRedis:
    def __init__(self):
        self.d = {}

    def get(self, k):
        return self.d.get(k)

    def set(self, k, v):
        self.d[k] = v

    def delete(self, k):
        self.d.pop(k, None)


def t6_end_to_end():
    print("\n[6] end-to-end: check_resource_insufficient 走 fair 分支")
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    si = raw["scheduler"]["schedulerInfo"]
    leaf = si["rootQueue"]["childQueues"]["queue"][0]
    leaf["usedResources"]["memory"] = 6144
    leaf["pendingContainers"] = 3

    class _FakeRM:
        def cluster_scheduler(self):
            return _Resp({"scheduler": {"schedulerInfo": si}})

        def cluster_metrics(self):
            return _Resp({"clusterMetrics": {
                "containersPending": 0, "reservedMB": 0, "availableMB": 4096,
            }})

    old_rm, old_redis, old_conf = res.rm, res.redis_con, res.yarn_conf
    try:
        res.rm = _FakeRM()
        res.redis_con = _FakeRedis()
        res.yarn_conf = lambda k, d: "fair" if k == "scheduler_type" else d
        result = res.check_resource_insufficient()
        check("报出 fair 队列打满",
              result is not None
              and any("已用尽资源上限" in e["describe"] for e in result.data),
              result.data if result else None)
    finally:
        res.rm, res.redis_con, res.yarn_conf = old_rm, old_redis, old_conf


def main():
    t1_fair_fixture()
    t2_auto_and_config()
    t3_capacity_preserved()
    t4_fair_root_fallback()
    t5_fair_errors()
    t6_end_to_end()
    passed = sum(1 for _, ok in _results if ok)
    print(f"\n{passed}/{len(_results)} 通过")
    sys.exit(0 if passed == len(_results) else 1)


if __name__ == "__main__":
    main()
