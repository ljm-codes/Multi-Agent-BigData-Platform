"""
验证本轮修复是否生效 (能跑的部分都实测, 不依赖集群的业务状态)

覆盖:
  1. check_disk_abnormal      UNHEALTHY + 坏盘不再重复记录
  2. check_job_timeout:59     success_jobs 为 None 不再 TypeError
  3. check_job_timeout:91     改用 describe=, CheckResult 不再 ValidationError
  4. check_resource_abnormal  无叶子队列不再 TypeError; getdel 已换 get+delete
  5. check_file_handle        能正确解析 YARN /jmx (需 RM 在线)

用法: python tests/verify_fixes.py
"""
import pathlib
import sys

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

PASS, FAIL, SKIP = "✅", "❌", "⏭️ "
_results = []


def check(name: str, ok: bool, detail: str = ""):
    _results.append((name, ok))
    print(f"  {PASS if ok else FAIL} {name}")
    if detail:
        print(f"      {detail}")


class _Resp:
    def __init__(self, data):
        self.data = data


class _FakeRM:
    def __init__(self, nodes=None, scheduler=None, metrics=None):
        self._nodes = nodes
        self._scheduler = scheduler
        self._metrics = metrics

    def cluster_nodes(self):
        return _Resp({"nodes": {"node": self._nodes or []}})

    def cluster_scheduler(self):
        return _Resp({"scheduler": {"schedulerInfo": self._scheduler or {}}})

    def cluster_metrics(self):
        return _Resp(self._metrics)


# ---------------- 1. check_disk 去重 ----------------
def t1_disk_dedup():
    print("\n[1] check_disk_abnormal —— UNHEALTHY + 坏盘去重")
    import Tools.check_disk_abnormal as disk

    report = "2/4 local-dirs are bad: /hadoop/data1, /hadoop/data2"
    disk.rm = _FakeRM(nodes=[{"id": "node1:44539", "state": "UNHEALTHY",
                              "healthReport": report}])
    result = disk.check_disk()
    data = result.data if result else []
    check("UNHEALTHY + 坏盘 -> 只记录 1 条", len(data) == 1, f"实际 {len(data)} 条")
    check("带的 bad_disk_ratio 字段保留",
          bool(data) and data[0].get("bad_disk_ratio") == "2/4",
          f"data[0]={data[0] if data else None}")

    # 只命中关键词、解析不出坏盘比 -> 仍应报 1 条且无 ratio
    disk.rm = _FakeRM(nodes=[{"id": "node2:44539", "state": "UNHEALTHY",
                              "healthReport": "no space left on device"}])
    data = (disk.check_disk() or type("X", (), {"data": []})).data
    check("UNHEALTHY + 仅关键词 -> 1 条且无 ratio",
          len(data) == 1 and "bad_disk_ratio" not in data[0],
          f"data={data}")

    # RUNNING + 坏盘 -> 仍要报, 带 ratio
    disk.rm = _FakeRM(nodes=[{"id": "node3:44539", "state": "RUNNING",
                              "healthReport": report}])
    data = (disk.check_disk() or type("X", (), {"data": []})).data
    check("RUNNING + 坏盘 -> 1 条且带 ratio",
          len(data) == 1 and data[0].get("bad_disk_ratio") == "2/4",
          f"data={data}")

    # 健康节点 -> 不报
    disk.rm = _FakeRM(nodes=[{"id": "node1:44539", "state": "RUNNING",
                              "healthReport": ""}])
    check("健康节点 -> 返回 None", disk.check_disk() is None)


# ---------------- 2 & 3. check_job_timeout ----------------
def t2_job_timeout_none():
    print("\n[2] check_job_timeout:59 —— success_jobs 为 None 不再崩")
    import Tools.check_job_timeout as jot

    jot.request_get = lambda uri, params=None, timeout=10: None
    try:
        result = jot.check_spark_job_timeout("application_1788067973316_0001")
        check("request_get 全返回 None -> 不抛异常", True, f"返回 {result!r}")
    except Exception as e:
        check("request_get 全返回 None -> 不抛异常", False,
              f"{type(e).__name__}: {e}")


def t3_job_timeout_describe():
    print("\n[3] check_job_timeout:91 —— message= 已改 describe=")
    import Tools.check_job_timeout as jot
    import inspect

    src = inspect.getsource(jot.check_spark_job_timeout)
    check("源码中不再出现 message=", "message=" not in src)
    check("源码中出现 describe=", "describe=" in src)


# ---------------- 4. check_resource_abnormal ----------------
def t4_resource_guard():
    print("\n[4] check_resource_abnormal —— 无叶子队列 + getdel 替换")
    import Tools.check_resource_abnormal as res

    # schedulerInfo 无 queues -> get_all_queue_info 返回 None
    res.rm = _FakeRM(scheduler={"queues": {"queue": []}})
    try:
        result = res.check_resource_insufficient()
        check("无叶子队列 -> 不抛异常", True, f"返回 {result!r}")
    except Exception as e:
        check("无叶子队列 -> 不抛异常", False, f"{type(e).__name__}: {e}")

    import inspect
    src = inspect.getsource(res)
    # 找真实的调用而非注释里提到的名字
    check("redis_con.getdel 调用已移除", "redis_con.getdel(" not in src)
    check("改为 get + delete",
          'redis_con.get("cluster-metrics")' in src
          and 'redis_con.delete("cluster-metrics")' in src)


# ---------------- 5. check_file_handle ----------------
def t5_file_handle():
    print("\n[5] check_file_handle —— 解析 YARN /jmx (需要 RM 在线)")
    import Tools.check_file_handle_abnormal as fh
    from Tools.share_check_tool import manager

    # 直接看真实 JMX 能否被新解析逻辑吃下
    from Tools.share_check_tool import request_get
    file_data = request_get(f"{manager}/jmx",
                            params={"qry": "java.lang:type=OperatingSystem"})
    if not file_data:
        check("RM /jmx 可达", False, "拿不到数据, 跳过 (VM 未开?)")
        return
    beans = file_data.get("beans") or []
    bean = beans[0] if beans else {}
    open_cnt = bean.get("OpenFileDescriptorCount", 0)
    max_cnt = bean.get("MaxFileDescriptorCount", 0)
    check("beans 是非空 list 且含目标 bean",
          isinstance(beans, list) and bool(beans)
          and bean.get("name") == "java.lang:type=OperatingSystem",
          f"OpenFileDescriptorCount={open_cnt} MaxFileDescriptorCount={max_cnt}")
    check("数据非空 -> 旧代码的守卫不再误杀", bool(beans))

    try:
        result = fh.check_file_handle()
        ratio = open_cnt / max_cnt if max_cnt else 0.0
        check("check_file_handle 不抛异常", True,
              f"返回 {result!r} (当前使用率 {ratio:.4f}, 阈值 0.8/0.9)")
    except Exception as e:
        check("check_file_handle 不抛异常", False, f"{type(e).__name__}: {e}")

    # Redis 无 file_handle 记录时, check_times 必须返回 4 元组而非 None
    fh.redis_con.delete("file_handle")
    try:
        r = fh.check_times()
        check("check_times 空记录时返回 4 元组", isinstance(r, tuple) and len(r) == 4,
              f"返回 {r!r}")
    except Exception as e:
        check("check_times 空记录时返回 4 元组", False, f"{type(e).__name__}: {e}")


# ---------------- 6. check_spark_data_skew 档位抢占 ----------------
def _fake_stage(sid: int, records: list):
    """造一个 stage 的 /stages/{id}?details=true 响应(只要 tasks 里的两个字段)"""
    return {
        "stageId": sid,
        "attemptId": 0,
        "status": "COMPLETE",
        "tasks": {
            str(i): {"taskId": i, "status": "SUCCESS",
                     "taskMetrics": {"shuffleReadMetrics": {"recordsRead": r},
                                     "jvmGcTime": 100}}
            for i, r in enumerate(records)
        },
    }


def t6_skew_severity_not_preempted():
    """
    「轻微倾斜先跑完，把严重倾斜挡在后面」—— 这是 as_completed 顺序决定的，线上很难稳定复现，
    所以离线构造顺序: stage 1 秒回(比值 6，只到 warning 线)、stage 2 慢 0.4 秒(比值 25，过 error 线)。
    """
    print("\n[6] check_spark_data_skew —— warning 不能抢占 error")
    import time as _t
    import Tools.spark.check_spark_data_skew as skew

    def fake_get_latest_stage_info(stage_id, app_id):
        if stage_id == 1:
            return _fake_stage(1, [6000] + [1000] * 9)       # max/median = 6.0  -> warning
        _t.sleep(0.4)                                        # 让它"晚完成"
        return _fake_stage(2, [25000] + [1000] * 9)          # max/median = 25.0 -> error

    skew.get_latest_stage_info = fake_get_latest_stage_info
    result = skew.check_stage_summary([1, 2], "application_fake")

    check("两个 stage 都超 warning 线时, 结果取 error 档",
          result is not None and result.type == "error",
          f"实际 type={getattr(result, 'type', None)!r}  describe={getattr(result, 'describe', '')!r}")
    check("报的是最严重的那个 stage(2), 不是先完成的那个(1)",
          result is not None and "阶段 2" in (result.describe or ""),
          f"describe={getattr(result, 'describe', '')!r}")

    # 反向对照: 只有轻微倾斜时, 仍然是 warning(别把 warning 一起修坏了)
    def only_mild(stage_id, app_id):
        return _fake_stage(stage_id, [6000] + [1000] * 9)

    skew.get_latest_stage_info = only_mild
    result = skew.check_stage_summary([1], "application_fake")
    check("只有轻微倾斜 -> 仍是 warning",
          result is not None and result.type == "warning",
          f"实际 type={getattr(result, 'type', None)!r}")


# ---------------- 7. 内核 OOM 的节点侧判据 ----------------
# 这段 removeReason 是从 2026-09-14 的真实抓包里抄的(内核 OOM 那轮, app ..._0014 的
# executor 2 / node3)。注意它和外部 `kill -9` 那轮**逐字节相同** —— 判据分不开才对。
_KERNEL_OOM_REMOVE_REASON = (
    "Container from a bad node: container_1789381857190_0014_01_000003 on host: node3. "
    "Exit status: 137. Diagnostics: [2026-09-14 21:43:59.549]Container killed on request. Exit code is 137\n"
    "[2026-09-14 21:43:59.549]Container exited with a non-zero exit code 137. \n"
    "[2026-09-14 21:43:59.550]Killed by external signal\n."
)


def t7_kernel_oom_by_node_memory():
    """
    内核 OOM 的判据: removeReason 上分不开, 靠**节点侧内存实测值**区分

    正例 = 内核 OOM 那轮 node3 死前的实测值(95.19%, 3109/3267MB)
    反例 = 同一台机器被 ssh kill -9 那轮(全程最大 57.23%)
    两者用的是**同一段 removeReason**。
    """
    print("\n[7] check_spark_OOM —— 内核 OOM 靠节点内存分辨")
    import Tools.spark.check_spark_OOM as oom

    dead = {"id": "2", "hostPort": "node3:45579", "isActive": False,
            "removeTime": "2026-09-14T13:43:59.549GMT",
            "removeReason": _KERNEL_OOM_REMOVE_REASON, "executorLogs": {}}

    oom.ensure_node_mem_sampler = lambda: None        # 别真去起采样线程

    # 0) 先验**查表逻辑本身**(真 redis + 回看窗口), 再造一个假的节点样本
    import json as _json
    import time as _time
    now = _time.time()
    key = "node-mem:testhost"
    try:
        oom.redis_con.delete(key)
        # 窗口内(5 秒前)一个高值 + 窗口外(1 小时前)一个更高值 —— 后者不能被算进来
        oom.redis_con.lpush(key, _json.dumps({"t": now - 5, "mem_util": 95.19,
                                              "phys_mb": 3109, "containers": 2}))
        oom.redis_con.lpush(key, _json.dumps({"t": now - 3600, "mem_util": 99.9}))
        peak, _ = oom.node_mem_peak_near("testhost", now)
        check("查表: 取回看窗口内的最大值(窗口外的不算进来)", peak == 95.19, f"peak={peak!r}")
        peak, _ = oom.node_mem_peak_near("no-such-host", now)
        check("查表: 没有样本 -> (None, None), 即'测不到'", peak is None, f"peak={peak!r}")
    except Exception as e:
        check("查表逻辑(需要 redis 在线)", False, f"{type(e).__name__}: {e}")
    finally:
        try:
            oom.redis_con.delete(key)
        except Exception:
            pass

    oom.request_get = lambda uri, params=None, timeout=10: [dict(dead)]

    # 1) 节点内存被打满 -> 报"疑似内核 OOM"
    oom.node_mem_peak_near = lambda host, ts: (95.19, {"phys_mb": 3109, "containers": 2})
    r = oom.check_OOM("application_fake")
    check("节点内存被打满(95.19%) -> 报出疑似内核 OOM",
          r is not None and r.type == "warning" and "内核 OOM" in (r.describe or ""),
          f"type={getattr(r, 'type', None)!r} describe={(getattr(r, 'describe', '') or '')[:90]!r}")

    # 2) 同一段 removeReason, 但节点内存正常(外部 kill -9 那轮的实测值) -> 维持不报
    oom.node_mem_peak_near = lambda host, ts: (57.23, {"phys_mb": 1844, "containers": 1})
    r = oom.check_OOM("application_fake")
    check("节点内存正常(57.23%) -> 不报(和外部 kill 分得开)", r is None,
          f"实际 {(getattr(r, 'describe', None))!r}")

    # 3) 测不到(没有任何样本) -> 不能瞎报, 但也不能说"没超"
    oom.node_mem_peak_near = lambda host, ts: (None, None)
    r = oom.check_OOM("application_fake")
    check("没有节点样本 -> 不报(测不到 != 没超)", r is None,
          f"实际 {(getattr(r, 'describe', None))!r}")


# ---------------- 8. 内存超限文本的两种真实措辞 ----------------
# 2026-09-15 把 `error` 档真正验通(oom_limit 注入 -> 容器被 YARN 按物理内存超限杀,
# app ..._0001)之后才暴露的 bug: 原来的正则只认 `beyond physical memory limits`,
# 而**两种真实文本一个都不匹配** —— 所以 error 档永远报不出来。
#
# 这两条字面量都来自**抓到的真实来源**, 不是照记忆写的:
#   ① YARN 原文  = Hadoop 3.4.1 ContainersMonitorImpl.formatErrorMessage 的格式串
#   ② Spark 改写 = spark-yarn_2.12-3.1.3.jar 常量池里抓出来的,
#      也是 `/allexecutors` 的 removeReason 里**实际**出现的那条
_SPARK_EXCEED_REASON = (
    "Container killed by YARN for exceeding physical memory limits. "
    "1.2 GB of 1 GB physical memory used. Consider boosting spark.executor.memoryOverhead."
)
_YARN_BEYOND_REASON = (
    "Container [pid=12345,containerID=container_1_1_01_000002] is running 1234567B "
    "beyond the 'PHYSICAL' memory limit. Current usage: 1.2 GB of 1 GB physical memory used. "
    "Killing container."
)


def t8_memory_limit_texts():
    """
    内存超限文本的分档 —— 关键是**真实措辞**两种都要认

    ⚠️ 这个测试的价值全在样本真实性上。原先那版用"构造文本"验判据是**循环论证**
    (文本照着正则造, 当然能过), 所以才让 error 档一直报不出来而没人发现。
    这里两条正例都是抓来的/从格式串还原的, 不是编的。
    """
    print("\n[8] check_spark_OOM —— 内存超限文本的两种真实措辞")
    from Tools.spark.check_spark_OOM import classify_removed_executor as c

    check("Spark 改写的文本 -> memory(本次实测抓到的那条)",
          c(_SPARK_EXCEED_REASON) == "memory", f"{c(_SPARK_EXCEED_REASON)!r}")
    check("YARN 原文(beyond the 'PHYSICAL') -> memory",
          c(_YARN_BEYOND_REASON) == "memory", f"{c(_YARN_BEYOND_REASON)!r}")
    check("YARN 原文的 virtual 变体 -> memory",
          c(_YARN_BEYOND_REASON.replace("PHYSICAL", "VIRTUAL")) == "memory")
    # 反向: 别把外部 kill 一起认成内存 —— 这几条也是实测抓到的真实文本
    check("外部 kill -9 的真实文本 -> 排除",
          c("Container killed on request. Exit code is 137\n"
            "Container exited with a non-zero exit code 137. \n"
            "Killed by external signal\n") is None)
    check("驱动侧回收 -> 排除", c("Executor killed by driver.") is None)
    check("只有退出码 -> 降级 uncertain",
          c("Container exited with a non-zero exit code 137") == "uncertain")


# ---------------- 9. check_spark_OOM P2 的「测不到」出口 ----------------
def _fake_allexecutors(on_heap_exec: int, total_tasks: int, max_memory: int = 434031820):
    """造一个 /allexecutors 响应: 只有一个 active executor(只带判定用到的字段)"""
    return [{
        "id": "1", "hostPort": "node2:44705", "isActive": True,
        "totalTasks": total_tasks, "maxMemory": max_memory,
        "peakMemoryMetrics": {"OnHeapExecutionMemory": on_heap_exec},
    }]


def t9_p2_sampling_blind():
    """
    P2 的「测不到」: executor 跑过任务、执行内存峰值却读到 0 时, 不能静默 return None

    门槛用**不受心跳采样影响**的硬证据 —— 阶段溢写。四种组合都要验:
      采样为 0 + 有溢写   -> 报 warning, 且措辞是"判定无效"而不是"即将出现OOM"
      采样为 0 + 没有溢写 -> 不报(只打日志, 不进告警队列)
      采样有值但没过 0.9  -> 不报 —— 这是"真的没超", 不是"没测到"
      刚起来的空闲 executor -> 不报(没跑过任务就不该算"测不到")
    另外要防住: 原来的 P2 正例(水位 >= 0.9 且 有溢写)不能被这次重构改坏。
    """
    print("\n[9] check_spark_OOM —— P2「测不到」要可见(也不能把原判据改坏)")
    import Tools.spark.check_spark_OOM as oom

    oom.ensure_node_mem_sampler = lambda: None
    spill_yes = lambda sid, app_id: {"diskBytesSpilled": 320 * 1024 * 1024}   # noqa: E731
    spill_no = lambda sid, app_id: {"diskBytesSpilled": 0}                    # noqa: E731

    def with_executor(on_heap_exec, total_tasks):
        oom.request_get = lambda uri, params=None, timeout=10: (
            [{"jobId": 1, "stageIds": [3]}] if uri.endswith("/jobs")
            else _fake_allexecutors(on_heap_exec, total_tasks))

    # 1) 采样为 0 + 有溢写 -> 报, 而且是"判定无效"的措辞
    with_executor(0, 1134)
    oom.get_latest_stage_info = spill_yes
    r = oom.check_OOM("application_fake")
    check("采样为 0 + 有溢写 -> 报 warning",
          r is not None and r.type == "warning",
          f"实际 type={getattr(r, 'type', None)!r}")
    check("措辞是「测不到/判定无效」, 不能假装「即将出现OOM」",
          r is not None and "测不到" in (r.describe or "")
          and "即将出现OOM" not in (r.describe or ""),
          f"describe={(getattr(r, 'describe', '') or '')[:90]!r}")
    check("data 里带上了是哪个 executor / 哪个阶段",
          r is not None and isinstance(r.data, list) and r.data
          and r.data[0].get("stage_id") == 3 and r.data[0].get("executor_id") == "1",
          f"data={r.data if r else None}")

    # 2) 采样为 0 + 没有溢写 -> 只打日志, 不进队列(否则几乎每个健康应用都报)
    oom.get_latest_stage_info = spill_no
    r = oom.check_OOM("application_fake")
    check("采样为 0 + 没有溢写 -> 不报(只打日志)", r is None,
          f"实际 {(getattr(r, 'describe', None))!r}")

    # 2b) executor 拿得到、但作业列表取不到 -> 也不能报, 而且日志里必须说"测量无效",
    #     不能印成"没有阶段溢写"(那就是把测量失败静默成没有数据)
    oom.request_get = lambda uri, params=None, timeout=10: (
        None if uri.endswith("/jobs") else _fake_allexecutors(0, 1134))
    r = oom.check_OOM("application_fake")
    check("作业列表取不到 -> 不报(测不到 != 没超)", r is None,
          f"实际 {(getattr(r, 'describe', None))!r}")

    # 3) 采样是几百 KB 级的噪声(短任务作业的实测值) -> 仍然算"没采到"
    #    门槛不能用"精确等于 0"就是这一条逼出来的: 现实中读到的是 64KB 这种非零小数,
    #    卡等于 0 的话这个判断几乎永不触发(见 P2_SAMPLING_BLIND_RATIO 的实测依据)。
    with_executor(65536, 1134)          # 实测值: 64KB / 434MB = 0.015%
    oom.get_latest_stage_info = spill_yes
    r = oom.check_OOM("application_fake")
    check("采样 64KB(池子的 0.015%) + 有溢写 -> 仍算「测不到」, 要报",
          r is not None and r.type == "warning" and "测不到" in (r.describe or ""),
          f"实际 {(getattr(r, 'describe', None))!r}")

    # 3b) 采样确实是"量出来了"(远超 1% 门槛)但没过 0.9 -> 这是"真的没超"
    with_executor(100 * 1024 * 1024, 1134)      # 100/434 = 0.23
    r = oom.check_OOM("application_fake")
    check("采样 100MB(池子的 23%) -> 不报(这是「真的没超」, 不是「没测到」)", r is None,
          f"实际 {(getattr(r, 'describe', None))!r}")

    # 4) 刚起来、还没跑过任务的 executor -> 不该算"测不到"
    with_executor(0, 0)
    r = oom.check_OOM("application_fake")
    check("空闲 executor(没跑过任务) -> 不报", r is None,
          f"实际 {(getattr(r, 'describe', None))!r}")

    # 5) 原判据回归: 水位 >= 0.9 且有溢写 -> 仍报「即将出现OOM」
    with_executor(400 * 1024 * 1024, 1134)      # 400/434 = 0.92
    oom.get_latest_stage_info = spill_yes
    r = oom.check_OOM("application_fake")
    check("水位 0.92 + 有溢写 -> 仍报「即将出现OOM」(原判据没被改坏)",
          r is not None and "即将出现OOM" in (r.describe or ""),
          f"describe={(getattr(r, 'describe', '') or '')[:80]!r}")


if __name__ == "__main__":
    print("=" * 72)
    print("修复验证")
    print("=" * 72)
    for fn in (t1_disk_dedup, t2_job_timeout_none, t3_job_timeout_describe,
               t4_resource_guard, t5_file_handle, t6_skew_severity_not_preempted,
               t7_kernel_oom_by_node_memory, t8_memory_limit_texts,
               t9_p2_sampling_blind):
        try:
            fn()
        except Exception as e:
            check(f"{fn.__name__} 执行失败", False, f"{type(e).__name__}: {e}")

    ok = sum(1 for _, v in _results if v)
    total = len(_results)
    print("\n" + "=" * 72)
    print(f"结果: {ok}/{total} 通过")
    print("=" * 72)
    sys.exit(0 if ok == total else 1)
