"""
检查 Spark OOM

测试：
内核的OOM与yarn OOM 在 removeReason 中是一模一样的，所以无法直接通过 removeReason 来判断是否是内核 OOM。
因此，这里需要通过节点内存滚动采样来判断是否是内核 OOM。

内核 OOM 杀掉的容器, 在 YARN 诊断里和外部 `kill -9` **逐字节相同**
均为：
`Container killed on request. Exit code is 137` + `Killed by external signal`
因为 137 = ExitCode.FORCE_KILLED, NodeManager 的 ContainerLaunch 直接走 "killed" 分支,
然而 信号是谁发的 从来未被记录， 同时 SIGKILL 本身也不携带发送者。
最终导致一致，而无法区分的情况

但 ！ 好在 节点 情况不会隐藏 内核 OOM 死前, 整机内存是满的。

**为什么必须滚动采样**: 这个值是**瞬时**的 —— 容器一死内存就还回去了
(实测 95.19% -> 61.85% -> 55.39%, 两秒内)。等发现 executor 死了再去查,
拿到的是"已经还回去"的正常值, 信号就没了。所以这里自己起一个轻量采样线程,
把各节点的 memUtilization 记进 redis 的定长列表, 判定时 回看死亡时刻之前 的样本。

注意：
节点侧的 dmesg 是持久证据, 但那个要 ssh（非轻量，用于Agent层进行分析）, 不在 REST 的范围内。

"""

import datetime
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from SPO.check_result import ExecutorMetrics, CheckResult
from Tools.log_settings import LogSetting
from Tools.share_check_tool import (proxy, request_get, get_latest_stage_info,
                                    redis_con, rm, yarn_conf)

logger = LogSetting.create(__name__)

_oom_conf = yarn_conf('spark', {}) or {}

# 节点内存利用率超过多少算"被内核 OOM 打满"。
NODE_MEM_SUSPECT_RATIO = float(_oom_conf.get('node_mem_suspect_ratio', 85.0))

# 采样间隔(秒)
NODE_MEM_SAMPLE_INTERVAL = float(_oom_conf.get('node_mem_sample_interval', 5))

# 回看窗口(秒): 从 executor 的 removeTime 往前找多久内的样本
NODE_MEM_LOOKBACK_SECONDS = float(_oom_conf.get('node_mem_lookback_seconds', 90))

# 每个节点保留多少个样本(5 秒一个 -> 240 个约 20 分钟)
NODE_MEM_MAX_SAMPLES = int(_oom_conf.get('node_mem_max_samples', 240))
_NODE_MEM_TTL = 3600  # 1 小时过期


def _node_mem_sample_once() -> None:
    """
    采一次全部节点的内存利用率并追加进 redis

    读失败**必须显式说出来** —— 不能静默跳过, 否则"没采到"和"没超"就分不开了。
    """
    try:
        data = rm.cluster_nodes().data
    except Exception as e:
        logger.error(f"节点内存采样失败(测量无效): {type(e).__name__}: {e}")
        return
    nodes = (data or {}).get("nodes", {}).get("node", [])
    if not nodes:
        logger.warning("节点内存采样失败(测量无效): 响应里没有 nodes")
        return
    now = time.time()
    for n in nodes:
        host = (n.get("id") or "").split(":")[0]
        if not host:
            continue
        sample = {
            "t": round(now, 1),
            # memUtilization 是 NM 实测的整机利用率(= nodePhysicalMemoryMB / 真实 MemTotal),
            "mem_util": n.get("memUtilization"),
            "phys_mb": (n.get("resourceUtilization") or {}).get("nodePhysicalMemoryMB"),
            # 对照用: 按容器分配量算的 YARN 账 —— 内存是在容器账外被吃掉的, 这个值到时候照样正常
            "used_mb": n.get("usedMemoryMB"),
            "containers": n.get("numContainers"),
        }
        key = f"node-mem:{host}"
        try:
            redis_con.lpush(key, json.dumps(sample))
            redis_con.ltrim(key, 0, NODE_MEM_MAX_SAMPLES - 1)
            redis_con.expire(key, _NODE_MEM_TTL)
        except Exception as e:
            logger.error(f"节点内存样本写 redis 失败: {type(e).__name__}: {e}")


def _node_mem_sampler_loop() -> None:
    while True:
        _node_mem_sample_once()
        time.sleep(NODE_MEM_SAMPLE_INTERVAL)


_node_mem_sampler_started = False
_node_mem_sampler_lock = threading.Lock()


def ensure_node_mem_sampler() -> None:
    """确保后台采样线程在跑(每个进程只起一次)"""
    global _node_mem_sampler_started
    with _node_mem_sampler_lock:
        if _node_mem_sampler_started:
            return
        threading.Thread(target=_node_mem_sampler_loop, daemon=True,
                         name="node-mem-sampler").start()
        _node_mem_sampler_started = True
        logger.info(f"已启动节点内存采样线程: 每 {NODE_MEM_SAMPLE_INTERVAL}s 一次, "
                    f"阈值 {NODE_MEM_SUSPECT_RATIO}%(内核 OOM 靠它检出)")


def node_mem_peak_near(host: str, ts: float):
    """
    取 host 这台节点在 ts 之前 NODE_MEM_LOOKBACK_SECONDS 秒内的**最大** memUtilization

    用最大值而不是最接近的那个点: executor 的 removeTime 是驱动侧收到通知的时刻,
    比真实死亡晚 1~2 秒(实测容器已消失时 removeTime 还没到), 而且内核杀之前节点内存
    会先攀升一段时间 —— 回看窗口里的峰值才是"它当时有多满"。

    :return: (peak_ratio, sample) —— **没有任何可用样本时返回 (None, None)**,
             调用方必须把 None 当成"测不到", 不能当成"没超"。
    """
    if not host:
        return None, None
    try:
        raw = redis_con.lrange(f"node-mem:{host}", 0, -1)
    except Exception as e:
        logger.error(f"读节点内存样本失败(测不到): {type(e).__name__}: {e}")
        return None, None
    if not raw:
        return None, None
    lo = ts - NODE_MEM_LOOKBACK_SECONDS
    best, best_sample = None, None
    for item in raw:
        try:
            s = json.loads(item)
        except Exception:
            continue
        t, ratio = s.get("t"), s.get("mem_util")
        if t is None or ratio is None or not (lo <= t <= ts + 5):
            continue
        if best is None or ratio > best:
            best, best_sample = ratio, s
    return best, best_sample


def _remove_time_epoch(executor_info: dict, fallback: float) -> float:
    """executor 的 removeTime(GMT 字符串) -> epoch 秒; 拿不到就用 fallback"""
    raw = executor_info.get("removeTime")
    if not raw:
        return fallback
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fGMT", "%Y-%m-%dT%H:%M:%SGMT"):
        try:
            return datetime.datetime.strptime(raw, fmt).replace(
                tzinfo=datetime.timezone.utc).timestamp()
        except ValueError:
            continue
    return fallback


# 内存超限杀的判据 —— **必须同时覆盖两种真实措辞**, 它们不是同一个字符串:
#
#   ① YARN 原文 (Hadoop 3.4.1 ContainersMonitorImpl.formatErrorMessage):
#        "Container [pid=..,containerID=..] is running 1234B beyond the 'PHYSICAL' memory limit."
#      注意是 "beyond **the 'PHYSICAL'** memory **limit**" —— 有 the、有引号、单数。
#
#   ② Spark 改写后的 (spark-yarn_2.12-3.1.3.jar 里 "Container killed by YARN for
#      exceeding physical memory limits.")。**Spark 会替换掉 YARN 的原文** ——
#      checker 走 `/allexecutors` 拿到的 removeReason 是**这一条**, 不是 ①。
#
# 2026-09-15 实测踩到的坑: 这里原先是 `beyond physical memory limits` —— 照着记忆里的
# 措辞写的, 结果 **① 和 ② 一个都不匹配**, error 档永远报不出来。当时用"构造文本"做的
# 单元验证是**循环论证**(文本照着正则造, 当然能过)。
# 真正把 error 档验通(oom_limit 注入 -> 容器被 YARN 按物理内存超限杀)之后才暴露出来。
# 教训: 判据里的字面量必须来自**抓到的真实样本**, 不能来自记忆。
#
# 分三档的其余部分不变: 有内存专有文本 -> error; 有明确的外部 kill 标记 -> 排除;
# 只有 137、别的都没有 -> 降级 warning(保留召回, 但不再断言是内存问题)。
# 137 = 128+9(SIGKILL), 143 = 128+15(SIGTERM) —— YARN 杀容器是先 SIGTERM、
# 宽限期过了才 SIGKILL, 所以 内存超限杀也可能落在 143 上 (实测到一个 143 的
# 外部杀: "Container exited with a non-zero exit code 143 ... Killed by external signal")。
# 因此不能拿退出码当前置门槛, 真正的判据是诊断文本里的内存专有字样, 退出码只用于降级。
CONTAINER_KILLED = re.compile(r"Container exited with a non-zero exit code (?:137|143)\b")
MEMORY_LIMIT = re.compile(r"beyond the '(?:PHYSICAL|VIRTUAL)' memory limit"     # YARN 原文
                          r"|exceeding (?:physical|virtual) memory limits"      # Spark 改写
                          r"|beyond (?:physical|virtual) memory limits",        # 旧版本兜底
                          re.IGNORECASE)
NOT_OOM = re.compile(r"Killed by external signal|Container killed on request|"
                     r"Executor killed by driver", re.IGNORECASE)


def classify_removed_executor(remove_reason: str):
    """
    按 removeReason 判断被杀 executor 的死因

    抽成纯函数是为了能用真实抓到的文本做验证 —— 这套集群造不出真正的内存超限杀,
    没法端到端验正例, 至少要把判据本身钉死。

    :return: "memory"    YARN 内存超限专有文本, 确定是内存杀
             "uncertain" 只有非零退出码, 死因不明
             None        明确不是内存问题(外部信号 / 驱动侧回收), 或压根没有退出码
    """
    if MEMORY_LIMIT.search(remove_reason):
        return "memory"
    if NOT_OOM.search(remove_reason):
        return None
    if CONTAINER_KILLED.search(remove_reason):
        return "uncertain"
    return None


# 检查运行时的OOM情况
def check_OOM(app_id: str):
    uri = f"{proxy}/proxy/{app_id}/api/v1/applications/{app_id}/allexecutors"
    # 内核 OOM
    ensure_node_mem_sampler()

    all_executors_info = request_get(uri)
    killed_executors = []  # removeReason 里有内存超限文本 —— 报 error
    uncertain_executors = []  # 只有 137、死因不明 —— 报 warning
    suspected_kernel_oom = []  # 被信号杀 + 无内存文本, 但节点当时内存被打满 —— 报 warning
    if not all_executors_info:
        return None
    for executor_info in all_executors_info:
        if not executor_info.get("isActive", False):
            # 被杀死
            remove_reason = executor_info.get("removeReason", "")
            logs = executor_info.get("executorLogs", {})
            metrics = ExecutorMetrics(
                executor_id=executor_info.get("id", ""),
                host=executor_info.get("hostPort", ""),
                stdout_log=logs.get("stdout", ""),
                stderr_log=logs.get("stderr", "")
            )
            # 判定顺序即置信度顺序: 内存文本 > 外部 kill 标记 > 仅凭退出码。
            verdict = classify_removed_executor(remove_reason)
            if verdict == "memory":
                killed_executors.append(metrics)
            elif verdict == "uncertain":
                # 只是被杀, 死因不明 —— 保留线索但不断言是内存
                uncertain_executors.append(metrics)
            elif CONTAINER_KILLED.search(remove_reason):
                # 这里 => "被信号杀了、但没有任何内存文本"(已经被排除了外部 kill 标记 / 驱动侧回收)。
                # 这里查 节点侧证据 : 它死之前那台机器的内存是不是被打满了。
                host = (executor_info.get("hostPort") or "").split(":")[0]
                ts = _remove_time_epoch(executor_info, time.time())
                peak, sample = node_mem_peak_near(host, ts)
                if peak is not None and peak >= NODE_MEM_SUSPECT_RATIO:
                    suspected_kernel_oom.append(
                        {"metrics": metrics, "host": host, "peak": peak, "sample": sample})
        else:
            max_memory = executor_info.get("maxMemory", 0)
            peak_memory = executor_info.get("peakMemoryMetrics", {})
            heap_execution_memory = peak_memory.get("OnHeapExecutionMemory", 0)
            memory_usage_ratio = heap_execution_memory / max_memory if max_memory else 0.0
            if memory_usage_ratio >= 0.9:
                app_jobs = request_get(f"{proxy}/proxy/{app_id}/api/v1/applications/{app_id}/jobs")
                if not app_jobs:
                    return None
                app_run_job = app_jobs[0]
                stage_ids = app_run_job.get("stageIds", [])
                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = {
                        executor.submit(get_latest_stage_info, stage_id, app_id): stage_id
                        for stage_id in stage_ids
                    }
                    for future in as_completed(futures):
                        try:
                            stage_id = futures[future]
                            stage_infos = future.result()
                            if not stage_infos:
                                continue
                            disk_spilled = stage_infos.get("diskBytesSpilled", 0)
                            if disk_spilled > 0:
                                return CheckResult(
                                    type="warning",
                                    describe=f"即将出现OOM，请立马检查阶段 {stage_id}，已经写入磁盘 {disk_spilled / 1024 / 1024:.2f} MB数据",
                                    data=None
                                )
                        except Exception as e:
                            # logger.error(f"检查阶段 {stage_id} 内存情况失败: {e}")
                            print(f"检查阶段 {stage_id} 内存情况失败: {e}")
                            continue

    if killed_executors:
        return CheckResult(
            type="error",
            describe=f"应用{app_id}存在因内存不足被杀的 Executor",
            data=killed_executors
        )
    if suspected_kernel_oom:
        # 节点内存高 ≠ 一定是内核杀的 —— 抢占、缩容
        # 也可能同时发生。证据强度是"节点侧实测 + 死亡时刻吻合", 够报"疑似"不够断言。
        worst = max(suspected_kernel_oom, key=lambda x: x["peak"])
        s = worst["sample"] or {}
        return CheckResult(
            type="warning",
            describe=(f"应用{app_id}有 {len(suspected_kernel_oom)} 个 Executor **疑似被内核 OOM 杀掉**: "
                      f"它们的 removeReason 里没有任何内存文本(内核杀的不留文本), 但所在节点 "
                      f"{worst['host']} 在死亡前实测内存利用率 {worst['peak']:.1f}%"
                      f"(物理 {s.get('phys_mb')}MB, 容器 {s.get('containers')} 个)。"
                      f"注意 YARN 的容器内存账此时可能是正常的 —— 内存被吃在容器账外。"
                      f"建议核查该节点内存占用，或调小 executor 数量/内存"),
            data=[x["metrics"] for x in suspected_kernel_oom]
        )
    if uncertain_executors:
        # 曾经这里一律报 error。有 137 但没有任何内存证据时报 error 会把
        # 节点故障/抢占/手工 kill 全算成 OOM, 所以降级成 warning 并说清依据。
        return CheckResult(
            type="warning",
            describe=f"应用{app_id}有 {len(uncertain_executors)} 个 Executor 被 SIGKILL"
                     f"(退出码 137), 但 removeReason 里没有内存超限文本, 无法断定是内存问题",
            data=uncertain_executors
        )
    return None
