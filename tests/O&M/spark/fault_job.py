#!/usr/bin/env python3
"""
Spark 故障注入作业 —— 给 Tools/spark/ 下的 checker 造真实故障

和 tests/flink/FaultJob.java 是同一套思路: 作业先在集群上制造某种异常, 再保持
RUNNING(--hold-seconds), 期间在 Windows 侧跑 checker, 看能不能检出来。

用 PySpark 而不是编译 Java jar: 集群三台机器的
/export/server/miniconda3/envs/spark_study/bin/python3 都自带 pyspark 3.1.3,
不用构建, 改故障只需要改这个文件。

用法(在 node1 上, 见 submit.sh):
    spark-submit --master yarn --deploy-mode cluster fault_job.py --fault skew --hold-seconds 300

设计约束(从 checker 的判定逻辑倒推, 不是随意加的):
 1. 应用必须保持 RUNNING —— checker 走 YARN 代理取数, 应用一结束代理就 404。
    所以故障动作结束后要 hold。
 2. 故障要落在 jobs[0] 上 —— 除 netfail 外, 四个 checker 都只取 `jobs[0].stageIds`。
    所以每个故障模式只触发一个 action(一个 job)。
 3. 驱动侧异常必须自己吞掉 —— 否则 driver 退出, 应用变 FINISHED, checker 没得看。

已实测的两个 PySpark 约束:
 - `sc._jvm` **不能进闭包**: cloudpickle 会报 "'JavaPackage' object is not callable",
   所以任务里没法直接调 JVM。需要 Java 异常名的故障只能用等价文本(Python 异常
   类名/消息里带上同样的字符串), 见 fault_cls / fault_fmt 的说明。
 - `partitionBy(n, func)` 用自定义 Python 分区函数可用, 分区完全可控 ——
   倾斜故障靠它才能保证每个分区都有数据(用 hash 分区会有空分区, 中位数变 0 被跳过)。
"""
import argparse
import os
import socket
import subprocess
import sys
import threading
import time


def log(msg: str) -> None:
    print(f"[fault_job] {msg}", flush=True)


# ---------------------------------------------------------------- 工具

def kill_executors(hosts, tag: str) -> None:
    """
    从 driver 通过 ssh 杀掉各节点的 executor JVM, 模拟 executor/节点丢失

    shuffle 数据在 executor 本地盘上(该集群 aux-services 只有 mapreduce_shuffle,
    没有 Spark 外部 shuffle 服务), 杀掉 executor 等于 shuffle 数据一起没了 ——
    下游 reduce 阶段取数时就会拿 FetchFailedException。
    """
    # 不能用 jps: 非交互 ssh 的 PATH 里没有 $JAVA_HOME/bin, 会 "jps: command not found"
    # (而且 ssh 的退出码仍是 0, 静默失败)。pgrep 在 /usr/bin, 按完整命令行匹配类名。
    # 先把 pid 打出来再杀 —— 日志里要留「确实杀到了」的证据。
    #
    # 模式首字母用 [C] 包起来不是强迫症: pgrep -f 匹配的是**整个命令行**, 而这条命令
    # 自己的命令行里就含 "CoarseGrainedExecutorBackend", 于是它会匹配到自己 ——
    # 实测把执行它的那个 shell 的 pid 一起打了出来, 而 kill -9 $pids 会先杀 executor
    # (pid 小)、最后把自己所在的 shell 杀掉。表现:
    #   * 驱动所在的那台永远报 rc=-9(那台上连本地 ssh 客户端都被匹配进去、被 SIGKILL),
    #     另外两台报 rc=255(远程 shell 被杀、连接断掉), 且 -9 那台连 pids 都打不出来;
    #   * 杀到哪些 pid 全靠 pid 大小顺序, 不可靠。
    # 用 [C] 之后模式串本身不再匹配自己, 执行器的 YarnCoarseGrainedExecutorBackend 照旧命中。
    cmd = ('pids=$(pgrep -f "[C]oarseGrainedExecutorBackend" | tr "\\n" " "); '
           'echo "pids=$pids"; [ -n "$pids" ] && kill -9 $pids; true')
    for host in hosts:
        try:
            r = subprocess.run(
                ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", host, cmd],
                capture_output=True, text=True, timeout=20,
            )
            log(f"[{tag}] kill {host} -> rc={r.returncode} {r.stdout.strip()[:100]} {r.stderr.strip()[:120]}")
        except Exception as e:
            log(f"[{tag}] kill {host} 失败: {type(e).__name__}: {e}")


def executor_hosts(sc) -> list:
    """
    现场问 Spark: 现在哪些机器上**真的**有 executor

    为什么需要它 —— 2026-09-15 踩的坑: netfail 原先在 submit.sh 里**写死**
    `--kill-hosts node3`, 而集群容量从 8192 改成 2048 之后, AM(1024) 独占一台、
    两个 executor(2048) 落到另外两台 —— node3 恰好成了**只剩驱动的那台**。
    于是那次注入杀了个寂寞, FetchFailedException 一次都没构造出来, job 正常 SUCCEEDED。
    整批看下来**像是 checker 漏检, 其实是注入没生效** —— 这正是最该避免的混淆。

    主机的落点会随容量/调度/动态分配变化, 所以不能写死, 要现场问。

    :return: ['node1', 'node2', ...] (去重后的主机名, 不含端口)
    """
    hosts = []
    try:
        # getExecutorMemoryStatus() 的 key 形如 "node1:41654"
        jmap = sc._jsc.sc().getExecutorMemoryStatus()
        it = jmap.keySet().iterator()
        while it.hasNext():
            hosts.append(str(it.next()).rsplit(":", 1)[0])
    except Exception as e:
        log(f"查 executor 主机失败: {type(e).__name__}: {e}")
    # 驱动自己那台不算 —— 杀了它等于把应用干掉, 而且我们要的是"活着的 executor
    # 去发现别人的 shuffle 取不到", 驱动死了就没人发现了
    me = socket.gethostname()
    return sorted({h for h in hosts if h and h != me})


def start_killer(hosts, interval: float, tag: str, times: int = 0) -> threading.Thread:
    """
    后台杀 executor。times=0 表示一直杀到进程退出。

    反复杀是为了对付重算: 只杀一次的话, Spark 重新拉起 executor 后重算 map 阶段
    就可能成功, 最新的 stage attempt 变成 SUCCEEDED, checker 反而看不到失败
    (它取的是最新一次 attempt)。

    但杀多了会撞上 Spark 的 executor 失败上限
    (spark.yarn.max.executor.failures, 默认 max(2*executor数, 3)), 应用直接 FAILED,
    checker 就没得看了 —— 所以只取一次证据的场景(netfail 之外)传 times=1。
    """
    stop = threading.Event()

    def loop():
        n = 0
        while not stop.is_set():
            kill_executors(hosts, tag)
            n += 1
            if times and n >= times:
                return
            stop.wait(interval)

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    return t


def skew_partition_func(key: str) -> int:
    """热点 key 落 0 号分区, 冷 key 均摊到其余分区 —— 分区记录数完全可控"""
    if key == "hot":
        return 0
    return 1 + (int(key[1:]) % (SKEW_PARTITIONS - 1))


SKEW_PARTITIONS = 8


def build_skew_data(n: int, hot_records: int, cold_records: int):
    data = ["hot"] * hot_records
    for i in range(n - 1):
        data.extend([f"c{i}"] * cold_records)
    return data


# ---------------------------------------------------------------- 各故障模式

def fault_normal(sc, args):
    """
    健康基线 —— 所有 checker 都应当返回 None

    故意跑两个 job: 用来确认 /jobs 返回的顺序(jobs[0] 到底是最早还是最新的 job)。
    四个 checker 都只取 jobs[0], 顺序搞错的话它们看的就不是「当前这个 job」。
    """
    base = sc.parallelize(range(args.records), args.partitions).map(lambda x: x * 2)
    # 顺序有讲究: /jobs 是新的在前, jobs[0] 是最后跑的那个 job。四个 checker 都只看
    # jobs[0], 所以要把「有 shuffle 的 job」放在最后, 基线才有意义 —— 否则 checker
    # 看的是一个没有 shuffle 的 count 阶段, 返回 None 说明不了任何问题。
    log(f"normal job0(count) 完成: {base.count()} 条")
    total = base.map(lambda x: (x % args.partitions, x)).reduceByKey(lambda a, b: a + b).values().sum()
    log(f"normal job1(reduceByKey) 完成: {total} 条")


def fault_skew(sc, args):
    """
    数据倾斜 —— 0 号分区的记录数是其余分区的 hot/cold 倍

    目标 checker: check_spark_data_skew
    判定条件是 shuffle 读记录数的 max/median > 5, 再用 jvmGcTime 的比值决定
    error 还是 warning。
    """
    global SKEW_PARTITIONS
    SKEW_PARTITIONS = args.partitions
    n = args.partitions
    data = build_skew_data(n, args.hot_records, args.cold_records)
    pairs = sc.parallelize(data, n).map(lambda k: (k, 1)).partitionBy(n, skew_partition_func)
    counts = pairs.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
    median = sorted(counts)[n // 2]
    log(f"skew 各分区记录数: {counts}  max/median={max(counts) / median if median else 'inf'}")


def fault_stuck(sc, args):
    """
    任务卡住不推进 —— 活跃任务数、完成任务数长时间不变

    目标 checker: check_spark_job_timeout (靠 redis 记录前后两次的任务数快照)
    作业会一直 RUNNING 到 --stuck-seconds 结束, 所以不另外 hold。
    """
    # 先跑一个能成功的 job: check_spark_job_timeout 依赖 success_jobs 非空, 否则
    # check_tasks_num_is_run 永远返回 False(它把「一个 success job 都没有」当成
    # 应用刚开始)。只挂住第一个 job 的应用永远不会被这个 checker 报出来。
    log(f"stuck: 先跑一个正常 job -> {sc.parallelize(range(1000), 2).map(lambda x: x + 1).count()} 条")

    def hang(_):
        time.sleep(args.stuck_seconds)
        return [1]

    log(f"stuck: {args.partitions} 个任务各挂起 {args.stuck_seconds}s")
    sc.parallelize(range(args.partitions), args.partitions).mapPartitions(hang).count()


def fault_oom(sc, args):
    """
    executor 容器超过 YARN 物理内存上限, 被 NodeManager 杀掉(退出码 137)

    目标 checker: check_spark_OOM 的「因内存不足被杀的 Executor」分支。

    做法是在 Python worker 里逐步申请大块内存: 分块 + 小睡是为了让 RSS 缓慢爬升,
    给 NodeManager 的内存监控线程(默认 3s 一次)留出发现时间 —— 一次性申请大块
    容易先触发内核 OOM killer, worker 单独被杀而容器还活着, 退出码就不是 137 了。

    注意: Python worker 的内存算在容器 RSS 里, 但不在 JVM 堆里, 所以撑爆容器只能
    靠它。容器上限 = --executor-memory + overhead(再按 YARN 最小分配单元向上取整):
    executor-memory=512m 时约 1GB, 默认申请 1400MB 足够越界。
    """
    def rss_mb(pid):
        try:
            for line in open(f"/proc/{pid}/status"):
                if line.startswith("VmRSS"):
                    return int(line.split()[1]) // 1024
        except Exception:
            return -1
        return -1

    def blow_up(_):
        chunks = []
        for i in range(args.blow_mb // 32):
            buf = bytearray(32 * 1024 * 1024)
            for off in range(0, len(buf), 4096):   # 逐页触碰, 否则只是虚拟内存
                buf[off] = 1
            chunks.append(buf)
            if i % 4 == 3:
                # 把 python worker 自己和 JVM(容器主进程)的 RSS 都打出来当证据
                log(f"  已申请 {(i + 1) * 32} MB  python_rss={rss_mb(os.getpid())}MB "
                    f"jvm_rss={rss_mb(os.getppid())}MB")
                time.sleep(0.5)
        log(f"  申请完成, 保持 {args.oom_hold}s 不释放 "
            f"(python_rss={rss_mb(os.getpid())}MB)")
        time.sleep(args.oom_hold)
        return [len(chunks)]

    log(f"oom: 把 executor 容器撑到约 {args.blow_mb} MB, 并保持 {args.oom_hold}s")
    sc.parallelize(range(args.partitions * 2), args.partitions * 2).mapPartitions(blow_up).count()


def fault_oom_kill9(sc, args):
    """
    executor 容器被 SIGKILL 杀掉 —— 拿到真实的「退出码 137」诊断文本

    目标 checker: check_spark_OOM 的「因内存不足被杀的 Executor」分支。

    为什么不只靠上面那个内存撑爆的注入: 实测在 PySpark 下撑不爆 —— python worker
    的内存不计入容器的进程树(pyspark.daemon 会 setpgid 到新进程组, 而 YARN 的
    procfs 进程树按进程组统计), 900MB 分配 + 保持 90s 都没触发 pmem 检查。
    这里改用 kill -9 直接杀 JVM: 容器退出码同样是 137, 走的是 YARN 同一套
    「非零退出码」诊断路径, 拿到的 removeReason 文本和真实内存杀一致。
    """
    def hold(_):
        time.sleep(args.oom_hold)
        return [1]

    # 只杀一次: 证据(被杀的 executor + 137 诊断)拿到就够了, 反复杀会撞
    # spark.yarn.max.executor.failures 把应用直接干掉
    log(f"oom_kill9: 起 {args.partitions} 个任务挂住, {args.kill_interval}s 后杀掉 executor")
    start_killer(args.kill_hosts.split(","), args.kill_interval, "oom_kill9", times=1)
    sc.parallelize(range(args.partitions), args.partitions).mapPartitions(hold).count()


def fault_oom_limit(sc, args):
    """
    executor 容器超过**它自己的** YARN 内存上限, 被 NodeManager 杀掉

    目标: `check_spark_OOM` 的 **error 档** —— 它认的是 removeReason 里那句内存专有文本
    "is running beyond physical memory limits"(见 Tools/spark/check_spark_OOM.py 的
    MEMORY_LIMIT 正则)。这句文本**由 NodeManager 的 ContainersMonitorImpl 产生, 和
    cgroup 无关**, 只要容器自己的 RSS 超过它自己的额度就会出现。

    本集群在此之前**从没产生过**这句文本(NM 日志里 "beyond physical" 0 条), 原因不是
    监控没开 —— 实测 `yarn.nodemanager.pmem-check-enabled=true`(yarn-default, 未被覆盖)。
    真正的原因是**超卖发生在节点层、不在容器层**: 从 NM 日志数出来的容器额度是
    `memory:2048, vCores:2`(1g 堆 + 384m overhead = 1408 向上取整到最小分配单元 1024),
    4 个容器 = 8192MB 正好吃满 YARN 对外宣称的 8192, 而 node2/3 物理只有 3345MB。
    单节点塞 2 个 2048 容器 → 4096 > 3345, **但每个容器自己都没超过 2048** ——
    容器的监控没有理由出手, 于是由**内核**兜底, removeReason 里没有任何内存文本。

    为什么不能用 python worker 吃内存: **已实测证伪** —— pyspark.daemon 把 worker
    setpgid 到新进程组, 而 YARN 的 procfs 进程树**按进程组**统计, 所以 worker 不在容器的
    统计范围里(900MB 分配 + 保持 90s 都没触发 pmem 检查)。详见 fault_oom_kill9 的 docstring。

    所以唯一在容器进程组内、又能把 RSS 撑过额度的, 就是 **executor JVM 自己**。
    JVM 的 RSS = 堆用量 + 非堆(~300MB: metaspace/code cache/线程/GC), 而:

      * **堆被 `-Xmx` 卡死, 不可能超** —— 所以唯一的旋钮是 `spark.executor.memoryOverhead`
        (它和 executor-memory 一起决定容器额度)。默认 overhead(384m)恰好盖住非堆的
        ~300MB, 这是 Spark 的设计意图 —— **默认参数下永远撑不爆**;
      * 把 overhead 压到 0, 容器额度就掉到 **1024MB**, 而 RSS 仍要 ~1300MB, 必然越界;
      * 光有额度还不够, **堆要真的用起来**: 用 `collect_list`(ObjectHashAggregateExec)
        把数据收进普通 Java 对象 —— 它不经 TaskMemoryManager、不会被溢写, 只会涨堆。
        注意 value 必须**非常量**, 否则会被折叠成同一个对象、堆根本不涨(见 fault_spill 坑三)。

    **曾经试过、已被证伪的做法**(别再走): 在 `spark.executor.extraJavaOptions` 里再给一个
    `-Xmx` 覆盖掉 Spark 那个。实测 Spark 直接拒绝启动 ——
        java.lang.Exception: spark.executor.extraJavaOptions is not allowed to specify
        max heap memory settings (was '-Xmx1600m'). Use spark.executor.memory instead.
    这是 `SparkConf.validateSettings` 里的硬校验, 应用 30 秒就 FAILED(退出码 13)。

    真实对应: 生产上就是 memoryOverhead 给小了 —— 教科书级的
    "container running beyond physical memory limits"。

    也正因为是**容器层**超限(不是节点层), 节点还有余量, 内核轮不到出手。
    """
    from pyspark.sql import SparkSession, functions as F

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(args.partitions))
    # 非常量: 常量会被折叠, collect_list 的缓冲区里全是同一个对象的引用, 堆不涨。
    # 这里要的恰恰是**每行一个不同的 2KB 字符串**真的驻留到堆里。
    df = (spark.range(0, args.records, 1, args.map_partitions)
          .withColumn("v", F.concat(
              F.repeat(F.lit("x"), max(args.spill_value_bytes - 8, 1)),
              F.lpad(F.col("id").cast("string"), 8, "0"))))
    # **组数要适中, 且 shuffle 分区必须是 1** —— 这两个都是踩过坑才定下来的:
    #
    # * 全部数据进**一个** group 会炸: collect_list 产出的是一行**巨大的** list,
    #   而 AggregationIterator.generateResultProjection 要把整行拼成一个 UnsafeRow
    #   (BufferHolder.grow), 投影那一下就要一次性申请整行大小。实测:
    #       java.lang.OutOfMemoryError: Java heap space
    #         at ...codegen.BufferHolder.grow
    #         at ...AggregationIterator.generateResultProjection
    #         at ...PythonRDD$.writeIteratorToStream
    #   聚合缓冲区本身没事, 是**输出行**太大 —— 所以按量分成多组, 每组只留几 MB。
    #
    # * 但组数也不能超过 spark.sql.objectHashAggregate.sortBased.fallbackThreshold
    #   (默认 128): 超了会退化成排序聚合、把数据写到**磁盘**, 堆就不涨了。
    #   这里显式抬高阈值, 免得组数与默认值撞上(数值本身不是重点, 确定性才是)。
    #
    # * shuffle 分区设为 1: 内存要**同时**驻留才有用 —— 分成 8 个 task 的话每组数据
    #   先后持有一小段, 峰值只有总量的 1/8, 堆根本涨不起来。
    spark.conf.set("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "1000000")
    grouped = df.groupBy((F.col("id") % args.spill_keys).alias("k")).agg(F.collect_list("v"))
    grouped.foreachPartition(lambda rows: [None for _ in rows])

    total = args.records * args.spill_value_bytes
    log(f"oom_limit 完成: {args.records} 行 × {args.spill_value_bytes} 字节 ≈ {total / 1024 / 1024:.0f}MB "
        f"以 collect_list 缓冲区(普通 Java 对象、不经 TaskMemoryManager)的形式驻留 JVM 堆, "
        f"目的是让容器 RSS 越过它自己的额度、由 NodeManager 报出 beyond physical memory limits")


def fault_shrink(sc, args):
    """
    动态分配缩容 —— 空闲 executor 被 Spark 主动释放

    目标: 验证 check_spark_OOM 会不会把「缩容回收」误报成「内存不足被杀」。

    回收走的是 YARN 的停止容器路径, 容器退出码同样是 137, 诊断文本里同样有
    "Container exited with a non-zero exit code 137." —— 如果 checker 只按这个文本
    判定, 那么任何开了动态分配的作业在收缩时都会被报成 OOM。

    这里先用满所有 executor 干活, 之后 main() 里的 hold 期间应用闲置,
    executorIdleTimeout 一到就会回收。动态分配的开关在 main() 里按故障模式打开。
    """
    n = sc.parallelize(range(args.records), args.partitions).map(lambda x: x * 2).count()
    log(f"shrink: 已用满 executor 跑完 {n} 条, 之后保持空闲等待回收 "
        f"(idle timeout={args.shrink_idle_timeout}s)")


def fault_spill(sc, args):
    """
    内存不足前的磁盘溢写 —— 执行内存不够就往磁盘写

    目标 checker: check_spark_OOM 的「即将出现OOM」分支(P2)。它是**与**条件:
      条件一 executor 级 peakMemoryMetrics.OnHeapExecutionMemory / maxMemory >= 0.9
      条件二 某个 stage 的 diskBytesSpilled > 0

    用**排序**当载体, 因为 SortExec 经 TaskMemoryManager 分配 —— 既占执行内存池、也会溢写。
    踩过的坑(每一级都有 explain 计划或实测指标作证据, 见 tests/spark/explain_plans.py):

      1. RDD 的 groupByKey 不行: PySpark 版在 map 侧就 combine, 聚合发生在 **Python worker
         里**, JVM 从头到尾没持有数据 —— 既不溢写, peakExecutionMemory 也不涨。
         PySpark 的 sortByKey 同样不行(用的是 Python 侧 ExternalSorter)。
      2. `groupBy().agg(collect_list(v))` 也不行 —— explain 显示算子是
         **ObjectHashAggregateExec**: `collect_list` 是 TypedImperativeAggregate, 它的缓冲区
         是**普通 Java 对象、不经 TaskMemoryManager**, 于是三个指标恒为 0。
         实测同一个任务跑了 17 秒、GC 了 8.7 秒(内存**确实**在被大量分配), 而
         peakExecutionMemory / OnHeapExecutionMemory / diskBytesSpilled 全是 0。
         **P2 的两条判据都盯被追踪的内存, 所以这个算子结构上永远触发不了 P2。**
         连带后果: 那 800MB 全压在「容器账外」(Java 对象 + PySpark worker), 集群被压到
         内核 OOM(dmesg: `Out of memory: Kill process <java> ... anon-rss:1159600kB`),
         容器因此拿到 137 —— 而 YARN 的 removeReason 里只有 "Killed by external signal",
         **没有内存专有文本**, 于是 OOM checker 按新判据把它判成「排除」。
         注入造出的现象和要验的判据整个错位了。
      3. value 必须是**非常量**: `F.repeat(F.lit("x"), 2048)` 会被折叠 —— explain 里直接写成
         `collect_list(xxxxxx...xxx)`, 2048 个 x 内联进算子, shuffle 只有 5.6MB。
         而且**常量排序键会被 `EliminateSorts` 整个剪掉**。

    排序之后接 `select("id")` 再交给 action, 这个顺序是必须的:
      * Sort 仍然在计划里(explain 确认: 上层加一个 Project 不会把 Sort 带走), 2KB 的 v
        由 JVM 在排序时消化;
      * action 只拿到 id, **不再把 800MB 一行行灌进 Python worker**(PySpark worker 的内存
        不计入 YARN 容器 —— 上面那条内核 OOM 路径就是这么来的)。
      * 反例(别用): 排完直接 `groupBy().count()` —— explain 显示 Sort 被剪掉了, 因为聚合
        不需要顺序。这个坑真实存在, 见 explain_plans.py 的对照组。

    **`--records` 不能调小**: executor 级 `peakMemoryMetrics` 是**按心跳采样**的(默认 10 秒),
    stage 短于心跳间隔就永远采不到那个峰值(P2 条件一恒为 0)。400 万条是为了让 stage
    跑够十几秒、稳过心跳 —— 见 submit.sh 里 spill 那条的注释。
    """
    from pyspark.sql import SparkSession, functions as F

    spark = SparkSession.builder.getOrCreate()   # 复用已存在的 SparkContext

    df = (spark.range(0, args.records, 1, args.map_partitions)
          # 每行不同的 2KB 字符串: 常量会被折叠, 而常量排序键会被 EliminateSorts 剪掉
          .withColumn("v", F.concat(
              F.repeat(F.lit("x"), max(args.spill_value_bytes - 8, 1)),
              F.lpad(F.col("id").cast("string"), 8, "0"))))

    # 排序键非常量 -> Sort 保留; SortExec 经 TaskMemoryManager 分配 -> 占池子 + 溢写
    sorted_df = df.sortWithinPartitions("v").select("id")
    # 把计划里跟本次判定有关的算子记到驱动日志里 —— 事后能确认 Sort 真的执行了,
    # 而不是"没检出"和"算子在不在"分不清
    plan = sorted_df._jdf.queryExecution().toString()
    ops = [ln.strip() for ln in plan.split("== Physical Plan ==")[-1].splitlines()
           if "Sort [" in ln or "Exchange" in ln]
    log(f"spill: 物理计划里的排序相关算子 -> {ops}")
    sorted_df.foreachPartition(lambda rows: [None for _ in rows])

    total = args.records * args.spill_value_bytes
    log(f"spill 完成: {args.records} 行 × {args.spill_value_bytes} 字节 ≈ {total / 1024 / 1024:.0f}MB "
        f"参与排序; 执行内存池只有 spark.memory.fraction=0.1 那一小块, 排序必然溢写")


def fault_oom_kernel(sc, args):
    """
    把节点压到**内核 OOM** —— 专门用来验证「节点物理内存」这条检测信号

    **这个注入不是给 checker 验检出用的**, 它的产物恰恰是"YARN 看不见的 OOM":

    它在 JVM 里造出大量**不被 TaskMemoryManager 追踪**的对象(`collect_list` ->
    ObjectHashAggregateExec, 缓冲区是普通 Java 对象), 而本集群
    `yarn.nodemanager.resource.memory-mb` **没配、吃默认的 8192MB**,
    真实内存却只有 3.2G —— 于是节点被压过真实上限, 由**内核** SIGKILL 掉容器。
    后果: removeReason 里只有 "Killed by external signal"、没有任何内存文本,
    和外部 kill -9 **完全同形**(实测两类样本 192 vs 37 条, 归一化后逐字节相同)。

    它的价值在于那"一刻"的**节点级实测值**: /ws/v1/cluster/nodes 的
    `resourceUtilization.nodePhysicalMemoryMB`(NM 实测的整机物理内存占用) 应该顶到
    接近 MemTotal, 而 `usedMemoryMB`(按容器分配量算的 YARN 账) 仍然很低。
    实测成立的话, 这就是 REST 上唯一能检出内核 OOM 的信号 —— 采样见
    `tests/probe_node_memory.py`。

    历史: 这段代码原本就是 `fault_spill`。它对 P2 完全无效(既不占执行池也不溢写),
    所以 spill 改用了排序; 这段保留下来专用于制造内核 OOM。
    """
    from pyspark.sql import SparkSession, functions as F

    spark = SparkSession.builder.getOrCreate()   # 复用已存在的 SparkContext
    spark.conf.set("spark.sql.shuffle.partitions", str(args.partitions))
    df = (spark.range(0, args.records, 1, args.map_partitions)
          .withColumn("v", F.repeat(F.lit("x"), args.spill_value_bytes)))
    # 按 id % spill_keys 分组: 分组数少, 但每组要把所有 value 收进内存
    grouped = df.groupBy((F.col("id") % args.spill_keys).alias("k")).agg(F.collect_list("v"))
    grouped.foreachPartition(lambda rows: [None for _ in rows])
    total = args.records * args.spill_value_bytes
    log(f"oom_kernel 完成: {args.records} 行 × {args.spill_value_bytes} 字节 ≈ {total / 1024 / 1024:.0f}MB "
        f"以**不被执行池追踪的 Java 对象**形式驻留 heap —— 目的就是把节点压到内核 OOM")


def _hdfs_delete(sc, path: str) -> None:
    """
    驱动侧删 HDFS 路径, 不存在就当成功

    只能走驱动侧的 Java 网关: sc._jvm 进不了任务闭包(cloudpickle 报
    "'JavaPackage' object is not callable"), 但在 driver 上用它是标准做法。
    """
    jvm = sc._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(sc._jsc.hadoopConfiguration())
    fs.delete(hpath, True)


def fault_src(sc, args):
    """
    外部数据源故障 —— 任务执行时输入文件已不存在(真 java.io.FileNotFoundException)

    目标 checker: check_other_spark_abnormal 的 外部数据源 分支。

    **不能用「直接读一个不存在的路径」**: Spark 在 HadoopRDD.getPartitions 里做
    分区发现, 抛的是 org.apache.hadoop.mapred.InvalidInputException("Input path
    does not exist: ..."), 而且抛在**作业注册之前** —— 实测 /jobs 和 /stages 都是
    空的, 五个 checker 全返回 None(application_..._0015 / _0018)。那不是漏检逻辑
    的问题, 是根本没有可查的对象: 驱动侧的 pre-job 异常对任何走 REST 的 checker
    都不可见。而且注释原先写的 FileNotFoundException 也不对, 异常类名是
    InvalidInputException。

    生产里的数据源故障是「提交时路径在、执行时文件没了」—— 那才是任务侧失败,
    也是 checker 看得见的形态。这里复现后者:

      1. 用 Spark 写一个真实存在的文件, 对它跑一次 action —— HadoopRDD 的
         partitions 是 lazy val, 这一步算完就缓存在 RDD 对象上, 之后不再分区发现
      2. 驱动侧删掉文件
      3. 对**同一个 RDD 对象**再跑一次 action —— 任务按缓存下来的 split 去开文件,
         抛 "java.io.FileNotFoundException: File does not exist: ..."
    """
    path = args.src_path
    log(f"src: 造一个真实文件 {path}")
    _hdfs_delete(sc, path)                       # 重跑时清掉上次残留, 免得 saveAsTextFile 报已存在
    sc.parallelize(range(200), args.partitions).saveAsTextFile(path)

    rdd = sc.textFile(path, args.partitions)
    log(f"src: 第一次取数(此时文件在) -> {rdd.count()} 条")

    log(f"src: 驱动侧删掉 {path}")
    _hdfs_delete(sc, path)

    log("src: 第二次取数 —— 期望任务侧 FileNotFoundException")
    rdd.count()


def fault_auth(sc, args):
    """
    认证/权限问题 —— 任务里往一个没有写权限的本地目录写文件

    目标 checker: check_other_spark_abnormal 的 认证/权限 分支。
    规则匹配的是 "Permission denied" 这类文本, 所以用 Python 的 PermissionError
    也能命中(消息形如 "[Errno 13] Permission denied: '/root/...'"), 且这是真实失败:
    executor 以 hadoop 身份跑, /root 是 700 root。
    """
    target = args.denied_local_path

    def boom(_):
        with open(target, "w") as f:
            f.write("x")
        return 1

    log(f"auth: 往无权限路径 {target} 写文件")
    sc.parallelize(range(args.partitions), args.partitions).map(boom).count()


def fault_connrefused(sc, args):
    """
    外部数据源故障 —— 任务里连一个没人监听的端口

    目标 checker: check_other_spark_abnormal 的 外部数据源 分支(Connection refused)。
    """
    port = args.dead_port

    def boom(_):
        s = socket.socket()
        s.settimeout(5)
        try:
            s.connect((args.dead_host, port))
        finally:
            s.close()
        return 1

    log(f"connrefused: 连接 {args.dead_host}:{port}")
    sc.parallelize(range(args.partitions), args.partitions).map(boom).count()


def fault_cls(sc, args):
    """
    类/依赖冲突 —— 任务里抛带 ClassNotFoundException 字样的异常

    目标 checker: check_other_spark_abnormal 的 类/依赖冲突 分支。

    **注意这是模拟文本, 不是真的 Java 异常**: PySpark 的 sc._jvm 进不了闭包
    (cloudpickle 报 'JavaPackage' object is not callable, 已实测), 任务里调不到
    JVM, 抛不出 Java 异常。checker 这边做的是对 failureReason/errorMessage 的正则
    匹配, 所以这里给一段与真实 Py4J 报错同形的文本, 验的是「文本能对上 + 字段接得通」。
    真实 Java 异常在任务里的落盘形状没被这个用例覆盖 —— 见 README 的说明。
    """
    def boom(_):
        raise RuntimeError(
            "py4j.protocol.Py4JJavaError: An error occurred while calling z:java.lang.Class.forName.\n"
            ": java.lang.ClassNotFoundException: com.ops.fault.NotExist"
        )

    log("cls: 抛 ClassNotFoundException 文本")
    sc.parallelize(range(args.partitions), args.partitions).map(boom).count()


def fault_fmt(sc, args):
    """
    数据格式/类型不匹配 —— 任务里抛带 NumberFormatException 字样的异常

    目标 checker: check_other_spark_abnormal 的 数据格式 分支。
    同 fault_cls, 是等价文本而非真实 Java 异常。
    """
    def boom(_):
        raise RuntimeError(
            "py4j.protocol.Py4JJavaError: An error occurred while calling z:java.lang.Integer.parseInt.\n"
            ': java.lang.NumberFormatException: For input string: "not-a-number"'
        )

    log("fmt: 抛 NumberFormatException 文本")
    sc.parallelize(range(args.partitions), args.partitions).map(boom).count()


def fault_netfail(sc, args):
    """
    网络超时 —— shuffle 数据丢失导致 FetchFailedException

    目标 checker: check_spark_network_timeout 的「阶段因网络问题失败」分支。

    步骤:
      1. 跑一次完整 shuffle 作业, 让 shuffle 文件落到各 executor 本地盘
      2. 杀掉一台的 executor —— 它那份 shuffle 数据随之消失(没有外部 shuffle 服务)
      3. 对同一个 RDD 再取一次数: map 阶段被 SKIPPED(旧产出还在、调度器认为可用),
         reduce 阶段去拉那台已经死掉的 executor 上的 block → FetchFailedException

    三个容易踩错的地方(都实测过):

    * **杀进程和第二次取数之间不能等。** 原来这里睡了一个 kill_interval(5s), 结果
      等第二次取数提交时驱动早已处理完 executor 丢失、把 map 输出标记为不可用,
      于是 Spark 直接重算 map 侧(实测出现全新的 stage id、写出同样大小的 shuffle、
      整个作业 2 秒跑完), reduce 全程从活着的 executor 取数 —— 永远构造不出失败。
      现在立刻取数, 靠调度器还认为旧产出可用这一点走到「真的去拉」那条路上。

    * **只杀一台、且只杀一轮。** 三台全杀光的话 reduce 任务会连人带数据一起死, 报的是
      ExecutorLostFailure(会被重试), 而且没有活着的 executor 能"发现"数据取不到。
      一直杀则会把 600 秒的 hold 累计到 executor 失败上限, 应用在窗口中途被打成 FAILED。

    * **失败窗口要够宽。** 实测 24 个 reduce 任务的阶段只跑 0.99 秒, 而杀进程那一下
      是一次 ssh 往返(~1 秒) —— 差一点才撞上, 两轮都没撞上。所以 submit.sh 里把
      partitions 抬到 120 并用 maxReqsInFlight=1 把 block 拉取串行化, 让阶段持续几十秒,
      杀进程一定落在它中间。

    阶段失败后 Spark 会重算 map 阶段并重试, 最终 job 是 SUCCEEDED —— 所以证据是
    **FAILED 的那次 attempt**(failureReason 里带 FetchFailedException), 这正是
    checker 改用 get_stage_summary 的 failed 侧要覆盖的场景。
    """
    pairs = (sc.parallelize(range(args.records), args.partitions)
             .map(lambda x: (x % args.partitions, x))
             .partitionBy(args.partitions, lambda k: k))

    log("netfail: 第一轮 shuffle, 把数据写到 executor 本地盘")
    first = pairs.count()
    log(f"netfail: 第一轮完成 {first} 条, 开始杀 executor")

    # 只杀**一轮**(times=1), 而且只杀一台(见 submit.sh 的 --kill-hosts)。两个原因:
    #
    # a) 三台全杀光的话, reduce 任务会跟着自己所在的那个 executor 一起死, 报的是
    #    ExecutorLostFailure(调度器会重试该任务, 阶段本身不失败), 而且**没有任何活着的
    #    executor 能去"发现"别人的 shuffle 取不到**。我们要的恰恰是后者 ——
    #    FetchFailedException: The relative remote executor(Id: N) ... is dead。
    #    留两台活着, 它们上面的 reduce 任务去拉第三台的数据就会拿到这个异常。
    #
    # b) 一直杀(原来 times=0、每 5 秒一轮、一轮三台)会把 600 秒的 hold 累计到
    #    67 轮 ≈ 200 次, 正好撞上 spark.yarn.max.executor.failures —— 应用被
    #    "Max number of executor failures (200) reached" 打成 FAILED, 而且在 ~555s,
    #    比 hold 先到, 应用会在检查窗口中途消失(实测两次 attempt 都是这个时长)。
    # `--kill-hosts auto`(submit.sh 里的默认) 现场问 Spark 挑一台**真的有 executor** 的。
    # 写死主机名踩过坑: 落点会随容量/调度变, 杀到一台只有驱动的机器 = 注入静默失效,
    # 而表现和"checker 漏检"一模一样(见 executor_hosts 的说明)。
    # 只取一台: 必须留至少一台活着的 executor, 否则没有谁能"发现"别人的 shuffle 取不到,
    # 报的会是 ExecutorLostFailure 而不是我们要的 FetchFailedException。
    kill_hosts = args.kill_hosts.split(",")
    if kill_hosts == ["auto"]:
        kill_hosts = executor_hosts(sc)[:1]
        log(f"netfail: --kill-hosts=auto -> 现场查到有 executor 的机器, 选 {kill_hosts}")
        if not kill_hosts:
            log("netfail: ⚠️ 一台有 executor 的机器都没查到, 这次注入必然无效")
    start_killer(kill_hosts, args.kill_interval, "netfail", times=1)

    # 杀进程和第二次取数之间**不能等**。原来这里睡了一个 kill_interval(5s), 实测的结果是:
    # 等第二次取数提交时, 驱动早已处理完 executor 丢失、把 map 输出标记成不可用,
    # 于是 Spark 直接**重算 map 侧**(实测出现全新的 stage id: partitionBy 又跑一遍、
    # 写出同样大小的 shuffle, 整个作业 2 秒跑完), reduce 全程从活着的 executor 取数 ——
    # 永远构造不出 FetchFailedException。两轮实测都是这个形态, 没有一次失败。
    #
    # 要出 FetchFailedException, 必须让「reduce 正在取数」和「executor 正在死」**重叠**:
    # 所以这里立刻取数、不等, 靠 fetch 窗口把重叠期拉长到远大于杀进程周期
    # (见 submit.sh 里 netfail 的 maxReqsInFlight/maxSizeInFlight)。
    log("netfail: 第二轮读数 —— 期望 FetchFailedException")
    pairs.count()


def fault_fetchwait(sc, args):
    """
    网络超时 —— shuffle 拉取等待时间占比过高

    目标 checker: check_spark_network_timeout 的「shuffle 拉取等待占比过高」分支
    (shuffleFetchWaitTime / executorRunTime > 0.3)。

    手法是限制 reduce 侧同时拉取的请求数与单次传输量, 让 fetch 成为瓶颈:
    每个 task 要拉很多个 block, 却只能一个一个来。
    """
    rdd = (sc.parallelize(range(args.records), args.map_partitions)
           .map(lambda x: (x % args.partitions, 1))
           .groupByKey(args.partitions)
           .mapValues(sum))
    total = rdd.values().sum()
    log(f"fetchwait 完成: {total} 条")


FAULTS = {
    "normal": fault_normal,
    "skew": fault_skew,
    "stuck": fault_stuck,
    "oom": fault_oom,
    "oom_kill9": fault_oom_kill9,
    "oom_kernel": fault_oom_kernel,
    "oom_limit": fault_oom_limit,
    "shrink": fault_shrink,
    "spill": fault_spill,
    "src": fault_src,
    "auth": fault_auth,
    "connrefused": fault_connrefused,
    "cls": fault_cls,
    "fmt": fault_fmt,
    "netfail": fault_netfail,
    "fetchwait": fault_fetchwait,
}


def main():
    ap = argparse.ArgumentParser(description="Spark 故障注入作业")
    ap.add_argument("--fault", required=True, choices=sorted(FAULTS))
    ap.add_argument("--hold-seconds", type=int, default=300,
                    help="故障动作结束后保持 RUNNING 的秒数, 期间在 Windows 侧跑 checker")
    ap.add_argument("--records", type=int, default=200000)
    ap.add_argument("--partitions", type=int, default=8)
    ap.add_argument("--map-partitions", type=int, default=16)
    ap.add_argument("--hot-records", type=int, default=400000)
    ap.add_argument("--cold-records", type=int, default=40000)
    ap.add_argument("--blow-mb", type=int, default=1400, help="oom: 每个任务申请的堆外内存 MB")
    ap.add_argument("--oom-hold", type=int, default=90,
                    help="oom/oom_kill9: 撑住内存(或挂住任务)的秒数, 给内存监控留足采样时间")
    ap.add_argument("--spill-keys", type=int, default=16)
    ap.add_argument("--spill-value-bytes", type=int, default=2048,
                    help="spill: 每个 value 的字节数(靠它把聚合量撑到执行内存池以上, "
                         "堆记录数不现实 —— 见 fault_spill 的说明)")
    ap.add_argument("--stuck-seconds", type=int, default=3600)
    ap.add_argument("--shrink-idle-timeout", default="10s", help="shrink: executor 空闲多久后被回收")
    ap.add_argument("--kill-hosts", default="node1,node2,node3",
                    help="要杀 executor 的机器, 逗号分隔; 传 'auto' 则现场问 Spark "
                         "挑一台真的有 executor 的(netfail 用这个, 别写死主机名 —— 见 "
                         "executor_hosts 的说明)")
    ap.add_argument("--kill-interval", type=float, default=5.0)
    ap.add_argument("--src-path", default="hdfs://node1:8020/user/hadoop/spark-fault-src",
                    help="src: 先建后删的 HDFS 路径(任务侧 FileNotFoundException 用)")
    ap.add_argument("--denied-local-path", default="/root/spark-fault-no-perm.txt")
    ap.add_argument("--dead-host", default="node1")
    ap.add_argument("--dead-port", type=int, default=9)
    args = ap.parse_args()

    from pyspark import SparkConf, SparkContext

    conf = SparkConf()
    conf.setAppName(f"spark-fault-{args.fault}")
    if args.fault == "shrink":
        # shrink 就是要让动态分配回收 executor。集群没有外部 shuffle 服务, 必须开
        # shuffle 跟踪, 否则回收会丢 shuffle 数据。
        conf.set("spark.dynamicAllocation.enabled", "true")
        conf.set("spark.dynamicAllocation.shuffleTracking.enabled", "true")
        conf.set("spark.dynamicAllocation.initialExecutors", str(args.partitions))
        conf.set("spark.dynamicAllocation.minExecutors", "1")
        conf.set("spark.dynamicAllocation.maxExecutors", str(args.partitions))
        conf.set("spark.dynamicAllocation.executorIdleTimeout", args.shrink_idle_timeout)
    else:
        # 其余故障要关掉动态分配: 否则 executor 被杀后集群会另给一个, 干扰「被杀的 executor」证据,
        # 而且缩容本身也会产生 137 记录, 混淆判定
        conf.set("spark.dynamicAllocation.enabled", "false")

    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    log(f"应用已启动 app_id={sc.applicationId} fault={args.fault}")

    try:
        FAULTS[args.fault](sc, args)
        log(f"{args.fault} 故障注入完成")
    except Exception as e:
        # 驱动侧异常必须吞掉 —— 让应用继续 RUNNING, 否则 checker 没得看
        log(f"{args.fault} 触发异常(预期内): {type(e).__name__}: {str(e)[:600]}")

    log(f"保持 RUNNING {args.hold_seconds}s, 现在可以在 Windows 侧跑 checker")
    time.sleep(args.hold_seconds)
    log("hold 结束, 退出")
    sc.stop()


if __name__ == "__main__":
    main()
