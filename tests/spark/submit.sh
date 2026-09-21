#!/usr/bin/env bash
# 在 node1 上提交 Spark 故障作业, yarn cluster 模式
#
# 用 cluster 而不是 client: 驱动跑在 AM 容器里, 不依赖 ssh 会话存活,
# 且 YARN 代理(node1:8089/proxy/{app_id})能直接转发出 Spark UI —— checker 走的
# 就是这个路径。用法:
#
#   bash submit.sh normal                  # 健康基线, 先跑这个
#   bash submit.sh skew                     # 数据倾斜
#   bash submit.sh oom                      # executor 容器被 137 杀掉
#   bash submit.sh spill                    # 磁盘溢写
#   bash submit.sh stuck                    # 任务卡住不推进
#   bash submit.sh src                      # 数据源故障(HDFS 文件执行时被删)
#   bash submit.sh auth                     # 权限问题
#   bash submit.sh connrefused              # 数据源故障(端口不通)
#   bash submit.sh cls                      # 类/依赖冲突(等价文本)
#   bash submit.sh fmt                      # 数据格式异常(等价文本)
#   bash submit.sh netfail                  # 网络超时(shuffle 数据丢失)
#   bash submit.sh fetchwait                # 网络超时(拉取等待占比高)
#   bash submit.sh oom_limit                # 容器层内存超限(拿 beyond physical memory limits 文本)
#
# 提交后会打印 app_id, 再在 Windows 侧跑:
#   python tests/check_spark_live.py --app <app_id>
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_HOME="${SPARK_HOME:-/export/server/spark3}"
PY="${PY:-/export/server/miniconda3/envs/spark_study/bin/python3}"
FAULT="${1:-normal}"
shift || true

JOB="$HERE/fault_job.py"
if [ ! -f "$JOB" ]; then
  echo "找不到 $JOB" >&2
  echo "先在 Windows 上把 tests/spark/ 传到 node1 的 /home/hadoop/spark-fault/" >&2
  exit 1
fi

# 应用从提交到 Spark UI 可经代理访问要 ~180s(AM 容器分配 + SparkContext 初始化),
# 所以 hold 不能太短 —— 300s 时等就绪几乎把可检查窗口吃光了。批次是显式 kill 上一个
# 应用的(见 batch_spark_live.sh), 加长 hold 不损失吞吐, 只影响手动单跑。
HOLD=600
ARGS=()
CONF=()

# ---------------------------------------------------------------------------
# 集群容量变了 (2026-09-15): 三台 yarn-site.xml 都设了
#   yarn.nodemanager.resource.memory-mb = 2048
# 原先没配、吃默认的 8192 —— 而三台物理只有 4778 / 3266 / 3266 MB, YARN 在
# 「把 3.3G 的机器当 8G 卖」。改完之后集群总容量是 **3 × 2048 = 6144MB**。
#
# 于是作业参数必须跟着算, 否则直接排不下:
#   * 容器额度 = (executor-memory + memoryOverhead) 再向 minimum-allocation(1024) 取整。
#     --executor-memory 1g -> 1024 + 384 = 1408 -> **2048MB/个**;
#     驱动 (cluster 模式下 AM 容器 = spark.driver.memory + overhead) 默认 1g -> 也是 2048MB。
#   * 所以 `--num-executors 3` 那一版要 **1024*2 + 3*2048 = 8192MB**, 超了 6144, 排不下 ——
#     全部降到 **2 个 executor**: AM 1024 + 2×2048 = **5120 ≤ 6144** ✓
#   * **--executor-memory 保持 1g 不动**是故意的: spill 的 P2 是拿「执行池占满 ~69MB」
#     验通的, 池子大小直接跟这个值走, 降成 512m 那组标定就作废了。
#   * 每台只有 2048, 装不下两个 2048 容器 -> 2 个 executor **必然落在不同节点**,
#     不会再出现"2 个容器挤在 3.3G 机器上"那种节点级超卖。
# ---------------------------------------------------------------------------
case "$FAULT" in
  normal)      CONF=(--executor-memory 1g --executor-cores 2 --num-executors 2)
               ARGS=(--records 200000 --partitions 8) ;;
  # 热点分区 40 万条、冷分区 4 万条 -> 倾斜比 10(阈值 5)
  skew)        CONF=(--executor-memory 1g --executor-cores 2 --num-executors 2)
               ARGS=(--partitions 8 --hot-records 400000 --cold-records 40000) ;;
  # 单任务挂 1 小时, 作业一直 RUNNING 且任务数不变
  stuck)       CONF=(--executor-memory 1g --executor-cores 1 --num-executors 2)
               ARGS=(--partitions 4 --stuck-seconds 3600) ;;
  # 容器上限 = 512m + overhead(约 384m) 按最小分配单元向上取整 ≈ 1GB。
  # 只起 1 个 executor、申请 900MB: 节点总共才 4G(已用 2G), 一次性申请太大
  # 会先触发内核 OOM killer —— worker 单独被杀、容器活着, 退出码就不是 137 了。
  oom)         CONF=(--executor-memory 512m --executor-cores 1 --num-executors 1)
               ARGS=(--partitions 2 --blow-mb 900) ;;
  # 直接 kill -9 executor JVM, 容器退出码同样是 137
  oom_kill9)   CONF=(--executor-memory 1g --executor-cores 1 --num-executors 2)
               ARGS=(--partitions 3 --oom-hold 120) ;;
  # 动态分配缩容: 空闲 executor 被回收, 退出码同样是 137
  shrink)      CONF=(--executor-memory 1g --executor-cores 1 --num-executors 2)
               ARGS=(--records 200000 --partitions 3) ;;
  # 排序要落在 JVM 里(SortExec 经 TaskMemoryManager), 且数据量要远超执行内存池:
  # 池子 = (1024-300)*0.1 ≈ 69MB, 参与排序的数据 = records * 2048 字节。
  #
  # P2 条件一读的是 executor 级 peakMemoryMetrics.OnHeapExecutionMemory, 而那个字段
  # 是**按心跳采样**的(生产默认 10s)。实测 2x2(见 README「spill / P2」一节):
  #   400k + 10s -> 0         (stage 只跑 ~1 秒, 采样点根本落不进去)
  #   400k +  1s -> 0.942  ✓  (只有 1 个 executor 撞上)
  #   4M   + 10s -> 0.587     (stage 12.9 秒 -> 只够 1.3 个采样点, 撞不上池子占满的瞬间)
  #   4M   +  1s -> 1.000  ✓  (三个 executor 全是满池 69.0M)
  # 两个变量各管一件事: **数据量**决定池子能不能真占满(4M 时才顶到满池),
  # **采样点数**决定看不看得见(点数 ≈ stage 墙钟 / 心跳间隔)。
  # 所以这里两个一起调, 但**不动心跳**(不改被测环境):
  #   * 1600 万条 × 2KB = 32GB, 让执行池长时间处于占满状态;
  # 注意 **不能靠调 map-partitions 来拉长 stage**: 实测 4M+16 分区是 12.9 秒,
  # 4M+64 分区是 10.6 秒 —— wave 数涨 4 倍、单 task 时间跌 4 倍, 正好抵消。
  # stage 墙钟由「总字节数 ÷ 集群吞吐」决定, 只跟数据量走。所以要加采样点就只能加数据。
  spill)       CONF=(--executor-memory 1g --executor-cores 2 --num-executors 2
                     --conf spark.memory.fraction=0.1)
               ARGS=(--records 16000000 --partitions 8 --map-partitions 64
                     --spill-value-bytes 2048) ;;
  # **内核 OOM 发生器** —— 不是给 checker 验检出用的, 是给「节点物理内存」这条检测信号造场景的。
  # 用聚合版注入(缓冲区是普通 Java 对象、不吃执行池)把每台 executor 的 JVM 撑到 ~1.2GB,
  # 再加 python worker, 在真实内存只有 3.2G 的节点上必然越界 —— 由**内核** SIGKILL 掉容器,
  # removeReason 里不会有任何内存文本。
  # 采样脚本: tests/probe_node_memory.py (看 nodePhysicalMemoryMB 是否顶到接近 MemTotal)
  oom_kernel)  CONF=(--executor-memory 1g --executor-cores 2 --num-executors 2
                     --conf spark.memory.fraction=0.1)
               ARGS=(--records 400000 --partitions 8 --map-partitions 16
                     --spill-value-bytes 2048) ;;
  # **容器层**内存超限 —— 拿 removeReason 里的 "beyond physical memory limits" 文本。
  #
  # 和上面的 oom_kernel 是**两回事**, 别混: oom_kernel 走"节点超卖 -> 内核兜底",
  # removeReason 里没有任何内存文本(和外部 kill -9 逐字节相同); 这里要的是**容器自己**
  # 超自己的额度, 由 NodeManager 的 ContainersMonitorImpl 报出内存专有文本 ——
  # check_spark_OOM 的 error 档认的就是这句话。
  #
  # 两个数字要一起看(默认参数下**永远撑不爆**, 下面有算式):
  #   * 容器额度 = executor-memory + memoryOverhead 再向最小分配单元(1024)取整;
  #   * JVM 的 RSS = 堆用量 + 非堆(~300MB: metaspace/code cache/线程/GC)。
  #
  # **overhead 是这个注入唯一的旋钮**, 因为堆被 -Xmx 卡死、不可能超:
  #   默认 overhead(384m) 恰好盖住非堆的 ~300MB —— 这是 Spark 的设计意图。
  #   所以 --executor-memory 1g + 默认 overhead 时容器是 2048MB, 而 RSS 最多 ~1300MB,
  #   差着 700MB, 撑不爆。把 overhead 压到 0, 容器就是 **1024MB**, 而 RSS 仍要 ~1300MB,
  #   于是必然越界 —— 这就是生产上"memoryOverhead 给小了"的教科书形态。
  #
  # 曾经试过、**已被证伪**的做法(别再走): 在 spark.executor.extraJavaOptions 里再给一个
  # -Xmx 覆盖掉 Spark 那个。实测 Spark 直接拒绝启动 ——
  #   java.lang.Exception: spark.executor.extraJavaOptions is not allowed to specify
  #   max heap memory settings (was '-Xmx1600m'). Use spark.executor.memory instead.
  # 这是 SparkConf.validateSettings 里的硬校验, 应用 30 秒就 FAILED(退出码 13)。
  # 也**不能**靠 python worker 吃内存: 它被 setpgid 到新进程组, 不在容器的 procfs
  # 统计范围里(已实测, 见 fault_job.fault_oom_kill9 的说明)。
  # 所以"堆用满 + overhead 压没"是仅剩的、纯配置可行的路子。
  #
  # 只起 1 个 executor: 目标容器要越界, 但**节点不能越界** —— 否则内核先出手, 又变成
  # oom_kernel 那种"没有内存文本"的死法。node2/3 常驻 1237MB, 加 AM ~400MB + 目标容器
  # ~1100MB ≈ 2737MB < 物理 3345MB, 留得住余量。
  #
  # max.executor.failures 放宽: 容器被杀 -> Spark 换个 executor 重试 -> 又被杀, 这个循环
  # 正是我们要的(检查窗口内一直有"被内存杀掉的 executor"), 但默认上限 max(2*1,3)=3 次
  # 就把应用打成 FAILED, 应用一没代理就 404, checker 什么都读不到。
  oom_limit)   CONF=(--executor-memory 1g --executor-cores 1 --num-executors 1
                     --conf spark.executor.memoryOverhead=0
                     --conf spark.yarn.max.executor.failures=1000)
               # --partitions 1: 内存要**同时**驻留, 分成多个 task 的话峰值只有总量的
               #   1/8, 堆涨不起来。--spill-keys 100: 总量 800MB 摊成 100 组、每组 8MB,
               #   免得 collect_list 产出一行 800MB 的 list 把投影那步撑爆(实测过)。
               ARGS=(--records 400000 --partitions 1 --map-partitions 16
                     --spill-value-bytes 2048 --spill-keys 100) ;;
  src)         CONF=(--executor-memory 1g --executor-cores 2 --num-executors 2) ;;
  auth)        CONF=(--executor-memory 1g --executor-cores 1 --num-executors 2)
               ARGS=(--partitions 2) ;;
  connrefused) CONF=(--executor-memory 1g --executor-cores 1 --num-executors 2)
               ARGS=(--partitions 2) ;;
  cls)         CONF=(--executor-memory 1g --executor-cores 1 --num-executors 2)
               ARGS=(--partitions 2) ;;
  fmt)         CONF=(--executor-memory 1g --executor-cores 1 --num-executors 2)
               ARGS=(--partitions 2) ;;
  # 要反复杀 executor 才能让失败窗口一直在, 所以放宽失败上限 ——
  # 默认只允许 max(2*executor数, 3)=6 次, 撞上应用就直接 FAILED 了。
  # 实测教训: 上限 200 时, 每 5 秒杀一轮 × 一轮三台 × 600 秒 hold = 67 轮 ≈ 200 次,
  # 正好卡在第 200 次把应用打成 FAILED("Max number of executor failures (200) reached"),
  # 而且发生在 ~555s —— 比 hold 先到, 应用会在检查窗口中途消失。现在改成只杀一轮
  # (fault_job 里 times=1), 上限留着当安全网。
  #
  # --kill-hosts 只留一台: 三台全杀光的话 reduce 任务会连人带数据一起死, 报的是
  # ExecutorLostFailure(会被重试), 而不是我们要的 FetchFailedException —— 详见 fault_netfail。
  #
  # partitions 决定**取数窗口**有多宽 —— 注入成败全在这里。
  #
  # 窗口 = [杀进程落地, reduce 取数结束]。杀进程那一下是一条**冷 ssh**(从 AM 容器里
  # subprocess 起 ssh, 没有连接复用), 2026-09-16 实测**约 3 秒**才真的把 JVM 杀掉
  # (app ..._0012: 31.4 发起 -> 容器 34.435 才死), 不是早期注释里估的 1 秒。
  # 而 partitions=120 时 reduce 阶段只有 **3.0 秒**(31.554 -> 34.549) —— 杀进程落地
  # 只比阶段结束早 **114 毫秒**, 一个 FAILED attempt 都没出来。
  #
  # 阶段时长 ≈ ⌈reduce任务数 / 并发槽⌉ × 每任务串行拉的块数 × 单块耗时:
  #   120 任务 / 4 槽 = 30 波, 每任务拉 120 块 -> 30 × 120 × ~0.15ms ≈ 3 秒 ✓与实测相符
  # partitions 同时决定 map 产物块数和 reduce 任务数, 所以它是二次方杠杆:
  #   480 任务 / 4 槽 = 120 波, 每任务拉 480 块 -> 约 16 秒(预估), 给 3 秒的杀进程留出余量。
  # 只动这一个变量(不要同时抬 --records 或降 --executor-cores), 否则分不清是哪个在起作用。
  netfail)     CONF=(--executor-memory 1g --executor-cores 2 --num-executors 2
                     --conf spark.yarn.max.executor.failures=1000
                     --conf spark.reducer.maxReqsInFlight=1
                     --conf spark.reducer.maxSizeInFlight=1m)
               # --kill-hosts auto: 现场问 Spark「哪台真的有 executor」再杀。
               # 原先写死 node3 —— 容量改成 2048 后 AM 独占一台、两个 executor 落到另外
               # 两台, node3 恰好成了只剩驱动的那台, 杀了个寂寞、注入静默失效, 而表现和
               # "checker 漏检"一模一样(app ..._0010 实测)。落点会随容量/调度变, 不能写死。
               ARGS=(--records 1000000 --partitions 480 --kill-hosts auto) ;;
  # 拉取串行化: 同时只允许 1 个在途请求, 单次最多 1MB
  fetchwait)   CONF=(--executor-memory 1g --executor-cores 2 --num-executors 2
                     --conf spark.reducer.maxReqsInFlight=1
                     --conf spark.reducer.maxSizeInFlight=1m)
               ARGS=(--records 400000 --partitions 8 --map-partitions 64) ;;
  *) echo "未知故障: $FAULT" >&2; exit 1 ;;
esac

# spark.yarn.archive: 集群没配 spark.yarn.jars/archive 时, 每次提交都要把整个
# SPARK_HOME/jars(约 200MB)传到 HDFS 暂存目录, 一次提交多花 40 秒。预先把 jars
# 打成 zip 放到 HDFS 上复用:
#   cd /export/server/spark3/jars && zip -q -r /tmp/spark-libs.zip .
#   hdfs dfs -put -f /tmp/spark-libs.zip /user/hadoop/spark-libs.zip
ARCHIVE="hdfs://node1:8020/user/hadoop/spark-libs.zip"
if ! hdfs dfs -test -e "$ARCHIVE"; then
  echo "⚠️  $ARCHIVE 不存在, 回落成每次上传 SPARK_HOME/jars(慢)" >&2
  ARCHIVE=""
fi

echo "=== 提交 spark-fault-$FAULT (hold ${HOLD}s) ==="
echo "python: $PY"
echo

LOG=/tmp/spark-fault-submit-$FAULT.log
"$SPARK_HOME/bin/spark-submit" \
  --master yarn \
  --deploy-mode cluster \
  --name "spark-fault-$FAULT" \
  --conf spark.yarn.submit.waitAppCompletion=false \
  ${ARCHIVE:+--conf "spark.yarn.archive=$ARCHIVE"} \
  --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=$PY" \
  --conf "spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=$PY" \
  --conf "spark.executorEnv.PYSPARK_PYTHON=$PY" \
  --conf "spark.driver.memory=512m" \
  ${CONF[@]+"${CONF[@]}"} \
  "$JOB" --fault "$FAULT" --hold-seconds "$HOLD" ${ARGS[@]+"${ARGS[@]}"} "$@" 2>&1 | tee "$LOG"
# 注意 ${ARR[@]+"${ARR[@]}"} 这个写法不是啰嗦: 本机是 bash 4.2, 开了 set -u 时
# 展开空数组会报 "ARGS[@]: unbound variable" 直接退出(src 这种不带参数的故障就踩过)

APP_ID=$(grep -oE 'application_[0-9]+_[0-9]+' "$LOG" | tail -1)
if [ -z "$APP_ID" ]; then
  # waitAppCompletion=false 时 spark-submit 拿不到 app_id(它不等应用跑起来就返回了),
  # 按作业名去 YARN 里捞
  for _ in $(seq 1 20); do
    APP_ID=$(curl -s "http://node1:8088/ws/v1/cluster/apps?states=ACCEPTED,RUNNING" | python3 -c "
import json, sys
name = 'spark-fault-$FAULT'
apps = json.load(sys.stdin).get('apps', {}).get('app', [])
ids = [a['id'] for a in apps if a.get('name') == name]
print(max(ids) if ids else '')
" 2>/dev/null)
    [ -n "$APP_ID" ] && break
    sleep 2
  done
fi

echo
echo "=== app_id: ${APP_ID:-未取到} ==="
echo "在 Windows 侧验证:"
echo "  python tests/check_spark_live.py --app ${APP_ID:-<app_id>}"
echo
echo "提前结束作业:"
echo "  yarn application -kill ${APP_ID:-<app_id>}"
