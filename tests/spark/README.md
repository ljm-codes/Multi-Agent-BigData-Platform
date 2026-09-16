# Spark 故障注入作业

给 `Tools/spark/` 下（以及 `Tools/check_job_timeout.py`）的 checker 造真实故障，用来验证检出能力，以及验证健康集群下不误报。

## 环境

|        |                                                                                                  |
|--------|--------------------------------------------------------------------------------------------------|
| Spark  | 3.1.3，`/export/server/spark3`，走 YARN（`yarn cluster` 模式）                                          |
| Python | `/export/server/miniconda3/envs/spark_study/bin/python3`（3.8.20，自带 pyspark 3.1.3），node1/2/3 三台一致 |
| 构建     | **不需要构建** —— 作业是 PySpark 脚本，改参数只改 `fault_job.py`                                                 |
| 部署     | `tests/spark/` 整个目录传到 node1 的 `~/spark-fault/`                                                   |
| 取数     | checker 走 `http://node1:8089/proxy/{app_id}/api/v1/...`（YARN WebAppProxy）                        |

### 集群侧前置条件：YARN 的轮询式内存超限判定必须打开

`check_spark_OOM` 的 **error 档**认的是 removeReason 里的
`is running beyond physical memory limits`。这句话由 **NodeManager 的
`ContainersMonitorImpl.checkLimit()`** 产生 —— 而本集群原先**永远产生不了**：
三台 NM 日志**全历史**里 `beyond physical` 都是 **0 条**。

**根因是 YARN-11967。** `checkLimit()` 开头就短路（`ContainersMonitorImpl.java:751`）：

```java
if (strictMemoryEnforcement && !elasticMemoryEnforcement) {
  // When cgroup-based strict memory enforcement is used alone without
  // elastic memory control, the oom-kill would take care of it.
  return;                    // ← 真正的超限判定全在下面，永远到不了
}
```

而 yarn-default.xml 的两个默认值正好互相矛盾：

| 属性                                          | 默认值       | 含义                |
|---------------------------------------------|-----------|-------------------|
| `yarn.nodemanager.resource.memory.enforced` | **true**  | 以为 cgroup 严格强制已开启 |
| `yarn.nodemanager.resource.memory.enabled`  | **false** | cgroup 其实没开       |

`true && !false` = 真 → **轮询判定废掉、cgroup 也没兜底** ——
这台集群对容器内存**没有任何强制机制**。NM 启动日志里会自己报出这两个值，
`grep "memory control enabled"` 就能看到。

**已在三台 `yarn-site.xml` 加的三条**（无注释，原因就记在这里）：

```xml

<property>
    <name>yarn.nodemanager.resource.memory.enforced</name>
    <value>false</value>
</property>
<property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
</property>
<property>
    <name>yarn.nodemanager.pmem-check-enabled</name>
    <value>true</value>
</property>
```

- **`enforced=false`** 解开短路。语义上它本来就是对的 —— `enforced` 指的是
  「**cgroup** 严格内存强制」，而本集群 cgroup 没开，`true` 是在说谎。
- **`vmem-check-enabled=false` 不是可选项，是必须一起改的。** `checkLimit` 里
  **vmem 检查排在前面而且是 `else if`**：

  ```java
  if (isVmemCheckEnabled() && isProcessTreeOverLimit(..., vmemLimit)) {
      msg = formatErrorMessage("virtual", ...);      // beyond VIRTUAL memory limits
  } else if (isPmemCheckEnabled() && isProcessTreeOverLimit(..., pmemLimit)) {
      msg = formatErrorMessage("physical", ...);     // beyond PHYSICAL memory limits
  }
  ```

  原先 vmem 检查只是因为短路才没生效，`enforced=false` 会让它**第一次真正激活**。
  而容器的 vmem 额度只有 `容器内存 × vmem-pmem-ratio = 1024 × 2.1 = 2150MB`，
  实测本集群 JDK8 JVM 的 `VmSize` 是 **2890~3096MB** ——
  `isProcessTreeOverLimit` 的第二条分支（aged 进程超限）必然命中。
  **后果是每个容器约 6 秒后被 `beyond virtual memory limits` 杀掉**，
  而且 `MEMORY_LIMIT` 正则**也匹配 "virtual"**，会伪装成「error 档验证成功」。
  （JVM 虚拟内存超额是常态，`vmem-check-enabled=false` 也是 Spark on YARN 的通行配置。）
- `pmem-check-enabled=true` 本来就是默认值，显式写出来是为了钉死，不依赖发行版差异。

**为什么不走「开 cgroup」那条路**（YARN-11967 的正解）：两个原因 ——
① 本集群做不到：`container-executor` 不是 root + setuid 6050、
`container-executor.cfg` 还是模板原文、`/sys/fs/cgroup/memory` 对 hadoop 不可写；
② **就算做了也拿不到要验的文本**：`memory.enabled=true` 时 `checkLimit()` 依旧短路
（那句注释就是为这个场景写的），超限改由 **cgroup 的 OOM killer** 处理，
容器退出时不会有 `beyond physical memory limits` —— 换成了另一种信号形态。

> 排查时走过两条死路，记下来别再走：
> ① `Using ResourceCalculatorProcessTree: null` 是**红鲱鱼** —— `clazz == null` 时
> `ResourceCalculatorProcessTree.getResourceCalculatorProcessTree()` 会自动回退到
> `ProcfsBasedProcessTree`；
> ② **别拿整份日志的累计计数当频率**。曾把 NM 日志跨一年多的 882 条
> `CPU usage is not yet available` 当成「监控每轮都在跳过」，实际按容器数只有 **6 条**
> （启动瞬态）。日志计数必须按应用/容器分组看。

**改了配置要重启 NM 才生效**（`sbin/yarn-daemon.sh stop/start nodemanager`，三台，串行；
RM 不用动）。回滚点：每台的 `yarn-site.xml.bak`。

### 集群容量：为什么是 6144MB（2026-09-15 改）

同一趟还改了 `yarn.nodemanager.resource.memory-mb = 2048`。**这是内核 OOM 的根因**：
原先没配 → `-1`，而 `yarn.nodemanager.resource.detect-hardware-capabilities=false`
→ 回落默认 **8192** —— 而三台物理只有 **4778 / 3266 / 3266 MB**。
**YARN 在把 3.3G 的机器当 8G 卖**，任何真吃内存的作业都会让节点越界、由**内核**兜底。

取值依据（`memory-mb ≈ 物理 − 常驻`，向上取整到 `minimum-allocation-mb`=1024 的倍数）：

| 节点    | 物理   | used | buff/cache | available | 原(默认) | 现        |
|-------|------|------|------------|-----------|-------|----------|
| node1 | 4778 | 2588 | 650        | 1851      | 8192  | **2048** |
| node2 | 3266 | 1293 | 1179       | 1679      | 8192  | **2048** |
| node3 | 3266 | 1288 | 812        | 1666      | 8192  | **2048** |

**node2/3 是"贴着边"给的，说清楚**：严格安全值应该是 1024（`3266 − 1288 ≈ 1978`），
但 1024 一个 2048 的 executor 都放不下、作业直接排不下。2048 是**能让 harness 跑起来
的最小值**，最坏情况（所有容器同时吃满）超约 75MB，由各自的 page cache（812~1179MB）兜。
把那两台的 `mysqld`(358M) + `gnome-shell`(138M) + `gnome-software`(65M) 清掉能腾出
~560MB，就从容了（gnome 要 root，`gdm`/`Xorg` 都是 root 进程）。

**harness 的连带改动**（`submit.sh`，注释里也写了算式）：

| 参数                    | 原                  | 现                      | 理由                                   |
|-----------------------|--------------------|------------------------|--------------------------------------|
| `--num-executors`     | 3（8 个故障里）          | **2**                  | `AM 1024 + 3×2048 = 8192 > 6144` 排不下 |
| `spark.driver.memory` | 默认 1g → AM 容器 2048 | **512m** → AM **1024** | 否则 AM 独占一台                           |
| `--executor-memory`   | 1g                 | **不动**                 | ⭐ 池子仍 ~69MB，`spill` 的 P2 标定不作废       |

**两个已知代价**（都接受，但要记着）：

1. **内核 OOM 从此不容易主动复现了** —— `oom_kernel` 注入原本依赖"节点超卖"才能压出内核
   OOM，改完节点不会再被 YARN 排爆。那条检测规则（`node_mem_suspect_ratio=85`）**保留**，
   正反样本（95.19% vs 57.23%）也验过了，只是**以后没法再拿这个场景调阈值**。
2. **整批用例需要重跑确认** —— 依赖 executor 数的 `netfail`/`skew`/`spill` 场景形态都变了。

> **为什么内核 OOM 的检测不能删**：改 memory-mb **消不掉**这条路径的成因。PySpark 的
> worker 被 `pyspark.daemon` `setpgid` 到**新进程组**，而 YARN 的 procfs 进程树**按进程组
> 统计** —— **Python worker 的内存不计入容器**。所以"JVM 没超自己的额度、但账外的
> Python worker 把节点压垮"这条路依然存在，`pmem` 轮询检查对它无能为力。
> 改配置只是让它变少，不是消灭。

## 提交

```bash
# 在 node1 上
cd ~/spark-fault
bash submit.sh normal          # 健康基线，先跑这个
bash submit.sh skew            # 数据倾斜
bash submit.sh oom             # executor 容器内存撑爆
bash submit.sh oom_kill9       # executor 被 kill -9（容器退出码 137）
bash submit.sh shrink          # 动态分配缩容（也是 137，用来验误报）
bash submit.sh spill           # 磁盘溢写
bash submit.sh stuck           # 任务卡住不推进
bash submit.sh src             # 数据源故障（HDFS 路径不存在）
bash submit.sh auth            # 权限问题
bash submit.sh connrefused     # 数据源故障（端口不通）
bash submit.sh cls             # 类/依赖冲突
bash submit.sh fmt             # 数据格式异常
bash submit.sh netfail         # 网络超时（shuffle 数据丢失）
bash submit.sh fetchwait       # 网络超时（拉取等待占比高）
```

提交后会打印 `app_id`（`waitAppCompletion=false` 时 spark-submit 自己拿不到，脚本按作业名去 YARN 里捞）。

```bash
# 在 Windows 侧
python tests/check_spark_live.py --app <app_id> --dump
```

`--dump` 会把原始 JSON 存到 `tmp/`，排查「字段名/路径对不对」比读 checker 代码快。

**加速提交**：集群没配 `spark.yarn.jars`/`spark.yarn.archive`，默认每次提交都要把整个
`SPARK_HOME/jars`（约 200MB）传到 HDFS 暂存目录，一次多花 40 秒。`submit.sh` 里指向了
预先生成的归档：

```bash
cd ${SPARK_HOME}/jars && zip -q -r /tmp/spark-libs.zip .
hdfs dfs -put -f /tmp/spark-libs.zip ~/spark-libs.zip
```

## 故障模式与对应 checker

| `--fault`     | 目标 checker                       | 注入手法                                                                                                  |
|---------------|----------------------------------|-------------------------------------------------------------------------------------------------------|
| `normal`      | 全部                               | 健康基线，**应当全部返回 `None`**                                                                                |
| `skew`        | `check_spark_data_skew`          | 0 号分区 40 万条、其余 7 个分区各 4 万条（比值 10，阈值 5）                                                                |
| `oom`         | `check_spark_OOM` P1             | Python worker 逐步申请 900MB 撑爆容器                                                                         |
| `oom_kill9`   | `check_spark_OOM` P1             | ssh 到各节点 `kill -9` executor JVM，容器退出码 137                                                             |
| `shrink`      | `check_spark_OOM`（误报验证）          | 开动态分配 + 10s 空闲超时，让集群自己回收 executor                                                                     |
| `spill`       | `check_spark_OOM` P2             | `spark.memory.fraction=0.1` 下对 800MB 数据做 `SortExec` 排序 —— 占满执行池并溢写磁盘                                  |
| `oom_kernel`  | （**不是**给 checker 验检出用的）          | 聚合版注入（缓冲区是不被执行池追踪的 Java 对象）把节点压到**内核 OOM** —— 专门用来验证「节点物理内存」这条检测信号（配 `tests/probe_node_memory.py` 采样） |
| `stuck`       | `check_spark_job_timeout`        | 先跑一个成功 job，再挂住一个（任务数长时间不变）                                                                            |
| `src`         | `check_other_spark_abnormal` 数据源 | 先建文件跑一次 action，再驱动侧删掉、对同一 RDD 再取数（真 `FileNotFoundException`，落在任务侧）                                    |
| `auth`        | `check_other_spark_abnormal` 权限  | 任务往 `/root/...` 写文件（真 `Permission denied`）                                                            |
| `connrefused` | `check_other_spark_abnormal` 数据源 | 任务连没人监听的端口（真 `Connection refused`）                                                                    |
| `cls`         | `check_other_spark_abnormal` 类冲突 | 抛带 `ClassNotFoundException` 字样的异常                                                                     |
| `fmt`         | `check_other_spark_abnormal` 格式  | 抛带 `NumberFormatException` 字样的异常                                                                      |
| `netfail`     | `check_spark_network_timeout` P1 | shuffle 写完后杀掉 executor，再对同一 RDD 取数                                                                    |
| `fetchwait`   | `check_spark_network_timeout` P2 | `maxReqsInFlight=1` + `maxSizeInFlight=1m`，让拉取串行化                                                     |

`cls` / `fmt` 是**等价文本，不是真 Java 异常**，原因见下面的「已知坑」第 1 条。

## 设计约束（为什么作业要"挂着"）

1. **应用必须保持 `RUNNING`** —— checker 走 YARN 代理取数，应用一结束代理就返回
   YARN 的 HTML 页面（**状态码还是 200**），所有 checker 静默返回 `None`。所以有 `--hold-seconds`。
2. **每个故障模式只触发一个 action（一个 job）** —— 除 `check_spark_job_timeout` 外的
   四个 checker 都只取 `jobs[0]`，而 `/jobs` 是**新的在前**，`jobs[0]` 是最后跑的那个 job。
   （`netfail` 是例外，它跑两个 job：第一次 shuffle + 第二次取数。因为新的在前，
   `jobs[0]` 恰好就是出问题的那次取数，所以成立 —— 但顺序不能反。）
3. **驱动侧异常必须自己吞掉** —— 否则 driver 退出、应用变 FINISHED，checker 没得看。
4. **挂起类故障要连着几次轮询不变才算** —— `check_spark_job_timeout` 靠 redis 存前后两次
   的任务数快照，第一次调用只写快照，要隔一会儿再调第二次；而且它要求应用里已经有
   `SUCCEEDED` 的 job，所以 `stuck` 模式先跑一个正常 job 再挂住。

## 已知坑（踩过的，别再踩）

1. **`sc._jvm` 不能进闭包** —— cloudpickle 序列化时直接报
   `TypeError: 'JavaPackage' object is not callable`。PySpark 任务里调不到 JVM，
   所以任务内抛不出 Java 异常，`cls`/`fmt` 只能用同形文本代替。真 Java 异常要走
   驱动侧（`sc.textFile` 之类）才有。
2. **PySpark 的 shuffle 记录数不是记录数** —— `shuffleReadMetrics.recordsRead` /
   `ShuffleWriteMetrics.recordsWritten` 在 PySpark 作业里记的是**序列化批次**：
   实测 4000 条记录 → `groupByKey` 报 4、`partitionBy` 报 28；68 万条 → 报 135。
   同一个集群上跑 Scala 作业（`spark-examples` 的 `GroupByTest`）则是真条数
   （每个 map task 100000 条、reduce task 读 100416/99466/99684/100434 条）。
   对倾斜判定来说，PySpark 下这个比值只是**噪声近似**，数据量小的时候不能信。
3. **PySpark 的 Python worker 内存不计入 YARN 容器** —— `pyspark.daemon` 会
   `setpgid` 到新进程组，而 YARN 的 procfs 进程树是按进程组统计的。实测在
   512m 堆 + 约 1GB 上限的容器里申请 900MB 并保持 90s，NodeManager 毫无反应
   （`pmem-check-enabled=true`，日志里没有任何超限记录）。所以 `oom` 撑不爆容器是
   环境特性，不是作业写错了 —— 要 137 的证据得用 `oom_kill9`。
4. **`jps` 不在非交互 ssh 的 PATH 里** —— `ssh node2 jps` 报 `command not found`，
   而 ssh 的退出码仍是 0，静默失败。用 `pgrep -f CoarseGrainedExecutorBackend`。
5. **杀 executor 会撞失败上限** —— `spark.yarn.max.executor.failures` 默认
   `max(2*executor数, 3)`，反复杀会让应用直接 `FAILED`（诊断：
   `Max number of executor failures (6) reached`）。只取一次证据的场景杀一次就够，
   要持续失败的场景（`netfail`）得把这个上限调大。
   **上限要按算术算，不能拍脑袋**：实测 netfail 设成 200，而杀进程是
   「每 5 秒一轮 × 一轮三台 × 600 秒 hold」= 67 轮 ≈ 200 次，正好卡在第 200 次把应用
   打成 `FAILED`，而且发生在 **~555s —— 比 hold 先到，应用在检查窗口中途就没了**
   （两轮 attempt 时长都是 555s 左右，日志里也没有 `hold 结束`，只有
   `Diagnostics: Max number of executor failures (200) reached`）。
   现在 netfail 改成只杀一轮、只杀一台（见 `submit.sh`），上限只当安全网。
6. **动态分配缩容也是 137** —— 回收走的也是 YARN 停容器路径，诊断文本里同样有
   `Container exited with a non-zero exit code 137.`。所以「137 = 内存不足」这个
   判定本身是有条件的，`shrink` 模式就是用来量化这个误报的。
7. **驱动侧「作业注册前」的异常，checker 一律看不见** —— 最初 `src` 写成
   `sc.textFile(不存在的路径).count()`，实测抛的是
   `org.apache.hadoop.mapred.InvalidInputException: Input path does not exist`
   （**不是** `FileNotFoundException`），位置在 `HadoopRDD.getPartitions` 的分区发现阶段。
   而这个异常抛在**作业注册之前**：`/jobs` 和 `/stages` 都是空数组，五个 checker
   全返回 `None`（实测 `application_..._0015` / `_0018`）。这不是漏检逻辑的问题，
   是 checker 走 REST 根本没有可查的对象 —— 任何依赖 `/jobs[0]` 的判定都到不了。
   所以 `src` 改成任务侧失败：生产里的数据源故障也正是「提交时路径在、执行时文件没了」
   这个形态。

8. **`pgrep -f` 会匹配到执行它自己的那条命令** —— `kill_executors` 原来用
   `pgrep -f CoarseGrainedExecutorBackend`，而这条命令**自己的命令行里就含这个字符串**，
   于是它把执行它的 shell 也匹配了进去（实测把自己 shell 的 `$$` 都打了出来），
   紧接着 `kill -9 $pids` 顺带把那个 shell 杀掉。表现很有迷惑性：
   **驱动所在的那台永远报 `rc=-9`**（那台上连本地 ssh 客户端也被匹配进去、被 SIGKILL，
   而且 pids 根本打不出来），**另外两台报 `rc=255`**（远程 shell 被杀、连接断掉）；
   杀到哪些 pid 全靠 pid 大小顺序，不可靠。
   修法是把模式首字母用字符组包起来：`pgrep -f "[C]oarseGrainedExecutorBackend"` ——
   模式串本身不再匹配自己，而执行器的 `YarnCoarseGrainedExecutorBackend` 照旧命中。
   修完三台都是 `rc=0` 且都打出了 executor 的 pid。
   （和 checker 里「静默返回 None」是同一类毛病：**命令的失败被自己的成功掩盖了**。）

## 验证结果

2026-09-13 在真实集群上逐个故障实测（批次驱动：`bash tests/batch_spark_live.sh [故障...]`）。

| 故障            | 目标 checker                       | 实测结论                                                                                                                                                                                                                                                                                                                                                                   |
|---------------|----------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `normal`      | 全部                               | ✅ 5/5 返回 `None`，零误报                                                                                                                                                                                                                                                                                                                                                    |
| `skew`        | `check_spark_data_skew`          | ✅ 两档都验过：10:1 注入 → `warning`（实测比 9.29，越 warning 线 5.0 未过 error 线 10.0）；20:1 注入（`--hot-records 800000`）→ **`error`**（实测比 25.50，与 `skew_threshold_sweep.py` 记录一致）。严重程度**只看倾斜比**，与 GC 无关；另见「已知问题」第 6 条（档位抢占的 bug）                                                                                                                                                          |
| `fmt` / `cls` | `check_other` 数据格式 / 类冲突         | ✅ 检出 `error`                                                                                                                                                                                                                                                                                                                                                           |
| `auth`        | `check_other` 认证/权限              | ✅ 检出 `error`                                                                                                                                                                                                                                                                                                                                                           |
| `connrefused` | `check_other` 数据源                | ✅ 检出 `error`（但见下方「已知问题」第 2 条）                                                                                                                                                                                                                                                                                                                                          |
| `src`         | `check_other` 数据源                | ✅ 检出 `error`，25 条任务级 `java.io.FileNotFoundException` 证据                                                                                                                                                                                                                                                                                                                |
| `oom_kill9`   | `check_spark_OOM` P1             | ✅ 修复后实测**不误报**（2026-09-14 netfail 两轮里共 95 / 100 个真实的 137 杀，`removeReason` 全含 `Killed by external signal`，新判据逐条判为「排除」）                                                                                                                                                                                                                                                  |
| `oom_limit`   | `check_spark_OOM` **error 档**    | ✅ **2026-09-15 验通 `error`**（P1 的正例，一直缺的那个）。两处证据：① NM 日志（YARN 原文）`Container [pid=44279,...] is running 28237824B beyond the 'PHYSICAL' memory limit. Current usage: 1.0 GB of 1 GB physical memory used`；② `check_spark_live.py` 报 `type='error'`/「存在因内存不足被杀的 Executor」。**`PHYSICAL` 2 次、`VIRTUAL` 0 次** —— 确认走的是物理分支（vmem 已关，见「集群侧前置条件」）。同轮另外 4 个 checker 全 `None`，无误报 |
| `fetchwait`   | `check_spark_network_timeout` P2 | ✅ 检出 `error`（wait_ratio 0.371）                                                                                                                                                                                                                                                                                                                                         |
| `stuck`       | `check_spark_job_timeout`        | ✅ 检出 `warning`（**修复 `state`→`status` 后才可能触发**，见「已知问题」第 1 条）                                                                                                                                                                                                                                                                                                            |
| `shrink`      | `check_spark_OOM`（误报验证）          | ✅ 未误报（该次 executor 没被回收；误报风险由 `netfail` 实测到，见第 3 条）                                                                                                                                                                                                                                                                                                                     |
| `spill`       | `check_spark_OOM` P2             | ✅ **检出 `warning`（2026-09-14，P2 首次成立）**：`即将出现OOM，请立马检查阶段 0，已经写入磁盘 319.52 MB数据`。**最终配置是 1600 万条 + 默认 10s 心跳**（没有改被测环境）：条件一实测 `69.0M / 72.3M = 1.00`（2/3 台采到满池）、条件二 `335,039,401` 字节。踩过的两个坑（算子不被追踪、executor 级峰值按心跳采样）见下方专节                                                                                                                                                |
| `netfail`     | `check_spark_network_timeout` P1 | ✅ 检出 `error`，**FAILED 分支命中**（2026-09-14：修好杀进程助手和注入时序后 2/2 可复现，且自愈完成后再跑一次仍能检出，见「已知问题」第 5 条）。此前两轮都只走 P2 分支。**⚠️ 2026-09-15 容量改动后注入失效、未复现 —— 见「已知问题」第 7 条**（checker 本身没动，不是检出能力退化）                                                                                                                                                                                        |

**仍未验证的分支**：**没有了 —— 2026-09-15 全部验通**（最后一项是 `check_spark_OOM` 的
`error` 档，见上表的 `oom_limit` 行）。留一条**复现路径的注意事项**：

- `check_spark_data_skew` 的 `error` 分支**结论成立、但复现路径没落盘**。
  它只由倾斜比决定（`skew_ratio > skew_error_ratio`，默认 10.0），验证时是**临时传参**
  把热点抬到 20:1（`skew_threshold_sweep.py` 实测比 25.50，代码注释里也记了这个数），
  但 `submit.sh` 里 `skew` 至今仍是 `--hot-records 400000`（10:1，实测比 9.29 → 只出
  `warning`）。**照现在的仓库状态跑批次，`skew` 只会出 warning。** 要复现 error 档得
  临时追加 `--hot-records 800000`。
  （另：「需要中位 `jvmGcTime > 0`」是**旧判据**的说法 —— 老代码用
  `skew_ratio / jvm_gc_time_skew_ratio > 2.0` 分档，中位 GC 为 0 时哨兵值会让除法抛异常、
  被 `except` 吞掉。判据早已改成只看倾斜比，见 `check_spark_data_skew.py` 的注释。）

### `spill` / P2：**已经验通了**（2026-09-14），以及为什么以前一直验不到

P2 是**与**条件。以前一条都成立不了，原因不在 checker，而在**注入本身** —— 两个条件
各有一个独立的坑，而且**都不在同一个地方**：

| P2 的条件                                                         | 卡在哪                                     |
|----------------------------------------------------------------|-----------------------------------------|
| 一：`peakMemoryMetrics.OnHeapExecutionMemory / maxMemory >= 0.9` | ①算子必须是「被追踪」的；②executor 级峰值是**按心跳采样**的   |
| 二：某 stage 的 `diskBytesSpilled > 0`                             | 同样要求被追踪的算子（溢写是 `TaskMemoryManager` 的行为） |

#### 坑一：算子不被 `TaskMemoryManager` 追踪时，指标恒为 0（**与任务时长无关**）

`groupBy().agg(collect_list(v))` 的物理算子是 **`ObjectHashAggregateExec`** ——
`collect_list` 属于 `TypedImperativeAggregate`，它的缓冲区是**普通 Java 对象、
不经 `TaskMemoryManager`**，因此 `peakExecutionMemory`／执行内存池／溢写**三样都不会发生**。

实测那一轮的任务是跑了 **17 秒、其中 8.7 秒在 GC** 的（内存确实在被大量分配），
而 `peakExecutionMemory`、`OnHeapExecutionMemory`、`diskBytesSpilled` **全是 0**。
计划证据见 `tests/spark/explain_plans.py`（`explain` 是惰性的，跑它不提交作业）。

#### 坑二：executor 级 `peakMemoryMetrics` 是**按心跳采样**的（默认 10 秒一次）

字段本身没问题 —— 同一份 `/allexecutors` 里 `JVMHeapMemory`、`MajorGC*` 一直有值，
因为**堆一直在用，随便哪个采样点都看得见**；而 `OnHeapExecutionMemory` 只有在
「采样点恰好落在某个被追踪算子占用执行池的时刻」才非零。

换上 `SortExec` 之后，**task 级** `peakExecutionMemory` 立刻是 `53,477,376`（51MB）/task、
stage 级 622MB —— **但 executor 级仍然读 0**，因为那个 stage 只跑了约 1 秒，采样点落不进去。

#### 受控实验：两个变量，一张 2×2

心跳间隔和任务时长各动一次（每组只改一个变量，其余全不动）：

| 数据量     | 心跳      | stage 墙钟  | 采样点数     | 采到的 `OnHeapExecutionMemory`                | 条件一 | P2 |
|---------|---------|-----------|----------|--------------------------------------------|-----|----|
| 400k    | 10s     | ~1s       | ~0.1     | 0（三台全 0）                                   | ✗   | ❌  |
| 400k    | **1s**  | ~1s       | ~1       | 68,131,447 / 72,338,636 = **0.942**（1/3 台） | ✓   | ✅  |
| 4M      | 10s     | 12.9s     | ~1.3     | 40.5M = 0.587                              | ✗   | ❌  |
| 4M      | **1s**  | ~13s      | ~13      | 69.0M = **1.00**（三台全是满池）                   | ✓   | ✅  |
| 4M      | 10s     | 10.6s     | ~1.1     | 67.1M = 0.972（1/3 台）                       | ✓   | ✅  |
| **16M** | **10s** | **34.4s** | **~3.4** | 69.0M = **1.00**（2/3 台）                    | ✓   | ✅  |

**两个变量的分工由此分开**：

- **数据量**决定"执行池能不能真占满"：400k 时最好也只到 0.942，16M 时能顶到满池 1.00；
- **采样点数 ≈ stage 墙钟 ÷ 心跳间隔**，决定"看不看得见"：4M/10s 只有 1.3 个点，
  撞上池子没满的瞬间就只剩 0.587；16M/10s 有 3.4 个点，2/3 台都顶到了 1.00。

**一个反直觉的实测**：**不能靠调 `--map-partitions` 拉长 stage**。4M + 16 分区是 12.9 秒，
4M + 64 分区反而只有 10.6 秒 —— wave 数涨 4 倍、单 task 时间跌 4 倍，正好抵消。
**stage 墙钟只跟总字节数走**（总字节 ÷ 集群吞吐），要加采样点就只能加数据。

**这条判据仍然不是"必然"**：16M 那轮三台里仍有一台没采到（30.5M = 0.44）。P2 能成立是因为
**只要有一台 active executor 的比值 ≥ 0.9 就够**，所以 2/3 命中即可。
换句话说：**"没报 P2"里混着「真的没超」和「没采到」两种，从 REST 上分不开** ——
这正是「让检测不了变得可见」要解决的问题。

#### 以前那两种归因为什么不成立

不是它们错，而是**它们从来没被有效检验过**：两次实验用的都是 `ObjectHashAggregate`
这种不被追踪的算子，指标**恒为 0、与时长无关** —— 所以「任务时长」这个变量在两次里
都不可能产生任何差异。当时被当成「反例」的那一轮，其实只是又一次的 0。
（原先那张「曾经验证/证伪过的结论」表因此一并作废，不留了。）
唯一还站得住的线索是 SPARK-34005（3.1.x 只在心跳携带 `SparkListenerExecutorMetricsUpdate`
时更新峰值）—— 它和坑二是同一件事的两个侧面。

#### 修好之后的注入形态（`fault_spill`，也是唯一验通过的那版）

```python
df = spark.range(...).withColumn("v", < 每行不同的2KB字符串 >)  # 必须非常量!
sorted_df = df.sortWithinPartitions("v").select("id")  # SortExec 经 TaskMemoryManager
sorted_df.foreachPartition(...)  # action 只拿到 id
```

三个细节都是必需的，每一个都有对照：

1. **`v` 必须非常量**：常量会被折叠（计划里直接内联 2048 个 `x`，shuffle 只写了 5.6MB），
   而且**常量排序键会被 `EliminateSorts` 整个剪掉**。
2. **排序之后先 `select("id")` 再交给 action**：Sort 仍在计划里（上层加一个 Project 带不走它），
   2KB 的 `v` 由 JVM 在排序时消化，**不再把 800MB 一行行灌进 Python worker** ——
   而 PySpark worker 的内存**不计入 YARN 容器**，那正是把节点压到**内核 OOM** 的路径
   （`dmesg`: `Out of memory: Kill process <java> ... anon-rss:1159600kB`）。
3. **不能接聚合**：排完直接 `groupBy().count()`，explain 显示 **Sort 被剪掉了**
   （聚合不需要顺序）。这个坑真实存在，`explain_plans.py` 里有对照组。

#### 生产上要记住的

条件一读的是 executor 级 `peakMemoryMetrics`，而它**按心跳采样**（默认 10 秒一次）。
能读到多少，取决于 **stage 墙钟 ÷ 心跳间隔** 有几个采样点、以及池子当时占没占满：

- **执行内存尖峰短于心跳间隔的作业，这个字段永远是 0** —— 不是没异常，是**测不到**；
- 十几秒的 stage 也只有 ~1 个采样点，常常撞在池子没满的瞬间（实测读到 0.587）；
- 要稳定读到接近 1.0，得是**几十秒以上、真的把执行池占满**的作业（16M 那轮 34.4 秒 / 3.4 个点 / 2 台到 1.00）。

这就是「让检测不了变得可见」的实证依据：`check_spark_OOM` 应该在
「executor 跑过任务、但执行内存峰值全 0」时打日志或降级，而不是静默 `return None`
让人以为集群健康。**"没报 P2"里混着「真的没超」和「没采到」两种，从 REST 上分不开。**

（想换判据的话先想清楚：task 级 `peakExecutionMemory` 是**按 stage 求和**的，不是峰值，
和 `maxMemory` 不是一回事，不能直接套 0.9。）

#### 另外：一条旧说法要更正

以前写着「spill 这类作业从提交到真正开始调度要 ~9 分钟」，所以配了 `--wait 900`。
2026-09-14 实测多次都是 **20~40 秒**就绪（集群空载 + `spark.yarn.archive` 已预热）——
那个 9 分钟是当时和别的应用挤在一起量到的，不是常态。`--wait-job-done` 仍然要留，
但 `--wait 900` 属于白白浪费。

## 已知问题

1. **`Tools/check_job_timeout.py` 的过滤参数名错**（已修，已复测）。Spark REST 的
   `/jobs` 只认 `status`，传 `state` 会被静默忽略、返回全部 job，导致
   `success_jobs` 里含正在运行的那个 job，`check_tasks_num_is_run` 一进门就
   `return False` —— **该 checker 永远报不出超时**。详见代码注释。
2. **`Connection refused` 被两个 checker 同时认领**（已修）。它既在 `check_other` 的
   `data_source_pattern` 里，也在 `check_spark_network_timeout` 的 `network_pattern` 里，
   同一个故障被报成「外部数据源故障」+「网络超时」。修法不是简单去重，而是按语义分家：
   **被拒绝是立即返回的、全程没有等待，它根本不是超时**，所以从网络侧移除、留在数据源侧。
   顺带收掉了第二处重叠 —— 数据源侧原来那个裸 `Timeout` 会匹配 `Connection timed out`，
   把网络超时也吞进来。现在两个正则**逐条比对过，交集为空**（见代码注释）。

   **残留的模糊情况**（没解决，说清楚）：executor 的 shuffle 服务挂了时，取数会抛
   `FetchFailedException`，其 cause 就是 `Connection refused` —— 这**确实是网络问题**，
   但会被 `check_other` 归成「外部数据源故障」。单靠正则分不开，要分得看异常嵌套结构。
3. **`tests/check_spark_live.py` 的就绪检测从没生效过**（已修）。它的 REST 基址只写到
   `/proxy/{app_id}`，于是 `{base}/jobs` 请求到的是 **Spark UI 的 HTML 页面**（也是 200），
   `.json()` 永远解析失败 → 「还没就绪」永远成立 → 每次都干等 `--wait` 用完才硬跑 checker。
   表现就是每轮都打印 `⚠️ 等超时, 继续跑(结果可能不准)`。**在这之前所有「全部返回 None」
   的轮次都不可信**（checker 是在任意时刻跑的，不是作业就绪后）；反之，凡是报出异常的轮次
   结论仍然成立 —— 故障当时确实在数据里。修完才能放心用 `--wait-job-done`。
4. **`check_spark_OOM` 对任意非零退出码都报 OOM**（已修）。`137 = 128+9 = SIGKILL`，
   而 137 只说明「被 SIGKILL」，不说明为什么：`kill -9`、YARN 抢占、节点故障、缩容
   全长这样。实测 `netfail`（ssh `kill -9`，与内存无关）让它报出 **72 条**
   「因内存不足被杀的 Executor」。
   修法是**按证据强度分档**，顺序即置信度顺序：
   内存专有文本 → `error`；
   `Killed by external signal` / `Container killed on request` / `Executor killed by driver`
   → 排除；只有非零退出码 → 降级 `warning`。
   判据抽成了纯函数 `classify_removed_executor`。

   **三个必须记住的点**：
    - **`oom_kill9` 之前那个「✅ 检出」是误报被当成了成功。** 修完之后它正确地不再上报。
    - ⚠️ **当初那版单元验证是循环论证，害得 `error` 档一直是坏的而没人发现。**
      正则写的是 `beyond physical memory limits` —— 照着记忆里的措辞写的，
      而**两种真实文本一个都不匹配**：YARN 原文是
      `is running ...B beyond the 'PHYSICAL' memory limit`（有 the、有引号、单数），
      Spark 改写后（`/allexecutors` 里实际给到 checker 的那条）是
      `Container killed by YARN for exceeding physical memory limits`。
      **用"照着正则造的文本"去验正则，当然能过。**
      2026-09-15 真正把 `error` 档验通（`oom_limit` 注入）之后才暴露，已修：
      正则同时覆盖两种真实措辞，回归见 `tests/verify_fixes.py` 第 8 条
      （样本是抓来的真文本 + 从源码格式串还原的，不是编的）。
      **教训：判据里的字面量必须来自抓到的真实样本。**
    - **退出码不能当前置门槛**：实测到一个 **143**（128+15，SIGTERM）的外部杀。YARN 杀容器
      是先 SIGTERM、宽限期过了才 SIGKILL，所以**内存超限杀也可能落在 143 上**。
      正因如此判据必须以内存文本为准，退出码只用于降级。
    - **内核 OOM 的漏报（2026-09-14 定位 + 解决）**：**内核 OOM 杀手**杀掉 executor 时，
      YARN 的 `removeReason` 和「外部 `kill -9`」**逐字节相同** —— 只有
      `Container killed on request / non-zero exit code 137 / Killed by external signal`，
      一个内存专有词都没有（137 = `ExitCode.FORCE_KILLED`，NM 的 `ContainerLaunch`
      直接走 "killed" 分支，**信号是谁发的从来没被记录**，SIGKILL 也不带发送者）。
      executor 层、task 层（`ExecutorLostFailure` 把同一段文本原样嵌进去）都分不开 ——
      实测两类样本 192 vs 37 条归一化后完全一致。
      **根因**：这套集群的容器**没有 cgroup 内存限制**（`DefaultContainerExecutor` +
      `yarn-site.xml` 里没有 resources-handler），内存超限的判定权交给内核；再叠上
      `yarn.nodemanager.resource.memory-mb` 没配、吃默认的 **8192MB**，而三台真实内存只有
      **3.2G / 3.2G / 4.7G** —— YARN 把 3.2G 的机器当 8G 卖，内核 OOM 几乎必然。

      **解决**：判据改看**节点侧实测值**。`/ws/v1/cluster/nodes` 的 `memUtilization`
      （= `nodePhysicalMemoryMB / 真实 MemTotal`，NM 自己算好的，不需要节点清单）。
      同一天实测**同一台 node3**：

      |              | 死前 `memUtilization`     | dmesg    | `removeReason` |
            |--------------|-------------------------|----------|----------------|
      | 内核 OOM       | **95.19%**（3109/3267MB） | 有 OOM 记录 | 一模一样           |
      | 外部 `kill -9` | 全程最大 **57.23%**         | 无        | 一模一样           |

      阈值取 85% 在本环境能干净分开。**注意这个值是瞬时的**（容器一死内存就还回去：
      95% → 62% → 55%），所以 checker 里配了一个**滚动采样线程**（`ensure_node_mem_sampler`，
      默认 5 秒一次，样本进 redis 的定长列表），判定时回看死亡时刻之前 90 秒内的峰值。
      **等发现 executor 死了再查是来不及的** —— 那时候读到的已经是还回去之后的正常值。
      阈值/间隔/回看窗口都在 `config.yaml` 的 `hadoop.yarn.spark` 段可配。
      判据回归见 `tests/verify_fixes.py` 第 7 条（正例 95.19% / 反例 57.23%，用的是同一段
      removeReason）。

      **残留**：报的是 `warning`（"疑似"）而不是 `error` —— 节点内存高不等于一定是内核杀的
      （抢占、缩容可能同时发生）。另外采样密度 = checker 进程活着时的密度；如果平台改成
      "发现异常才按需调用"，那一刻证据已经消失，只能落到「死因不明」再由 Agent 上节点查
      `dmesg`（那个是持久证据）。

5. **失败后重试成功的 stage，对 checker 完全不可见**（已修，新发现）。原来的
   `get_latest_stage_info` 按 `attemptId` 降序取「最后一次 attempt」，而 Spark 会重试
   stage —— 重试成功后最新 attempt 就是 `COMPLETE`，于是永远不进 FAILED 分支。
   实测 `application_..._0040`：stage 1 的 `attempt 0 = FAILED`
   （`FetchFailedException: ... remote executor(Id: 3) ... is dead`）、`attempt 1 = COMPLETE`，
   5 个 checker **全部返回 `None`**。生产里最早出现的劣化信号恰恰是这类可恢复的瞬时失败。
   （**那份抓包来自 `spill` 那轮，不是 `netfail`** —— 它的 stage 名是
   `foreachPartition at fault_job.py:289`、200 个 task。netfail 自己一直没造出过失败，
   见「验证结果」里它的那一行。）

   **修法**：`share_check_tool` 新增 `get_stage_summary`，一次取回两个视角 ——
   `latest`（最近一次 attempt）和 `latest_failed`（最近一次 FAILED 的 attempt）。
   失败判定（`check_other`、`network_timeout` 的失败分支）改用 **failed 侧**，
   指标判定（倾斜比、溢写字节、拉取等待占比）继续用 **latest 侧** ——
   失败 attempt 的指标是残缺的，拿它算比值没有意义。所以这不能用「改 `pick_stage_info`
   的取法」一刀切，必须按用途分开取。
   `StageAbnormalInfo` 也加了 `attempt_id`：`attempt_id > 0` 说明这是**已经自愈**的失败，
   仍然值得看，但和「当前正在失败」不是一回事。

   **离线回归**：用抓到的 0040 响应重放，`network_timeout_check` 现在能命中该
   `FetchFailedException` 并带上 `attempt_id=0`；修复前返回 `None`。
   `check_other` 不会把同一条误归成「其他异常」（不是它的 pattern）。

   **实况回归（2026-09-14）**：netfail 的注入修好之后（杀进程助手 + 去掉取数前的 sleep +
   拉长 reduce 取数期，详见 `fault_job.py` / `submit.sh` 的注释）拿到了真实的自愈场景 ——
   stage 3 的 `attempt 0 = FAILED`（`FetchFailedException: The relative remote
   executor(Id: 3), which maintains the block data to fetch is dead`）、
   `attempt 1 = COMPLETE`，作业最终 `SUCCEEDED`。在**自愈完成之后**再跑一次 checker，
   仍然报出这条失败；把**同一份抓包**喂给旧逻辑（`pick_stage_info`，只看最新 attempt）
   取到的是 `attempt 1 / COMPLETE` —— 那个分支根本进不去。这条修复的价值至此有了实测区分，
   不再是"离线重放说了算"。

6. **`check_spark_data_skew` 里 warning 会抢占 error**（已修，2026-09-14 新发现）。
   原来的 `check_stage_summary` 在遍历 stage 的循环里**直接 `return`**：

   ```python
   for future in as_completed(futures):      # ← 完成顺序不确定
       ...
       if severity == "error":
           return CheckResult(type="error", ...)
       return CheckResult(type="warning", ...)   # ← 也是直接 return
   ```

   于是**第一个超过 warning 线的 stage 就决定了结果**，不管它是哪一档。一个比值 6 的
   轻微倾斜 stage 只要先跑完，就把比值 25 的严重倾斜**整个挡在后面** —— 那个 future
   连取都没取（日志里连它的比值都不会打印）。而且这不是巧合：**严重倾斜的 stage
   通常更慢、更容易晚完成，所以 warning 往往是赢的那个** → 多 stage 作业里 error 档报不出来。
   `skew` 注入是单 stage，所以一直没暴露。

   **修法**：把超阈值的 stage 先收集起来，遍历完再取**最严重**的那个定档。
   **回归测试**在 `tests/verify_fixes.py` 第 6 条 —— 这个 bug 由完成顺序决定，
   线上很难稳定复现，所以测试里用「轻微倾斜的秒回、严重倾斜的 sleep 0.4 秒」来
   确定性地构造那个顺序（先复现失败、再修）。

7. **`netfail` 的注入在容量改动后失效 —— 未复现，待查（2026-09-15）**。
   **先强调：`check_spark_network_timeout` 的代码这轮一行没动**，它 2026-09-14 是验通过的
   （2/2 可复现）。退化的是**注入能不能构造出故障**，不是检出能力。

   **症状**：整批回归 10 个故障里 9 个干净，只有 `netfail` 5 个 checker 全 `None`。
   原始证据：`stage 2 = SKIPPED`（map 产物被认为可用）、`stage 3`（reduce，120 任务）
   `COMPLETE` 跑完，job `SUCCEEDED` —— **没有任何 FAILED 的 stage attempt**。

   **已修的一半（真 bug）**：注入写死了 `--kill-hosts node3`，而容量从 8192 改成 2048 后
   AM(1024) 独占一台、两个 executor(2048) 落到另外两台，**node3 恰好成了只剩驱动的那台**
   —— 杀了个寂寞。修法是 `executor_hosts(sc)` 现场问 Spark `getExecutorMemoryStatus()`
   挑一台真的有 executor 的机器，`submit.sh` 改成 `--kill-hosts auto`。
   重跑确认这次**真杀到了**（`kill node1 -> rc=0 pids=102578 102589`，
   `allexecutors` 里 executor 2 变 `inactive`）。

   **但依然没复现**。剩下的假设（按优先级）：
    1. **杀进程落地太晚** —— ssh 往返实际耗时可能比预期长，等 `kill -9` 生效时 reduce
       阶段已经跑完。**最省事的验证**：把 fetch 窗口再拉宽（提高 `--partitions`，或降
       `--executor-cores`/并发），看是否就复现了。
    2. **取数走了新拉起的 executor** —— 同主机 node1 上随即起了 id=3，需要看 **task 级
       fetch 指标**才能定（`/stages/{id}/{attempt}/taskSummary` 里那几个 shuffle 字段）。
    3. ~~map 产物被重算而非重取~~ —— `stage 2 = SKIPPED` 这条证据目前不支持它。

   **排查时的注意**：`--stages` 端点不返回 `submissionTime`/`completionTime`，
   时间线要用别的办法拿（stage attempt 端点或 Spark event log）。
   单轮实验约 5~10 分钟，可以省掉 checker 的 420 秒等待，直接盯驱动日志里有没有 FAILED stage。

   **判据上要小心**：这个故障的失败长得和"checker 漏检"**一模一样** —— 今天已经被
   写死主机名那次骗过一轮。**下次先确认注入有没有生效**（executor 是否真的变 inactive、
   有没有出现 FAILED 的 attempt），再谈 checker 报没报。
