# Flink 故障注入作业

给 `Tools/flink/` 下的 checker 造真实故障，用来验证检出能力，以及验证健康集群下不误报。

## 环境

|       |                                                                 |
|-------|-----------------------------------------------------------------|
| Flink | 1.18.1，session 集群，`rest.address=node1` `rest.port=7788`         |
| Java  | 1.8（集群 `1.8.0_451`，本机编译用 `D:\Java\java8`）                       |
| 构建    | `javac` + `jar` 直接编，**不用 Maven**（本机 `mvn` 不在 PATH，`.mvn/` 是空目录） |
| 依赖来源  | 本机 `~/.m2/repository/org/apache/flink` 里已有的 30 个 1.18.1 jar     |

## 构建

```bash
bash tests/flink/build.sh          # Windows(Git Bash) 上执行
```

产物 `tests/flink/build/fault-flink.jar`，**只含自己的类**（Flink 自身的 jar 由集群提供，塞进来会导致冲突）。

## 提交

把 `fault-flink.jar`、`submit.sh`、`probe.sh` 传到 node1 的 `/home/hadoop/` 下，然后：

```bash
bash submit.sh normal              # 健康基线，先跑这个
bash submit.sh skew                # 数据倾斜
bash submit.sh hang                # 子任务挂起
bash submit.sh backpressure        # 反压
bash submit.sh checkpoint          # checkpoint 失败
bash submit.sh oom                 # TaskManager 高水位
bash submit.sh watermark_stalled   # 水位线停滞
```

## 故障模式与对应 checker

| `--fault`           | 目标 checker                 | 注入手法                         |
|---------------------|----------------------------|------------------------------|
| `normal`            | 全部                         | 健康基线，**应当全部返回 `None`**       |
| `skew`              | `check_flink_data_skew`    | 99% 记录打同一个 key，落同一子任务        |
| `hang`              | `check_flink_job_hang`     | 源发完数据后 hold 住不推进             |
| `backpressure`      | `check_flink_backpressure` | keyBy 后接慢算子（`--slow-ms`）     |
| `checkpoint`        | `check_checkpoint`         | `snapshotState` 里抛异常         |
| `oom`               | `check_flink_oom`          | 每个子任务保留 `--heap-fill-mb` 堆内存 |
| `watermark_stalled` | `check_flink_watermarks`   | 只发一次水位线，永不推进                 |
| `watermark_uninit`  | `check_flink_watermarks`   | 源不发水位线（期望 `Long.MIN_VALUE`）  |

## 设计约束（为什么作业要"挂着"）

这些约束是从 checker 的判定逻辑倒推出来的，不是随意加的：

1. **作业必须保持 `RUNNING`** — `share_check_tool.clean_finished_job_records()` 会把非 RUNNING 的作业过滤掉，结束了的作业
   checker 根本看不到。所以有 `--hold-seconds`。
2. **挂起 / 水位线停滞需连续 3 次轮询不变** — `HANG_TIMES_THRESHOLD = 3`，而 `poll_interval = 15s`，即至少 **45 秒**
   。默认 `hold-seconds=180` 留足余量。
3. **数据倾斜要求各子任务都有非零记录** — `check_vertex_data_skew` 在 `median_records == 0` 时直接跳过，所以不能把非热点
   key 压成 0 条。
4. **`oom` 模式有风险** — 保留大量堆内存可能真的把 TaskManager 打挂。默认 600MB 是保守值，按需调 `--heap-fill-mb`。

## 待验证的三个疑点

`probe.sh` 就是为了回答这三个问题写的，跑 `normal` 时顺手执行：

- **Q1** — `/applications/{app_id}` 接不接 YARN 的 `application_xxx_yyy`？Flink 文档说 `applicationid` 是「32 位十六进制字符串」，而
  checker 传的是 YARN app id。**若不认，7 个 Flink checker 全部要改。**
- **Q2** — `/taskmanagers/{id}` 的 `metrics` 里，键名究竟是 `heapUsed`/`heapMax`（checker 读的）还是 Flink
  标准的 `Status.JVM.Memory.Heap.Used`？**若键名不对，`check_flink_oom` 恒不触发。** 另外 Flink 的 metrics
  值是字符串，`heap_used / heap_max` 会 `TypeError`。
- **Q3** — 子任务的 `duration` 对 RUNNING 状态是恒定值还是递增？**若恒定，`check_flink_job_hang` 会在 3 次轮询后对每个健康作业误报挂起
  **（因为 RUNNING 的状态本来就不变）。schema 里另有 `status-duration` 字段，那才是为"在当前状态停留多久"设计的。
