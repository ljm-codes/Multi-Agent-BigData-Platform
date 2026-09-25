package com.ops.fault;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 故障注入作业 —— 一个类覆盖多种异常, 用 --fault 选择。
 *
 * 设计约束(来自 checker 的判定逻辑):
 *   1. checker 只观测 RUNNING 状态的作业, 所以作业必须存活到观测完毕 -> --hold-seconds
 *   2. 挂起 / 水位线停滞需连续 3 次轮询不变(3 × poll_interval 15s = 45s) -> 用 --hold-seconds 覆盖
 *   3. 数据倾斜要求各子任务都有非零记录(中位数为 0 时 checker 会跳过)
 *
 * 用法:
 *   flink run -c com.ops.fault.FaultJob fault-flink.jar --fault normal --hold-seconds 120
 */
public class FaultJob {

    // ---------------- 参数 ----------------
    // 必须可序列化: RateSource 持有它的引用, Flink 会把这个闭包序列化后发到 TaskManager
    static class Opts implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        String fault = "normal";
        int parallelism = 3;
        long holdSeconds = 120;
        int ratePerSec = 200;
        long records = 200000;
        int slowMs = 0;          // backpressure: 每个元素 sleep 的毫秒数
        int recordSize = 0;      // 每条记录填充到多少字节(0 表示不填充)
        int heapFillMb = 0;      // oom: 每个并行子任务保留的堆内存
        int checkpointIntervalMs = 10_000;
        int checkpointTimeoutMs = 60_000;
        String checkpointDir = "file:///tmp/flink-checkpoints";

        static Opts parse(String[] args) {
            Opts o = new Opts();
            for (int i = 0; i < args.length - 1; i++) {
                String k = args[i], v = args[i + 1];
                if (!k.startsWith("--")) continue;
                switch (k) {
                    case "--fault": o.fault = v; break;
                    case "--parallelism": o.parallelism = Integer.parseInt(v); break;
                    case "--hold-seconds": o.holdSeconds = Long.parseLong(v); break;
                    case "--rate": o.ratePerSec = Integer.parseInt(v); break;
                    case "--records": o.records = Long.parseLong(v); break;
                    case "--slow-ms": o.slowMs = Integer.parseInt(v); break;
                    case "--record-size": o.recordSize = Integer.parseInt(v); break;
                    case "--heap-fill-mb": o.heapFillMb = Integer.parseInt(v); break;
                    case "--checkpoint-interval-ms": o.checkpointIntervalMs = Integer.parseInt(v); break;
                    case "--checkpoint-timeout-ms": o.checkpointTimeoutMs = Integer.parseInt(v); break;
                    case "--checkpoint-dir": o.checkpointDir = v; break;
                    default: System.out.println("[warn] 未知参数 " + k);
                }
            }
            return o;
        }
    }

    // ---------------- 数据源 ----------------
    /**
     * 可控速率的字符串源。
     * emit 阶段结束后进入 hold 阶段: 不再发数据但保持 RUNNING, 让 checker 有时间观测。
     * 用 --fault 决定 key 的分布(倾斜与否)以及是否发水位线。
     */
    public static class RateSource extends RichSourceFunction<String> {
        private final Opts opts;
        private final boolean skewed;
        private final boolean emitWatermark;

        private volatile boolean running = true;
        private transient long counter = 0;

        RateSource(Opts opts, boolean skewed, boolean emitWatermark) {
            this.opts = opts;
            this.skewed = skewed;
            this.emitWatermark = emitWatermark;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            long start = System.currentTimeMillis();
            long intervalNs = opts.ratePerSec > 0 ? 1_000_000_000L / opts.ratePerSec : 0;
            long nextEmit = System.nanoTime();

            // 水位线故障时只发一次固定水位线, 之后不再推进 -> stalled / uninitialized
            boolean watermarkOnce = "watermark_stalled".equals(opts.fault);
            if (emitWatermark && !watermarkOnce) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.emitWatermark(new Watermark(System.currentTimeMillis()));
                }
            }

            while (running && counter < opts.records) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(record(counter));
                    counter++;
                    if (emitWatermark) {
                        if (watermarkOnce) {
                            // 只在第 0 条发一次水位线, 此后永不推进
                            if (counter == 1) {
                                ctx.emitWatermark(new Watermark(System.currentTimeMillis()));
                            }
                        } else {
                            ctx.emitWatermark(new Watermark(System.currentTimeMillis()));
                        }
                    }
                }
                if (intervalNs > 0) {
                    nextEmit += intervalNs;
                    long sleep = nextEmit - System.nanoTime();
                    if (sleep > 0) {
                        // 用毫秒粒度 sleep, 纳秒自旋留给剩余部分
                        Thread.sleep(sleep / 1_000_000, (int) (sleep % 1_000_000));
                    }
                }
            }

            long elapsedSec = (System.currentTimeMillis() - start) / 1000;
            System.out.println("[FaultJob] 源已发送 " + counter + " 条, 耗时 " + elapsedSec
                    + "s, 进入 hold " + opts.holdSeconds + "s (作业保持 RUNNING)");

            // hold: 不发数据, 但作业仍是 RUNNING, 供 checker 观测
            long holdEnd = System.currentTimeMillis() + opts.holdSeconds * 1000L;
            while (running && System.currentTimeMillis() < holdEnd) {
                Thread.sleep(500);
            }
        }

        private static final char[] FILLER = new char[1024];

        static {
            java.util.Arrays.fill(FILLER, 'x');
        }

        private String record(long i) {
            long ts = System.currentTimeMillis();
            String base;
            if (skewed) {
                // 99% 记录打同一个 key, 落到同一个子任务 -> 真实的数据倾斜
                boolean hot = (i % 100) != 0;
                base = (hot ? "hot" : ("k" + (i % 97))) + "\t" + i + "\t" + ts;
            } else {
                base = "k" + (i % 37) + "\t" + i + "\t" + ts;
            }
            return pad(base);
        }

        /**
         * 把记录填充到 --record-size 字节。
         * 制造反压时必须填充: 网络缓冲池按内存计, 若记录只有几十字节, 要花几十分钟
         * 才能填满下游缓冲, 期间 backpressureLevel 一直是 ok, 测不出反压。
         */
        private String pad(String s) {
            if (opts.recordSize <= s.length()) {
                return s;
            }
            StringBuilder sb = new StringBuilder(opts.recordSize);
            sb.append(s);
            while (sb.length() < opts.recordSize) {
                sb.append(FILLER, 0, Math.min(FILLER.length, opts.recordSize - sb.length()));
            }
            return sb.toString();
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    // ---------------- 各故障算子 ----------------
    /** 慢算子: 制造反压 */
    public static class SlowMap extends RichMapFunction<String, String> {
        private final int slowMs;

        SlowMap(int slowMs) { this.slowMs = slowMs; }

        @Override
        public String map(String value) throws Exception {
            if (slowMs > 0) Thread.sleep(slowMs);
            return value;
        }
    }

    /** 堆内存占用: 制造 TaskManager 高水位 */
    public static class HeapFiller extends RichFlatMapFunction<String, String> {
        private final int fillMb;
        private final List<byte[]> retained = new ArrayList<>();

        HeapFiller(int fillMb) { this.fillMb = fillMb; }

        @Override
        public void flatMap(String value, Collector<String> out) {
            if (fillMb > 0 && retained.isEmpty()) {
                for (int i = 0; i < fillMb; i++) {
                    retained.add(new byte[1024 * 1024]); // 每块 1MB
                }
                System.out.println("[FaultJob] 已保留 " + fillMb + "MB 堆内存");
            }
            out.collect(value);
        }
    }

    // 注: 曾用「snapshotState 里抛异常」注入 checkpoint 失败, 但那会让作业整体失败并进入
    // RESTARTING 状态, 而 checker 只扫 state == "RUNNING" 的作业 —— 重启中的作业它看不见,
    // 注入反而测不出东西。现改为把 checkpointTimeout 调到极小(见 --checkpoint-timeout-ms),
    // checkpoint 因超时被标记为 FAILED 且带 failure_message, 作业本身继续 RUNNING。

    // ---------------- 主流程 ----------------
    public static void main(String[] args) throws Exception {
        Opts opts = Opts.parse(args);
        System.out.println("[FaultJob] fault=" + opts.fault + " parallelism=" + opts.parallelism
                + " hold=" + opts.holdSeconds + "s records=" + opts.records + " rate=" + opts.ratePerSec);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(opts.parallelism);

        // checkpoint 相关的故障需要开启 checkpoint
        boolean needCheckpoint = opts.fault.startsWith("checkpoint") || opts.fault.equals("normal")
                || opts.fault.equals("skew") || opts.fault.equals("backpressure")
                || opts.fault.equals("oom");
        if (needCheckpoint) {
            env.enableCheckpointing(opts.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setCheckpointTimeout(opts.checkpointTimeoutMs);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2_000);
            env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            env.getCheckpointConfig().setCheckpointStorage(opts.checkpointDir);
        }

        boolean skewed = opts.fault.equals("skew");
        // 只有水位线类故障才发水位线; 其它模式不引入事件时间语义
        boolean emitWatermark = opts.fault.startsWith("watermark");

        DataStream<String> source = env.addSource(
                new RateSource(opts, skewed, emitWatermark), "fault-source");

        DataStream<String> stream = source;

        if (opts.fault.equals("hang")) {
            // 挂起: 源发完数据后在 hold 阶段完全不推进, 子任务状态与时长都不再变化
            // (源本身即卡住点, 无需额外算子)
            System.out.println("[FaultJob] hang 模式: 依赖源的 hold 阶段停滞");

        } else if (opts.fault.equals("oom")) {
            stream = stream.flatMap(new HeapFiller(opts.heapFillMb)).name("heap-filler");

        } else if (opts.fault.equals("backpressure")) {
            // 反压只体现在"慢算子的上游 task"上, 而 source 任务没有输入门(input gate),
            // Flink 无法为它上报背压 —— 直接把慢算子接在源后面会永远测不出 backpressureLevel。
            // 因此中间插一个 fast-map: 它会被下游的 slow-map 堵住, 从而上报 high。
            stream = stream.keyBy(v -> v.split("\t")[0])
                    .map(new SlowMap(0)).name("fast-map");
            stream = stream.keyBy(v -> v.split("\t")[0])
                    .map(new SlowMap(opts.slowMs)).name("slow-map");
        } else {
            // normal / skew / watermark_*: 都走 keyBy + 聚合, 保证有 shuffle
            stream = stream.keyBy(v -> v.split("\t")[0])
                    .map(new SlowMap(opts.slowMs)).name("keyed-map");
        }

        stream.addSink(new DiscardingSink<>()).name("discard");

        env.execute("FaultJob-" + opts.fault);
    }
}
