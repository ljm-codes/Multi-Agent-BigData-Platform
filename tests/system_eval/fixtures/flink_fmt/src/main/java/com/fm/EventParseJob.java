package com.fm;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * C3 故障 fixture：amount 字段直接 Integer.parseInt，遇到 N/A 抛 NumberFormatException。
 * fixed-delay 重启让 YARN 应用保持 RUNNING，供 checker 从 Flink REST 的 exception history 取证。
 */
public class EventParseJob {

    private static final Pattern AMOUNT = Pattern.compile("\"amount\"\\s*:\\s*\"([^\"]*)\"");

    /** 故意不处理脏数据；修复后应返回 0 或跳过。CVA 可直接测试这个静态方法。 */
    public static int parseAmount(String line) {
        Matcher matcher = AMOUNT.matcher(line == null ? "" : line);
        if (!matcher.find()) {
            return 0;
        }
        return Integer.parseInt(matcher.group(1));
    }

    public static void main(String[] args) throws Exception {
        String topic = "eval-c3-bad";
        String bootstrap = "node1:9092";
        String group = "eval-c3-flink-format-" + System.currentTimeMillis();
        for (int i = 0; i < args.length - 1; i++) {
            if ("--topic".equals(args[i])) {
                topic = args[i + 1];
            } else if ("--bootstrap".equals(args[i])) {
                bootstrap = args[i + 1];
            } else if ("--group".equals(args[i])) {
                group = args[i + 1];
            }
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10_000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(100, 5_000L));

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(group)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "eval-kafka")
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) {
                        int amount = parseAmount(value);
                        return "PARSED=" + amount;
                    }
                })
                .name("parse-amount")
                .print();

        env.execute("eval-c3-flink-format");
    }
}
