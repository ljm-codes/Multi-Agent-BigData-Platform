# C3：Flink Kafka 脏数据故障 fixture

- 主类：`com.fm.EventParseJob`
- 输入：Kafka topic 中一条 `{"user_id":"bad","amount":"N/A","ts":...}`
- 故障：`parseAmount` 直接 `Integer.parseInt("N/A")` → `NumberFormatException`
- fixed-delay 重启（100 次 / 5s）让应用保持 RUNNING
- 修复：`parseAmount` 捕获异常，返回 0 或跳过脏数据
- 资源：JM 768m、TM 1024m、1 slot、并行度 1
