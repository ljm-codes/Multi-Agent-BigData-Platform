# C2：Spark 脏数据故障 fixture

输入 CSV（valid 行合计 60）：

```text
user,amount
A,10
B,20
C,N/A
D,30
```

`parse_amount` 对 `N/A` 直接 `int()`，任务侧抛 `NumberFormatException`；驱动捕获后 `sleep(300)` 保持 RUNNING，供 checker 检出。修复后应输出 `TOTAL_AMOUNT=60`（脏数据按 0 或丢弃都可）。
