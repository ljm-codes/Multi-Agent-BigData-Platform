"""
把候选注入的**物理计划**打出来 —— 只为看计划, 不执行任何作业

起因: spill 注入实测 `peakExecutionMemory=0`、`diskBytesSpilled=0`、`OnHeapExecutionMemory=0`,
而同一个任务是跑了 17 秒、GC 了 8.7 秒的 —— 内存明明在被大量分配, 指标却全 0。
explain 一看就清楚了(2026-09-14 实测):

  * 算子确实是 **ObjectHashAggregateExec** —— `collect_list` 是 TypedImperativeAggregate,
    它的缓冲区是**普通 Java 对象、不经 TaskMemoryManager**, 所以既不报 peakExecutionMemory、
    也不占执行内存池、更不会溢写。P2 的两条判据都盯着被追踪的内存, 于是永远看不到。
  * `v = F.repeat(F.lit("x"), 2048)` 是**可折叠常量**: 计划里直接写成
    `collect_list(xxxxxx...xxx)`, 2048 个 x 内联进算子, shuffle 因此只写了 5.6MB。

改成排序是因为 SortExec 经 TaskMemoryManager 分配 —— 既占执行内存池、也会溢写。
这里专门确认"哪些写法会让 Sort 被优化器剪掉"(之前 `count()` 当 action 就剪过一次)。

用法(node1 上, local 模式, 只解析计划、不提交作业、不碰 YARN):
    PYSPARK_PYTHON=<py> $SPARK_HOME/bin/spark-submit --master local[1] \
        --conf spark.ui.enabled=false explain_plans.py
"""
import sys

from pyspark.sql import SparkSession, functions as F

RECORDS = 400000
VALUE_BYTES = 2048
MAP_PARTS = 16
SHUFFLE_PARTS = 8


def banner(title: str, note: str = ""):
    print("\n" + "=" * 78)
    print(f"### {title}")
    if note:
        print(f"### {note}")
    print("=" * 78)


def big_v():
    """每行不同的 2KB 字符串 —— 不能是常量, 否则会被折叠、Sort 也可能被消除"""
    return F.concat(F.repeat(F.lit("x"), VALUE_BYTES - 8),
                    F.lpad(F.col("id").cast("string"), 8, "0"))


def has_sort(plan_text: str) -> str:
    phys = plan_text.split("== Physical Plan ==")[-1]
    sorts = [ln.strip() for ln in phys.splitlines()
             if "Sort [" in ln]
    return (f"Sort 节点 {len(sorts)} 个" + (f" -> {sorts[0][:55]}" if sorts else "  **没有 Sort**")
            + f" | Exchange={'有' if 'Exchange' in phys else '无'}")


def main():
    spark = (SparkSession.builder
             .appName("plan-inspect")
             .config("spark.memory.fraction", "0.1")
             .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTS))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    base = spark.range(0, RECORDS, 1, MAP_PARTS)
    df = base.withColumn("v", big_v())

    cases = [
        ("对照: 现在的 groupBy().agg(collect_list(常量 value))",
         "确认对象哈希聚合; collect_list 的入参被折叠成常量字面量",
         base.withColumn("v", F.repeat(F.lit("x"), VALUE_BYTES))
             .groupBy(F.col("id") % 16).agg(F.collect_list("v"))),

        ("提议: sortWithinPartitions('v')  (非常量 key)",
         "fault_spill 打算改成这个; action 走 df.rdd.count()",
         df.sortWithinPartitions("v")),

        ("sortWithinPartitions('v') + select('id')",
         "模拟「输出里不含 v」—— 看列剪裁会不会把 Sort 带走",
         df.sortWithinPartitions("v").select("id")),

        ("sortWithinPartitions('v') + groupBy().count()",
         "**对照**: 已知会被剪掉的那种写法(count 不需要顺序)",
         df.sortWithinPartitions("v").groupBy().count()),

        ("orderBy('v')  (全局排序, 带 range 分区 Exchange)",
         "备选: 溢写压力更大(每个分区 100MB)",
         df.orderBy("v")),
    ]

    plans = {}
    for title, note, frame in cases:
        banner(title, note)
        try:
            plans[title] = frame._jdf.queryExecution().toString()
            print(plans[title])
        except Exception as e:
            print(f"取计划失败: {type(e).__name__}: {e}")

    print("\n" + "=" * 78)
    print("汇总(Sort 有没有被优化器剪掉):")
    for title, _, _ in cases:
        if title in plans:
            print(f"  {title}\n      {has_sort(plans[title])}")
    print("=" * 78)
    print("注意: explain 是惰性的, 上面一个作业都没执行 —— 这个脚本不会压爆节点")
    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
