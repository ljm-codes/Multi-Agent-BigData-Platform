"""C2 故障 fixture：脏数据让 UDF 抛 NumberFormatException，驱动捕获后保持 RUNNING。"""
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

INPUT = "hdfs://node1:8020/app/eval/c2/input.csv"


def parse_amount(value):
    # BUG：没有处理脏数据，N/A 会直接抛异常
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(
            "java.lang.NumberFormatException: For input string: %r" % (value,)
        )


def main():
    spark = SparkSession.builder.appName("eval-c2-spark-format").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.option("header", "true").csv(INPUT)
    parse_udf = F.udf(parse_amount, IntegerType())
    total = (
        df.select(parse_udf(F.col("amount")).alias("amount"))
        .agg(F.sum("amount"))
        .collect()[0][0]
    )
    print("TOTAL_AMOUNT=%s" % total)
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        print("EXPECTED_FAILURE=%s: %s" % (type(exc).__name__, exc))
        # 让 YARN 应用保持 RUNNING，给 checker 取证窗口
        time.sleep(300)
