#!/bin/bash
set -e

for _ in $(seq 1 90); do
    if "$HADOOP_HOME/bin/hdfs" dfs -ls / >/dev/null 2>&1; then
        break
    fi
    sleep 2
done

"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /user/hadoop /app/spark/cmd /app/flink/cmd /tmp/logs
"$HADOOP_HOME/bin/hdfs" dfs -chown -R hadoop:hadoop /user/hadoop /app
"$HADOOP_HOME/bin/hdfs" dfs -chmod -R 777 /tmp/logs

# 预上传 Spark jars，提交时走 spark.yarn.archive（不存在时应用会自动跳过）
if [ ! -f /tmp/spark-libs.zip ]; then
    (cd "$SPARK_HOME/jars" && zip -qr /tmp/spark-libs.zip .)
fi
"$HADOOP_HOME/bin/hdfs" dfs -put -f /tmp/spark-libs.zip /user/hadoop/spark-libs.zip
