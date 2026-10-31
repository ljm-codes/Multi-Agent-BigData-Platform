#!/bin/bash
set -e

export JAVA_HOME=/opt/java/openjdk
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
export SPARK_HOME=/export/server/spark3
export FLINK_HOME=/opt/flink
export PATH=/usr/local/bin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$FLINK_HOME/bin:$PATH

: "${SSH_PASSWORD:=hadoop}"
echo "hadoop:${SSH_PASSWORD}" | chpasswd

mkdir -p /run/sshd /data/hadoop/tmp /data/hadoop/logs /data/hadoop/pids \
    /data/hdfs/name /data/hdfs/data /data/yarn/local /data/yarn/log
chown -R hadoop:hadoop /data
ssh-keygen -A >/dev/null 2>&1 || true
/usr/sbin/sshd

run_as_hadoop() {
    runuser -u hadoop -- env \
        JAVA_HOME="$JAVA_HOME" \
        HADOOP_HOME="$HADOOP_HOME" \
        HADOOP_CONF_DIR="$HADOOP_CONF_DIR" \
        YARN_CONF_DIR="$YARN_CONF_DIR" \
        HADOOP_LOG_DIR=/data/hadoop/logs \
        YARN_LOG_DIR=/data/hadoop/logs \
        HADOOP_PID_DIR=/data/hadoop/pids \
        SPARK_HOME="$SPARK_HOME" \
        FLINK_HOME="$FLINK_HOME" \
        PATH="$PATH" \
        "$@"
}

ROLE="${ROLE:-all}"
if [ "$ROLE" = "all" ] || [ "$ROLE" = "master" ]; then
    if [ ! -f /data/hdfs/name/current/VERSION ]; then
        run_as_hadoop "$HADOOP_HOME/bin/hdfs" namenode -format -force -nonInteractive
    fi
fi

if [ "$ROLE" = "all" ]; then
    run_as_hadoop "$HADOOP_HOME/bin/hdfs" namenode &
    run_as_hadoop "$HADOOP_HOME/bin/hdfs" datanode &
    run_as_hadoop "$HADOOP_HOME/bin/yarn" resourcemanager &
    run_as_hadoop "$HADOOP_HOME/bin/yarn" nodemanager &
    run_as_hadoop "$HADOOP_HOME/bin/yarn" proxyserver &
    run_as_hadoop /usr/local/bin/init-hdfs.sh &
elif [ "$ROLE" = "master" ]; then
    run_as_hadoop "$HADOOP_HOME/bin/hdfs" namenode &
    run_as_hadoop "$HADOOP_HOME/bin/yarn" resourcemanager &
    run_as_hadoop "$HADOOP_HOME/bin/yarn" proxyserver &
    run_as_hadoop /usr/local/bin/init-hdfs.sh &
else
    run_as_hadoop "$HADOOP_HOME/bin/hdfs" datanode &
    run_as_hadoop "$HADOOP_HOME/bin/yarn" nodemanager &
fi

wait
