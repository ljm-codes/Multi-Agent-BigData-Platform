FROM eclipse-temurin:8-jdk-jammy

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        openssh-server curl ca-certificates wget tar gzip zip procps \
        netcat-openbsd git hostname iproute2 libgomp1 passwd util-linux \
    && rm -rf /var/lib/apt/lists/*

ARG HADOOP_VERSION=3.3.6
ARG SPARK_VERSION=3.1.3
ARG FLINK_VERSION=1.18.1
ARG FLINK_SCALA=2.12

RUN set -eux; \
    mkdir -p /opt /export/server; \
    curl -fSL --retry 3 "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
        | tar -xz -C /opt; \
    mv "/opt/hadoop-${HADOOP_VERSION}" /opt/hadoop; \
    curl -fSL --retry 3 "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz" \
        | tar -xz -C /export/server; \
    mv "/export/server/spark-${SPARK_VERSION}-bin-hadoop3.2" /export/server/spark3; \
    curl -fSL --retry 3 "https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_${FLINK_SCALA}.tgz" \
        | tar -xz -C /opt; \
    mv "/opt/flink-${FLINK_VERSION}" /opt/flink

RUN useradd -m -s /bin/bash hadoop \
    && echo 'hadoop:hadoop' | chpasswd \
    && chown -R hadoop:hadoop /opt/hadoop /export/server /opt/flink

COPY conf/core-site.xml conf/hdfs-site.xml conf/yarn-site.xml conf/mapred-site.xml conf/hadoop-env.sh /etc/hadoop/conf/
COPY conf/sshd_config /etc/ssh/sshd_config.d/99-cluster.conf
COPY bin/ /usr/local/bin/
COPY entrypoint.sh init-hdfs.sh /usr/local/bin/
RUN cp /etc/hadoop/conf/*.xml /etc/hadoop/conf/hadoop-env.sh /opt/hadoop/etc/hadoop/ \
    && chmod +x /usr/local/bin/hadoop /usr/local/bin/hdfs /usr/local/bin/yarn \
        /usr/local/bin/spark-submit /usr/local/bin/flink \
        /usr/local/bin/entrypoint.sh /usr/local/bin/init-hdfs.sh \
    && ln -sf /usr/local/bin/hadoop /usr/bin/hadoop \
    && ln -sf /usr/local/bin/hdfs /usr/bin/hdfs \
    && ln -sf /usr/local/bin/yarn /usr/bin/yarn \
    && ln -sf /usr/local/bin/spark-submit /usr/bin/spark-submit \
    && ln -sf /usr/local/bin/flink /usr/bin/flink \
    && for j in java javac jar jps jstack jmap jstat; do ln -sf "/opt/java/openjdk/bin/$j" "/usr/bin/$j"; done \
    && mkdir -p /run/sshd /data/hadoop/tmp /data/hadoop/logs /data/hadoop/pids \
        /data/hdfs/name /data/hdfs/data /data/yarn/local /data/yarn/log \
    && chown -R hadoop:hadoop /data \
    && echo 'PATH="/usr/local/bin:/opt/java/openjdk/bin:/opt/hadoop/bin:/export/server/spark3/bin:/opt/flink/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin"' > /etc/environment

ENV JAVA_HOME=/opt/java/openjdk
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/etc/hadoop/conf
ENV SPARK_HOME=/export/server/spark3
ENV FLINK_HOME=/opt/flink
ENV PATH=/usr/local/bin:/opt/hadoop/bin:/export/server/spark3/bin:/opt/flink/bin:$PATH

EXPOSE 22 8020 9870 8088 8089 8042
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
