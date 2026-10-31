FROM eclipse-temurin:8-jre AS jre

FROM debian:bullseye-slim

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates bzip2 build-essential libgomp1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=/opt/java/openjdk/bin:$PATH

ARG MICROMAMBA_VERSION=latest
RUN curl -Ls "https://micro.mamba.pm/api/micromamba/linux-64/${MICROMAMBA_VERSION}" \
        | tar -xvj -C /usr/local bin/micromamba
ENV MAMBA_ROOT_PREFIX=/opt/micromamba

COPY requirements.txt /opt/sandbox/requirements.txt
RUN micromamba create -y -p /export/server/miniconda3/envs/spark_study \
        -c conda-forge python=3.8 pip \
    && /export/server/miniconda3/envs/spark_study/bin/python -m pip install \
        --no-cache-dir -r /opt/sandbox/requirements.txt

COPY init-sandbox.sh /usr/local/bin/init-sandbox.sh
RUN chmod +x /usr/local/bin/init-sandbox.sh

VOLUME ["/export/server/miniconda3"]
CMD ["/usr/local/bin/init-sandbox.sh"]
