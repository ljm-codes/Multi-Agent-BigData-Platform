FROM python:3.8-slim-bullseye

ENV PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
ENV PIP_DEFAULT_TIMEOUT=120

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /opt/sandbox/requirements.txt
RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir -r /opt/sandbox/requirements.txt

COPY init-sandbox.sh /usr/local/bin/init-sandbox.sh
RUN chmod +x /usr/local/bin/init-sandbox.sh

VOLUME ["/export/server/miniconda3"]
CMD ["/usr/local/bin/init-sandbox.sh"]
