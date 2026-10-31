# syntax=docker/dockerfile:1.6

FROM maven:3.9.9-eclipse-temurin-8 AS maven

FROM node:20-bullseye-slim AS node

FROM node:20-bullseye-slim AS web
WORKDIR /src
RUN corepack enable && corepack prepare pnpm@10.33.0 --activate \
    && pnpm config set registry https://registry.npmmirror.com
COPY static/source/package.json static/source/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
COPY static/source/ ./
RUN pnpm build

FROM python:3.12-slim-bookworm AS app

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates git libstdc++6 libgomp1 fontconfig libfreetype6 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=maven /opt/java/openjdk /opt/java/openjdk
COPY --from=maven /usr/share/maven /usr/share/maven
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -sf /usr/local/lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && ln -sf /usr/local/lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx \
    && npm config set registry https://registry.npmmirror.com \
    && (npm install -g mcp-maven-deps || true)

ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=/usr/share/maven/bin:/opt/java/openjdk/bin:/usr/local/bin:$PATH
ENV PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
ENV PIP_DEFAULT_TIMEOUT=120

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .
COPY --from=web /dist /app/static/dist
COPY docker/app/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh \
    && mkdir -p /root/.m2/repository /app/tmp_code /app/log /app/cache /app/tmp

EXPOSE 8000
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["python", "run.py"]
