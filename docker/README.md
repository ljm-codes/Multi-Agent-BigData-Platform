# Docker 部署

一套 compose 拉起：API + Postgres/Redis + 单节点 Hadoop/YARN + Spark/Flink 客户端 + Python 3.8 沙箱。
默认是**单节点伪分布式**，省内存；第二 worker 和巡检/运维进程用 profile 按需开启。

## 按内存选模式

| VM 可用内存 | 启动命令 | 包含 |
|---|---|---|
| 4 GB | `docker compose -f docker-compose.lite.yml up -d` | app + Postgres + Redis + 沙箱；不连集群，UI/对话/本地沙箱可用 |
| 8 GB | `docker compose up -d` | 单节点伪分布式：node1 跑 NameNode/DataNode/RM/NM/Proxy/SSH，app 2g |
| 12 GB+ | `docker compose --profile full --profile workers up -d` | 再加 node2 worker、checker、ops |

lite 用 `config/config.apponly.yaml`（`connect_to_cluster: false`）；另外两种用 `config/config.docker.yaml`。

## 服务与版本

| 服务             | 说明                                                               | 版本 / 路径                                     |
|----------------|------------------------------------------------------------------|---------------------------------------------|
| `app`          | FastAPI + Vue 静态页 + Python 3.8 沙箱调用方                             | Python 3.12、JDK 8、Maven 3.9.9、Node 20       |
| `checker`      | 可选（`--profile workers`）：检测调度 `Tools.main_check_tool:main_check`       | 复用 `app` 镜像                                 |
| `ops`          | 可选（`--profile workers`）：运维 Agent `agent/operations_agent_run.py`     | 复用 `app` 镜像                                 |
| `node1`        | 默认单节点：NameNode + DataNode + RM + NM + WebAppProxy + SSH + Spark/Flink | Hadoop 3.3.6、Spark 3.1.3、Flink 1.18.1、JDK 8 |
| `node2`        | 可选（`--profile full`）：DataNode + NodeManager + SSH                | 同上                                          |
| `sandbox-init` | 初始化 Python 3.8 沙箱环境（一次性）                                         | Python 3.8 + pyspark 3.1.3                  |
| `postgres`     | LangGraph checkpoint/store + 业务表                                 | PostgreSQL 16                               |
| `redis`        | JWT、检测队列、运行状态                                                    | Redis 7                                     |

配置模板里的 MySQL、Elasticsearch、Milvus、RabbitMQ 当前代码并没有使用，这里不启动；
需要时再按实际接入的模块单独加服务。

沙箱环境路径固定为 `/export/server/miniconda3/envs/spark_study`，通过命名卷
`sandbox_env` 同时挂到 `app`、`node1`（以及可选的 `node2`）。这样：

- `app` 里的本地 pytest 用同一套 Python 3.8/pyspark；
- Spark on YARN 的 driver/executor 在 node 上也能找到 `SPARK_PYTHON`。

## 前置条件

- Docker Engine/Desktop（Linux containers）+ Docker Compose v2
- 内存：4 GB 走 lite、8 GB 走单节点、12 GB+ 走完整；磁盘 20 GB 以上
- 首次构建比运行更吃内存；4 GB 的 VM 建议临时给到 6 GB 或加 swap，构建完再降
- 能访问 Maven Central、PyPI、conda-forge、npm（首次构建和 Flink 依赖下载需要）

如果 Docker 跑在 VMware/VirtualBox/Hyper-V 的 Linux 虚拟机里，直接改**虚拟机设置**里的内存；
`C:\Users\<你>\.wslconfig` 只对 Windows 的 WSL2 生效，不要改错地方。

## 快速开始

```bash
cd docker
cp .env.example .env
# 编辑 .env：模型 Key、SSH/DB 密码（密码要和 config/config.docker.yaml 一致）

# 8 GB 单节点（默认）
docker compose build        # 首次 10~30 分钟
docker compose up -d
docker compose ps           # app + node1 healthy；lite 只有 app

# 4 GB 只跑 UI/对话/本地沙箱
docker compose -f docker-compose.lite.yml build
docker compose -f docker-compose.lite.yml up -d

docker compose exec app python scripts/init_admin.py \
  --email admin@example.com --password Admin@123

# 打开 http://localhost:8000/ 登录
```

lite 的 `exec` 也要带 `-f docker-compose.lite.yml`。

常用地址：

- 前端：`http://localhost:8000/`
- YARN：`http://localhost:8088/`
- HDFS：`http://localhost:9870/`

## 目录

```text
docker/
├── docker-compose.yml            # 单节点默认；--profile full/workers 扩展
├── docker-compose.lite.yml       # 4GB：只要 UI/对话/本地沙箱
├── .env.example
├── config/config.docker.yaml     # 单节点/完整模式
├── config/config.apponly.yaml    # lite 模式（connect_to_cluster: false）
├── app/                          # API 镜像
├── sandbox/                      # Python 3.8 环境镜像 + 依赖
├── cluster/                      # Hadoop/Spark/Flink/SSH 节点镜像
└── postgres/init/                # 建表脚本
```

## 沙箱（Python 3.8）

`sandbox/spark/module.txt` 是原来 Windows conda 环境的 pip freeze（该目录被 `.gitignore` 忽略）。
仓库里提交的是裁剪后的 `docker/sandbox/requirements.txt`，保留了生成代码会用到的：

- pyspark 3.1.3、pandas 2.0.3、numpy 1.24.4、scipy、scikit-learn、matplotlib、plotly
- xgboost、catboost、jieba
- pytest（原 conda freeze 里没有，但 CVA 会执行 `python -m pytest`，这里补上）

镜像基于官方 `python:3.8-slim-bullseye` + pip（清华 PyPI 源），不依赖 conda 源。
首次 `sandbox-init` 会把镜像里的 Python 3.8 运行时复制到卷内
`/export/server/miniconda3/envs/spark_study`，并生成带 `PYTHONHOME` / `LD_LIBRARY_PATH`
的 `bin/python3` 包装器；app 和 node 共用这个卷。

加包：编辑 `docker/sandbox/requirements.txt`，然后重建并清掉旧卷（卷不会自动刷新）：

```bash
docker compose build --no-cache sandbox-init
docker compose down
docker volume rm multiagent-bigdata_sandbox_env
docker compose up -d
```

lite 模式把上面的 `docker compose` 换成 `docker compose -f docker-compose.lite.yml`，
卷名换成 `multiagent-lite_sandbox_env`。

验证：

```bash
docker compose exec app /export/server/miniconda3/envs/spark_study/bin/python3 \
  -c "import pyspark, pandas, numpy, pytest; print(pyspark.__version__)"
```

`app` 的配置里对应：

```yaml
agent:
  DAA:
    test_python: /export/server/miniconda3/envs/spark_study/bin/python3
    python_lib_dir: /export/server/miniconda3/envs/spark_study/lib/python3.8/site-packages
    spark_test_master: local[*]
```

## 集群

- 默认 `node1` 是 **all**：NameNode、DataNode、ResourceManager、NodeManager、WebAppProxy、sshd，
  同时是 Spark/Flink 提交端（单节点伪分布式）。
- `--profile full` 才启动 `node2`（DataNode/NodeManager/SSH）；`node1` 的 RM 会自动发现它。
- 应用通过 SSH（`hadoop` 用户，密码来自 `.env` 的 `SSH_PASSWORD`）在 `node1` 执行：
    - `spark-submit ... --master yarn --deploy-mode cluster`
    - `flink run-application -t yarn-application`
- HDFS：`hdfs://node1:8020`，WebHDFS/UI `node1:9870`
- YARN：RM `node1:8088`，WebAppProxy `node1:8089`
- NodeManager：`yarn.nodemanager.resource.memory-mb=2048`；`node1` 容器 4g
- 守护进程堆已压到 NameNode/RM 384m、DataNode/NM 256m、Proxy 128m（`conf/hadoop-env.sh`）
- 首启 `init-hdfs.sh` 会自动建 `/user/hadoop`、`/app/spark/cmd`、`/app/flink/cmd`、`/tmp/logs`，
  并把 Spark jars 打成 `spark-libs.zip` 上传到 HDFS
- 如果部署机访问 archive.apache.org 不稳定，把三个 tarball 放到 `docker/cluster/vendor/`
  （文件名见 `cluster/Dockerfile` 的 ARG），构建会优先用本地包

验证：

```bash
docker compose exec node1 hdfs dfs -ls /
docker compose exec node1 spark-submit --version
docker compose exec node1 flink --version
docker compose exec app python -c "from Tools.node_ssh import run; print(run('node1', 'hostname'))"
```

### 扩 worker

```bash
docker compose --profile full up -d        # 启动 node2
docker compose --profile full --profile workers up -d  # 再加 checker/ops
```

继续加节点就复制 `node2` 为 `node3`（`ROLE: worker`），并给它单独的 `node3_data:/data` 卷
（NodeManager 的 local/log 目录不能和别的 worker 共用）；`node1` 的 YARN 会自动发现。

### 集群日志

```bash
docker compose exec node1 tail -f /data/hadoop/logs/hadoop-hadoop-namenode-node1.log
docker compose exec node1 tail -f /data/hadoop/logs/hadoop-hadoop-resourcemanager-node1.log
docker compose exec node1 tail -f /data/yarn/log/container_*.log 2>/dev/null
# node2 只在 --profile full 下存在
```

## 应用配置

- 标准/完整模式挂载 `config/config.docker.yaml`；lite 挂载 `config/config.apponly.yaml`
- `.env` 通过 compose `env_file` 传给 `app`（以及可选启用的 `checker`/`ops`）
- Postgres/Redis 密码在 `.env` 和配置文件里各有一份，**改一处要同步另一处**
- 沙箱路径、YARN/HDFS 地址、Spark 提交路径都已经按 compose 服务名/容器路径写好
- 模型：配置里的 `model.name` 与 `.env` 的 `DEEPSEEK_API_KEY` 配套；
  如果要走自建网关，按 `langchain-deepseek` 的 `DEEPSEEK_API_BASE` 设置（本仓库不读 `DEEPSEEK_BASE_URL`）
- 邮件告警默认留空；需要时填 `email` 段和 `EMAIL_AUTH_CODE`
- 标准/完整模式 `app.connect_to_cluster: true`；lite 是 `false`（HDFS/SSH 列出的地址不会真连）

## 常用命令

```bash
docker compose logs -f app
docker compose restart app
docker compose build app && docker compose up -d app
docker compose exec app bash
docker compose down                 # 保留数据卷
docker compose down -v              # 清空 Postgres/HDFS/沙箱/Maven 缓存
docker compose --profile full --profile workers up -d
docker compose --profile full --profile workers down
docker compose -f docker-compose.lite.yml logs -f app
docker compose -f docker-compose.lite.yml down -v
```

数据卷：标准/完整模式是 `pg_data`、`hdfs_data_node1`、`hdfs_data_node2`、`sandbox_env`、`m2`、`app_cache`、`app_tmp`；
lite 没有 HDFS 卷。`tmp_code/` 和 `log/` 是宿主机目录（bind mount），方便直接看生成的项目和日志。

## 排查

| 现象                   | 处理                                                                                                       |
|----------------------|----------------------------------------------------------------------------------------------------------|
| `sandbox-init` 失败    | 看 `docker compose logs sandbox-init`；多半是 conda/PyPI 网络或某个包没有 cp38 wheel                                  |
| `app` 起不来            | 先确认 `config.yaml` 已挂载、`static/dist` 在镜像里；看 `logs/app`                                                    |
| `node1` 一直 unhealthy | NameNode 首次格式化失败或端口被占用；看 `/data/hadoop/logs`                                                             |
| Spark 提交失败           | 确认 `node1` 上 `spark-submit --version`、`hdfs dfs -ls /user/hadoop/spark-libs.zip`、sandbox 路径存在  |
| Flink 提交失败           | 确认 `node1` 的 `flink --version`、`HADOOP_CONF_DIR` 和 `yarn` 可用；命令必须是 `run-application -t yarn-application` |
| Flink 编译失败           | `app` 内 `mvn -v`、JDK 8；首次构建需要 Maven Central；可预先灌满 `m2` 卷                                                 |
| 登录报“用户不存在”           | 先执行 `docker compose exec app python scripts/init_admin.py ...`                                           |
| 内存不足                 | 先降级：4GB 用 lite，8GB 用单节点；虚拟机在 hypervisor 设置里加内存，改 `.wslconfig` 只对 WSL2 有效                          |
| 看不到 node2/checker/ops | 它们默认在 profile 里：`--profile full`（node2）、`--profile workers`（checker/ops）                                  |
| 前端“打开 IDE”不可用       | 容器里没有 Windows IDE/注册表；这是宿主机的桌面功能，Docker 部署下只保留代码链接展示                                                        |

## 安全说明

这是**演示/内网**部署：SSH 用密码、Postgres/Redis 用默认密码、端口未做 TLS 和访问控制。
不要直接暴露到公网；上生产请换密钥/强密码、限制端口、给 Postgres/Redis 加网络隔离。

另外，这里的“沙箱”是 **Python 3.8 版本隔离**，不是安全隔离：生成代码由 API 进程以子进程方式执行，
和 API 在同一个容器里。不要把沙箱当成不可信代码的执行边界。
