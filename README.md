<h1 style="text-align:center;">
Multi-Agent BigData Platform（多智能体大数据平台）
</h1>

**⚠️ 注意：本项目目前是 Demo 版本，功能可用但仍在持续完善。**

## 项目介绍

### 它是什么

Multi-Agent BigData Platform 是一个以 `Agent` 为主、`大数据` 为背景、`后端` 为辅的多智能体大数据平台。

它不是传统意义上单纯的 `Data Agent`，而是把 **大数据开发**、**大数据分析**、**大数据运维** 三件事放在同一套 Agent 编排里。

平台**不部署在大数据节点上**，而是独立部署在一台服务器上，通过 YARN / Flink REST、HDFS WebHDFS、SSH
去访问集群。它像插件一样可插拔，不侵入、也不影响集群本身的运行。

### 为什么做它

1. 市面上没有同类开源、覆盖「开发 + 分析 + 运维」全链路的完整平台。
2. 通用编码工具（Claude Code、Codex 等）什么都会做，但都不专注某个领域；这个平台只面向大数据场景。
3. 通过技术栈硬约束保证模型「专一」：开发 Agent 只会 `Java + Flink`，分析 Agent 只会 `Python + Spark`，不做跨栈乱写。

### 能力一览

| 链路 | 能力                                          | 入口              |
|----|---------------------------------------------|-----------------|
| 开发 | 按任务写 Java + Flink 应用，编译失败自动修复               | 对话              |
| 分析 | 按任务写 Python + Spark 分析代码，语法检查自动修复           | 对话              |
| 质检 | 读代码 → 写测试 → 跑浅层 → **人工确认** → 跑深层 → **提交确认** | 开发/分析完成后自动进入    |
| 运维 | 巡检 Spark/Flink/集群异常 → 定位原因 → 给建议或转人工        | 后台常驻（暂未接入 HTTP） |

## 架构与思想

![项目架构图（手绘）](Multi-agent-BigData-Platform.drawio.png)

### 开发链路

```
用户消息
  └─ handler            把消息并入 state
      └─ dis_chat       主调度：只做意图判断与任务派发，强制结构化输出
          ├─ data_develop   Java + Flink：写代码 / 修代码 / 登记提交信息
          └─ data_analysis  Python + Spark：写代码 / 修代码 / 登记提交信息
              └─ 语法检查    mvn compile / compile()，失败打回（最多 4 次）
                  └─ code_verify 子图
                      ├─ 读代码、写测试
                      ├─ 浅层测试  验证业务结果
                      ├─ 人工确认  interrupt：允许 / 拒绝 / 强制终止
                      ├─ 深层测试  验证健壮性；超时转后台继续跑
                      ├─ 提交确认  interrupt：自动提交 / 手动提交
                      └─ work_end  汇总结果返回用户
```

### 运维链路

```
YARN 运行中的应用
  └─ checker（Spark 5 项 / Flink 7 项 / 集群 3 项）
      └─ 统一错误池（PostgreSQL log.checker_log）+ Redis 通知队列
          └─ Worker（n_workers）抢 app_leases 锁
              └─ 运维 Agent：只读取证 → 输出 handover（转开发）或 summary（记录）
```

### 几个关键设计

- **主调度不写代码**：`dis_chat` 只输出「派给谁、任务是什么」，避免上下文膨胀和越权。
- **强制结构化输出**：调度与质检都绑定 Pydantic 模型，必须调用工具返回结果，不解析自由文本。
- **人工确认用 LangGraph `interrupt`**：图在节点中间停住，前端拿到 payload 后决定继续、打回或终止。
- **两层测试**：浅层验证业务结果，深层验证抗造；深层超时用 `TaskSupervisor` 移交后台，完成后由感知层把结果送回原会话。
- **旁挂部署**：平台不装在大数据节点上，所有集群操作都走 REST / SSH / WebHDFS。

## 技术栈

| 层     | 选型                                                           |
|-------|--------------------------------------------------------------|
| Web   | FastAPI 0.135 + Uvicorn 0.42                                 |
| Agent | LangGraph 1.2（StateGraph / interrupt / checkpointer / store） |
| 模型    | DeepSeek（`deepseek-flash`），支持 thinking 模式                    |
| 数据    | PostgreSQL（会话状态 + 业务表）、Redis（JWT 密钥、运维队列）                    |
| 集群    | YARN REST、Flink REST、HDFS WebHDFS、节点侧 SSH                    |
| 前端    | Vue 3 + Vite 6 + Pinia + vue-router                          |
| 其他    | SQLAlchemy 2.0、APScheduler、bcrypt、PyJWT、MCP（Maven 依赖查询）      |

Python 3.12，Node 18+。

## 项目结构

```text
.
├── agent/                  各 Agent 的图定义与提示词
│   ├── develop_main_agent.py   主图：handler → dis_chat → 子 Agent → 质检 → work_end
│   ├── data_develop_agent.py   Java + Flink 开发 Agent
│   ├── data_analysis_agent.py  Python + Spark 分析 Agent
│   ├── code_verify_agent.py    质检子图（浅层/深层/人工/提交）
│   ├── _operations_agent.py    运维 Agent
│   ├── operations_agent_run.py 运维 Worker 主循环
│   └── prompt/                 各 Agent 的系统提示词（Markdown）
├── middleware/             模型调用洋葱链、后台结果感知
├── Tasks/                  后台任务总线、感知器、JWT 密钥轮换
├── Tools/                  工具集：代码读写、命令执行、SSH、各类 checker
├── SPO/                    状态模型、数据库模型、统一响应信封
├── routers/                HTTP 层：ai_chat / manager / open_app / start_file
├── load_config/            config.yaml 解析与占位符替换
├── scripts/                init_admin.py、mock_backend.py
├── static/source/          前端源码（Vue3 + Vite）
├── static/dist/            前端构建产物（后端直接托管）
├── sql/postgresql.sql      建库建表脚本
├── tests/                  端到端脚本与运维验证脚本（不是 pytest 用例）
├── main.py                 FastAPI 应用、中间件、图初始化
└── run.py                  启动入口
```

## 安装与部署

### 环境要求

- Python 3.12
- PostgreSQL（会话状态 + 业务表）
- Redis（JWT 密钥；运维队列取数需 **Redis ≥ 5.0**）
- Node 18+ / pnpm（前端）
- 可选：可访问的 YARN / Flink / HDFS / SSH，只有真实提交与运维才需要

### 1. 配置

```bash
cp .env.template .env          # 填 DEEPSEEK_API_KEY 等
cp config.yaml.template config.yaml   # 改数据库、集群地址、模型参数
```

`config.yaml` 里的 `${path:default}` 占位符会在加载时被替换，可以引用其它配置项。

### 2. 建库

```bash
psql -U postgres -f sql/postgresql.sql
```

`log.checker_log`、`admin_info`、`admin_thread`、`log.recommend_approval_log` 等表都由这个脚本创建；
LangGraph 的 `checkpoints` / `store` 表由程序启动时自动建。

### 3. 安装 Python 依赖

目前还没有 `requirements.txt`，核心依赖：

```bash
pip install fastapi uvicorn sqlalchemy "psycopg[binary]" redis bcrypt pyjwt apscheduler \
            "langgraph>=1.2" "langchain>=1.4" langchain-deepseek langchain-mcp-adapters \
            hdfs yarn-api-client pydantic python-dotenv pyyaml
```

### 4. 初始化第一个管理员

登录查的是**邮箱**，库里没有账号谁也进不去：

```bash
python scripts/init_admin.py --email admin@example.com --password Admin@123
```

### 5. 构建前端

```bash
cd static/source
pnpm install
pnpm build        # 产物输出到 static/dist
```

### 6. 启动

```bash
python run.py     # 默认 0.0.0.0:8000
```

打开 `http://127.0.0.1:8000/`，用上一步的邮箱密码登录。

### 只想看界面、不连数据库

```bash
python scripts/mock_backend.py   # 假后端，127.0.0.1:8010
```

不连 Postgres / Redis、不跑 LangGraph，用假数据把界面跑起来，适合调前端样式。

## 接口一览

| 方法       | 路径                               | 说明                              |
|----------|----------------------------------|---------------------------------|
| POST     | `/auth/admin/login`              | 登录，返回 JWT                       |
| GET/POST | `/auth/admin/recommend/register` | 推荐码校验 / 被推荐人注册                  |
| POST     | `/auth/admin/build/link`         | 生成邀请链接（一次性短码）                   |
| GET      | `/auth/admin/admin_info`         | 账号列表（邮箱脱敏）                      |
| POST     | `/auth/admin/account/status`     | 启用 / 封禁                         |
| POST     | `/auth/admin/account/update`     | 修改用户名 / 邮箱 / 权限（仅 permission=2） |
| POST     | `/ai/chat/stream`                | 流式对话（SSE）                       |
| POST     | `/ai/chat`                       | 非流式对话                           |
| POST     | `/ai/resume`                     | 回复 interrupt                    |
| GET      | `/ai/threads`                    | 会话列表                            |
| GET      | `/ai/history`                    | 会话历史                            |
| PUT      | `/ai/thread/archive`、`/archives` | 归档（单个 / 批量）                     |
| DELETE   | `/ai/thread/delete`、`/deletes`   | 删除（必须先归档）                       |
| GET      | `/ai/background`                 | 后台任务列表                          |
| WS       | `/ai/ws/background?token=`       | 后台任务推送                          |
| GET      | `/open/app/get_ides`             | 探测本机 IDE                        |
| POST     | `/open/app/open_file`            | 用 IDE 打开文件 / 项目目录               |

SSE 事件：`meta`、`update`、`token`、`interrupt`、`thread_switch`、`done`、`error`。

## 项目的不足

1. 仍是 Demo，缺少 `requirements.txt` / 依赖锁文件。
2. **运维链路还没接入 HTTP 层**，只有后台进程，前端暂时看不到巡检结果。
3. 运维取队列用的是 `ZPOPMIN`，需要 Redis ≥ 5.0；低于这个版本的异常会被吞掉，表现为「队列永远是空的」。
4. 运维 Agent 只能通过 SSH 拉代码，且相同代码每次都要重新做 AST 解析。
5. MCP 目前只用于 Maven 依赖查询，还没做通用 MCP / Skill 接入。
6. LangGraph 的 `store` 与 `cache` 已编译进图，但还没有节点声明 `cache_policy`，实际未启用。
7. 会话标题取首条用户消息前 10 字，重命名和置顶只存在浏览器本地。

## 项目的未来方向

1. 代码解析结果本地落库，避免反复 AST 解析。
2. 运维链路 API 化，前端补上巡检与告警面板。
3. 补依赖清单与一键部署脚本。
4. 扩充评测集，把「能跑通」变成「可衡量」。

## License

见 [LICENSE](LICENSE)。
