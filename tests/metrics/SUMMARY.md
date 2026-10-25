# Agent 端到端测试数据摘要（2026-10-25）

> 本文是本次端到端测试的速查摘要。原始 JSON 已在收尾清理时删除（体积过大，避免污染仓库）；下表保留文件名/体积作为记录，重新运行 `tests/agent_e2e_metrics.py` 可再生成完整数据。

## 1. 结论

- Spark 与 Flink 两条真实任务链路都走通了：主调度 → 数据 Agent → CVA 浅层/深层 → 人工确认 → 提交确认。
- 两条正式提交都已在 YARN 上成功执行：
  - Spark：`application_1792913295382_0001`，FINISHED / SUCCEEDED，输出偶数 50、奇数 50、PASS。
  - Flink：`application_1792913295382_0005`，FINISHED / SUCCEEDED，输出 `hello=2, flink=2, world=1, is=1, fast=1`。
- 本次真实测试发现并修复了 16 处生产/提交链路问题，见第 6 节。
- 唯一不完美：Flink 的**完整 Agent 运行**报告 `flink_submit.json` 最终仍是打包失败时的结果；应用是通过修好后的提交函数对同一份生成项目重新提交成功的。若需要一份“Flink Agent 全绿 + submit 成功”的报告，需再跑一次完整 Flink Agent（约 120 万 input tokens）。

## 2. 测试环境与方式

- YARN RM：`node1:8088`（STARTED / ACTIVE）
- HDFS：RPC `node1:8020`，WebHDFS `node1:9870`
- SSH：`node1:22`；Postgres `localhost:5432`；Redis `localhost:6379`
- 模型：`deepseek-flash`
  - 主调度 / CVA：`thinking=disabled`
  - 数据 Agent：`thinking=enabled`
  - 测试脚本与 `run.py` 都应用了 `ChatDeepSeek.reasoning_content` 回传补丁
- 测试脚本：`tests/agent_e2e_metrics.py`
- 指标采集：LangGraph event streaming v3（experimental）
  - 原始 `tasks` channel：节点开始/结束、`input`、`result`、耗时
  - 原始 `tools` channel：工具 `tool-started` / `tool-finished`、输入/输出、耗时
  - `stream.messages` + 子图 messages：LLM 消息、工具调用、token usage
  - `stream.values` / `stream.output`：状态快照与最终 state
  - `stream.interrupts`：人工确认/提交确认
- 测试图使用 `InMemorySaver`；生产应用用 Postgres（本次没有走 FastAPI/SSE/WebSocket 入口）。

## 3. 运行清单

| 键 | 任务 | 提交 | 段数 | 最终结果 | 报告 |
|---|---|---|---:|---|---|
| smoke_chat | “你好，简单介绍一下你能做什么” | 否 | 1 | 正常回答 | `smoke_chat.json` |
| spark_small | Spark 统计 1~100 奇偶数量 | 否 | 3 | 手动提交（no） | `spark_small.json` |
| flink_small | Flink WordCount | 否 | 3 | 手动提交（no） | `flink_small.json` |
| spark_submit | Spark 统计 1~100 奇偶数量 | **是** | 3 | 已提交，YARN SUCCEEDED | `spark_submit.json` |
| flink_submit | Flink WordCount | **是** | 3 | Agent 运行在打包阶段失败 | `flink_submit.json` |
| flink_submit_result | 对现有项目直接重提 | **是** | — | YARN SUCCEEDED | `flink_submit_result.json` |
| spark_submit_result | YARN 应用结果 | — | — | SUCCEEDED + 实际输出 | `spark_submit_result.json` |

报告体积：

| 文件 | 体积 | 说明 | 当前状态 |
|---|---:|---|---|
| `smoke_chat.json` | 32 KB | 聊天 smoke | 已清理 |
| `spark_small.json` | 4.2 MB | Spark 未提交完整指标 | 已清理 |
| `flink_small.json` | 1.3 MB | Flink 未提交完整指标 | 已清理 |
| `spark_submit.json` | 4.1 MB | Spark 正式提交 Agent 指标 | 已清理 |
| `flink_submit.json` | 33 MB | Flink 正式提交 Agent 指标（含大量 CVA 测试循环） | 已清理 |
| `spark_submit_result.json` | 361 B | Spark 应用结果 | 已清理 |
| `flink_submit_result.json` | 3.1 KB | Flink 直接重提结果 | 已清理 |

## 4. Token 用量汇总

| 运行 | main | data_develop | data_analysis | code_verify |
|---|---:|---:|---:|---:|
| smoke_chat | 1,867 | 0 | 0 | 0 |
| spark_small | 1,867 | 0 | 42,706 | 191,484 |
| flink_small | 2,006 | 104,697 | 0 | 63,704 |
| spark_submit | 1,847 | 0 | 48,990 | 219,156 |
| flink_submit | 2,007 | 115,266 | 0 | 1,179,802 |

说明：

- `spark_small` 由旧采集器生成，`agents` 段里的 data_analysis/code_verify 用量为空；上表数值是从节点 task 结果补算的。`spark_submit` 已用修复后的采集器，归属正确。
- `flink_submit` 的 code_verify 1.18M token 主要来自 CVA 测试循环（113 节点/62 工具的首段），不是正常任务成本。正常 Flink 非提交运行是 63,704。

## 5. 各轮关键指标

### 5.1 smoke_chat

- 1 段，3 个节点，耗时约 2.2s。
- 主调度 1 次 LLM 调用，1,867 tokens。
- 用于验证：主图可导入、真实模型可用、v3 messages 投影、最终 state 输出。

### 5.2 spark_small（未提交）

- 3 段：initial 110.2s（55 nodes / 32 tools）→ resume 61.8s（5 nodes）→ submit-no 4ms（3 nodes）。
- 浅层测试 5 个全部 passed；深层完成；最终 `人工拒绝自动提交，采用手动提交任务`。
- 最慢节点：`code_verify` 87.6s、`ver_deep_exec` 58.1s、`ver_com_exec` 48.6s、`data_analysis` 20.3s。
- 最慢工具：`register_submit_command` 2.0s；其余代码读取/模块查询均 <100ms。
- 报告：`spark_small.json`。

### 5.3 flink_small（未提交）

- 3 段：initial 193.7s（17 nodes / 14 tools）→ resume 7.9s（5 nodes）→ submit-no 7ms（3 nodes）。
- 浅层 3 个用例全部通过；深层完成；最终手动提交（no）。
- 最慢节点：`data_develop` 161.4s、`code_verify` 29.3s、`ver_com_exec` 12.3s。
- 最慢工具：`initialize_project` 56.2s、`verify_big_data_app_code` 11.3s、`get_jar_by_maven` 10.4s、`register_submit_command` 2.0s。
- 报告：`flink_small.json`。

### 5.4 spark_submit（正式提交成功）

- 3 段：initial 80.7s（49 nodes / 29 tools）→ resume 44.3s（7 nodes / 1 tool）→ submit 33.1s（3 nodes）。
- 最终：`spark 应用已提交：app_name=parity_count_1_to_100`。
- YARN：`application_1792913295382_0001`，FINISHED / SUCCEEDED。
- 实际输出（YARN 日志）：
  ```
  偶数数量 / even count (n % 2 == 0): 50
  奇数数量 / odd count  (n % 2 != 0): 50
  校验结果 / result : PASS（奇偶合计等于总数）
  ```
- 报告：`spark_submit.json`；应用结果：`spark_submit_result.json`。

### 5.5 flink_submit（Agent 运行；提交失败）

- 3 段：initial 204.9s（113 nodes / 62 tools）→ resume 9.2s（9 nodes / 2 tools）→ submit 10.1s（3 nodes）。
- 最终输出：
  ```
  未找到 .../flink-wordcount-app/target 下的 jar 包
  ```
- 根因（两层）：
  1. CVA 生成的 `WordCountVerifyTest` 把 `null` 放进 `env.fromCollection`，Flink source 抛 `IllegalArgumentException`；
  2. 提交函数执行 `mvn clean package` 时又跑了一遍测试，测试失败导致 jar 没产出；且函数没有检查 Maven 返回码。
- 报告：`flink_submit.json`（体积大，主要来自 CVA 的 57 次 LLM 调用/测试循环）。

### 5.6 flink_submit_result（对现有项目直接重提成功）

- 修复提交函数后，对 `flink-wordcount-app` 直接重提：
  - 打包：`mvn clean package -Dmaven.test.skip=true`
  - HDFS 上传：`/app/flink/jar/flink-wordcount-app-0.1.0.jar`、`/app/flink/cmd/WordCountApp.cmd`
  - 提交：`flink run-application -t yarn-application -d ...`
- YARN：`application_1792913295382_0005`，FINISHED / SUCCEEDED。
- 实际输出（YARN 日志）：
  ```
  WordCount> (hello,1)
  WordCount> (world,1)
  WordCount> (hello,2)
  WordCount> (flink,1)
  WordCount> (flink,2)
  WordCount> (is,1)
  WordCount> (fast,1)
  ```
- 结果文件：`flink_submit_result.json`。

## 6. 真实测试发现并修复的问题

| # | 问题 | 修复位置 |
|---|---|---|
| 1 | `model.params.thinking` 被当 OpenAI 客户端参数直接传，真实调用报 `unexpected keyword argument 'thinking'` | `load_config/config.py` 新增 `get_model_params()`，thinking 放进 `extra_body` |
| 2 | 主调度 `tools=[]` 时没绑定 `DevelopAgentOutState`，模型自由文本→被判无法解析 | `agent/develop_main_agent.py` 始终绑定输出工具，无其它工具时 `tool_choice="any"` |
| 3 | CVA 条件边返回 `Command`，LangGraph 当节点名哈希，报 `unhashable type: 'dict'` | `agent/code_verify_agent.py` 条件边改 `Send`，新增 `ver_error` 节点回传父图 |
| 4 | CVA 深层 `completed` 后又选 `goto="user"`，人工→深层死循环 | 深层完成只允许 submit，路由兜底 + 提示词硬规则 |
| 5 | `langchain.mcp` 与 `fastmcp 3.1.1` 不兼容（缺 `fastmcp.client.group`） | `agent/data_develop_agent.py` 改用已安装的 `langchain-mcp-adapters` |
| 6 | HDFS URL 缺 `http://`，`InsecureClient` 报 `InvalidSchema` | `agent/code_verify_agent.py` 自动补 scheme |
| 7 | Flink 模板 pom 没有 JUnit 测试依赖 | `Tools/data_develop_agent_tools.py` 加 `junit:4.13.2` + `flink-test-utils` |
| 8 | Flink 提交：`status(strict=True)` 目录不存在直接抛异常 | 改用 `_hdfs_ensure_dir` |
| 9 | Flink 提交：HDFS jar/cmd 已存在时上传失败 | `upload(..., overwrite=True)` |
| 10 | Flink 1.18 `run-application` 不认 `-yjm/-ytm/-yn/-ys`；前台模式被 SSH 30s 截断 | 改 `-D jobmanager.memory.process.size` / `taskmanager.memory.process.size` / `taskmanager.numberOfTaskSlots`；加 `-d`；超时 120s；检查 rc |
| 11 | 提交打包重跑 CVA 测试，测试失败→无 jar | `mvn clean package -Dmaven.test.skip=true`，并检查 Maven 返回码 |
| 12 | CVA 把 `null` 放进 Flink `fromCollection` | 提示词补充：null/空行容错直接测纯函数，不走 Flink source |
| 13 | 指标采集：CVA 子图消息只 drain 未记录；根 `model` 消息无法归属 | `tests/agent_e2e_metrics.py` 记录子图 messages，按根 data Agent 归属，过滤非 AIMessage |
| 14 | 提交测试 recursion_limit=100 被 CVA 工具循环撞爆 | 测试脚本提高到 250 |
| 15 | 主调度允许错误的 Agent/app_type 组合 | `distribute_task_to_agent` 硬校验 data_develop→flink、data_analysis→spark |
| 16 | Flink 提交的 `app_name` 是 Redis bytes，生成了 `b'WordCountApp'.cmd` 文件名 | 解码 `app_name` 并校验非空 |

## 7. JSON 报告字段（供后续查看/训练数据筛选）

每份 Agent 报告顶层：

```text
thread_id, user_id, input, started_at, segments[]
```

每个 `segment`：

| 字段 | 内容 |
|---|---|
| `kind` | initial / resume / background |
| `duration_ms` | 该段图运行耗时 |
| `nodes[]` | 节点名、agent、namespace、input（完整 state）、result、error、起止时间、耗时 |
| `tools[]` | tool_name、tool_call_id、agent、namespace、input、output、error、耗时 |
| `agents` | main / data_develop / data_analysis / code_verify / root；含 llm_calls、usage、llm_messages、conversation、nodes、tools |
| `interrupts` | 人工确认/提交确认的原始 payload |
| `final_state` | 该段最终 state values（含 output_msg/metadata 等） |

常用筛选方式：

- 某 Agent 的完整对话：`segments[*].agents.<agent>.conversation`
- 某 Agent 的 token：`segments[*].agents.<agent>.usage`
- 节点入参/耗时：`segments[*].nodes`
- 工具输入/输出/耗时：`segments[*].tools`
- 最终输出：`segments[-1].final_state.output_msg`

## 8. 注意事项

- `spark_small.json` 是旧采集器生成的，`agents` 段里 data_analysis/code_verify 用量为空；第 4 节数值从节点结果补算。`spark_submit.json` 已修复归属。
- `flink_submit.json` 的最终结果仍是打包失败；成功提交结果在 `flink_submit_result.json`。若要单份全绿报告，需重跑完整 Flink Agent。
- v3 event streaming 是 LangGraph experimental API；测试使用了 `recursion_limit=250`。
- 运行时大量 `PydanticSerializationUnexpectedValue(... context ...)` 警告是 `create_agent` 上下文序列化噪音，不影响结果。
- 测试图使用 InMemorySaver；本次未经过 FastAPI SSE/WebSocket 入口。
- 本轮没有出现 `handed_off`，后台任务/QueuePerceptron 的真实链路没有被这两次任务触发。
- 原始测试 JSON 已清理；需要完整对话/节点入参时重新运行 `tests/agent_e2e_metrics.py`（注意 token 成本）。
- 当时 JSON 里包含完整代码、工具输出和项目路径；重新生成后如需分享，请先检查环境信息。

## 9. 下一步

- 前端界面（用户先放放）。
- 可选：重跑一次完整 Flink Agent，拿一份 `flink_submit.json` 全绿报告（预计约 120 万 input tokens）。
- 接 FastAPI 入口/SSE/WebSocket、Postgres checkpointer、以及一次真实的 `handed_off` 后台续跑 smoke。
