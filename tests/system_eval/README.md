# system_eval：系统化 Agent 评测

真实 HTTP 入口（登录 → SSE → resume → 后台 → 运维看板）的 9 用例评测：分析 / 开发 / 运维各 3 例。
数据写到 `tests/system_eval_data/<run_id>/`，不使用 `tests/metrics`。

## 快速开始

```bash
# 全量（分析+开发+运维）：会临时改 config.yaml 指向独立评测库，结束时恢复
python -m tests.system_eval.run_all

# 只跑某一环节
python -m tests.system_eval.run_all --stages analysis
python -m tests.system_eval.run_all --ids A1,B2

# 断点续跑（跳过已完成的用例）
python -m tests.system_eval.run_all --run-id <已有 run_id> --resume
```

跑前确认：VM 的 HDFS/YARN/Hive/ZK/Kafka、本机 Postgres/Redis 已启动；`tests/O&M/spark` 可被复制到 node1；
`D:\conda\envs\spark_sandbox` 与 `Maven/mvn-3.9.9` 存在。预检会检查并落盘。

## 隔离与告警

- `config.yaml` 备份后临时改：`connect_to_cluster=true`、`databases.postgres.db=multiagentbigdataplatform_eval`；
  跑完恢复原文件（`python -m tests.system_eval.config_env restore` 可手动兜底）。
- 独立评测库，不污染现有业务/会话数据；跑完保留库供复查。
- 服务进程内 patch 飞书/邮件发送为 no-op，调用记到 `<run>/alerts.jsonl`；不修改业务代码。
- A/B 阶段用 `role=noops` 服务：允许真实提交，但停掉巡检/运维 Worker，避免正常作业被巡检误判。
- C 阶段用 `role=ops` 服务：真实 checker + 运维 Worker。

## 用例

见 `cases/*.md`。每个文件 frontmatter 是策略/期望值，`## 用户输入` 下的内容就是实际发送的 prompt
（Markdown 保存；B 环节用 `$words_topic` 等占位符在运行时替换，实际发送文本另存到 run 目录的 `prompt.md`）。

## 数据目录

```
tests/system_eval_data/<run_id>/
├── run.json / preflight.json / summary.md / summary.json
├── usability_findings.md
├── server_noops.log / server_ops.log / alerts.jsonl
├── db_snapshot.json
└── cases/<id>/
    ├── prompt.md / case.json / events.jsonl / timeline.json / result.json
    ├── tool_traces.json / verification.jsonl / background.jsonl
    ├── cluster.json / ops.json / rubric.json / outcome.json
    └── artifacts/          # 生成的业务代码/测试代码快照
```

## Rubric

六维加权（完成度 20%、正确性 30%、验证完整性 20%、工具效率 10%、体验/延迟 10%、健壮性 10%）。
自动分数在 `rubric.json`；主观“好用”结论在 `usability_findings.md`，必须能指到具体用例文件。
