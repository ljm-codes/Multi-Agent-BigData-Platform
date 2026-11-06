"""
离线验证 job__restart 的人工审核链路（不需要集群 / Postgres / Redis / ssh / 大模型）

覆盖:
  1. 命令解析/配对/值校验/回复归一化
  2. worker_handle 的中断 -> PAUSED、lease 保留
  3. /ops/agent/resume 的核心逻辑（假图）
  4. 值不合法 -> 保留原值；结构违规 -> 拒绝

用法: python "tests/O&M/verify_job_restart_hitl.py"
"""
import asyncio
import json
import shlex
import sys
from pathlib import Path
from types import SimpleNamespace

# 导入 Tools.* 前给 yarn_api_client 打桩（share_check_tool 会在模块级建 RM）
import yarn_api_client
import yarn_api_client.resource_manager as _rm_mod


class _OfflineRM:
    def __init__(self, *args, **kwargs):
        pass


yarn_api_client.ResourceManager = _OfflineRM
_rm_mod.ResourceManager = _OfflineRM

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import load_config.config  # noqa: E402,F401
from Tools import job_command as J  # noqa: E402

PASS, FAIL = "✅", "❌"
_results = []


def check(name: str, ok_: bool, detail: str = ""):
    _results.append((name, ok_))
    print(f"  {PASS if ok_ else FAIL} {name}")
    if detail:
        print(f"      {detail}")


def section(title: str):
    print("\n" + "-" * 72)
    print(title)
    print("-" * 72)


SPARK = (
    "/export/server/spark3/bin/spark-submit --master yarn --deploy-mode cluster "
    "--name parity_count --conf spark.yarn.submit.waitAppCompletion=false "
    "--conf spark.executor.memoryOverhead=256m --conf spark.sql.shuffle.partitions=2 "
    "--driver-memory 1g --executor-memory 1g --num-executors 2 --executor-cores 1 "
    "hdfs:///app/spark/parity_count/parity_count.py"
)
FLINK = (
    "flink run-application -t yarn-application -d -c com.fm.WordCountApp -p 1 "
    "-D jobmanager.memory.process.size=1024m -D taskmanager.memory.process.size=1024m "
    "-D taskmanager.numberOfTaskSlots=1 -D yarn.provided.lib.dir=hdfs:///app/flink/jar "
    "hdfs:///app/flink/jar/wordcount-flink-0.1.0.jar"
)


def t1_commands():
    section("[1] 命令解析 / 配对 / 改参 / 校验")

    for label, cmd in (("spark", SPARK), ("flink", FLINK)):
        tokens = J.split(cmd)
        rows = J.rows(tokens)
        check(f"{label}: rows/flatten 往返", J.flatten_rows(rows) == tokens,
              str(J.flatten_rows(rows)[:3]))
        check(f"{label}: 第一行是程序", rows[0].get("program") is True
              and rows[0]["flag"] == tokens[0], str(rows[0]))

    # 改参：带前缀的键要能匹配
    new, ignored = J.change_command("SPARK", {"config spark.executor.memoryOverhead": "512m"}, SPARK)
    check("spark: config 前缀能匹配", "spark.executor.memoryOverhead=512m" in new and not ignored,
          str(ignored))
    new, ignored = J.change_command("SPARK", {"spark.sql.shuffle.partitions": "3"}, SPARK)
    check("spark: 裸属性名能匹配", "spark.sql.shuffle.partitions=3" in new and not ignored,
          str(ignored))
    new, ignored = J.change_command("SPARK", {"--driver-memory": "2g"}, SPARK)
    check("spark: plain flag 能匹配", " --driver-memory 2g " in new and not ignored, str(ignored))
    new, ignored = J.change_command("FLINK", {"-D taskmanager.memory.process.size": "2048m"}, FLINK)
    check("flink: -D 前缀能匹配", "taskmanager.memory.process.size=2048m" in new and not ignored,
          str(ignored))
    new, ignored = J.change_command("FLINK", {"p": "2"}, FLINK)
    check("flink: -p 能匹配", " -p 2 " in new and not ignored, str(ignored))

    # 泛化键 / 非法值：不整单失败
    new, ignored = J.change_command("FLINK", {"D": "x"}, FLINK)
    check("裸 D 被拒且命令不变", new == FLINK and ignored, str(ignored))
    new, ignored = J.change_command("SPARK", {"p": "0"}, SPARK)
    check("非法值跳过、保留原值", new == SPARK and ignored and ignored[0]["reason"], str(ignored))

    # 人工改命令：值非法回填原值，结构违规直接拒绝
    tokens = J.split(SPARK)
    i = tokens.index("--num-executors") + 1
    original = tokens[i]
    bad = list(tokens); bad[i] = "0"
    eff, ignored, err = J.validate_command(tokens, bad, "SPARK")
    check("数量改 0 -> 回填原值", err is None and eff[i] == original and ignored,
          f"{eff[i]} {ignored}")
    bad = list(tokens); bad[i] = "3"
    eff, ignored, err = J.validate_command(tokens, bad, "SPARK")
    check("数量改 3 -> 通过", err is None and eff[i] == "3" and not ignored, f"{err} {ignored}")

    bad = list(tokens); bad[tokens.index("--name") + 1] = "evil"
    _, _, err = J.validate_command(tokens, bad, "SPARK")
    check("改黑名单 --name -> 拒绝", err is not None and "黑名单" in err, str(err))

    bad = list(tokens); bad[0] = "bash"
    _, _, err = J.validate_command(tokens, bad, "SPARK")
    check("改程序名 -> 拒绝", err is not None and "程序" in err, str(err))

    bad = list(tokens) + ["&&", "rm", "-rf", "/"]
    _, _, err = J.validate_command(tokens, bad, "SPARK")
    check("shell 控制符 -> 拒绝", err is not None and "shell" in err, str(err))

    # 未改动的旧格式不误报
    legacy = "spark-submit --master yarn --driver-memory 512 --num-executors 2 app.py"
    lt = J.split(legacy)
    _, ignored, err = J.validate_command(lt, lt, "SPARK")
    check("未改动的无单位 512 不误报", err is None and not ignored, f"{err} {ignored}")

    # 回复归一化
    check("yes", J.normalize_answer("yes")[0] == "yes")
    check("bare no -> 错误", J.normalize_answer("no")[3] is not None)
    check("{pass:true} 不再失败开放", J.normalize_answer({"pass": True})[3] is not None,
          str(J.normalize_answer({"pass": True})))
    a = J.normalize_answer({"type": "no", "new_command": "spark-submit --num-executors 3 app.py"})
    check("new_command 归一化", a[0] == "no" and a[1][:2] == ["spark-submit", "--num-executors"],
          str(a))
    a = J.normalize_answer({"type": "no", "new_command_rows": [
        {"flag": "spark-submit", "value": "", "program": True},
        {"flag": "--num-executors", "value": "3"}]})
    check("new_command_rows 归一化",
          a[0] == "no" and a[1] == ["spark-submit", "--num-executors", "3"], str(a))


def t2_worker():
    section("[2] worker_handle: 中断 -> PAUSED，lease 保留")

    import agent.operations_agent_run as RUN

    records = {"finish": [], "lease": [], "warn": [], "start": []}
    RUN._load_error_info = lambda aid: (([{
        "id": 1, "checker_error": None, "app_type": "SPARK", "check_type": "x",
        "error_type": "error", "check_describe": "d", "repeat_count": 1,
    }], "SPARK"))
    RUN._start_agent_run = lambda aid, sid, app_type, n: records["start"].append(
        (aid, sid, app_type, n))
    RUN._prepare_app_file = lambda aid, app_type: ""

    def finish(sid, status, out_msg="", error="", *, finished=True):
        records["finish"].append((status, out_msg, error, finished))

    RUN._finish_agent_run = finish
    RUN._release_lease = lambda aid, sid: records["lease"].append((aid, sid))
    RUN.send_warning_msg = lambda **kw: records["warn"].append(kw)

    class FakeGraph:
        def __init__(self, result):
            self.result = result

        async def ainvoke(self, state, config, context):
            return self.result

    def run_case(result):
        for v in records.values():
            v.clear()
        RUN.get_ops_agent = lambda: FakeGraph(result)
        asyncio.run(RUN.worker_handle("app1", "sid"))

    run_case({"__interrupt__": [SimpleNamespace(value={"kind": "job_restart"}, id="i1")]})
    check("中断 -> PAUSED + finished=False",
          records["finish"] == [("PAUSED", "等待人工审核", "", False)], str(records["finish"]))
    check("中断 -> 不释放 lease", records["lease"] == [], str(records["lease"]))

    run_case({"type": "info", "out_msg": "ok"})
    check("正常 -> DONE + 释放 lease",
          records["finish"] == [("DONE", "ok", "", True)]
          and records["lease"] == [("app1", "sid")], str(records))

    run_case({"type": "error", "out_msg": "boom"})
    check("错误 -> ERROR + 告警 + 释放 lease",
          records["finish"] == [("ERROR", "", "boom", True)]
          and records["warn"] and records["lease"] == [("app1", "sid")], str(records))


def t3_resume():
    section("[3] /ops/agent/resume 核心（假图）")

    import routers.ops_control as OC
    from SPO.operations_agent_state_model import OperationsAndReconcileContext

    base = ("spark-submit --master yarn --name app --num-executors 2 "
            "--executor-memory 1g hdfs:///app/job.py")
    value = {"kind": "job_restart", "app_id": "app1", "app_name": "app",
             "app_type": "spark", "command": base,
             "command_tokens": shlex.split(base), "command_rows": []}

    records = {"finish": [], "running": [], "lease": [], "warn": []}
    OC._mark_run_running = lambda sid: records["running"].append(sid)

    def finish(sid, status, out_msg="", error="", *, finished=True):
        records["finish"].append((status, out_msg, error, finished))

    OC._finish_agent_run = finish
    OC._release_lease = lambda aid, sid: records["lease"].append((aid, sid))
    OC.send_warning_msg = lambda **kw: records["warn"].append(kw)

    class FakeGraph:
        def __init__(self, interrupts, values, result):
            self.interrupts = interrupts
            self.values = values
            self.result = result
            self.calls = []

        async def aget_state(self, config):
            return SimpleNamespace(interrupts=self.interrupts, values=self.values)

        async def ainvoke(self, payload, config, context):
            self.calls.append((payload, config, context))
            return self.result

    def make(iid="i1", result=None, interrupts=None):
        return FakeGraph(
            interrupts if interrupts is not None else [SimpleNamespace(value=value, id=iid)],
            {"app_id": "app1"},
            result if result is not None else {"type": "info", "out_msg": "done"},
        )

    def reset():
        for v in records.values():
            v.clear()

    def expect_error(name, graph, answer):
        reset()
        try:
            asyncio.run(OC._resume_ops_session(graph, "sid", answer))
            check(name, False, "没有抛 ResumeError")
        except OC.ResumeError:
            check(name, not graph.calls and not records["running"],
                  f"calls={len(graph.calls)} running={records['running']}")

    expect_error("没有中断 -> 400", make(interrupts=[]), {"type": "yes"})
    expect_error("改程序名 -> 400", make(), {"type": "no", "new_command": "bash -c 'rm -rf /'"})
    expect_error("{pass:true} -> 400", make(), {"pass": True})
    expect_error("bare no -> 400", make(), "no")

    reset()
    graph = make()
    data = asyncio.run(OC._resume_ops_session(graph, "sid", {"type": "yes"}))
    check("yes -> resume=yes + base_command",
          graph.calls[0][0].resume == {"type": "yes", "base_command": base},
          str(graph.calls[0][0].resume))
    check("yes -> DONE + 释放 lease + RUNNING 标记",
          records["finish"] == [("DONE", "done", "", True)]
          and records["lease"] == [("app1", "sid")] and records["running"] == ["sid"],
          str(records))
    ctx = graph.calls[0][2]
    check("yes -> context 正确", isinstance(ctx, OperationsAndReconcileContext)
          and ctx.app_id == "app1" and ctx.session_id == "sid", str(ctx))
    check("yes -> 响应", data["interrupted"] is False and data["output_msg"] == "done", str(data))

    reset()
    graph = make()
    answer = {"type": "no", "new_command":
              "spark-submit --master yarn --name app --num-executors 0 "
              "--executor-memory 1g hdfs:///app/job.py"}
    data = asyncio.run(OC._resume_ops_session(graph, "sid", answer))
    resumed = graph.calls[0][0].resume
    check("非法值 -> 保留原值", "--num-executors 2" in resumed["new_command"]
          and "--num-executors 0" not in resumed["new_command"], resumed["new_command"])
    check("非法值 -> ignored_values 回传", bool(data["ignored_values"]), str(data["ignored_values"]))

    reset()
    graph = make()
    asyncio.run(OC._resume_ops_session(graph, "sid", {"type": "no", "msg": "不同意"}))
    check("异议 -> 不提交新命令", graph.calls[0][0].resume.get("msg") == "不同意"
          and "new_command" not in graph.calls[0][0].resume, str(graph.calls[0][0].resume))
    check("异议 -> DONE + 释放 lease", records["lease"] == [("app1", "sid")], str(records))

    reset()
    graph = make(result={"__interrupt__": [SimpleNamespace(value=value, id="i2")]})
    data = asyncio.run(OC._resume_ops_session(graph, "sid", {"type": "yes"}))
    check("再次中断 -> PAUSED 且不释放 lease",
          records["finish"] == [("PAUSED", "等待人工审核", "", False)] and not records["lease"],
          str(records))
    check("再次中断 -> 响应带新 interrupt", data["interrupted"] is True and data["interrupt"],
          str(data)[:120])

    reset()
    graph = make(interrupts=[SimpleNamespace(value=value, id="i1"),
                             SimpleNamespace(value=value, id="i2")])
    asyncio.run(OC._resume_ops_session(graph, "sid", {"type": "yes"}))
    check("多 interrupt -> id 映射", isinstance(graph.calls[0][0].resume, dict)
          and "i1" in graph.calls[0][0].resume, str(graph.calls[0][0].resume))


def t4_tool_interrupt():
    section("[4] 真实 job__restart 工具：interrupt -> resume")

    from typing import TypedDict
    from langchain_core.messages import AIMessage
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.graph import MessagesState, StateGraph, START, END
    from langgraph.prebuilt import ToolNode
    from langgraph.types import Command
    import Tools.operations_agent_tools as OPS

    class S(MessagesState):
        app_id: str
        app_type: str

    base = ("spark-submit --master yarn --name app --num-executors 2 "
            "--executor-memory 1g hdfs:///app/job.py")
    ssh_calls = []

    class FakeResult:
        ok = True
        rc = 0
        stdout = "submitted"
        stderr = ""
        error_kind = ""
        message = ""
        cost_ms = 1

    OPS._commit_cmd_of = lambda app_type, name: (base, "db")
    OPS._client_host = lambda: "node1"

    def fake_run(host, command, timeout=0):
        ssh_calls.append((host, command))
        return FakeResult()

    OPS.node_ssh.run = fake_run

    builder = StateGraph(S)
    builder.add_node("tools", ToolNode([OPS.job_restart]))
    builder.add_edge(START, "tools")
    builder.add_edge("tools", END)
    graph = builder.compile(checkpointer=InMemorySaver())

    def first_turn(thread_id):
        ai = AIMessage(content="", tool_calls=[{
            "name": "job__restart", "id": "c1",
            "args": {"dry_run": False, "app_name": "app"},
        }])
        state = {"messages": [ai], "app_id": "app1", "app_type": "SPARK"}
        return asyncio.run(graph.ainvoke(
            state, config={"configurable": {"thread_id": thread_id}}))

    def resume(thread_id, answer):
        return asyncio.run(graph.ainvoke(
            Command(resume=answer),
            config={"configurable": {"thread_id": thread_id}}))

    r1 = first_turn("hitl-yes")
    interrupts = r1.get("__interrupt__") or []
    check("工具确实中断", bool(interrupts), str(r1)[:160])
    value = interrupts[0].value if interrupts else {}
    check("中断 payload 带 kind/命令表",
          value.get("kind") == "job_restart" and value.get("command_tokens")
          and value.get("command_rows"), str(value)[:160])
    base_cmd = value.get("command", "")

    ssh_calls.clear()
    r2 = resume("hitl-yes", {"type": "yes", "base_command": base_cmd})
    data = json.loads(r2["messages"][-1].content)
    check("yes -> 执行一次且 executed=True",
          len(ssh_calls) == 1 and data.get("status") == "ok"
          and data["data"].get("executed") is True, str(data)[:180])
    check("yes -> 用 pipefail 执行", "bash -o pipefail" in ssh_calls[0][1],
          ssh_calls[0][1][:140])

    # 修改命令
    r1 = first_turn("hitl-edit")
    base_cmd = r1["__interrupt__"][0].value["command"]
    ssh_calls.clear()
    edited = base.replace("--num-executors 2", "--num-executors 3")
    r2 = resume("hitl-edit", {"type": "no", "new_command": edited,
                             "base_command": base_cmd})
    data = json.loads(r2["messages"][-1].content)
    check("改命令 -> 用新命令执行", len(ssh_calls) == 1
          and "--num-executors 3" in ssh_calls[0][1]
          and "--num-executors 3" in str(data["data"].get("command", "")),
          str(data)[:180])

    # 异议
    r1 = first_turn("hitl-object")
    base_cmd = r1["__interrupt__"][0].value["command"]
    ssh_calls.clear()
    r2 = resume("hitl-object", {"type": "no", "msg": "不同意", "base_command": base_cmd})
    data = json.loads(r2["messages"][-1].content)
    check("异议 -> 不执行 + objection",
          not ssh_calls and data.get("status") == "ok"
          and data["data"].get("objection") == "不同意", str(data)[:180])

    # 失败开放格式：不执行
    r1 = first_turn("hitl-bad")
    base_cmd = r1["__interrupt__"][0].value["command"]
    ssh_calls.clear()
    r2 = resume("hitl-bad", {"pass": True})
    data = json.loads(r2["messages"][-1].content)
    check("非法回复 -> 不执行", not ssh_calls and data.get("status") == "error",
          str(data)[:180])

    # 审核期间命令源变了：以 answer.base_command 为准，执行人工审核时看到的那条
    r1 = first_turn("hitl-base")
    ssh_calls.clear()
    approved = ("spark-submit --master yarn --name app --num-executors 5 "
                "--executor-memory 1g hdfs:///app/job.py")
    r2 = resume("hitl-base", {"type": "yes", "base_command": approved})
    data = json.loads(r2["messages"][-1].content)
    check("base_command 为准 -> 执行审核时的命令",
          len(ssh_calls) == 1 and "--num-executors 5" in ssh_calls[0][1]
          and data["data"].get("command") == approved, str(data)[:180])


def main():
    t1_commands()
    t2_worker()
    t3_resume()
    t4_tool_interrupt()
    passed = sum(1 for _, ok_ in _results if ok_)
    print("\n" + "=" * 72)
    print(f"结果: {passed}/{len(_results)} 通过")
    print("=" * 72)
    if passed != len(_results):
        sys.exit(1)


if __name__ == "__main__":
    main()
