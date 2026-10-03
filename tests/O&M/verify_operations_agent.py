"""
验证 agent/_operations_agent.py 的图（离线，不需要集群 / 库 / 大模型）

覆盖:
  1. 注册表 -> 图的接线（工具分级覆盖、最高级别）
  2. 正常收敛: chat -> summary -> chat_end（含 app_lease 解锁、CheckLog 落库）
  3. 同级别并行 / 跨级别串行
  4. 越级调用被拒（网关）
  5. 降到低级别被放行
  6. 工具 retryable 失败后的重试上限
  7. 模型输出无法解析 -> chat_end error
  8. worker_handle 层的入参整理（app_type / checker_error）

用法: python "tests/O&M/verify_operations_agent.py"

⚠️ 本文件**离线可跑**, 靠三个桩:
  * 导入 Tools.* 之前给 yarn_api_client 打桩 —— Tools/share_check_tool.py 在模块级
    构造 ResourceManager, 那个类会主动探测 RM(探测不到抛 "No active RMs found")。
  * 导入 agent._operations_agent 期间给 builtins.open 装垫片 —— 该模块用
    open("./prompt/operations_agent.md") 这种 CWD 相对路径读手册（缺陷, 见 t1 输出）。
  * 导入之后替换 大模型 / SessionLocal / AsyncSessionLocal / send_warning_msg,
    并把 22 个工具的 .func 换成桩 —— 图里 chat_end 会删 app_lease、app_summary 会改
    CheckLog, 所以桩必须**先于任何 invoke** 装好。
"""
import asyncio
import builtins
import collections
import concurrent.futures
import itertools
import json
import pathlib
import subprocess
import sys
import threading
import time
import traceback
import types
from dataclasses import dataclass

# --------------------------------------------------------------- 导入前打桩
# 必须在任何 Tools.* 之前(见文件开头的说明)
import yarn_api_client
import yarn_api_client.resource_manager as _rm_mod


class _OfflineRM:
    def __init__(self, *args, **kwargs):
        pass


yarn_api_client.ResourceManager = _OfflineRM
_rm_mod.ResourceManager = _OfflineRM

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# agent/_operations_agent.py:52 读的是 CWD 相对路径, 文件实际在 agent/prompt/ 下。
# 这里只做**读取重定向**, 不改被测代码; 这条本身作为缺陷证据在 t0 里报出来。
_REAL_OPEN = builtins.open
_PROMPT_REL = "./prompt/operations_agent.md"
_PROMPT_ABS = PROJECT_ROOT / "agent" / "prompt" / "operations_agent.md"


def _open_shim(file, *args, **kwargs):
    if isinstance(file, str) and file.replace("\\", "/") == _PROMPT_REL:
        return _REAL_OPEN(_PROMPT_ABS, *args, **kwargs)
    return _REAL_OPEN(file, *args, **kwargs)


builtins.open = _open_shim
try:
    import agent._operations_agent as OA
finally:
    builtins.open = _REAL_OPEN

from langchain_core.messages import AIMessage, ToolMessage  # noqa: E402
from langchain_core.tools import tool as tool_dec  # noqa: E402
from langgraph.graph import END, START, MessagesState, StateGraph  # noqa: E402
from langgraph.prebuilt import ToolNode  # noqa: E402
from SPO.agent_state_model import (OperationsAndReconcileContext,  # noqa: E402
                                   OperationsAndReconcileInputState)
from SPO.tool_result import ToolResult, fail, ok  # noqa: E402
from Tools.registry import _TOOL_REGISTRY, get_priority_tools  # noqa: E402
import agent.operations_agent_run as RUN  # noqa: E402
import middleware.fail_tool_middleware as FTM  # noqa: E402
from middleware.compose import chain_tool_call_wrappers  # noqa: E402

PASS, FAIL = "✅", "❌"
_results = []
_defects = []


def check(name: str, ok_: bool, detail: str = "", defect: str = ""):
    _results.append((name, ok_))
    print(f"  {PASS if ok_ else FAIL} {name}")
    if detail:
        print(f"      {detail}")
    if not ok_ and defect:
        _defects.append(defect)


def note(text: str):
    print(f"      · {text}")


def blocked(detail: str):
    """本条判定被上游缺陷挡住, 不是被测逻辑本身的问题"""
    print(f"      ⏸ 被上游缺陷挡住，本轮无法判定: {detail}")


def section(title: str):
    print("\n" + "-" * 72)
    print(title)
    print("-" * 72)


# --------------------------------------------------------------- 记录器 / 桩
CALLS = []                      # 工具调用记录
DB_LOG = []                     # 假会话收到的 SQL
WARNINGS = []                   # send_warning_msg 调用记录
OPENED = {"sync": 0, "async": 0}  # 同步/异步会话被开了几次
_SEQ = itertools.count()
CURRENT = {"tools": {}, "rows": []}


def _dump(result: ToolResult) -> str:
    return json.dumps(result.model_dump(), ensure_ascii=False)


class _Recorder(types.SimpleNamespace):
    """假 CheckLog 行: 只记录被赋了什么值"""


def _make_tool_stub(name: str):
    def _stub(**kwargs):
        behavior = CURRENT["tools"].get(name, ok({"stub": name}))
        rec = {"name": name, "args": kwargs, "seq": next(_SEQ),
               "thread": threading.get_ident(), "t0": time.monotonic()}
        CALLS.append(rec)
        result = behavior(kwargs) if callable(behavior) else behavior
        rec["status"] = result.status
        rec["t1"] = time.monotonic()
        return _dump(result)
    return _stub


def install_tool_stubs():
    """就地换掉注册表里每个工具的 .func —— ToolNode 与 agent 模块持有同一批对象引用"""
    for tool_obj in _TOOL_REGISTRY["OM"]:
        tool_obj.func = _make_tool_stub(tool_obj.name)


class FakeModel:
    """按脚本吐 AIMessage；bind_tools 只记录"模型看到了哪些工具" """

    def __init__(self, steps):
        self.steps = list(steps)
        self.bound_tools = []
        self.calls = []          # 每轮 ainvoke 收到的消息列表

    def bind_tools(self, tools, tool_choice=None):
        self.bound_tools = [getattr(t, "name", getattr(t, "__name__", str(t)))
                            for t in (tools or [])]
        return self

    async def ainvoke(self, messages, **kwargs):
        self.calls.append(list(messages))
        if not self.steps:
            raise AssertionError("模型脚本已用完, 但图还在要下一条回复(多跑了轮次?)")
        step = self.steps.pop(0)
        return step() if callable(step) else step


class FakeResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def all(self):
        return list(self._rows)

    def fetchall(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None


class FakeSession:
    """同步假会话(给 app_summary 用)"""

    def __init__(self, log, rows):
        self.log, self.rows = log, rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, stmt, *a, **k):
        self.log.append(("execute", str(stmt)))
        return FakeResult(self.rows)

    def scalar(self, stmt):
        # 跟真 Session.scalar 一致：返回**第一行/第一个值**，不是 Result 对象
        self.log.append(("scalar", str(stmt)))
        return self.rows[0] if self.rows else None

    def scalars(self, stmt):
        self.log.append(("scalars", str(stmt)))
        return FakeResult(self.rows)

    def commit(self):
        self.log.append(("commit", ""))


class FakeAsyncSession(FakeSession):
    """异步假会话(给 chat_end 用)"""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, stmt, *a, **k):
        self.log.append(("execute", str(stmt)))
        return FakeResult(self.rows)


def _record_warning(**kwargs):
    WARNINGS.append(kwargs)
    return True, "stubbed"


def install_collaborators():
    """把会碰外部世界的协作者换成假的(必须在任何 invoke 之前)"""
    install_tool_stubs()

    def _sync_session():
        OPENED["sync"] += 1
        return FakeSession(DB_LOG, CURRENT["rows"])

    def _async_session():
        OPENED["async"] += 1          # chat_end 每跑一次就开一次会话 -> 用它数 chat_end 跑了几次
        return FakeAsyncSession(DB_LOG, CURRENT["rows"])

    OA.SessionLocal = _sync_session
    OA.AsyncSessionLocal = _async_session
    OA.send_warning_msg = _record_warning
    FTM.send_warning_msg = _record_warning
    RUN.send_warning_msg = _record_warning


# --------------------------------------------------------------- 消息构造
def tool_ai(name, args, call_id):
    return AIMessage(content="", tool_calls=[{"name": name, "args": args, "id": call_id}])


def route_ai(nodes):
    return AIMessage(content=json.dumps({"nodes": nodes}, ensure_ascii=False))


def raw_ai(content):
    return AIMessage(content=content)


def summary_route(solve_ids, app_id, unsolvable=None, desc=""):
    info = {"solve_error_ids": solve_ids, "app_id": app_id}
    if unsolvable:
        info["unsolvable_error_ids"] = unsolvable
        info["unsolvable_description"] = desc
    return route_ai([{"node": "summary", "summary_info": info}])


ERROR_LIST = [{"id": 101, "error_describe": "容器被 kill", "check_type": "OOM",
               "error_type": "error", "repeat_count": 3}]


@dataclass
class _MiniCtx:
    """[8] 里迷你图用的 context（和图里那张用的是同一套字段）"""
    app_id: str
    session_id: str


def run_case(*, steps, tools=None, rows=None, error_list=None, is_cluster=False,
             app_id="application_1", app_type="SPARK", recursion_limit=40, sync=False):
    """驱动一次图，返回 (图输出 | None, 异常 | None, 假模型)"""
    CALLS.clear()
    DB_LOG.clear()
    WARNINGS.clear()
    OPENED["sync"] = OPENED["async"] = 0
    CURRENT["tools"] = tools or {}
    CURRENT["rows"] = rows or []
    FTM.fail_situation.clear()

    model = FakeModel(steps)
    OA.model = model
    sid = f"sid-{next(_SEQ)}"
    OA._tool_progress_savepoint.pop(sid, None)

    state = OperationsAndReconcileInputState(
        error_list=error_list if error_list is not None else ERROR_LIST,
        is_cluster=is_cluster, app_id=app_id, app_type=app_type)
    config = {"configurable": {"thread_id": sid}, "recursion_limit": recursion_limit}
    context = OperationsAndReconcileContext(app_id=app_id, session_id=sid)
    try:
        if sync:
            out = OA.operations_agent.invoke(state, config=config, context=context)
        else:
            out = asyncio.run(OA.operations_agent.ainvoke(state, config=config, context=context))
        return out, None, model
    except Exception as e:  # noqa: BLE001 —— 这里就是要抓图内部炸出来的东西
        return None, e, model


def show_error(err):
    """只打印最后几帧 —— 图内部的调用栈很长, 但根因在最后几帧"""
    if err is None:
        return
    lines = traceback.format_exception(type(err), err, err.__traceback__)
    note(f"异常: {type(err).__name__}: {err}")
    for line in "".join(lines).strip().splitlines()[-6:]:
        note(f"  {line.strip()}")


def show_calls():
    for c in CALLS:
        note(f"工具 {c['name']}({c['args']}) -> {c['status']}  seq={c['seq']} "
             f"thread={c['thread']} 耗时={c['t1'] - c['t0']:.3f}s")


def show_model_msgs(model):
    for i, msgs in enumerate(model.calls):
        kinds = []
        for m in msgs:
            if isinstance(m, ToolMessage):
                kinds.append(f"ToolMessage({m.name}): {str(m.content)[:60]}")
            elif isinstance(m, AIMessage):
                kinds.append(f"AIMessage(tool_calls={[tc['name'] for tc in m.tool_calls]})")
            else:
                kinds.append(type(m).__name__)
        note(f"第{i + 1}轮 -> {kinds}")


def all_tool_msgs(model):
    """模型从头到尾看到的全部 ToolMessage 内容"""
    return [str(m.content) for msgs in model.calls for m in msgs if isinstance(m, ToolMessage)]


def tool_msgs(model, round_index=1):
    """取第 round_index 轮模型看到的 ToolMessage 内容"""
    if len(model.calls) <= round_index:
        return []
    return [str(m.content) for m in model.calls[round_index] if isinstance(m, ToolMessage)]


# --------------------------------------------------------------- 用例
def t0_wiring():
    section("[0] 注册表 -> 图的接线")
    prio = get_priority_tools("OM")
    count = sum(len(v) for v in prio.values())
    check("22 个工具全部登记进优先级表", count == 22, f"实际 {count} 个: {sorted(prio)}")
    check("工具全在 tool_to_category 里（没有掉进 -1 的）",
          len(OA.tool_to_category) == count,
          f"tool_to_category={len(OA.tool_to_category)} / 注册表={count}")
    check("maximum_category = 4", OA.maximum_category == 4, f"实际 {OA.maximum_category}")
    check("registry 与图用的是同一批工具对象",
          set(t.name for t in OA.operations_tools) == set(OA.tool_to_category),
          f"图里 {len(OA.operations_tools)} 个")
    # 这条用子进程真跑一遍：从仓库根 import，模块自己会不会炸
    code = (
        "import sys, pathlib\n"
        f"sys.path.insert(0, r'{PROJECT_ROOT}')\n"
        "import yarn_api_client, yarn_api_client.resource_manager as rm\n"
        "class _R:\n"
        "    def __init__(self, *a, **k): pass\n"
        "yarn_api_client.ResourceManager = _R\n"
        "rm.ResourceManager = _R\n"
        "import agent._operations_agent\n"
        "print('IMPORT_OK')\n"
    )
    proc = subprocess.run([sys.executable, "-c", code], cwd=str(PROJECT_ROOT),
                          capture_output=True, text=True, timeout=180)
    tail = (proc.stderr or "").strip().splitlines()[-1:] or [""]
    check("从仓库根 import agent._operations_agent 不炸",
          proc.returncode == 0 and "IMPORT_OK" in proc.stdout,
          f"exit={proc.returncode} {tail[0]}",
          defect="agent/_operations_agent.py:52 `open(\"./prompt/operations_agent.md\")` 是 CWD 相对路径"
                 "（文件实际在 agent/prompt/ 下）-> 从仓库根 import 直接 FileNotFoundError，"
                 "入口 operations_agent_run.py 连导入都过不去")


def t0b_sync_invoke():
    section("[0b] 调用方还在不在用同步 invoke")
    src = (PROJECT_ROOT / "agent" / "operations_agent_run.py").read_text(encoding="utf-8")
    check("调用方用的是 ainvoke（仓库里不再有 operations_agent.invoke）",
          "operations_agent.ainvoke(" in src and "operations_agent.invoke(" not in src,
          "agent/operations_agent_run.py 的调用点")
    # 图自身仍只有异步入口：以后谁写同步 invoke 都会踩这个坑，这里把行为记下来
    out, err, model = run_case(steps=[summary_route([101], "application_1")], sync=True)
    note("图自身的同步 invoke: " +
         (f"{type(err).__name__}: {str(err).splitlines()[0]}" if err else "能跑"))


def t1_normal_converge():
    section("[1] 正常收敛: chat -> summary -> chat_end")
    app_id = "application_1"
    rows = [types.SimpleNamespace(id=101, is_solved=False, status="PENDING", update_time=None)]
    out, err, model = run_case(
        steps=[summary_route([101], app_id)],
        rows=rows, app_id=app_id)

    show_error(err)
    if out is not None:
        note(f"图输出: {out}")

    db_ops = [op for op, _ in DB_LOG]
    note(f"假会话收到的操作: {db_ops}")
    show_model_msgs(model)
    note(f"chat_end 这一次被跑了 {OPENED['async']} 次")
    check("chat_end 恰好跑一次",
          OPENED["async"] == 1,
          f"跑了 {OPENED['async']} 次。summary 挂在静态边上，返回值却是路由式的："
          f"返回 Send 会被当成一次额外的派发（同一步两个 task，见 [1b]），"
          f"返回字符串 'chat_end' 则直接 InvalidUpdateError: Expected dict",
          defect="agent/_operations_agent.py:396 `return \"chat_end\"`（字符串）与 :372/:380/:398 `return Send(...)` "
                 "都是路由式返回值, 但 summary/handover 挂的是静态边(:605/:606) —— 节点返回值只能是 state 更新 dict")

    check("summary 节点改到了 CheckLog 并提交",
          ("commit", "") in [(o, s) for o, s in DB_LOG],
          "DB_LOG 里只有 scalar、没有 commit: app_summary 的落库块在 session.scalar(stmt).all() 上抛 "
          "AttributeError（Session.scalar() 返回的是行对象，没有 .all()），被 except 吞掉后转成 Send",
          defect="agent/_operations_agent.py:383 session.scalar(stmt).all() —— select(CheckLog) 用 scalar() "
                 "拿到的是单行对象, 应为 scalars(stmt).all()（或 scalar 后不再 .all()）")
    check("行被标记为已解决",
          bool(rows[0].is_solved), f"is_solved={rows[0].is_solved} status={rows[0].status}")
    check("lease 被解锁(chat_end 走了 delete)",
          any("DELETE" in stmt.upper() for _, stmt in DB_LOG),
          f"DB_LOG={DB_LOG}")
    check("图返回 info", bool(out) and out.get("type") == "info", f"实际 {out}")


def t1b_summary_error_branch():
    """summary 的出错分支还返回 Send —— 单独验它（成功分支现在返回字符串，会先炸）"""
    section("[1b] summary 出错分支返回 Send：静态边 + Send 叠加")
    real_session = OA.SessionLocal

    class _BoomSession:
        def __enter__(self):
            raise RuntimeError("boom")
        def __exit__(self, *exc):
            return False

    OA.SessionLocal = lambda: _BoomSession()
    try:
        out, err, model = run_case(steps=[summary_route([101], "application_1")])
    finally:
        OA.SessionLocal = real_session
    show_error(err)
    note(f"chat_end 开了 {OPENED['async']} 次会话（一次运行里 chat_end 被调用的次数）")
    check("summary 出错时 chat_end 只跑一次",
          OPENED["async"] == 1,
          f"跑了 {OPENED['async']} 次：静态边 1 条 + Send 自己 1 条 -> 同一步两个 task，"
          f"两个都写 state['type'] -> InvalidUpdateError 打死整张图",
          defect="agent/_operations_agent.py:372/380/398 的 return Send('chat_end', ...) 与 :605 的静态边叠加, "
                 "同一步派发两次 chat_end")


def t2_same_level_parallel():
    section("[2] 同级别并行 / 跨级别串行")
    slow = lambda kwargs: (time.sleep(0.2), ok({"stub": "slow"}))[1]
    out, err, model = run_case(
        steps=[
            lambda: AIMessage(content="", tool_calls=[
                {"name": "yarn__get_app", "args": {"app_id": "application_1"}, "id": "c1"},
                {"name": "cluster__get_metrics", "args": {}, "id": "c2"},
            ]),
            summary_route([101], "application_1"),
        ],
        tools={"yarn__get_app": slow, "cluster__get_metrics": slow})

    show_error(err)
    show_calls()
    span = (max(c["t1"] for c in CALLS) - min(c["t0"] for c in CALLS)) if len(CALLS) == 2 else -1
    check("同级两个工具都执行了", len(CALLS) == 2, f"实际 {len(CALLS)} 次")
    check("两个工具是并行的(总耗时 < 两倍单次耗时)",
          span >= 0 and span < 0.35, f"两工具耗时跨度 {span:.3f}s（串行约 0.4s）")


def t3_skip_level_rejected():
    section("[3] 越级调用被拒（网关）")
    out, err, model = run_case(
        steps=[
            tool_ai("code__appearance", {"app_id": "application_1"}, "c1"),
            summary_route([101], "application_1"),
        ])

    show_error(err)
    show_calls()
    show_model_msgs(model)
    rejected = [m for m in tool_msgs(model) if "越级" in m]
    check("越级工具没有真的执行", len(CALLS) == 0, f"实际执行 {len(CALLS)} 次")
    check("模型收到了越级拒绝消息", bool(rejected), f"第二轮的 ToolMessage: {tool_msgs(model)}")
    check("网关没炸", err is None, f"{type(err).__name__}: {err}" if err else "",
          defect="agent/_operations_agent.py:418-430 `_skip_tool_msg_encapsulation` 里 "
                 "`for un in unfinished` 迭代的是 dict 的**键**（字符串）, 下一行 un.get(...) 必 AttributeError "
                 "-> 越级拒绝这条路直接炸（另一处 :426/:513 的 ToolMessage(tool_chall_id=...) 拼错, "
                 "实测 KeyError: 'tool_call_id'）")


def t4_downgrade_allowed():
    section("[4] 降到低级别被放行")
    out, err, model = run_case(
        steps=[
            tool_ai("yarn__get_app", {"app_id": "application_1"}, "c1"),
            tool_ai("log__list", {"host": "node1", "container_id": "container_1"}, "c2"),
            tool_ai("code__appearance", {"app_id": "application_1"}, "c3"),
            tool_ai("yarn__get_app", {"app_id": "application_1"}, "c4"),
            summary_route([101], "application_1"),
        ])

    show_error(err)
    show_calls()
    show_model_msgs(model)
    check("1->2->3 级依次放行后再降回 1 级也放行",
          [c["name"] for c in CALLS] == ["yarn__get_app", "log__list", "code__appearance",
                                         "yarn__get_app"],
          f"实际序列: {[c['name'] for c in CALLS]}")
    check("全程没有被拒绝", not any("越级" in m for m in tool_msgs(model)),
          f"ToolMessage: {tool_msgs(model)}")


def t5_retry_limit():
    section("[5] 工具失败后：放行 1 次重试，第 2 次重试被拒")
    boom = fail("timeout", "连接超时", cost_ms=1)
    check("桩工具确实返回 retryable error", boom.retryable is True)

    def drive(label, ids):
        """ids: 三次调用的 tool_call id（第 1 次失败 / 第 1 次重试 / 第 2 次重试）"""
        out, err, model = run_case(
            steps=[tool_ai("yarn__get_app", {"app_id": "application_1"}, i) for i in ids]
                  + [summary_route([101], "application_1")],
            tools={"yarn__get_app": boom})
        show_error(err)
        show_calls()
        show_model_msgs(model)
        note(f"[{label}] 账本 fail_situation = {dict(FTM.fail_situation)}")
        check(f"[{label}] 两次失败都记进账本（键是工具名）",
              dict(FTM.fail_situation) == {"yarn__get_app": 2},
              f"实际 {dict(FTM.fail_situation)}")
        check(f"[{label}] 只放行 1 次重试：工具执行 2 次，第 3 次被拒",
              len(CALLS) == 2 and any("最大重试次数" in m for m in all_tool_msgs(model)),
              f"工具执行 {len(CALLS)} 次；模型看到的 ToolMessage={all_tool_msgs(model)}")

    drive("同一个 id", ["same-id", "same-id", "same-id"])
    drive("每轮换新 id", ["call-1", "call-2", "call-3"])


def t6_unparsable():
    section("[6] 模型输出无法解析 -> chat_end error")
    out, err, model = run_case(steps=[raw_ai("我先看看再说，暂时没有结论")])
    show_error(err)
    note(f"图输出: {out}")
    note(f"DB_LOG: {[op for op, _ in DB_LOG]}")
    check("返回 error", bool(out) and out.get("type") == "error", f"实际 {out}")
    check("解析失败也要解锁 lease",
          any("DELETE" in stmt.upper() for _, stmt in DB_LOG), f"DB_LOG={DB_LOG}",
          defect="agent/_operations_agent.py:543-546 用 app_lease.app_id / app_lease.session_id, "
                 "而 app_lease 是 Core Table（要用 .c.app_id）-> 解锁永远抛 AttributeError 被吞掉, "
                 "锁从没被真正释放过")
    raw_msg = str(out.get("out_msg", "")) if out else ""
    try:
        decoded = json.loads(raw_msg).get("error_info", {}).get("description", "")
    except Exception:  # noqa: BLE001
        decoded = ""
    check("error 的 out_msg 说的是解析失败（而不是被别的错顶掉）",
          "解析" in decoded, f"out_msg 里解出来的 description={decoded!r}")
    check("错误描述是可读中文，不是 \\uXXXX 转义",
          "解析" in raw_msg,
          f"实际 out_msg={raw_msg[:100]}",
          defect="agent/_operations_agent.py forward_node 里 json.dumps(route)（转发 route 与解析失败两处）"
                 "没带 ensure_ascii=False -> 写进 error_desc_text 的中文是 \\uXXXX 转义串, "
                 "会一路传到调用方与告警邮件")


def t7_worker_handle():
    section("[7] worker_handle 层的入参整理")
    class _FakeRunSession:
        def __init__(self, rows, app_types):
            self.rows, self.app_types = rows, app_types

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, stmt, *a, **k):
            class _R:
                def __init__(self, rows, app_types):
                    self.rows, self.app_types = rows, app_types

                def fetchall(self):
                    return self.rows

                def first(self):
                    return self.app_types

                def all(self):
                    return self.rows
            return _R(self.rows, self.app_types)

    captured = {}

    class _GraphRecorder:
        async def ainvoke(self, state, config=None, context=None):
            captured["state"] = state
            captured["context"] = context
            return {"type": "info", "out_msg": "stubbed"}

    real_session, real_graph = RUN.SessionLocal, RUN.operations_agent
    try:
        RUN.operations_agent = _GraphRecorder()
        rows = [
            (1, "SPARK", "", "OOM", "error", "容器被杀", 3),          # 正常
            (2, "SPARK", "checker 自身炸了", "OOM", "error", "?", 1),  # checker_error -> 跳过
            (3, "SPARK", None, "倾斜", "warning", "任务倾斜", 5),
        ]
        RUN.SessionLocal = lambda: _FakeRunSession(rows, ("SPARK",))
        asyncio.run(RUN.worker_handle("application_1", "sid-1"))

        st = captured.get("state")
        ids = [e["id"] for e in st["error_list"]] if st else []
        check("checker_error 的行被跳过", ids == [1, 3], f"传给图的 error_list ids={ids}")
        check("非集群问题的 is_cluster=False", st is not None and st["is_cluster"] is False,
              f"is_cluster={st['is_cluster'] if st else None}")
        check("app_id 透传", st is not None and st["app_id"] == "application_1",
              f"app_id={st['app_id'] if st else None}")

        RUN.SessionLocal = lambda: _FakeRunSession(rows, ("CLUSTER",))
        asyncio.run(RUN.worker_handle("cluster", "sid-2"))
        st = captured.get("state")
        check("app_type=CLUSTER 时 is_cluster=True",
              st is not None and st["is_cluster"] is True,
              f"is_cluster={st['is_cluster'] if st else None}")

        class _ErrGraph:
            async def ainvoke(self, state, config=None, context=None):
                return {"type": "error", "out_msg": "解析失败"}

        WARNINGS.clear()
        RUN.operations_agent = _ErrGraph()
        asyncio.run(RUN.worker_handle("application_1", "sid-3"))
        check("图为 error 时调用方(worker_handle)会发告警",
              len(WARNINGS) > 0, f"WARNINGS={WARNINGS}")

        # 提交点：worker_handle 现在是 async def，但 operations_main 还在往线程池里 submit
        class _CountGraph:
            def __init__(self):
                self.n = 0

            async def ainvoke(self, *a, **kw):
                self.n += 1
                return {"type": "info", "out_msg": "ok"}

        counted = _CountGraph()
        RUN.operations_agent = counted
        RUN.SessionLocal = lambda: _FakeRunSession(rows, ("SPARK",))
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            # 与 operations_agent_run.py 里的提交点同款：executor.submit(run_async, worker_handle, …)
            future = ex.submit(RUN.run_async, RUN.worker_handle, "application_1", "sid-4")
            submitted = future.result()
        check("线程池 submit 出去的 worker_handle 真的被执行了",
              counted.n == 1,
              f"submit 拿回来的是 {type(submitted).__name__}（异步函数被丢进线程池，没人 await）"
              f"，图跑了 {counted.n} 次",
              defect="agent/operations_agent_run.py:171 executor.submit(worker_handle, app_id, session_id) —— "
                     "worker_handle 已改成 async def 但提交点没跟着改：拿到的是 coroutine，永远不会被 await "
                     "-> 运维 Agent 一次都不会真正跑")
    finally:
        RUN.SessionLocal, RUN.operations_agent = real_session, real_graph


def t8_tool_path_isolated():
    """工具这一步单独拆出来验：迷你图 + 真中间件，绕开图里的其它缺陷"""
    section("[8] 工具层单独验（迷你图 + 真中间件）")
    WARNINGS.clear()

    # 8a 把网关当普通函数直接调（它只用 runtime.context.session_id），看它造出来的临时 AI 消息
    def call_gateway(tool_calls, sid):
        state = {"messages": [AIMessage(content="", tool_calls=tool_calls)]}
        rt = types.SimpleNamespace(context=types.SimpleNamespace(session_id=sid, app_id="app-x"))
        return OA.tool_gateway(state, rt)

    FTM.fail_situation.clear()
    try:
        cmd = call_gateway([{"name": "yarn__get_app", "args": {"app_id": "a"}, "id": "call-x"}],
                           "sid-g1")
        got = [tc["name"] for tc in cmd.update["messages"][0].tool_calls]
        err_g = None
    except Exception as e:  # noqa: BLE001
        got, err_g = None, e
    check("网关造的临时 AI 消息是一条完整的工具调用",
          got == ["yarn__get_app"],
          f"异常 {type(err_g).__name__}: {err_g}" if err_g else f"实际 tool_calls={got}",
          defect="agent/_operations_agent.py:532 `clean_calls.append(tc)` 把整个带注解的 dict 塞进去了, "
                 "其中的 `_category` 会被 langchain 当成 tool_call 的关键字参数 -> "
                 "TypeError: tool_call() got an unexpected keyword argument '_category'")

    # 8a2 阈值两端各验一次：账本=1（失败过一次）放行重试；账本=2（重试也失败过）拒绝
    def gateway_refuse(count):
        FTM.fail_situation.clear()
        FTM.fail_situation["yarn__get_app"] = count
        try:
            cmd = call_gateway(
                [{"name": "yarn__get_app", "args": {"app_id": "a"}, "id": f"call-{count}"}],
                f"sid-g{count}")
            msgs = cmd.update["messages"]
            return [m for m in msgs if isinstance(m, ToolMessage) and "最大重试次数" in str(m.content)], None
        except Exception as e:  # noqa: BLE001
            return [], e

    refuse1, err1 = gateway_refuse(1)
    check("失败过一次时放行重试（prompt 允许 1 次重试）",
          not refuse1 and err1 is None,
          f"异常 {type(err1).__name__}: {err1}" if err1 else "网关生成了拒绝消息（不该拒）",
          defect="agent/_operations_agent.py:522 的阈值与 middleware 的记账起点差一档")
    refuse2, err2 = gateway_refuse(2)
    check("重试也失败后拒绝第 3 次调用",
          bool(refuse2) and err2 is None,
          f"异常 {type(err2).__name__}: {err2}" if err2 else "网关没生成拒绝消息",
          defect="agent/_operations_agent.py:522 ToolMessage(tool_call_id=...) 一构造就抛异常，"
                 "拒绝分支走不完")

    # 迷你图：start 造一条工具调用 -> tool 节点（挂真中间件）
    @tool_dec("boom", description="总是失败的工具")
    def boom(x: str = "") -> str:
        return json.dumps(fail("timeout", "连接超时").model_dump(), ensure_ascii=False)

    def build(call_id):
        graph = StateGraph(MessagesState, context_schema=_MiniCtx)

        def start(_state, runtime):
            return {"messages": [AIMessage(content="", tool_calls=[
                {"name": "boom", "args": {"x": "1"}, "id": call_id}])]}

        graph.add_node("start", start)
        graph.add_node("tool", ToolNode([boom],
                                        awrap_tool_call=chain_tool_call_wrappers([mw])))
        graph.add_edge(START, "start")
        graph.add_edge("start", "tool")
        graph.add_edge("tool", END)
        return graph.compile()

    def run_mini(app, sid):
        try:
            return asyncio.run(app.ainvoke({"messages": []},
                                           context=_MiniCtx(app_id="app-x", session_id=sid))), None
        except Exception as e:  # noqa: BLE001
            return None, e

    mw = FTM.ToolFailMarkMiddleware()
    FTM.fail_situation.clear()
    out, err = run_mini(build("call-1"), "sid-a")

    # 8b 中间件的锁字典
    check("工具调用中间件不会炸（_lock_dict 有初始化的锁）",
          err is None, f"{type(err).__name__}: {err}" if err else "工具正常返回",
          defect="middleware/fail_tool_middleware.py:41/75 `with self._lock_dict[sid]` 而 _lock_dict 从未写入 "
                 "-> 每次工具调用 KeyError（工具一跑就炸，图整体失败）")
    check("失败被记进 fail_situation（网关的重试上限靠它）",
          "boom" in FTM.fail_situation,
          f"fail_situation={dict(FTM.fail_situation)}（键应为工具名）")

    # 8c 同一个工具连续失败两次：计数该从 0 推到 1（网关判 >= 1 就拒 -> 正好允许重试 1 次）
    FTM.fail_situation.clear()
    WARNINGS.clear()
    out, err = run_mini(build("call-1"), "sid-a")             # 第 1 次失败
    note(f"第 1 次失败后 fail_situation = {dict(FTM.fail_situation)}")
    out2, err2 = run_mini(build("call-2"), "sid-a")           # 第 2 次失败
    note(f"第 2 次失败后 fail_situation = {dict(FTM.fail_situation)}  告警记录 = {WARNINGS}")
    check("计数能累加（第 2 次失败要把 boom 推到 2）",
          dict(FTM.fail_situation) == {"boom": 2},
          f"两次失败后实际 {dict(FTM.fail_situation)}（1 起计数：1=失败过 1 次）")

    # 阈值对不上：账本=1（只失败过一次）时，网关就已经拒了 -> 一次重试都不给
    FTM.fail_situation.clear()
    FTM.fail_situation["yarn__get_app"] = 1
    try:
        cmd3 = call_gateway([{"name": "yarn__get_app", "args": {"app_id": "a"}, "id": "call-z"}],
                            "sid-g3")
        refuse3 = [m for m in cmd3.update["messages"]
                   if isinstance(m, ToolMessage) and "最大重试次数" in str(m.content)]
        err_g3 = None
    except Exception as e:  # noqa: BLE001
        refuse3, err_g3 = [], e
    check("只失败过一次时，网关放行第一次重试（prompt: 最多重试 1 次）",
          not refuse3 and err_g3 is None,
          f"账本={{'yarn__get_app': 1}} 时网关就生成了拒绝消息" if refuse3
          else f"异常 {type(err_g3).__name__}: {err_g3}" if err_g3 else "网关放行",
          defect="agent/_operations_agent.py:522 判 `fail_situation.get(name) >= 1`，而中间件第 1 次失败就记 1 "
                 "-> 第一次重试直接被拒（等于 0 次重试），与 agent/prompt/operations_agent.md 的"
                 "「重试有上限：最大为1次」差一档；要么阈值改 >= 2，要么首次记 0")
    check("同一个工具第二次失败会发告警（to_thread 的写法是对的）",
          len(WARNINGS) > 0, f"告警记录={WARNINGS}")


if __name__ == "__main__":
    print("=" * 72)
    print("运维 Agent 图 验证（离线：桩掉模型 / 工具 / 会话，不连集群与库）")
    print("=" * 72)
    install_collaborators()
    for fn in (t0_wiring, t0b_sync_invoke, t1_normal_converge, t1b_summary_error_branch,
               t2_same_level_parallel, t3_skip_level_rejected, t4_downgrade_allowed,
               t5_retry_limit, t6_unparsable, t7_worker_handle, t8_tool_path_isolated):
        try:
            fn()
        except Exception as e:  # 用例自身炸了也要计入失败
            check(f"{fn.__name__} 执行失败", False, f"{type(e).__name__}: {e}")

    ok_count = sum(1 for _, v in _results if v)
    total = len(_results)
    print("\n" + "=" * 72)
    print(f"结果: {ok_count}/{total} 通过")
    if _defects:
        print(f"\n缺陷/疑点 {len(_defects)} 条:")
        for i, d in enumerate(_defects, 1):
            print(f"  {i}. {d}")
    print("=" * 72)
    sys.exit(0 if ok_count == total else 1)
