"""
Agent 端到端指标测试（真实主图 + event streaming v3）

采集：
  * 每个节点的运行时间、输入参数、输出/错误；
  * 每次工具调用的运行时间、输入、输出；
  * main / data_develop / data_analysis / code_verify 四个 Agent 的 token 用量；
  * 四个 Agent 的完整对话（LLM 消息 + 工具调用/结果）；
  * 每次运行的最终 state、interrupt。

用法（在能连 YARN RM / Redis / Postgres 的环境里）：
  python tests/agent_e2e_metrics.py --input "用 Spark(Python) 写一个...统计..."
  python tests/agent_e2e_metrics.py --input "..." --wait-background
  python tests/agent_e2e_metrics.py --input "..." --no-auto-pass

说明：
  * 使用 InMemorySaver/InMemoryStore，不依赖 Postgres；真实业务工具仍会连集群；
  * v3 event streaming 目前是 LangGraph 的 experimental API；
  * 默认自动通过人工确认（--no-auto-pass 关闭），提交确认默认回 "no"（--submit yes 才会自动提交）；
  * --wait-background 会等待 CVA 深层测试转后台后的结果，并继续采集后台续跑指标；
  * 报告写到 tests/metrics/ 下的 JSON。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from langchain_core.messages import AIMessage, BaseMessage
from langchain_deepseek import ChatDeepSeek
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.store.memory import InMemoryStore
from langgraph.stream import StreamChannel, StreamTransformer
from langgraph.types import Command

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from SPO.main_agent_state_model import MainStateContext  # noqa: E402

AGENT_KEYS = ("main", "data_develop", "data_analysis", "code_verify", "root")
_NAMED_AGENTS = {"data_develop", "data_analysis", "code_verify"}


# 与 run.py / tests/verify_ds_think_model.py 保持一致：
# ChatDeepSeek._get_request_payload 默认不回传 reasoning_content，多轮思考模式需要补上
if not getattr(ChatDeepSeek, "_reasoning_content_patched", False):
    _original_get_request_payload = ChatDeepSeek._get_request_payload

    def _patched_get_request_payload(self, input_, *, stop=None, **kwargs):
        payload = _original_get_request_payload(self, input_, stop=stop, **kwargs)
        try:
            messages = self._convert_input(input_).to_messages()
        except Exception:  # noqa: BLE001
            return payload

        payload_messages = payload.get("messages", [])
        id_to_payload = {}
        for i, m in enumerate(payload_messages):
            mid = m.get("id") or m.get("tool_call_id")
            if mid:
                id_to_payload[mid] = m

        for i, msg in enumerate(messages):
            if not isinstance(msg, AIMessage):
                continue
            reasoning = msg.additional_kwargs.get("reasoning_content")
            if not reasoning:
                continue
            target = id_to_payload.get(msg.id) if msg.id else None
            if target is None and i < len(payload_messages):
                target = payload_messages[i]
            if target is not None and target.get("role") == "assistant":
                target["reasoning_content"] = reasoning
        return payload

    ChatDeepSeek._get_request_payload = _patched_get_request_payload
    ChatDeepSeek._reasoning_content_patched = True


# ---------------- JSON 序列化 ----------------

def jsonable(value: Any) -> Any:
    if isinstance(value, BaseMessage):
        return {
            "type": type(value).__name__,
            "name": getattr(value, "name", None),
            "id": getattr(value, "id", None),
            "content": jsonable(getattr(value, "content", None)),
            "tool_calls": jsonable(getattr(value, "tool_calls", []) or []),
            "tool_call_id": getattr(value, "tool_call_id", None),
            "additional_kwargs": jsonable(getattr(value, "additional_kwargs", {}) or {}),
            "usage_metadata": jsonable(getattr(value, "usage_metadata", None)),
        }
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [jsonable(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if is_dataclass(value) and not isinstance(value, type):
        return jsonable(asdict(value))
    if hasattr(value, "model_dump"):
        try:
            return jsonable(value.model_dump())
        except Exception:  # noqa: BLE001
            pass
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


# ---------------- Agent 归属 ----------------

def scope_name(namespace: list[str]) -> str:
    if not namespace:
        return ""
    return str(namespace[0]).split(":", 1)[0]


_CVA_NODES = {"ver_handler", "ver_chat", "ver_com_exec", "ver_deep_exec", "ver_manual", "ver_submit", "ver_error"}


def agent_key(namespace: list[str], node: str | None, checkpoint_ns: str | None = None) -> str:
    # typed messages 的 namespace 可能是空；checkpoint_ns 才是嵌套图/子图的真实路径
    scope_text = " ".join(str(x) for x in (namespace or []))
    scope_text = f"{scope_text} {checkpoint_ns or ''}"
    for name in ("code_verify", "data_develop", "data_analysis"):
        if name in scope_text:
            return name
    first = scope_name(namespace or [])
    if first in _NAMED_AGENTS:
        return first
    if node in _NAMED_AGENTS:
        return node
    if node in _CVA_NODES:
        return "code_verify"
    if node == "dis_chat" or first == "dis_chat":
        return "main"
    return "root"


def _duration_ms(start: Any, end: Any) -> float | None:
    if isinstance(start, (int, float)) and isinstance(end, (int, float)) and end >= start:
        return float(end - start)
    return None


# ---------------- 原始事件收集器 ----------------

class MetricsStore:
    def __init__(self) -> None:
        self.events = 0
        self.tasks: dict[str, dict[str, Any]] = {}
        self.tools: dict[str, dict[str, Any]] = {}
        self.llm_messages: list[dict[str, Any]] = []
        self.raw_message_usage: list[dict[str, Any]] = []
        self.message_scopes: list[dict[str, Any]] = []
        self.consumed_messages = 0
        self.seen_message_ids: set[str] = set()
        self.state_snapshots: list[dict[str, Any]] = []
        self.subgraphs: list[dict[str, Any]] = []
        self.lifecycle: list[dict[str, Any]] = []

    def process(self, event: dict[str, Any]) -> None:
        method = event.get("method")
        params = event.get("params") or {}
        data = params.get("data")
        namespace = list(params.get("namespace") or ())
        ts = params.get("timestamp")
        seq = event.get("seq")

        if method == "tasks" and isinstance(data, dict):
            task_id = data.get("id")
            if task_id:
                rec = self.tasks.setdefault(task_id, {"id": task_id, "namespace": namespace})
                if "input" in data:
                    rec.update({
                        "name": data.get("name"),
                        "input": data.get("input"),
                        "triggers": data.get("triggers"),
                        "start_ts": ts,
                        "start_seq": seq,
                    })
                else:
                    rec.update({
                        "name": rec.get("name") or data.get("name"),
                        "result": data.get("result"),
                        "error": data.get("error"),
                        "interrupts": data.get("interrupts"),
                        "end_ts": ts,
                        "end_seq": seq,
                    })
        elif method == "tools" and isinstance(data, dict):
            call_id = data.get("tool_call_id")
            if call_id:
                rec = self.tools.setdefault(call_id, {"tool_call_id": call_id, "namespace": namespace})
                ev = data.get("event")
                if ev == "tool-started":
                    rec.update({
                        "tool_name": data.get("tool_name"),
                        "input": data.get("input"),
                        "start_ts": ts,
                        "start_seq": seq,
                    })
                elif ev in ("tool-finished", "tool-error"):
                    rec.update({
                        "output": data.get("output"),
                        "error": data.get("error"),
                        "end_ts": ts,
                        "end_seq": seq,
                    })
        elif method == "messages":
            payload, meta = data if isinstance(data, tuple) and len(data) == 2 else (data, {})
            meta_dict = meta if isinstance(meta, dict) else {}
            if isinstance(payload, dict):
                event_name = payload.get("event")
                if event_name == "message-start":
                    self.message_scopes.append({
                        "node": meta_dict.get("langgraph_node"),
                        "namespace": namespace,
                        "checkpoint_ns": meta_dict.get("langgraph_checkpoint_ns"),
                        "path": meta_dict.get("langgraph_path"),
                        "ts": ts,
                    })
                elif event_name == "message-finish":
                    usage = (
                        payload.get("usage")
                        or payload.get("usage_metadata")
                        or meta_dict.get("usage")
                    )
                    if usage:
                        self.raw_message_usage.append({
                            "node": meta_dict.get("langgraph_node"),
                            "namespace": namespace,
                            "checkpoint_ns": meta_dict.get("langgraph_checkpoint_ns"),
                            "usage": usage,
                            "ts": ts,
                        })
        elif method == "lifecycle":
            self.lifecycle.append({"namespace": namespace, "data": data, "ts": ts})

        self.events += 1

    def as_dict(self) -> dict[str, Any]:
        nodes = []
        for task in self.tasks.values():
            nodes.append({
                "agent": agent_key(task.get("namespace", []), task.get("name")),
                "name": task.get("name"),
                "namespace": task.get("namespace", []),
                "input": jsonable(task.get("input")),
                "result": jsonable(task.get("result")),
                "error": jsonable(task.get("error")),
                "interrupts": jsonable(task.get("interrupts")),
                "started_at_ms": task.get("start_ts"),
                "finished_at_ms": task.get("end_ts"),
                "duration_ms": _duration_ms(task.get("start_ts"), task.get("end_ts")),
            })
        nodes.sort(key=lambda item: item.get("started_at_ms") or 0)

        tools = []
        for call in self.tools.values():
            tools.append({
                "agent": agent_key(call.get("namespace", []), None),
                "tool_name": call.get("tool_name"),
                "tool_call_id": call.get("tool_call_id"),
                "namespace": call.get("namespace", []),
                "input": jsonable(call.get("input")),
                "output": jsonable(call.get("output")),
                "error": jsonable(call.get("error")),
                "started_at_ms": call.get("start_ts"),
                "finished_at_ms": call.get("end_ts"),
                "duration_ms": _duration_ms(call.get("start_ts"), call.get("end_ts")),
            })
        tools.sort(key=lambda item: item.get("started_at_ms") or 0)

        agents = {
            key: {
                "llm_calls": 0,
                "usage": _new_usage(),
                "raw_usage": _new_usage(),
                "llm_messages": [],
                "nodes": [],
                "tools": [],
                "conversation": [],
            }
            for key in AGENT_KEYS
        }

        # create_agent 在节点内部直接 ainvoke，其 model 消息没有父 namespace；
        # 根节点只有一个 data_analysis/data_develop 时，root 的 model/tools 归它。
        root_data_agents = {
            task.get("name")
            for task in self.tasks.values()
            if task.get("namespace") == [] and task.get("name") in ("data_analysis", "data_develop")
        }
        root_data_agent = next(iter(root_data_agents)) if len(root_data_agents) == 1 else None

        def _agent_of_message(message: dict[str, Any]) -> str:
            key = message.get("agent") or agent_key(
                message.get("namespace", []), message.get("node"), message.get("checkpoint_ns")
            )
            if key == "root" and root_data_agent and message.get("node") not in ("handler", "dis_chat"):
                return root_data_agent
            return key

        for message in self.llm_messages:
            key = _agent_of_message(message)
            agent = agents[key]
            agent["llm_calls"] += 1
            agent["llm_messages"].append(message)
            _merge_usage(agent["usage"], message.get("usage"))

        for item in self.raw_message_usage:
            key = agent_key(
                item.get("namespace", []), item.get("node"), item.get("checkpoint_ns")
            )
            _merge_usage(agents[key]["raw_usage"], item.get("usage"))

        for node in nodes:
            key = node["agent"]
            if key == "root" and root_data_agent and node.get("name") not in ("handler", "work_end", "dis_chat"):
                key = root_data_agent
            node["agent"] = key
            agents[key]["nodes"].append(node)
        for tool in tools:
            key = tool["agent"]
            if key == "root" and root_data_agent:
                key = root_data_agent
            tool["agent"] = key
            agents[key]["tools"].append(tool)

        for agent in agents.values():
            tools_by_id = {tool["tool_call_id"]: tool for tool in agent["tools"]}
            conversation = []
            for message in agent["llm_messages"]:
                conversation.append({
                    "type": "assistant",
                    "node": message.get("node"),
                    "namespace": message.get("namespace", []),
                    "text": message.get("text"),
                    "content": message.get("content"),
                    "reasoning": message.get("reasoning"),
                    "tool_calls": jsonable(message.get("tool_calls")),
                    "usage": jsonable(message.get("usage")),
                })
                for call in message.get("tool_calls") or []:
                    tool = tools_by_id.get(call.get("id"))
                    if tool:
                        conversation.append({
                            "type": "tool",
                            "tool_name": tool.get("tool_name"),
                            "tool_call_id": tool.get("tool_call_id"),
                            "input": tool.get("input"),
                            "output": tool.get("output"),
                            "error": tool.get("error"),
                            "duration_ms": tool.get("duration_ms"),
                        })
            agent["conversation"] = conversation

        return {
            "event_count": self.events,
            "nodes": nodes,
            "tools": tools,
            "agents": agents,
            "raw_message_usage": jsonable(self.raw_message_usage),
            "state_snapshots": jsonable(self.state_snapshots),
            "subgraphs": jsonable(self.subgraphs),
            "lifecycle": jsonable(self.lifecycle),
        }


class MetricsTransformer(StreamTransformer):
    """请求 tasks/tools/messages 原始 channel，并写入当前 MetricsStore"""

    required_stream_modes = ("messages", "tasks", "tools")
    store: MetricsStore | None = None

    def __init__(self, scope: tuple[str, ...] = ()) -> None:
        super().__init__(scope)
        self.channel = StreamChannel()

    def init(self) -> dict[str, Any]:
        return {"metrics": self.channel}

    def process(self, event: dict[str, Any]) -> bool:
        if MetricsTransformer.store is not None:
            MetricsTransformer.store.process(event)
        return True

    def finalize(self) -> None:
        pass

    def fail(self, err: BaseException) -> None:
        pass


def _new_usage() -> dict[str, int]:
    return {"calls": 0, "input_tokens": 0, "output_tokens": 0, "total_tokens": 0}


def _merge_usage(target: dict[str, int], usage: Any) -> None:
    if not usage:
        return
    if hasattr(usage, "model_dump"):
        usage = usage.model_dump()
    elif not isinstance(usage, dict):
        try:
            usage = dict(usage)
        except Exception:  # noqa: BLE001
            return
    got = False
    for key in ("input_tokens", "output_tokens", "total_tokens"):
        value = usage.get(key)
        if isinstance(value, (int, float)):
            target[key] += int(value)
            got = True
    calls = usage.get("calls")
    if isinstance(calls, (int, float)) and calls > 0:
        target["calls"] += int(calls)
    elif got:
        target["calls"] += 1


# ---------------- typed projections ----------------

def _record_llm_message(
        store: MetricsStore,
        output: Any,
        text_parts: list[str],
        reasoning_parts: list[str],
        *,
        node: str | None,
        namespace: list[str],
        checkpoint_ns: str | None,
) -> None:
    if not isinstance(output, AIMessage):
        return
    message_id = getattr(output, "id", None)
    if message_id:
        if message_id in store.seen_message_ids:
            return
        store.seen_message_ids.add(message_id)
    store.llm_messages.append({
        "node": node,
        "namespace": namespace,
        "checkpoint_ns": checkpoint_ns,
        "agent": agent_key(namespace, node, checkpoint_ns),
        "text": "".join(text_parts),
        "content": jsonable(getattr(output, "content", None)),
        "reasoning": "".join(reasoning_parts),
        "tool_calls": jsonable(getattr(output, "tool_calls", []) or []),
        "usage": getattr(output, "usage_metadata", None),
        "additional_kwargs": jsonable(getattr(output, "additional_kwargs", {}) or {}),
    })


async def consume_messages(stream: Any, store: MetricsStore) -> None:
    async for message in stream.messages:
        text_parts: list[str] = []
        async for delta in message.text:
            text_parts.append(delta)
        reasoning_parts: list[str] = []
        try:
            async for delta in message.reasoning:
                reasoning_parts.append(delta)
        except Exception:  # noqa: BLE001
            pass
        output = None
        try:
            output = await message.output
        except Exception:  # noqa: BLE001
            pass
        if not isinstance(output, AIMessage):
            # handler 的 HumanMessage 等非 LLM 消息不进入 token/对话统计，
            # 也不消耗 message_scopes 的序号（raw message-start 只对应 LLM 消息）。
            continue
        idx = store.consumed_messages
        store.consumed_messages += 1
        scope = store.message_scopes[idx] if idx < len(store.message_scopes) else {}
        _record_llm_message(
            store,
            output,
            text_parts,
            reasoning_parts,
            node=scope.get("node") or getattr(message, "node", None),
            namespace=scope.get("namespace") or list(getattr(message, "namespace", []) or []),
            checkpoint_ns=scope.get("checkpoint_ns"),
        )


async def consume_values(stream: Any, store: MetricsStore) -> None:
    async for snapshot in stream.values:
        store.state_snapshots.append({"ts": time.time(), "values": snapshot})
        if len(store.state_snapshots) > 5:
            store.state_snapshots.pop(0)


async def consume_subgraphs(stream: Any, store: MetricsStore) -> None:
    async for subgraph in stream.subgraphs:
        namespace = list(getattr(subgraph, "path", []) or [])
        store.subgraphs.append({
            "graph_name": getattr(subgraph, "graph_name", None),
            "path": namespace,
        })
        # 子图 messages 是独立 projection；必须消费，否则可能背压。
        # 同时要把内容记下来：CVA 的 LLM 调用只在这里出现，根 stream.messages 里没有。
        try:
            async for message in subgraph.messages:
                text_parts: list[str] = []
                async for delta in message.text:
                    text_parts.append(delta)
                reasoning_parts: list[str] = []
                try:
                    async for delta in message.reasoning:
                        reasoning_parts.append(delta)
                except Exception:  # noqa: BLE001
                    pass
                output = None
                try:
                    output = await message.output
                except Exception:  # noqa: BLE001
                    pass
                _record_llm_message(
                    store,
                    output,
                    text_parts,
                    reasoning_parts,
                    node=getattr(message, "node", None),
                    namespace=namespace,
                    checkpoint_ns="/".join(namespace) or None,
                )
        except Exception:  # noqa: BLE001
            pass


# ---------------- 运行与重试 ----------------

async def run_segment(
        graph: Any,
        payload: Any,
        thread_id: str,
        user_id: str,
        kind: str,
        timeout: float,
) -> tuple[dict[str, Any], dict[str, Any], list[Any]]:
    store = MetricsStore()
    MetricsTransformer.store = store
    started = time.perf_counter()
    started_at = datetime.now().isoformat()

    try:
        stream = await graph.astream_events(
            payload,
            config={"configurable": {"thread_id": thread_id}, "recursion_limit": 250},
            context=MainStateContext(thread_id=thread_id, user_id=user_id),
            version="v3",
            transformers=[MetricsTransformer],
        )

        async def consume() -> tuple[dict[str, Any], list[Any]]:
            async with stream:
                await asyncio.gather(
                    consume_messages(stream, store),
                    consume_values(stream, store),
                    consume_subgraphs(stream, store),
                )
                output = await stream.output() or {}
                interrupts = await stream.interrupts()
                return output, interrupts

        output, interrupts = await asyncio.wait_for(consume(), timeout=timeout)
    finally:
        MetricsTransformer.store = None

    segment = store.as_dict()
    segment.update({
        "kind": kind,
        "started_at": started_at,
        "duration_ms": (time.perf_counter() - started) * 1000,
        "interrupted": bool(interrupts),
        "interrupts": jsonable(interrupts),
        "final_state": jsonable(output),
    })
    return segment, output, interrupts


def _auto_answer(interrupts: list[Any], args: argparse.Namespace) -> Any:
    if not interrupts:
        return None
    first = interrupts[0]
    value = getattr(first, "value", first)
    if isinstance(value, str):
        # CVA 提交确认：submit_msg 是字符串，回 yes/no
        return args.submit
    if not args.auto_pass:
        return None
    # CVA 人工确认：回 dict
    return {"pass": True, "msg": "auto-passed by metrics test"}


async def run_with_resumes(
        graph: Any,
        payload: Any,
        kind: str,
        args: argparse.Namespace,
        thread_id: str,
        user_id: str,
) -> tuple[list[dict[str, Any]], dict[str, Any], list[Any]]:
    seg, output, interrupts = await run_segment(
        graph, payload, thread_id, user_id, kind, args.timeout
    )
    segments = [seg]
    resumes = 0
    while interrupts and resumes < args.max_resumes:
        answer = _auto_answer(interrupts, args)
        if answer is None:
            break
        seg, output, interrupts = await run_segment(
            graph, Command(resume=answer), thread_id, user_id, "resume", args.timeout
        )
        segments.append(seg)
        resumes += 1
    return segments, output, interrupts


async def wait_background(broker: Any, thread_id: str, timeout: float) -> list[Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        outcomes = broker.take_all(thread_id)
        if outcomes:
            return outcomes
        remaining = deadline - time.monotonic()
        try:
            await asyncio.wait_for(broker.wait(), timeout=min(5.0, remaining))
        except asyncio.TimeoutError:
            continue
    return []


def _contains_handoff(value: Any) -> bool:
    if isinstance(value, dict):
        if value.get("status") == "handed_off":
            return True
        return any(_contains_handoff(item) for item in value.values())
    if isinstance(value, (list, tuple, set)):
        return any(_contains_handoff(item) for item in value)
    if isinstance(value, str):
        return "handed_off" in value or "已转后台" in value
    return False


def has_handoff(segments: list[dict[str, Any]]) -> bool:
    return any(
        _contains_handoff(segment.get("nodes")) or _contains_handoff(segment.get("final_state"))
        for segment in segments
    )


def aggregate_usage(segments: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    totals = {key: _new_usage() for key in AGENT_KEYS}
    for segment in segments:
        for key, agent in (segment.get("agents") or {}).items():
            usage = agent.get("usage") or {}
            if not usage.get("calls"):
                continue
            _merge_usage(totals.setdefault(key, _new_usage()), usage)
    return totals


# ---------------- main ----------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Agent 端到端指标测试（event streaming v3）")
    parser.add_argument("--input", default=(
        "用 Spark(Python) 写一个分析脚本：生成 1~100 的整数，统计偶数和奇数的数量并打印结果。"
        "代码写到项目里，先不要提交到集群。"
    ), help="用户任务原话")
    parser.add_argument("--thread-id", default=None)
    parser.add_argument("--user-id", default="metrics-test")
    parser.add_argument("--timeout", type=float, default=1800, help="单次图运行超时（秒）")
    parser.add_argument("--max-resumes", type=int, default=3)
    parser.add_argument("--auto-pass", action=argparse.BooleanOptionalAction, default=True,
                        help="人工确认自动 pass")
    parser.add_argument("--submit", choices=("yes", "no"), default="no",
                        help="提交确认自动回复；默认 no，避免真实提交")
    parser.add_argument("--wait-background", action="store_true",
                        help="等待并采集深层测试转后台后的续跑")
    parser.add_argument("--background-timeout", type=float, default=900)
    parser.add_argument("--out", default=None)
    return parser.parse_args()


async def main() -> int:
    args = parse_args()

    # 延迟导入：让 --help/参数错误不必先连 RM
    from agent.develop_main_agent import builder

    graph = builder.compile(
        checkpointer=InMemorySaver(),
        store=InMemoryStore(),
    )

    thread_id = args.thread_id or f"{args.user_id}:metrics-{uuid4().hex}"
    user_id = args.user_id
    started_at = datetime.now().isoformat()

    try:
        segments, output, interrupts = await run_with_resumes(
            graph,
            {"input_msg": args.input},
            "initial",
            args,
            thread_id,
            user_id,
        )

        if args.wait_background:
            from Tasks.background_broker import (
                REGISTRY_STATUS,
                background_broker,
                background_registry,
                outcome_notice,
            )

            if not has_handoff(segments):
                print("[background] 本次没有 handed_off，跳过等待")
                outcomes = []
            else:
                outcomes = await wait_background(
                    background_broker, thread_id, args.background_timeout
                )
            for outcome in outcomes:
                notice = outcome_notice(outcome)
                bg_segments, bg_output, bg_interrupts = await run_with_resumes(
                    graph,
                    {
                        "input_msg": notice.content,
                        "deep_task_id": outcome.task_id,
                        "is_background": True,
                    },
                    "background",
                    args,
                    thread_id,
                    user_id,
                )
                segments.extend(bg_segments)
                background_registry.update(
                    outcome.task_id,
                    status=REGISTRY_STATUS.get(outcome.status, "completed"),
                    result=(bg_output or {}).get("output_msg", ""),
                    interrupt=None,
                )
    except Exception as exc:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        print(f"[失败] {type(exc).__name__}: {exc}")
        return 1

    report = {
        "thread_id": thread_id,
        "user_id": user_id,
        "input": args.input,
        "started_at": started_at,
        "segments": segments,
    }

    out_path = Path(args.out) if args.out else (
        ROOT / "tests" / "metrics" / f"agent_e2e_{thread_id.replace(':', '_')}_{int(time.time())}.json"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(jsonable(report), ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"thread_id: {thread_id}")
    print(f"segments: {len(segments)}")
    for index, segment in enumerate(segments):
        print(
            f"  [{index}] {segment.get('kind')} nodes={len(segment.get('nodes') or [])} "
            f"tools={len(segment.get('tools') or [])} interrupted={segment.get('interrupted')} "
            f"duration_ms={float(segment.get('duration_ms') or 0):.0f}"
        )
    usage = aggregate_usage(segments)
    for key in AGENT_KEYS:
        u = usage.get(key) or {}
        print(
            f"  {key:14s} llm_usage calls={u.get('calls', 0)} "
            f"input={u.get('input_tokens', 0)} output={u.get('output_tokens', 0)} "
            f"total={u.get('total_tokens', 0)}"
        )
    final_state = (segments[-1].get("final_state") if segments else {}) or {}
    print(f"final output_msg: {final_state.get('output_msg', '')}")
    print(f"report: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
