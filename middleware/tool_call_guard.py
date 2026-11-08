"""模型调用前的工具调用历史守卫。

OpenAI 兼容接口要求：AIMessage 里的每个 tool_call 后面必须有对应
tool_call_id 的 ToolMessage。工具被取消/异常/没有进入 ToolNode 时，
消息历史会少一条，下一次模型调用就会 400：
"insufficient tool messages following tool_calls message"。

这里在每次模型调用前补齐缺失的 ToolMessage（内容标为 error），
并把缺失的 tool_call_id/工具名记录下来，避免整轮任务直接失败。
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage, ToolMessage

from Tools.log_settings import LogSetting

logger = LogSetting.create(__name__)


def _call_id(call: Any) -> str:
    if isinstance(call, dict):
        return str(call.get("id") or "")
    return str(getattr(call, "id", "") or "")


def _call_name(call: Any) -> str:
    if isinstance(call, dict):
        return str(call.get("name") or "")
    return str(getattr(call, "name", "") or "")


def missing_tool_calls(messages: list[Any]) -> list[tuple[str, str]]:
    """返回 (tool_call_id, tool_name) 中没有任何 ToolMessage 回应的那些。"""
    answered: set[str] = set()
    for message in messages or []:
        if isinstance(message, ToolMessage):
            tool_call_id = str(getattr(message, "tool_call_id", "") or "")
            if tool_call_id:
                answered.add(tool_call_id)

    missing: list[tuple[str, str]] = []
    seen: set[str] = set()
    for message in messages or []:
        if not isinstance(message, AIMessage):
            continue
        for call in (getattr(message, "tool_calls", None) or []):
            tool_call_id = _call_id(call)
            if tool_call_id and tool_call_id not in answered and tool_call_id not in seen:
                seen.add(tool_call_id)
                missing.append((tool_call_id, _call_name(call)))
    return missing


def repair_missing_tool_messages(messages: list[Any]) -> list[ToolMessage]:
    """为缺失的 tool_call 补一条 error ToolMessage。"""
    return [
        ToolMessage(
            content=(f"工具 {name or '(unknown)'}（tool_call_id={tool_call_id}）"
                     f"没有返回 ToolMessage（可能被取消或异常）；这是系统补的错误结果，"
                     f"请重新调用该工具。"),
            tool_call_id=tool_call_id,
            status="error",
        )
        for tool_call_id, name in missing_tool_calls(messages)
    ]


def _record_missing(agent: str, thread_id: str, missing: list[tuple[str, str]]) -> None:
    logger.warning(
        "工具调用历史缺 ToolMessage agent=%s thread=%s missing=%s",
        agent, thread_id, missing,
    )
    anchor_dir = os.getenv("CVA_ANCHOR_DIR")
    if not anchor_dir:
        return
    try:
        path = Path(anchor_dir) / "tool_call_guard.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps({
                "ts": datetime.now(timezone.utc).isoformat(),
                "agent": agent,
                "thread_id": thread_id,
                "missing": [{"tool_call_id": cid, "tool": name} for cid, name in missing],
            }, ensure_ascii=False) + "\n")
    except Exception:  # noqa: BLE001
        logger.exception("记录工具调用守卫锚点失败")


class ToolCallHistoryGuard(AgentMiddleware):
    """create_agent 中间件：abefore_model 时补齐缺失的 ToolMessage。"""

    def __init__(self, agent: str):
        super().__init__()
        self.agent = agent

    async def abefore_model(self, state: Any, runtime: Any):
        messages = (state or {}).get("messages") or []
        repairs = repair_missing_tool_messages(messages)
        if not repairs:
            return None
        ctx = getattr(runtime, "context", None)
        thread_id = (
            str(ctx.get("thread_id") or "")
            if isinstance(ctx, dict)
            else str(getattr(ctx, "thread_id", "") or "")
        )
        _record_missing(self.agent, thread_id, missing_tool_calls(messages))
        return {"messages": repairs}
