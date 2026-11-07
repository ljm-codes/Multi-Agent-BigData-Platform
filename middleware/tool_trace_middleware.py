"""
工具调用轨迹切面。

三个 Agent（code_verify / data_analysis / data_develop）的工具调用都经过
langgraph ToolNode 的 awrap_tool_call；在这里统一采集 name/args/output，
写 ToolTraceStore，不侵入各业务工具。
"""
from __future__ import annotations

import asyncio
import uuid
from typing import Any, Awaitable, Callable

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest

from Tools.tool_trace_store import ToolTraceStore, tool_trace_store


def _call_get(call: Any, key: str, default: Any = None) -> Any:
    if isinstance(call, dict):
        return call.get(key, default)
    return getattr(call, key, default)


class ToolTraceMiddleware(AgentMiddleware):
    """把一次工具调用写成 running -> success/error 的 trace。"""

    def __init__(self, agent: str, store: ToolTraceStore | None = None):
        super().__init__()
        self.agent = agent
        self.store = store or tool_trace_store

    async def awrap_tool_call(
            self,
            request: ToolCallRequest,
            handler: Callable[[ToolCallRequest], Awaitable[Any]],
    ) -> Any:
        ctx = getattr(getattr(request, "runtime", None), "context", None)
        if isinstance(ctx, dict):
            thread_id = str(ctx.get("thread_id") or "")
        else:
            thread_id = str(getattr(ctx, "thread_id", "") or "")

        call = getattr(request, "tool_call", None) or {}
        tool_name = str(_call_get(call, "name", "") or "")
        trace_id = str(_call_get(call, "id", "") or uuid.uuid4().hex)
        args = _call_get(call, "args", None) or {}

        trace = await self.store.start(
            thread_id=thread_id,
            agent=self.agent,
            tool=tool_name,
            trace_id=trace_id,
            args=args,
        )

        try:
            result = await handler(request)
        except asyncio.CancelledError:
            await self.store.finish(trace, status="error", error="cancelled")
            raise
        except Exception as exc:  # noqa: BLE001
            await self.store.finish(
                trace, status="error", error=f"{type(exc).__name__}: {exc}"
            )
            raise

        output = result.content if isinstance(result, ToolMessage) else result
        status = "error" if getattr(result, "status", None) == "error" else "success"
        await self.store.finish(trace, output=output, status=status)
        return result
