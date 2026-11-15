"""DDA/DAA 工具调用前的项目资源漂移守卫。

廉价签名每次工具调用都算；签名变了才做内容 hash。
询问类工具放行，否则 Agent 被挡住后没法向用户提问。
"""
from __future__ import annotations

import asyncio
from typing import Any

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest

from Tasks.project_registry import admin_id_of
from Tools.log_settings import LogSetting
from services.project_resource_guard import check_project_fast

logger = LogSetting.create(__name__)

_ALLOW = {"info_inquiry", "get_project_resources", "get_cluster_env", "initialize_project"}


def _call_field(call: Any, key: str, default: Any = None) -> Any:
    if isinstance(call, dict):
        return call.get(key, default)
    return getattr(call, key, default)


class ProjectResourceGuard(AgentMiddleware):
    def __init__(self, agent: str):
        super().__init__()
        self.agent = agent

    async def awrap_tool_call(self, request: ToolCallRequest, handler):
        call = getattr(request, "tool_call", None) or {}
        name = str(_call_field(call, "name", "") or "")
        if name in _ALLOW:
            return await handler(request)
        state = getattr(request, "state", None) or {}
        project_id = str(state.get("project_id") or "") if isinstance(state, dict) else ""
        context = getattr(getattr(request, "runtime", None), "context", None)
        admin_id = admin_id_of(getattr(context, "user_id", None))
        if not project_id or not admin_id:
            return await handler(request)
        try:
            drift = await asyncio.to_thread(check_project_fast, admin_id, project_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning("项目资源守卫检查失败，放行：%s: %s", type(exc).__name__, exc)
            return await handler(request)
        if drift.get("clean", True) or drift.get("skipped"):
            return await handler(request)
        if drift.get("public_drift"):
            message = ("cluster_resources 与项目记录的公有快照不一致。"
                       "先不要继续调用工具；请调用 info_inquiry 让用户确认，"
                       "由管理员处理 public drift 后再继续。")
        else:
            message = ("项目的 src/main/resources 与基线不一致。"
                       "先不要继续调用工具；请调用 info_inquiry 让用户选择"
                       "保留修改（需要重新验证）还是回滚到 materialized snapshot。")
        logger.warning("项目资源守卫拦截 tool=%s agent=%s project=%s",
                       name, self.agent, project_id)
        return ToolMessage(
            content=message,
            tool_call_id=str(_call_field(call, "id", "") or ""),
            status="error",
        )
