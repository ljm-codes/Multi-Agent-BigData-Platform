import asyncio
import json
from asyncio import Lock
from typing import Callable, Any, Awaitable

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command

from SPO.tool_result import ToolResult
from Tools import LogSetting
from Tools.alarm_sender import send_warning_msg

logger = LogSetting.create(__name__)
fail_situation: dict[str, int] = {}  # 工具ID -> 失败次数


class ToolFailMarkMiddleware(AgentMiddleware):
    """
    用于处理工具失败时的处理中间件
    做法：
        解析工具返回的json格式，判断是否有对应的失败标记
        若有标记，那么将会添加到内存字典中，提供给工具网关进行识别
    """

    def __init__(self):
        super().__init__()
        self._lock_dict: dict[str, Lock] = {}  # sid -> 锁对象

    def wrap_tool_call(
            self,
            request: ToolCallRequest,
            handler: Callable[[ToolCallRequest], ToolMessage | Command[Any]],
    ) -> ToolMessage | Command[Any]:
        result = handler(request)
        sid = request.runtime.context.session_id
        app_id = request.runtime.context.app_id
        tool_id = request.tool_call.get("id", "")

        with self._lock_dict[sid]:
            if isinstance(result, ToolMessage):
                tool_res = result.content
                try:
                    tool_res_json = json.loads(tool_res)
                    if not isinstance(tool_res_json, ToolResult):
                        return result
                    if not (tool_res_json.status == "error" and tool_res_json.retryable):
                        return result
                    if fail_situation.get(tool_id, ""):
                        fail_count = fail_situation[tool_id] + 1
                        if fail_count >= 1:
                            # 调用告警发送函数
                            send_warning_msg(
                                app_id=app_id,
                                description=f"工具 {tool_id} 失败 {fail_count} 次"
                            )
                        fail_situation[tool_id] = fail_count
                    else:
                        fail_situation.setdefault(tool_id, 1)
                except Exception as e:
                    logger.error(f"解析工具返回的json格式失败，错误信息：{e}")
        return result

    async def awrap_tool_call(
            self,
            request: ToolCallRequest,
            handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command[Any]]],
    ) -> ToolMessage | Command[Any]:
        result = await handler(request)
        sid = request.runtime.context.session_id
        app_id = request.runtime.context.app_id
        tool_id = request.tool_call.get("id", "")

        async with self._lock_dict[sid]:
            if isinstance(result, ToolMessage):
                tool_res = result.content
                try:
                    tool_res_json = json.loads(tool_res)
                    if not isinstance(tool_res_json, ToolResult):
                        return result
                    if not (tool_res_json.status == "error" and tool_res_json.retryable):
                        return result
                    if fail_situation.get(tool_id, ""):
                        fail_count = fail_situation[tool_id] + 1
                        if fail_count >= 1:
                            # 调用告警发送函数
                            await asyncio.to_thread(send_warning_msg(
                                app_id=app_id,
                                description=f"工具 {tool_id} 失败 {fail_count} 次"
                            ))
                        fail_situation[tool_id] = fail_count
                    else:
                        fail_situation.setdefault(tool_id, 1)
                except Exception as e:
                    logger.error(f"解析工具返回的json格式失败，错误信息：{e}")
        return result
