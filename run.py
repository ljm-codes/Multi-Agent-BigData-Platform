"""
项目主运行入口
"""
import asyncio
import sys

if sys.platform.startswith("win"):
    try:
        asyncio.get_event_loop().close()
    except RuntimeError:
        pass
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from langchain_core.messages import AIMessage
from langchain_deepseek import ChatDeepSeek

# 针对 deepseek 的思考模型模型
if not getattr(ChatDeepSeek, "_reasoning_content_patched", False):
    _original_get_request_payload = ChatDeepSeek._get_request_payload


    def _patched_get_request_payload(self, input_, *, stop=None, **kwargs):
        payload = _original_get_request_payload(self, input_, stop=stop, **kwargs)

        try:
            messages = self._convert_input(input_).to_messages()
        except Exception:
            return payload

        payload_messages = payload.get("messages", [])
        # 优先按消息 id 匹配，退化为按索引匹配
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

            target = None
            if msg.id and msg.id in id_to_payload:
                target = id_to_payload[msg.id]
            elif i < len(payload_messages):
                target = payload_messages[i]

            if target is not None and target.get("role") == "assistant":
                target["reasoning_content"] = reasoning

        return payload


    ChatDeepSeek._get_request_payload = _patched_get_request_payload
    ChatDeepSeek._reasoning_content_patched = True

import uvicorn

from main import start_app
from load_config.config import config

app = start_app()

if __name__ == '__main__':
    server = config.get("server")
    host = server.get("host")
    port = server.get("port")
    uvicorn.run(
        app,
        host=host,
        port=port,
        loop="none",
        reload=False,
        log_level="warning",
    )
