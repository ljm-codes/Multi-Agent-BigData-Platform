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
import inspect
import signal

from main import start_app
from load_config.config import config
from Tools.log_settings import LogSetting

app = start_app()
logger = LogSetting.create(__name__)


# 这里查看 ctrl+c 后的“无界”栈，看看为什么会出现同时出现KeyboardInterrupt, asyncio.CancelledError
def dump_tasks(signum, frame):
    print("\n=== DUMPING ASYNCIO TASKS (before shutdown proceeds) ===", file=sys.stderr)
    try:
        loop = asyncio.get_event_loop()
        tasks = asyncio.all_tasks(loop)
        for task in tasks:
            if task.done():
                continue
            print(f"\n--- TASK: {task.get_name()} ---", file=sys.stderr)
            # 打印该任务当前挂起位置的调用栈
            for line in task.get_stack():
                frameinfo = inspect.getframeinfo(line)
                print(f"  {frameinfo.filename}:{frameinfo.lineno} in {frameinfo.function}", file=sys.stderr)
    except Exception as e:
        print(f"Failed to dump tasks: {e}", file=sys.stderr)
    print("=== END DUMP ===\n", file=sys.stderr)


if __name__ == '__main__':
    server = config.get("server")
    host = server.get("host")
    port = server.get("port")
    CONNECT_TO_CLUSTER = bool((config.get("app") or {}).get("connect_to_cluster", True))
    if not CONNECT_TO_CLUSTER:
        logger.warning(f"当前命令禁止链接集群，该项目的部分工具与功能将无法使用，但你仍能继续运行")
    signal.signal(signal.SIGINT, dump_tasks)
    uvicorn.run(
        app,
        host=host,
        port=port,
        loop="none",
        reload=False,
        log_level="warning",
        timeout_graceful_shutdown=5,
    )
