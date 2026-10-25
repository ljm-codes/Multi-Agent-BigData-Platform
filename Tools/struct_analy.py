import logging
import re
from typing import TypeVar

from langchain_core.messages import AIMessage, BaseMessage
from pydantic import BaseModel

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


def extract_text_content(message: BaseMessage) -> str:
    """提取消息文本：兼容 str 与 OpenAI 风格内容块列表（[{'type': 'text', 'text': ...}, ...]）。"""
    content = message.content
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and block.get('type') == 'text':
                parts.append(str(block.get('text', '')))
        return ''.join(parts)
    return ''


def strip_json_wrappers(text: str) -> str:
    """剥离常见 JSON 包装，返回最内层候选 JSON 文本（失败返回原文）：
    ```json 代码块、首尾引号包裹。复杂注入下模型可能产出包装过的 JSON，
    直接 model_validate_json 会失败，剥一层再试可避免被当作自然语言兜底。
    """
    s = text.strip()
    match = re.match(r'^```(?:json)?\s*(.*?)\s*```$', s, re.S)
    if match:
        s = match.group(1).strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ('"', "'"):
        s = s[1:-1].strip()
    return s


def parse_route_message(message: BaseMessage, model_type: type[T]) -> T | None:
    """将大模型输出的 AIMessage 解析为指定的 Pydantic 模型类型。

    支持两种形式：
    1. 模型以工具调用的形式返回；
    2. 模型将 JSON 写在 content 中（含代码块包装、正则片段等兜底）。

    无法解析时返回 None。
    """
    # 理想情况：模型以工具调用的形式返回
    if isinstance(message, AIMessage) and message.tool_calls:
        tool_call = message.tool_calls[0]
        if tool_call['name'] == model_type.__name__:
            try:
                return model_type.model_validate(tool_call["args"])
            except Exception as e:
                logger.error(f"进行解析时出现了异常{type(e).__name__}: {e}")
                return None
        # 当大模型调用其他工具时返回 None
        return None
    # 避免大模型将消息写在 content 中，所以需要进行解析
    content = extract_text_content(message)
    if content.strip():
        try:
            # 若为简单的 JSON 字符串则直接解析
            return model_type.model_validate_json(content)
        except Exception as e:
            logger.debug(f"进行解析时出现了异常{type(e).__name__}: {e}")
            # 尝试另一种方法：解包装后再解析
            stripped = strip_json_wrappers(content)
            if content != stripped.strip():
                try:
                    return model_type.model_validate_json(stripped)
                except Exception as e:
                    logger.error(f"进行解析时出现了异常{type(e).__name__}: {e}")

            match = re.search(r'\{.*}', content, re.S)
            if match:
                try:
                    return model_type.model_validate_json(match.group(0))
                except Exception as e:
                    logger.error(f"进行解析时出现了异常{type(e).__name__}: {e}")
        # 实在没招了，走告警
        logger.warning(f"无法解析消息 {content} 为 {model_type.__name__} 类型")
    return None
