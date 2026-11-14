import ast
import json
import re
from typing import Any, TypeVar, Literal

from langchain_core.messages import AIMessage, BaseMessage
from pydantic import BaseModel

from Tools.log_settings import LogSetting

logger = LogSetting.create(__name__)

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


def _call_field(call: Any, key: str, default: Any = None) -> Any:
    """兼容 dict 与对象两种 ToolCall 形态取字段。"""
    if isinstance(call, dict):
        return call.get(key, default)
    return getattr(call, key, default)


def parse_tool_call(message: BaseMessage, model_type: type[T]) -> tuple[T | None, Exception | None]:
    """从 AIMessage.tool_calls 中找出结构化输出调用并校验。

    作用：多个工具并行调用时，结构化输出不一定在第一个；同时保留校验异常供重试提示。
    参数：
        message：模型返回的消息。
        model_type：结构化输出 Pydantic 模型，工具名等于类名。
    返回：
        (parsed, error)；没有调用该工具时返回 (None, None)。
    """
    if not isinstance(message, AIMessage):
        return None, None
    for tool_call in (message.tool_calls or []):
        if _call_field(tool_call, "name") != model_type.__name__:
            continue
        try:
            return model_type.model_validate(_call_field(tool_call, "args") or {}), None
        except Exception as e:
            logger.error(f"进行解析时出现了异常{type(e).__name__}: {e}")
            return None, e
    return None, None


def strip_tool_call(message: AIMessage, name: str) -> AIMessage:
    """剔除指定工具的调用，交给 ToolNode 前清掉结构化输出工具。"""
    calls = [c for c in (message.tool_calls or []) if _call_field(c, "name") != name]
    if len(calls) == len(message.tool_calls or []):
        return message
    return message.model_copy(update={"tool_calls": calls})


def parse_route_message(message: BaseMessage, model_type: type[T]) -> T | None:
    """将大模型输出的 AIMessage 解析为指定的 Pydantic 模型类型。

    支持两种形式：
    1. 模型以工具调用的形式返回；
    2. 模型将 JSON 写在 content 中（含代码块包装、正则片段等兜底）。

    无法解析时返回 None。
    """
    # 理想情况：模型以工具调用的形式返回
    if isinstance(message, AIMessage) and message.tool_calls:
        parsed, _ = parse_tool_call(message, model_type)
        return parsed
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


def _escape_embedded_quotes(raw: str) -> str:
    """修复 JSON 字符串内部未转义的双引号。

    典型场景：LLM 在长文本里直接写了中文含义的英文双引号，如
        "未收到该订单的"支付成功"：……"
    这段里两个内层 " 未转义，导致 JSON 非法。

    思路：一次正向扫描、全程不抛异常——维护是否为字符串内部的状态；在字符串内
    遇到 " 时向后窥探一个非空白字符：
        若是 , : } ] 或到达末尾 → 真正的字符串结束引号，保留；
        否则 → 判定为嵌在文本里的裸双引号，改写为 \\"。
    返回修复后的字符串。
    """
    out: list[str] = []
    in_string = False
    n = len(raw)
    i = 0
    while i < n:
        ch = raw[i]
        if in_string:
            if ch == "\\":
                out.append(ch)
                if i + 1 < n:
                    i += 1
                    out.append(raw[i])
                i += 1
                continue
            if ch == '"':
                j = i + 1
                while j < n and raw[j] in " \t\r\n":
                    j += 1
                if j >= n or raw[j] in ",:}]":
                    in_string = False
                    out.append(ch)
                else:
                    out.append('\\"')
                i += 1
                continue
            out.append(ch)
            i += 1
        else:
            if ch == '"':
                in_string = True
            out.append(ch)
            i += 1
    return "".join(out)


def _is_truncated_json(raw: str) -> bool:
    """反向扫描检测 JSON 是否因输出上限被物理截断（场景②）。

    方向原理：截断的定义就是"尾部被砍"（数据从末尾缺失，或半截字符串停在末尾），
    故异常信号必然首先出现在末尾——从尾部往前扫，能在离异常点最近处立即命中，
    特别是遇到"有开启却无配对的闭合符"（depth 变负）可提前返回 True，
    无需走完全部字符。从前往后扫虽也正确，但要到达尾部才发现异常，性能低下。

    实测（3 万字符级真实样本）：反向仅需扫描数百字符即可判定，正向需全量扫描，
    反向约省 98%~100% 的字符处理。

    转义处理：引号是否为字符串结束符取决于其左侧连续反斜杠的奇偶（奇数被转义），
    因此遇到 " 需回看左侧 "\\"。注意：应传入已过 _escape_embedded_quotes 的串，
    避免裸引号干扰字符串状态跟踪。
    """
    in_string = False
    depth = 0
    i = len(raw) - 1
    while i >= 0:
        ch = raw[i]
        if ch == '"':
            bs = 0
            j = i - 1
            while j >= 0 and raw[j] == "\\":
                bs += 1
                j -= 1
            if bs % 2 == 1:  # 该引号被转义，属于字符串内容，不翻转状态
                i -= 1
                continue
            in_string = not in_string
        elif not in_string and ch in "}]":
            depth += 1
        elif not in_string and ch in "{[":
            depth -= 1
            if depth < 0:
                return True  # 开启符号无配对闭合 → 截断，提前返回
        i -= 1
    return in_string or depth > 0


def repair_invalid_tool_calls(
        ver_res: AIMessage
) -> tuple[Literal["ok", "error"], AIMessage | None]:
    """模型偶尔把 JSON 里的 true/false/null 写成 Python 的 True/False/None。

    LangChain 会把这些调用放进 invalid_tool_calls，tool_calls 为空。
    这里尽量按 Python 字面量解析参数，转成合法工具调用后继续跑。
    """
    invalid_calls = getattr(ver_res, "invalid_tool_calls", None) or []
    logger.info(f"埋点：原始的 invalid_tool_calls 为：{invalid_calls}")
    repaired_calls = []
    for call in invalid_calls:
        logger.info(f"埋点：进行了一次修复")
        raw_args = call.get("args")
        logger.info(f"埋点：原始参数：{raw_args}")
        if isinstance(raw_args, str):
            # 能进 invalid_tool_calls，参数必然已损坏（场景①Python 字面量 /
            # 场景③裸引号 / 两者共存），无需“是否干净”的判别，直接先修复裸引号。
            escaped = _escape_embedded_quotes(raw_args)
            logger.info(f"埋点：尝试使用json.loads解析参数")
            if _is_truncated_json(escaped):
                # 当出现超限被拦截时，直接返回错误描述，不再进行修复
                error_desc = (
                    f"疑似 JSON 截断/超限（无法修复，需缩小单次产出或分块重试），"
                    f"原始参数字符数={len(raw_args)}，末尾内容：{raw_args[-80:]!r}"
                )
                logger.warning(f"埋点：{error_desc}")
                return "error", AIMessage(content=error_desc, name="error: tool_max")
            try:
                args = json.loads(escaped)
                logger.info(f"埋点：解析后的参数：{args}")
            except Exception:  # noqa: BLE001
                # 场景① 或 场景①+③ 共存时，escaped 仍是合法 Python 字面量，
                # 因此这里必须解析修复后的串，而不是原始串（否则裸引号会再次导致失败）。
                try:
                    logger.info(f"埋点：尝试使用literal_eval解析参数")
                    args = ast.literal_eval(escaped)
                    logger.info(f"埋点：literal_eval解析后的参数：{args}")
                except Exception:  # noqa: BLE001
                    logger.info(f"埋点：解析失败，无法解析参数：{raw_args}")
                    continue
        else:
            args = raw_args
        if not isinstance(args, dict):
            logger.info(f"埋点：参数后的类型不是字典，无法修复：{args}")
            continue
        repaired_calls.append({
            "name": call.get("name"),
            "args": args,
            "id": call.get("id"),
            "type": "tool_call",
        })
    logger.info(f"埋点：修复后的calls为：{repaired_calls}")
    if not repaired_calls:
        return "error", None
    return "ok", ver_res.model_copy(update={
        "tool_calls": [*(getattr(ver_res, "tool_calls", None) or []), *repaired_calls],
        "invalid_tool_calls": [],
    })
