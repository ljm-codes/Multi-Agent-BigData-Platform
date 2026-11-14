"""
从 checkpoint 的 messages 里提取模型用量。

不落额外存储：AIMessage.usage_metadata 本身就在 LangGraph checkpoint 里，
这里只做口径归一、Agent 归属和跨消息合并。
"""
from __future__ import annotations

from typing import Any, Iterable
from uuid import uuid4

from langchain_core.messages import AIMessage, HumanMessage

_COUNTERS = ("uncached_input", "cache_read", "cache_write", "output", "total")


def _as_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def normalize_usage(usage: dict[str, Any] | None) -> dict[str, Any] | None:
    """把一次模型调用的 usage 归一成展示用的五项。

    DeepSeek/OpenAI：cache_read 已经包含在 input_tokens 里；
    Anthropic：input_tokens 不含缓存，total_tokens 另外包含 cache_read/cache_creation。
    """
    if not isinstance(usage, dict):
        return None
    input_tokens = _as_int(usage.get("input_tokens") or usage.get("prompt_tokens"))
    output_tokens = _as_int(usage.get("output_tokens") or usage.get("completion_tokens"))
    details = usage.get("input_token_details") or usage.get("prompt_tokens_details") or {}
    if not isinstance(details, dict):
        details = {}
    cache_read = _as_int(details.get("cache_read")) or _as_int(details.get("cached_tokens")) \
        or _as_int(usage.get("cache_read_input_tokens")) or _as_int(usage.get("prompt_cache_hit_tokens"))
    cache_write = _as_int(details.get("cache_creation")) \
        or _as_int(usage.get("cache_creation_input_tokens"))
    total = _as_int(usage.get("total_tokens"))
    if not input_tokens and not output_tokens and not cache_read and not total:
        return None

    base = input_tokens + output_tokens
    if total and total >= base + cache_read + cache_write - 1:
        uncached = input_tokens
    else:
        uncached = max(input_tokens - cache_read, 0)
    if not total:
        total = base
    component_total = uncached + cache_read + cache_write + output_tokens
    if total < component_total:
        total = component_total
    denom = uncached + cache_read
    return {
        "uncached_input": uncached,
        "cache_read": cache_read,
        "cache_write": cache_write,
        "output": output_tokens,
        "total": total,
        "hit_rate": round(cache_read / denom * 100, 1) if denom else 0.0,
    }


def merge_usage(*items: dict[str, Any] | None) -> dict[str, Any]:
    """多段归一化 usage 逐项相加，命中率按合计后的分子/分母重算。"""
    out = {key: 0 for key in _COUNTERS}
    for item in items:
        if not isinstance(item, dict):
            continue
        for key in _COUNTERS:
            out[key] += _as_int(item.get(key))
    denom = out["uncached_input"] + out["cache_read"]
    out["hit_rate"] = round(out["cache_read"] / denom * 100, 1) if denom else 0.0
    return out


def merge_raw_usage(*usages: dict[str, Any] | None) -> dict[str, Any] | None:
    """把多段原始 usage_metadata 合并成 LangChain 认得的 UsageMetadata 形状。

    用于把子 Agent 内部多次模型调用累计后写回父图消息。
    """
    merged: dict[str, Any] | None = None
    details: dict[str, int] = {}
    for usage in usages:
        if not isinstance(usage, dict):
            continue
        if merged is None:
            merged = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
        merged["input_tokens"] += _as_int(usage.get("input_tokens") or usage.get("prompt_tokens"))
        merged["output_tokens"] += _as_int(usage.get("output_tokens") or usage.get("completion_tokens"))
        merged["total_tokens"] += _as_int(usage.get("total_tokens"))
        raw_details = usage.get("input_token_details") or usage.get("prompt_tokens_details") or {}
        if isinstance(raw_details, dict):
            for key in ("cache_read", "cache_creation", "audio"):
                value = _as_int(raw_details.get(key))
                if value:
                    details[key] = details.get(key, 0) + value
        # 部分网关直接给原始字段；只在标准明细缺失时采用，避免重复计数
        if not details.get("cache_read"):
            for source in ("cache_read_input_tokens", "prompt_cache_hit_tokens"):
                value = _as_int(usage.get(source))
                if value:
                    details["cache_read"] = details.get("cache_read", 0) + value
        if not details.get("cache_creation"):
            value = _as_int(usage.get("cache_creation_input_tokens"))
            if value:
                details["cache_creation"] = details.get("cache_creation", 0) + value
    if merged is None:
        return None
    if not merged["total_tokens"]:
        merged["total_tokens"] = merged["input_tokens"] + merged["output_tokens"]
    if details:
        merged["input_token_details"] = details
    return merged


def usage_of_message(message: Any) -> dict[str, Any] | None:
    """单条消息的归一化用量；没有 usage_metadata 返回 None。"""
    return normalize_usage(getattr(message, "usage_metadata", None))


def last_response_metadata(messages: Iterable[Any]) -> dict[str, Any] | None:
    """取最近一条 AI 消息的 response_metadata（provider/model）。"""
    for message in reversed(list(messages or [])):
        if isinstance(message, AIMessage):
            meta = getattr(message, "response_metadata", None)
            if meta:
                return dict(meta)
    return None


def usage_ledger_message(
    name: str,
    usage: dict[str, Any] | None,
    response_metadata: dict[str, Any] | None = None,
) -> AIMessage | None:
    """零正文用量台账：异常/打回路径没法产生正常回复时，把消耗记进 checkpoint。

    history 与前端都会跳过空正文的 assistant 消息，只参与用量汇总。
    """
    if not isinstance(usage, dict) or not usage:
        return None
    return AIMessage(
        content="",
        name=name,
        id=f"usage:{uuid4().hex}",
        usage_metadata=usage,
        response_metadata=response_metadata or {},
        # 调度模型下一轮不要读到这条空消息；只用于 checkpoint 记账
        additional_kwargs={"usage_ledger": True},
    )


def agent_of_message(message: Any) -> str:
    """消息名 -> UI 上的 Agent 名（与前端 normalizeAgent 口径一致）。"""
    name = str(getattr(message, "name", "") or "").strip()
    if not name:
        return "dis_chat"
    if name in ("code_verify", "verify_agent") or name.startswith("ver_") or name.startswith("ver:"):
        return "code_verify"
    if name in ("data_analysis", "data_analysis_agent"):
        return "data_analysis"
    if name in ("data_develop", "data_develop_agent"):
        return "data_develop"
    if name in ("dis_chat", "work_end", "handler", "distribute_task_to_agent"):
        return "dis_chat"
    return name


def _model_meta(messages: list[Any]) -> tuple[str, str]:
    """取最近一条带 response_metadata 的 AI 消息的 provider/model。"""
    provider = ""
    model = ""
    for message in reversed(messages):
        if not isinstance(message, AIMessage):
            continue
        meta = getattr(message, "response_metadata", {}) or {}
        provider = provider or str(meta.get("model_provider") or "")
        model = model or str(meta.get("model_name") or "")
        if provider and model:
            break
    return provider, model


def usage_from_messages(messages: Iterable[Any]) -> dict[str, Any]:
    """按 Agent 汇总一批消息的用量。

    同一 id 的消息只算一次：work_end 会复制 dis_chat 的结构化消息（含用量）。
    """
    items = [message for message in messages or [] if isinstance(message, AIMessage)]
    by_agent: dict[str, dict[str, Any]] = {}
    seen_ids: set[str] = set()
    for message in items:
        usage = usage_of_message(message)
        if usage is None:
            continue
        message_id = str(getattr(message, "id", "") or "")
        if message_id:
            if message_id in seen_ids:
                continue
            seen_ids.add(message_id)
        agent = agent_of_message(message)
        by_agent[agent] = merge_usage(by_agent.get(agent), usage)
    provider, model = _model_meta(items)
    return {
        "by_agent": by_agent,
        "total": merge_usage(*by_agent.values()),
        "provider": provider,
        "model": model,
    }


def merged_raw_usage_of_messages(messages: Iterable[Any]) -> dict[str, Any] | None:
    """把一组 AI 消息的原始 usage_metadata 合并（子 Agent 内部多轮调用）。"""
    usages: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for message in messages or []:
        if not isinstance(message, AIMessage):
            continue
        usage = getattr(message, "usage_metadata", None)
        if not usage:
            continue
        message_id = str(getattr(message, "id", "") or "")
        if message_id:
            if message_id in seen_ids:
                continue
            seen_ids.add(message_id)
        usages.append(usage)
    return merge_raw_usage(*usages)


def usage_since_last_human(messages: Iterable[Any]) -> dict[str, Any]:
    """当前这一轮（最后一条 HumanMessage 之后）的用量。"""
    items = list(messages or [])
    start = 0
    for index in range(len(items) - 1, -1, -1):
        if isinstance(items[index], HumanMessage):
            start = index + 1
            break
    return usage_from_messages(items[start:])


def usage_by_turns(messages: Iterable[Any]) -> list[dict[str, Any]]:
    """按 HumanMessage 切轮返回每轮用量；空轮也保留，便于前端从末尾对齐。"""
    turns: list[list[Any]] = []
    current: list[Any] = []
    started = False
    for message in messages or []:
        if isinstance(message, HumanMessage):
            if started:
                turns.append(current)
            started = True
            current = []
            continue
        current.append(message)
    if started:
        turns.append(current)
    return [usage_from_messages(turn) for turn in turns]
