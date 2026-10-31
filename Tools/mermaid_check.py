"""Mermaid 语法校验与确定性修复：给人工确认前的 _splicing_doc 兜底"""
import re

from Tools.log_settings import LogSetting

logger = LogSetting.create(__name__)

try:
    import merman
except Exception as exc:  # 没装或平台没有 wheel 时降级，不阻塞图构建
    merman = None
    logger.warning("merman 不可用，Mermaid 校验已跳过：%s: %s", type(exc).__name__, exc)


def _strip_fence(text: str) -> str:
    """去掉模型偶尔带上的 ```mermaid 围栏"""
    s = str(text or "").strip()
    s = re.sub(r"^```(?:mermaid)?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*```$", "", s)
    return s.strip()


def _validate(source: str) -> tuple[bool, str]:
    """调用 merman 校验；没装 merman 或校验异常时视为通过"""
    if merman is None:
        return True, ""
    try:
        result = merman.Merman().validate(source, None)
        return bool(result.valid), result.error or ""
    except Exception as exc:  # 校验器自身故障不应阻塞确认流程
        logger.error("Mermaid 校验异常，按通过处理：%s: %s", type(exc).__name__, exc)
        return True, ""


def repair_mermaid(text: str) -> str:
    """按前端同款规则做保守修复。
    参数:
        text: 模型产出的 Mermaid 源码，可带围栏。
    返回:
        修复后的源码。
    """
    s = _strip_fence(text)
    # 方括号标签里出现括号但没加引号 -> 加双引号
    s = re.sub(
        r"\[([^\[\]\n]*\([^\[\]\n]*\)[^\[\]\n]*)]",
        lambda m: m.group(0)
        if m.group(1).strip().startswith(('"', "("))
        else f'["{m.group(1).replace(chr(34), chr(92) + chr(34))}"]',
        s,
    )
    # 多条语句挤在一行：]E --> F / )F --> G 等，在节点结束符后补换行
    s = re.sub(
        r"([])}])(?=[A-Za-z_][A-Za-z0-9_]*\s*(?:-->|---|==>|\[|\(|\{))",
        lambda m: m.group(1) + "\n",
        s,
    )
    # 箭头两侧统一空格
    s = re.sub(r"\s*-->\s*", " --> ", s)
    return "\n".join(line.strip() for line in s.splitlines() if line.strip())


def check_and_repair_mermaid(text: str) -> tuple[str, str]:
    """校验并修复 Mermaid。
    参数:
        text: 模型产出的 mermaid_flowgraph，可带围栏。
    返回:
        (可用源码, 错误信息)。合法时错误为空；修不好时源码为空串；
        空输入返回 ("", "")；merman 不可用时原样返回源码。
    """
    source = _strip_fence(text)
    if not source:
        return "", ""
    ok, error = _validate(source)
    if ok:
        return source, ""
    fixed = repair_mermaid(source)
    if fixed and fixed != source:
        ok, error = _validate(fixed)
        if ok:
            return fixed, ""
    return "", error or "Mermaid 语法校验失败"
