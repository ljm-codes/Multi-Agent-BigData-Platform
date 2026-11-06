"""
job__restart 的命令解析 / 校验

命令是 POSIX shell 形式（在集群节点上执行）：
    spark-submit --master yarn --conf spark.executor.memory=2g ... hdfs:///app/...py
    flink run-application -t yarn-application -D jobmanager.memory.process.size=1024m ...

约定：
  * 值不合法**不整单失败**：保留原值 / 丢掉新增项，调用方把 ignored 回报给 Agent / 前端；
  * 程序名、黑名单键、shell 控制符是结构问题，直接拒绝。
"""
import posixpath
import re
import shlex
from collections import Counter

# 替换黑名单：不允许替换的键值对（键写法与 com_params 一致，conf/config 前缀、--/- 前缀均可省略）
BLACKLIST: dict[str, set[str]] = {
    "SPARK": {"master", "deploy-mode", "name",
              "config spark.yarn.submit.waitAppCompletion",
              "config spark.yarn.appMasterEnv.PYSPARK_PYTHON",
              "config spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON",
              "config spark.executorEnv.PYSPARK_PYTHON",
              "py-files", "files"},
    "FLINK": {"yarn.provided.lib.dir", "yarn.application.name"},
}

_ALLOWED_PROGRAMS = {"spark-submit", "flink"}
_CONF_FLAGS = {"-D", "--conf", "--config"}
_SHELL_OPERATORS = {"|", "||", "&", "&&", ";", ">", ">>", "<", "<<",
                    "2>", "2>&1", "&>", "1>", "1>>"}
_COUNT_KEYS = {"p", "parallelism", "ys", "yn", "num-executors", "executor-cores",
               "driver-cores", "parallelism.default", "taskmanager.numberoftaskslots"}
_MEMORY_KEYS = {"yjm", "ytm", "driver-memory", "executor-memory",
                "jobmanager.memory.process.size", "taskmanager.memory.process.size",
                "spark.driver.memory", "spark.executor.memory"}
_COUNT_RE = re.compile(r"^[1-9]\d*$")
_MEMORY_RE = re.compile(r"^\d+(\.\d+)?[kKmMgGtT]$")


def split(command: str) -> list[str]:
    """POSIX 命令 -> token 列表；解析不了抛 ValueError"""
    if not isinstance(command, str) or not command.strip():
        raise ValueError("命令不能为空")
    try:
        tokens = shlex.split(command)
    except ValueError as exc:
        raise ValueError(f"命令解析失败: {exc}") from exc
    if not tokens:
        raise ValueError("命令不能为空")
    return tokens


def norm_banned(key: str) -> str:
    """把黑名单项/用户键统一成可比对的形态：去掉 =value、conf/config 前缀和 --/- 前缀"""
    k = str(key or "").strip()
    k = k.split("=", 1)[0].strip()
    for prefix in ("conf ", "config "):
        if k.startswith(prefix):
            k = k[len(prefix):].strip()
            break
    return k.lstrip("-").strip()


def _prop_key(key: str) -> str:
    """用户键 -> 属性名；config/conf/--config/-D 前缀都去掉"""
    k = str(key or "").strip()
    for prefix in ("--config ", "--conf ", "config ", "conf ", "-D ", "D "):
        if k.startswith(prefix):
            return k[len(prefix):].strip()
    return k


def _flag_key(key: str) -> str:
    return str(key or "").strip().lstrip("-").strip()


def _is_memory(key: str) -> bool:
    k = key.lower()
    if "fraction" in k:
        return False  # spark.memory.fraction 是比例，不是内存大小
    return (k in _MEMORY_KEYS or ".memory" in k or k.endswith(".process.size")
            or k.endswith("memoryoverhead"))


def _is_count(key: str) -> bool:
    k = key.lower()
    return (k in _COUNT_KEYS or k.endswith(".numberoftaskslots")
            or k.endswith(".partitions"))


def _is_bool(key: str) -> bool:
    k = key.lower()
    return k.endswith(".enabled") or k.endswith("waitappcompletion")


def validate_param(key: str, value) -> str | None:
    """按参数类型校验值；返回错误说明，None 表示通过"""
    prop = _prop_key(key)
    flag = _flag_key(prop)
    text = "" if value is None else str(value)
    if "\n" in text or "\r" in text:
        return "值里不能有换行"
    if text.strip() == "":
        return "值不能为空"
    if _is_count(prop) or _is_count(flag):
        if not _COUNT_RE.match(text.strip()):
            return "必须是正整数"
    elif _is_memory(prop) or _is_memory(flag):
        if not _MEMORY_RE.match(text.strip()):
            return "内存必须带单位（如 2g / 1024m）"
    elif _is_bool(prop) or _is_bool(flag):
        if text.strip().lower() not in ("true", "false"):
            return "只能是 true/false"
    return None


def rows(tokens: list[str]) -> list[dict]:
    """
    token -> 参数行

    第一个 token 是程序行（program=True）；其余按「flag + 值」配对，
    -D/--conf/--config 的 key=value 整体作为值；位置参数是 value-only 行。
    """
    if not tokens:
        return []
    out: list[dict] = [{"flag": tokens[0], "value": "", "program": True}]
    i = 1
    while i < len(tokens):
        tok = tokens[i]
        if tok in _CONF_FLAGS and i + 1 < len(tokens):
            out.append({"flag": tok, "value": tokens[i + 1]})
            i += 2
            continue
        if tok.startswith("-") and "=" in tok:
            flag, _, value = tok.partition("=")
            if flag in _CONF_FLAGS or flag.startswith("--") or flag.startswith("-D"):
                if flag.startswith("-D") and flag != "-D":
                    out.append({"flag": "-D", "value": tok[2:]})
                else:
                    out.append({"flag": flag, "value": value})
                i += 1
                continue
        if (tok.startswith("-") and i + 1 < len(tokens)
                and not tokens[i + 1].startswith("-")):
            out.append({"flag": tok, "value": tokens[i + 1]})
            i += 2
            continue
        out.append({"flag": "", "value": tok})
        i += 1
    return out


def flatten_rows(parameter_rows: list[dict]) -> list[str]:
    """参数行 -> token 列表（程序行只取 flag）"""
    tokens: list[str] = []
    for row in parameter_rows or []:
        flag = str(row.get("flag") or "").strip()
        value = str(row.get("value") or "")
        if row.get("program"):
            if flag:
                tokens.append(flag)
        elif flag and value != "":
            tokens.extend([flag, value])
        elif flag:
            tokens.append(flag)
        elif value:
            tokens.append(value)
    return tokens


def _row_identity(row: dict) -> tuple[str, str | None]:
    """配对用的身份：conf 用 (flag, 属性名)，其余用 (flag, None)"""
    flag = str(row.get("flag") or "")
    value = str(row.get("value") or "")
    if flag in _CONF_FLAGS and "=" in value:
        return flag, value.split("=", 1)[0].strip()
    return flag, None


def _row_value_info(row: dict) -> tuple[str, str] | None:
    """行 -> (用于校验的键, 值)；位置参数/开关返回 None"""
    flag = str(row.get("flag") or "")
    value = str(row.get("value") or "")
    if not flag:
        return None
    if flag in _CONF_FLAGS and "=" in value:
        key, _, val = value.partition("=")
        return key.strip(), val
    if value != "":
        return flag.lstrip("-").strip(), value
    return None


def _banned_values(tokens: list[str], banned: set[str]) -> dict[str, list[str]]:
    """从命令里抽出黑名单键对应的值，用于比较有没有被改"""
    out: dict[str, list[str]] = {}
    for row in rows(tokens)[1:]:
        flag = str(row.get("flag") or "")
        value = str(row.get("value") or "")
        if flag in _CONF_FLAGS and "=" in value:
            key, _, val = value.partition("=")
            nk = norm_banned(key)
            if nk in banned:
                out.setdefault(nk, []).append(val)
        elif flag.startswith("-"):
            nk = norm_banned(flag)
            if nk in banned:
                out.setdefault(nk, []).append(value)
    return out


def validate_command(
        original_tokens: list[str], new_tokens: list[str], app_type: str,
) -> tuple[list[str], list[dict], str | None]:
    """
    校验人工改过的命令

    :return: (可执行的 token 列表, 被忽略的项, 结构错误)
             结构错误 = 程序名/黑名单/shell 控制符；值错误只回填原值，放在 ignored 里
    """
    if not new_tokens:
        return [], [], "命令不能为空"

    new_program = new_tokens[0]
    if posixpath.basename(new_program) not in _ALLOWED_PROGRAMS:
        return new_tokens, [], f"不允许的程序: {new_program}"
    if original_tokens and new_program != original_tokens[0]:
        return new_tokens, [], "不允许修改提交程序"

    for tok in new_tokens:
        if tok in _SHELL_OPERATORS or "`" in tok or "$(" in tok:
            return new_tokens, [], f"命令里不允许 shell 控制符: {tok}"

    banned = {norm_banned(b) for b in (BLACKLIST.get(str(app_type).upper()) or set())}
    old_banned = _banned_values(original_tokens, banned)
    new_banned = _banned_values(new_tokens, banned)
    for key in set(old_banned) | set(new_banned):
        if Counter(old_banned.get(key) or []) != Counter(new_banned.get(key) or []):
            return new_tokens, [], f"不允许修改黑名单参数: {key}"

    original_rows = rows(original_tokens)
    new_rows = rows(new_tokens)
    old_counter = Counter((r.get("flag"), r.get("value")) for r in original_rows[1:])
    old_by_id: dict[tuple[str, str | None], str] = {}
    for row in original_rows[1:]:
        ident = _row_identity(row)
        old_by_id.setdefault(ident, str(row.get("value") or ""))

    effective_rows: list[dict] = [new_rows[0]]
    ignored: list[dict] = []
    for row in new_rows[1:]:
        pair = (row.get("flag"), row.get("value"))
        if old_counter.get(pair, 0) > 0:
            old_counter[pair] -= 1
            effective_rows.append(row)
            continue
        info = _row_value_info(row)
        if info is None:
            effective_rows.append(row)
            continue
        key, value = info
        reason = validate_param(key, value)
        if reason is None:
            effective_rows.append(row)
            continue
        ident = _row_identity(row)
        kept = old_by_id.get(ident)
        ignored.append({
            "flag": row.get("flag") or "",
            "value": value,
            "reason": reason,
            "kept": kept,
        })
        if kept is not None:
            effective_rows.append({**row, "value": kept})
        # 没有原值可回填的新增参数直接丢掉
    return flatten_rows(effective_rows), ignored, None


def normalize_answer(answer) -> tuple[str | None, list[str] | None, str | None, str | None]:
    """
    人工回复 -> (action, new_tokens, msg, error)

    action 只有 yes/no；no 必须带新命令（rows/tokens/command）或异议 msg。
    """
    if isinstance(answer, str):
        value = answer.strip().lower()
        if value in ("yes", "y"):
            return "yes", None, None, None
        if value in ("no", "n"):
            return None, None, None, "no 需要提供新命令或异议说明"
        return None, None, None, f"无法识别的回复: {answer!r}"

    if not isinstance(answer, dict):
        return None, None, None, "回复必须是 dict 或 yes/no"

    kind = str(answer.get("type") or "").strip().lower()
    if kind in ("yes", "y"):
        return "yes", None, None, None
    if kind not in ("no", "n"):
        return None, None, None, f"type 只能是 yes/no，收到 {kind!r}"

    raw_rows = answer.get("new_command_rows")
    if isinstance(raw_rows, list) and raw_rows:
        tokens = flatten_rows([r for r in raw_rows if isinstance(r, dict)])
        if tokens:
            return "no", tokens, None, None

    raw_tokens = answer.get("new_command_tokens")
    if isinstance(raw_tokens, list) and raw_tokens:
        tokens = [str(t) for t in raw_tokens]
        if tokens:
            return "no", tokens, None, None

    command = answer.get("new_command")
    if isinstance(command, str) and command.strip():
        try:
            return "no", split(command), None, None
        except ValueError as exc:
            return None, None, None, str(exc)

    msg = answer.get("msg")
    if isinstance(msg, str) and msg.strip():
        return "no", None, msg.strip(), None
    return None, None, None, "no 需要提供 new_command/new_command_tokens/new_command_rows 或 msg"


def change_command(
        app_type: str, com_params: dict, command: str,
) -> tuple[str, list[dict]]:
    """
    替换 com_params 里的参数

    :return: (新命令, ignored)；值不合法的项跳过替换、保留原值
    """
    tokens = split(command)
    banned = {norm_banned(b) for b in (BLACKLIST.get(str(app_type).upper()) or set())}
    ignored: list[dict] = []
    for raw_key, raw_value in (com_params or {}).items():
        key = str(raw_key).strip()
        if not key or norm_banned(key) in banned:
            continue
        flag_key = _flag_key(key)
        if flag_key.lower() in ("conf", "config", "d"):
            ignored.append({"key": key, "value": str(raw_value),
                            "reason": "请指定完整属性名，不能只给 conf/D"})
            continue
        reason = validate_param(key, raw_value)
        if reason:
            ignored.append({"key": key, "value": str(raw_value), "reason": reason})
            continue

        prop_key = _prop_key(key)
        replaced = False
        for i in range(len(tokens) - 1):
            tok = tokens[i]
            nxt = tokens[i + 1]
            if tok in _CONF_FLAGS and "=" in nxt:
                vk = nxt.split("=", 1)[0].strip()
                if vk == prop_key:
                    tokens[i + 1] = f"{vk}={raw_value}"
                    replaced = True
            elif tok.startswith("-") and tok.lstrip("-") == flag_key:
                if nxt.startswith("-"):
                    ignored.append({"key": key, "value": str(raw_value),
                                    "reason": "该参数是开关，原命令里没有可替换的值"})
                    replaced = True
                    break
                tokens[i + 1] = str(raw_value)
                replaced = True
        if not replaced:
            # 参数名不存在：按工具文档约定跳过
            continue
    return shlex.join(tokens), ignored
