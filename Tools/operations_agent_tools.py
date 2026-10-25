"""
运维 Agent 工具代码

工具名 = `类型__函数名`，网关按前缀分类型：同类型并发、跨类型顺序。
每个工具返回 SPO.tool_result.ToolResult 的 JSON —— **不用空值表示「没查到」**，
后端和失败类别随返回值给出，网关拿它做熔断/重试，不用猜。

    yarn__     RM REST：应用状态 / 容器→节点→日志 URL / 节点
    log__      容器日志：运行中走 ssh（服务端按关键字过滤），已结束走 HDFS 聚合日志
    node__     节点侧 ssh：内核 OOM 证据
    cluster__  集群级（app_id 为空串那类检查）：总量 / 队列 / JMX
    spark__    Stage / Executor / Job 指标
    flink__    Job / Vertex / Checkpoint / TaskManager 指标
    db__       按 id 取检查记录的明细（invoke 里只给摘要，明细按需取）
    code__     作业代码：总体样貌 / 按限定名取源码段 / 全文搜索
               （源码在 HDFS 上，自身没有文件访问的 Agent 靠这组工具看代码）
    job__      重提作业（写操作，默认只回显不执行）/ 需要动实际代码文件时，转发到对应的 代码开发 Agent
"""
import html
import json
import re
import shlex
import time
from typing import Any, Optional, Tuple

import requests
from langchain_core.tools import tool
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from SPO.postgresql_model import CheckLog
from SPO.tool_result import (ERR_PARSE, ERR_REFUSED, ERR_TIMEOUT, ERR_UNSUPPORTED,
                             ERR_UNKNOWN, ToolResult, empty, fail, not_found, ok)
from Tools import node_ssh
from Tools.code_analysis import JavaAnalyzer, PyAnalyzer
from Tools.db import SessionLocal
from Tools.log_settings import LogSetting
from Tools.registry import register_tool
from Tools.share_check_tool import (get_flink_rest_base, get_stage_summary,
                                    manager, proxy)
from load_config.config import config

logger = LogSetting.create(__name__)

AGENT_NAME = "OM"

# 返回体上限：超了就截断并置 truncated，不让 Agent 把「我只看到一截」当成「就这些」
_MAX_CHARS = 64 * 1024
_HTTP_TIMEOUT = 10
_LOG_HTTP_TIMEOUT = 20

_yarn_conf = ((config.get("hadoop") or {}).get("yarn") or {})
NM_LOG_DIRS = _yarn_conf.get("nm_log_dirs", "/data/nm-log")
# 应用结束后本地日志被聚合到 HDFS 的位置：{remote_app_log_dir}/{user}/bucket-<suffix>-tfile/{bucket}/{app_id}/
# bucket 号由应用 ID 决定、不在配置里 —— 路径里用通配符让 HDFS 自己展开，别去算
REMOTE_APP_LOG_DIR = _yarn_conf.get("remote_app_log_dir", "/tmp/logs")
LOG_USER = _yarn_conf.get("log_user", "hadoop")
# 重提作业用的命令行落在 HDFS 的哪个目录下（{app_type}/cmd/{app_name}.cmd）
STARTUP_CMD_DIR = _yarn_conf.get("startup_cmd_dir", "/app")

# grep -n 的命中行是「行号:内容」，-A 带出来的上下文行是「行号-内容」
_GREP_HIT = re.compile(r"^\d+:")
_PRE = re.compile(r"<pre[^>]*>(.*?)</pre>", re.S)


def _dump(result: ToolResult) -> str:
    return json.dumps(result.model_dump(), ensure_ascii=False)


def _get_json(uri: str, *, timeout: int = _HTTP_TIMEOUT,
              what: str = "端点", params: Optional[dict] = None) -> Tuple[Any, Optional[ToolResult], int]:
    """
    取 JSON，返回 (数据, 失败信封或None, 耗时ms)

    和 share_check_tool.request_get 的区别：那个把失败一律变成 None（checker 用着没问题，
    调度层再做三态翻译）；这里在取数处就分类，因为网关要按类别决定重试还是熔断。
    """
    t0 = time.time()
    try:
        # 不自动跟重定向：从定义的那个地址平台这侧往往不通，跟过去只会得到一句含糊的「连不上」。
        # 自己跟一次，失败时才能说出真正的原因。
        resp = requests.get(uri, timeout=timeout, params=params, allow_redirects=False)
        if resp.status_code in (301, 302, 303, 307, 308):
            location = resp.headers.get("Location", "")
            try:
                resp = requests.get(location, timeout=timeout, params=params)
            except requests.exceptions.RequestException:
                # 归 not_found 而不是 error：代理只在 AM 不在线时才回落到重定向，
                # 所以这不是「没问到」，是「对象已经没了」—— 不该重试、也不该熔断
                return None, not_found(
                    f"{what}被代理跳到了 {location}，但连不上那里 —— 应用多半已经结束",
                    cost_ms=int((time.time() - t0) * 1000)), int((time.time() - t0) * 1000)
    except requests.exceptions.Timeout:
        return None, fail(ERR_TIMEOUT, f"{what}请求超时({timeout}s): {uri}",
                          cost_ms=int((time.time() - t0) * 1000)), int((time.time() - t0) * 1000)
    except requests.exceptions.ConnectionError as e:
        return None, fail(ERR_REFUSED, f"{what}连不上: {e}",
                          cost_ms=int((time.time() - t0) * 1000)), int((time.time() - t0) * 1000)
    except requests.exceptions.RequestException as e:
        return None, fail(ERR_UNKNOWN, f"{type(e).__name__}: {e}",
                          cost_ms=int((time.time() - t0) * 1000)), int((time.time() - t0) * 1000)

    cost = int((time.time() - t0) * 1000)
    if resp.status_code == 404:
        return None, not_found(f"{what}不存在（HTTP 404）: {uri}", cost_ms=cost), cost
    if resp.status_code not in (200, 202):
        return None, fail(ERR_UNKNOWN, f"HTTP {resp.status_code}: {resp.text[:200]}",
                          cost_ms=cost), cost
    try:
        return resp.json(), None, cost
    except ValueError:
        # 应用结束后 YARN 代理会回落成 HTML，**状态码还是 200**
        # 否则网关按 HTTP 语义当成功，Agent 拿一堆 HTML 当数据
        ctype = resp.headers.get("Content-Type", "")
        return None, fail(ERR_PARSE,
                          f"返回的不是 JSON(Content-Type={ctype!r})，多半是应用已结束、代理回落成 HTML",
                          cost_ms=cost), cost


# ---------------------------------------------------------------------------
# yarn__
# ---------------------------------------------------------------------------

@register_tool(AGENT_NAME, 1)
@tool("yarn__get_app")
def yarn_get_app(app_id: str) -> str:
    """
    (工具优先级：1)
    取一个 YARN 应用的状态和终局。

    它返回的 state 决定后面每个工具的结果该怎么解读。

    Args:
        app_id: YARN 应用 ID，形如 application_1790322150352_0001

    Returns:
        data: {state, finalStatus, diagnostics, applicationType, queue, name,
               startedTime, finishedTime, allocatedMB, runningContainers}
        diagnostics 是 YARN 记的失败原因原文。
        注意：
            **若state 不是 RUNNING 时，后面的取数工具可能会陆续失败** —— 先看这个值再往下走。
    """
    app_id = (app_id or "").strip()
    if not app_id:
        return _dump(fail(ERR_UNKNOWN, "app_id 不能为空"))
    data, err, cost = _get_json(f"{manager}/ws/v1/cluster/apps/{app_id}",
                                what=f"应用 {app_id} ")
    if err:
        return _dump(err)
    app = (data or {}).get("app")
    if not app:
        return _dump(not_found(f"RM 里没有这个应用: {app_id}（可能已被清理，或 RM 重启过）",
                               cost_ms=cost))
    return _dump(ok(app, cost_ms=cost))


@register_tool(AGENT_NAME, 2)
@tool("yarn__get_app_containers")
def yarn_get_app_containers(app_id: str) -> str:
    """
    (工具优先级：2)
    列出应用的所有容器：在哪台节点、退出码是多少。

    用途是把「应用」落到「节点 + 容器」上 —— 后面的 log__ 工具要这两个值才能读日志。
    Spark 和 Flink 都适用，AM / JobManager 容器也在列表里。

    Args:
        app_id: YARN 应用 ID

    Returns:
        data: [{container_id, host, node_id, state, exit_status, allocated_mb,
                log_url, is_am}]
        注意：
        - **顺序不固定**（实测返回的是倒序），要 AM / JobManager 那个容器就认
            is_am=true，别取第一个。
        - **不含死因原文** —— 死因看 yarn__get_app 的 diagnostics。
    """
    app_id = (app_id or "").strip()
    if not app_id:
        return _dump(fail(ERR_UNKNOWN, "app_id 不能为空"))

    data, err, cost = _get_json(f"{manager}/ws/v1/cluster/apps/{app_id}/appattempts",
                                what=f"应用 {app_id} 的 attempt 列表 ")
    if err:
        return _dump(err)
    attempts = ((data or {}).get("appAttempts") or {}).get("appAttempt") or []
    if not attempts:
        return _dump(not_found(f"{app_id} 没有任何 attempt 记录", cost_ms=cost))

    containers, last_err, total_cost = [], None, cost
    for att in attempts:
        att_id = att.get("appAttemptId")
        if not att_id:
            continue
        # appAttempt 对象里的 containerId 就是 AM / JobManager 那个容器 —— 用它标 is_am
        am_container_id = att.get("containerId", "")
        cdata, cerr, ccost = _get_json(
            f"{manager}/ws/v1/cluster/apps/{app_id}/appattempts/{att_id}/containers",
            what=f"attempt {att_id} 的容器列表 ")
        total_cost += ccost
        if cerr:
            last_err = cerr
            continue
        # 实测根键是裸数组 `container`，**不是**文档示例写的 containers.container
        for c in (cdata or {}).get("container") or []:
            node_id = c.get("nodeId", "")
            cid = c.get("containerId", "")
            containers.append({
                "attempt_id": att_id,
                "container_id": cid,
                "is_am": bool(cid) and cid == am_container_id,
                "node_id": node_id,
                "host": node_id.split(":")[0],
                "allocated_mb": c.get("allocatedMB", ""),
                "state": c.get("containerState", ""),
                "exit_status": c.get("containerExitStatus", ""),
                "log_url": c.get("logUrl", ""),
            })

    if not containers:
        if last_err:
            return _dump(last_err)
        return _dump(empty(f"{app_id} 当前没有容器（应用可能还没分配到资源）",
                           cost_ms=total_cost))

    result = ok(containers, cost_ms=total_cost)
    if last_err:
        # 部分 attempt 取不到仍返回已拿到的，但要说出来 —— 静默少一半是这里最容易犯的错
        result.message = f"有 attempt 的容器列表没取到: {last_err.message}"
    return _dump(result)


@register_tool(AGENT_NAME, 2)
@tool("yarn__get_nodes")
def yarn_get_nodes() -> str:
    """
    (工具优先级：2)
    取集群所有节点的状态。

    判「是节点资源不够还是队列卡住」时用。

    Args: 无

    Returns:
        data: [{node_id, host, state, mem_utilization, used_memory_mb,
                available_memory_mb, num_containers, health_report}]
        其中：
            state 不是 RUNNING 表示节点不健康，health_report 里有原因。
            mem_utilization 是**瞬时值**，容器一死内存立刻归还，事后读不到出事那一刻。
    """
    data, err, cost = _get_json(f"{manager}/ws/v1/cluster/nodes",
                                what="节点列表 ")
    if err:
        return _dump(err)
    nodes = ((data or {}).get("nodes") or {}).get("node") or []
    if not nodes:
        return _dump(empty("RM 报的节点列表为空", cost_ms=cost))
    return _dump(ok([{
        "node_id": n.get("id", ""),
        "host": n.get("id", "").split(":")[0],
        "state": n.get("state", ""),
        "mem_utilization": n.get("memUtilization"),
        "used_memory_mb": n.get("usedMemoryMB"),
        "available_memory_mb": n.get("availMemMB"),
        "num_containers": n.get("numContainers"),
        "health_report": n.get("healthReport", ""),
    } for n in nodes], cost_ms=cost))


# ---------------------------------------------------------------------------
# log__
# ---------------------------------------------------------------------------

def _app_id_of(container_id: str) -> str:
    """
    container_<clusterTs>_<appSeq>_<attempt>_<seq> -> application_<clusterTs>_<appSeq>

    省得让 Agent 每次都把两个 id 都传对（少一个参数就少一类填错）。
    """
    parts = (container_id or "").split("_")
    if len(parts) < 5 or parts[0] != "container":
        return ""
    return f"application_{parts[1]}_{parts[2]}"


def _log_dir(app_id: str, container_id: str) -> str:
    return f"{NM_LOG_DIRS}/{app_id}/{container_id}"


def _fetch_http_log(url: str) -> Tuple[Optional[str], str, str]:
    """
    取 NM / JHS 的容器日志（logUrl 那套端点），返回 (内容, 状态, 说明)

    NM 和 JHS 是同一套形状：内容包在 `<pre>` 里；**不存在的文件也返回 200**，
    唯一的区分是 全页有没有 `<pre>`（空文件有、不存在的没有）。
    应用还活着时 NM 直接给；结束后 NM 会 307 跳到 JobHistoryServer，
    所以这里跟随重定向，同一个 URL 两种情况都能用。
    """
    try:
        resp = requests.get(url, timeout=_LOG_HTTP_TIMEOUT)
    except requests.exceptions.Timeout:
        return None, "error", f"取日志超时({_LOG_HTTP_TIMEOUT}s)"
    except requests.exceptions.ConnectionError as e:
        return None, "error", f"取日志连不上: {e}"
    except requests.exceptions.RequestException as e:
        return None, "error", f"{type(e).__name__}: {e}"

    if resp.status_code != 200:
        return None, "error", f"取日志 HTTP {resp.status_code}"
    match = _PRE.search(resp.text)
    if not match:
        return None, "not_found", "文件不存在（端点 200 + HTML 但没有 <pre>）"
    return html.unescape(match.group(1)), "ok", ""


def _ssh_error(result: node_ssh.SshResult) -> Optional[ToolResult]:
    """ssh **没执行成**才翻译成信封；执行了但 rc!=0 交给调用方按上下文判断"""
    if result.ok:
        return None
    return fail(result.error_kind, result.message, cost_ms=result.cost_ms)


_APPTRUNC = (
    f"{NM_LOG_DIRS}/{{app_id}}/{{container_id}} 不存在 —— 应用已经结束"
    f"（本地日志被聚合挪到 HDFS 后这个目录会被删掉）"
)


def _agg_log_glob(app_id: str) -> str:
    # bucket 号由应用 ID 决定，但配置里没有 —— 用通配符让 HDFS 自己展开
    return f"{REMOTE_APP_LOG_DIR}/{LOG_USER}/bucket-*/*/{app_id}/*"


def _list_aggregated(host: str, app_id: str, container_id: str) -> Tuple[Optional[list], Optional[ToolResult]]:
    """
    应用已结束时，从 HDFS 的聚合日志里解出该容器有哪些文件

    聚合文件是 TFile（`hdfs dfs -cat` 出来是二进制，必须 `-text`），它的键就是
    `{container_id}/{文件名}` —— 文件清单直接从键里抓，不用知道 bucket 号。
    """
    cmd = (
        f"AGG=$(hdfs dfs -ls {_agg_log_glob(app_id)} 2>/dev/null | awk '{{print $NF}}' | head -1); "
        f'if [ -z "$AGG" ]; then echo __NOAGG__; exit 0; fi; '
        f'echo __AGG__; hdfs dfs -text "$AGG" 2>/dev/null '
        f"| grep -aoE '{container_id}/[A-Za-z0-9._-]+' | sed 's|.*/||' | sort -u"
    )
    res = node_ssh.run(host, cmd)
    if not res.ok:
        return None, fail(res.error_kind, res.message, cost_ms=res.cost_ms)
    if "__NOAGG__" in res.stdout:
        return None, not_found(f"HDFS 上也没有该应用的聚合日志: {_agg_log_glob(app_id)}",
                               cost_ms=res.cost_ms)
    names = [ln.strip() for ln in res.stdout.split("__AGG__", 1)[-1].splitlines() if ln.strip()]
    if not names:
        return None, empty(f"聚合日志里没有 {container_id} 的文件（可能只聚合了别的节点）",
                           cost_ms=res.cost_ms)
    return names, None


def _filter_text(text: str, keywords: list, limit: int, context: int, raw_tail: int) -> dict:
    """
    对**已经拿到手里的整份文本**做筛选，语义和 ssh 那条路对齐

    等价于 `grep -i -n -E <kw> -A <context> <file> | tail -n <limit>`：
    命中的行带上其后 context 行组成块，整体取最后 limit 行。
    """
    lines = text.splitlines()
    pattern = re.compile("|".join(re.escape(k) for k in keywords), re.I) if keywords else None
    matched, blocks = 0, []
    for idx, line in enumerate(lines):
        if pattern is not None and pattern.search(line):
            matched += 1
            blocks.append(f"{idx + 1}:{line}")
            for extra in lines[idx + 1: idx + 1 + context]:
                blocks.append(f"{idx + 1}-{extra}")
    return {
        "matched": matched,
        "text": "\n".join(blocks[-limit:]),
        "raw_tail": "\n".join(lines[-raw_tail:]) if raw_tail else "",
    }


@register_tool(AGENT_NAME, 2)
@tool("log__list")
def log_list(host: str, container_id: str) -> str:
    """
    (工具优先级：2)
    列出某个容器的日志目录里有哪些文件（名字 + 字节数）。

    **读日志前先调它** —— 文件名按框架不同：Spark 是 stdout / stderr，
    Flink 是 jobmanager.log / jobmanager.err / taskmanager.log 等，
    还有 directory.info、launch_container.sh 这类公共文件。不要假设文件名。

    Args:
        host: 节点名，取自 yarn__get_app_containers 的 host
        container_id: 容器 ID，取自 yarn__get_app_containers

    Returns:
        data: {dir, files: [{name, size}], source}
        其中：
            - source="local"（应用还活着，读的是节点上的实时日志目录，size 有值）；
            - source="aggregated"（应用已结束，读的是 HDFS 上聚合下来的日志，size 为空）。
    """
    container_id = (container_id or "").strip()
    app_id = _app_id_of(container_id)
    if not app_id:
        return _dump(fail(ERR_UNKNOWN, f"container_id 格式不对: {container_id!r}"))

    d = _log_dir(app_id, container_id)
    # 显式判断存在性：本地目录只在应用 RUNNING 时存在，一结束就被聚合挪走并删除
    cmd = (f"D={shlex.quote(d)}; "
           f'if [ ! -d "$D" ]; then echo __NODIR__; exit 0; fi; '
           f"""find "$D" -maxdepth 1 -type f -printf '%f\\t%s\\n' | sort""")
    res = node_ssh.run(host, cmd)
    early = _ssh_error(res)
    if early is not None:
        return _dump(early)

    if "__NODIR__" in res.stdout:
        # 应用已结束：本地目录没了，但日志还在 HDFS 上（聚合的东西没丢）
        names, err = _list_aggregated(host, app_id, container_id)
        if err is not None:
            return _dump(err)
        return _dump(ok({"dir": _agg_log_glob(app_id), "source": "aggregated",
                         "files": [{"name": n, "size": None} for n in names]},
                        cost_ms=res.cost_ms))

    files = []
    for line in res.stdout.strip().splitlines():
        name, _, size = line.partition("\t")
        if name:
            files.append({"name": name, "size": int(size) if size.isdigit() else None})

    if not files:
        return _dump(empty(f"{d} 存在但没有文件", cost_ms=res.cost_ms))
    return _dump(ok({"dir": d, "source": "local", "files": files},
                    cost_ms=res.cost_ms))


@register_tool(AGENT_NAME, 2)
@tool("log__read")
def log_read(host: str, container_id: str, filename: str, log_url: str = "",
             level: str = "ERROR,FATAL,WARN", limit: int = 200,
             context: int = 20, raw_tail: int = 20) -> str:
    """
    (工具优先级：2)
    读容器里的一个日志文件，按关键字筛选。

    筛的是「**全文最后 N 条命中**」，不是「最近 N 行里的命中」—— 日志尾巴上如果全是
    INFO，后者会返回空，看起来像没问题。

    Args:
        host: 节点名，取自 yarn__get_app_containers 的 host
        container_id: 容器 ID，取自 yarn__get_app_containers
        filename: 文件名，先用 log__list 确认
        log_url: 该容器的 log_url（取自 yarn__get_app_containers）。
                 **应用已经结束时必须给** —— 本地日志被删了，只能从聚合日志里读
        level: 逗号分隔的关键字，不区分大小写、按字面匹配。默认 "ERROR,FATAL,WARN"；
               也可以填类名或异常名，例如 "FetchFailedException"
        limit: 最多返回多少行（默认 200，上限 2000）
        context: 每条命中后面带多少行上下文（默认 20）。Java 异常的调用栈没有级别前缀，
                不留上下文会把栈切半截，只剩异常名、没有调用链
        raw_tail: 额外返回文件末尾多少行原文（默认 20），回答「进程最后在干什么」

    Returns:
        data: {file, keywords, matched, text, raw_tail, source}
              matched 是命中条数（不含上下文行）。
        其中：
            - text 为空且 matched=0 表示没筛到 —— 可能只是没报错，也可能是关键字不对，
              看一眼 raw_tail 再判断。
            - source="local" 读的是节点上的实时文件；source="aggregated" 读的是 HDFS 聚合日志。
            - status = not_found 时 data.files 给出该容器实际有哪些文件，换一个再试。
    """
    container_id = (container_id or "").strip()
    app_id = _app_id_of(container_id)
    if not app_id:
        return _dump(fail(ERR_UNKNOWN, f"container_id 格式不对: {container_id!r}"))

    # 不 upper()：那样按类名筛（ApplicationMaster）会静默筛不到 —— 实测踩过
    keywords = [kw.strip() for kw in (level or "").split(",") if kw.strip()]
    pattern = "|".join(re.escape(kw) for kw in keywords) or "ERROR|FATAL|WARN"
    limit = max(1, min(int(limit), 2000))
    context = max(0, min(int(context), 200))
    raw_tail = max(0, min(int(raw_tail), 500))

    d = _log_dir(app_id, container_id)
    f = f"{d}/{filename}"
    cmd = (f"D={shlex.quote(d)}; F={shlex.quote(f)}; "
           f'if [ ! -d "$D" ]; then echo __NODIR__; exit 0; fi; '
           f'if [ ! -f "$F" ]; then echo __NOFILE__; ls -1 "$D"; exit 0; fi; '
           f"echo __MATCH__; grep -a -i -n -E {shlex.quote(pattern)} -A {context} \"$F\" | tail -n {limit}; "
           f'echo __RAW__; tail -n {raw_tail} "$F"')
    res = node_ssh.run(host, cmd)
    early = _ssh_error(res)
    if early is not None:
        return _dump(early)

    if "__NODIR__" in res.stdout:
        # 应用已结束：本地目录没了。日志还在 HDFS 上，但要读它得先知道 log_url ——
        # 别自己拼（节点里的 nodeId 端口和作业用户都拿不到），让 Agent 回传
        if not (log_url or "").strip():
            return _dump(not_found(
                _APPTRUNC.format(app_id=app_id, container_id=container_id)
                + "；要读已聚合的日志，把 yarn__get_app_containers 返回的 log_url 传进来",
                cost_ms=res.cost_ms))
        text, status, msg = _fetch_http_log(f"{log_url.rstrip('/')}/{filename}")
        if status == "not_found":
            return _dump(not_found(f"聚合日志里没有 {filename}（{msg}）"
                                   f"，先用 log__list 看该容器有哪些文件",
                                   cost_ms=res.cost_ms))
        if status == "error":
            return _dump(fail(ERR_UNKNOWN, msg, cost_ms=res.cost_ms))
        payload = _filter_text(text or "", keywords, limit, context, raw_tail)
        payload.update({"file": filename, "keywords": keywords, "source": "aggregated",
                        "note": "应用已结束，读的是 HDFS 聚合日志；过滤在本地做（这条路上没法服务端 grep）"})
        if len(json.dumps(payload, ensure_ascii=False)) > _MAX_CHARS:
            payload["text"] = payload["text"][:4000]
            payload["note"] += "；返回体过大已截断"
            return _dump(ok(payload, cost_ms=res.cost_ms, truncated=True))
        return _dump(ok(payload, cost_ms=res.cost_ms))

    if "__NOFILE__" in res.stdout:
        siblings = [ln.strip() for ln in res.stdout.split("__NOFILE__", 1)[1].splitlines() if ln.strip()]
        return _dump(ToolResult(status="not_found", message=f"{f} 不存在",
                                data={"dir": d, "files": siblings},
                                cost_ms=res.cost_ms))

    matched_text, raw_text = "", ""
    if "__MATCH__" in res.stdout:
        matched_text, _, raw_text = res.stdout.split("__MATCH__", 1)[1].partition("__RAW__")
    else:
        raw_text = res.stdout

    matched_text, raw_text = matched_text.strip("\n"), raw_text.strip("\n")
    payload = {
        "dir": d,
        "file": f,
        "keywords": keywords,
        "source": "local",
        # grep -n 的命中行是 `行号:内容`，-A 带出来的上下文行是 `行号-内容` —— 只数前者，
        # 否则 matched 会把上下文也算进去，虚高一截
        "matched": sum(1 for ln in matched_text.splitlines() if _GREP_HIT.match(ln)),
        "text": matched_text,
        "raw_tail": raw_text,
    }
    if len(json.dumps(payload, ensure_ascii=False)) > _MAX_CHARS:
        # 截了就必须说，并且保留「被削掉的是什么」这个信息
        payload["text"] = matched_text[:4000]
        payload["note"] = "返回体过大已截断，text 只剩开头 4000 字；用 level / limit 收窄再取"
        return _dump(ok(payload, cost_ms=res.cost_ms, truncated=True))
    return _dump(ok(payload, cost_ms=res.cost_ms))


# ---------------------------------------------------------------------------
# node__
# ---------------------------------------------------------------------------

@register_tool(AGENT_NAME, 2)
@tool("node__dmesg")
def node_dmesg(host: str, limit: int = 50) -> str:
    """
    (工具优先级：2)
    在节点上查内核日志里的内存不足 / OOM 记录。

    用于把「疑似被内核 OOM 杀掉」确定下来 —— 容器被内核杀掉时，YARN 记的 removeReason
    和外部 kill -9 **逐字节相同**，节点侧的内核日志是唯一能分辨的证据，而且它持久。

    Args:
        host: 节点名，取自 yarn__get_nodes 或 yarn__get_app_containers
        limit: 最多返回多少行（默认 50）

    Returns:
        data: {host, matched, lines, ring_buffer_lines}
        其中：
              matched=0 且 ring_buffer_lines 很小，说明内核环形缓冲区已经被后来的日志
              冲掉了 —— **那是「查不到」，不是「没发生」**。
    """
    limit = max(1, min(int(limit), 500))
    # /var/log/messages 是 root 600，hadoop 读不了（实测），所以 dmesg 是唯一的来源，
    # 而它是环形缓冲、会滚动 —— 所以顺带把缓冲大小报出来
    cmd = ("echo __LINES__; dmesg 2>/dev/null | wc -l; "
           "echo __HIT__; dmesg -T 2>/dev/null "
           f"| grep -iE 'out of memory|oom-kill|killed process' | tail -n {limit}")
    res = node_ssh.run(host, cmd)
    early = _ssh_error(res)
    if early is not None:
        return _dump(early)

    body = res.stdout
    if "__LINES__" not in body:
        return _dump(fail(ERR_UNKNOWN, f"节点返回的内容不是预期格式: {body[:200]}",
                          cost_ms=res.cost_ms))
    _, _, rest = body.partition("__LINES__")
    line_part, _, hit_part = rest.partition("__HIT__")
    total = int(line_part.strip()) if line_part.strip().isdigit() else 0
    lines = [ln for ln in hit_part.strip("\n").splitlines() if ln.strip()]
    payload = {"host": host, "matched": len(lines), "lines": lines,
               "ring_buffer_lines": total}
    if not lines:
        payload["note"] = ("内核日志里没有 OOM 记录。若 ring_buffer_lines 很小（缓冲区已被"
                           "滚动冲掉），这只能说明「查不到」，不能证明没发生过")
    return _dump(ok(payload, cost_ms=res.cost_ms))


# ---------------------------------------------------------------------------
# code__
# ---------------------------------------------------------------------------

_CODE_PATH = "{root}/{app_type}/code"


def _job_app(app_id: str, what: str):
    """
    查 YARN 拿这个应用，顺带认出 app_type（code__ 和 job__ 共用）

    :param what: 认不出类型时，提示「不知道去哪找什么」用的词
    :return: (app 字典, app_type, 失败信封, 耗时ms)
    """
    data, err, cost = _get_json(f"{manager}/ws/v1/cluster/apps/{app_id}", what=f"应用 {app_id} ")
    if err:
        return None, None, err, cost
    app = (data or {}).get("app") or {}
    kind = (app.get("applicationType") or "").lower()
    app_type = "spark" if "spark" in kind else ("flink" if "flink" in kind else "")
    if not app_type:
        return None, None, fail(ERR_UNKNOWN,
                                f"认不出应用类型，不知道去哪个目录找{what}: "
                                f"{app.get('applicationType')!r}", cost_ms=cost), cost
    return app, app_type, None, cost


def _fetch_job_code(app_id: str):
    """
    把作业的主运行文件取回来

    :return: (源码, app_type, 失败信封)；**失败时源码是 None**
    """
    app_id = (app_id or "").strip()
    if not app_id:
        return None, None, fail(ERR_UNKNOWN, "app_id 不能为空")

    _, app_type, err, cost = _job_app(app_id, "代码")
    if err:
        return None, None, err

    host = _client_host()
    if not host:
        return None, None, fail(ERR_UNKNOWN, f"从 manager 配置里解析不出客户端主机: {manager}",
                                cost_ms=cost)

    code_path = _CODE_PATH.format(root=STARTUP_CMD_DIR, app_type=app_type)
    res = node_ssh.run(host, f'hdfs dfs -cat {shlex.quote(code_path)} 2>&1')
    if not res.ok:
        return None, None, fail(res.error_kind, res.message, cost_ms=res.cost_ms)

    body = res.stdout
    # 存成目录时 hdfs 回的是 `cat: `/app/spark/code': Is a directory` —— 也在 "cat: " 之列，先判它
    if "Is a directory" in body:
        ls = node_ssh.run(host, f'hdfs dfs -ls {shlex.quote(code_path)} 2>&1')
        files = [ln.split()[-1] for ln in ls.stdout.splitlines()
                 if ln.strip() and not ln.startswith("Found")]
        return None, None, not_found(
            f"hdfs://{code_path} 是个目录不是文件，里面有 {files} —— 先确认哪个是主运行文件",
            cost_ms=ls.cost_ms)
    if "No such file or directory" in body or "File does not exist" in body:
        return None, None, not_found(f"HDFS 上没有这个作业的代码: hdfs://{code_path}",
                                     cost_ms=res.cost_ms)
    if "cat: " in body:
        # 剩下的 cat 报错是 RPC 失败 / 没权限那一类 —— 那是「没取到」，不能报成「没有」
        return None, None, fail(ERR_UNKNOWN, f"取代码失败: {body[:300]}", cost_ms=res.cost_ms)
    return body, app_type, None


def _analyzer_of(source: str):
    """
    把源码变成对应的分析器 —— **语言分派就在这一处**

    先当 Python 解，解不动再当 Java 解（代码路径没有扩展名，只能这么认）

    :return: PyAnalyzer / JavaAnalyzer，或失败信封
    """
    try:
        return PyAnalyzer(source_code=source)
    except (SyntaxError, ValueError) as exc:
        py_err = f"第 {getattr(exc, 'lineno', '?')} 行 {getattr(exc, 'msg', exc)}"

    try:
        analyzer = JavaAnalyzer(source_code=source)
    except RuntimeError as exc:  # 环境里没有 tree_sitter
        return fail(ERR_UNSUPPORTED, str(exc))

    if analyzer.has_error and not analyzer.class_hierarchy():
        head = (source.splitlines() or [""])[0][:80]
        return fail(ERR_UNSUPPORTED,
                    f"这份代码既不是 Python 也不是 Java（当成 Python 解: {py_err}；"
                    f"当成 Java 解: 有语法错误且没解析出任何类/方法）。文件首行: {head!r}")
    return analyzer


def _dump_capped(payload: dict, big_key: str, cost_ms: int = 0) -> str:
    """
    返回体超上限就截断 —— 截了必须说，别让 Agent 把「只看到一截」当成「就这些」

    :param big_key: 哪个字段是大头，只截它
    """
    if len(json.dumps(payload, ensure_ascii=False)) > _MAX_CHARS:
        val = payload[big_key]
        keep = 200 if isinstance(val, list) else 4000
        payload[big_key] = val[:keep]
        payload["note"] = f"返回体过大已截断，{big_key} 只保留了前 {keep}；收窄范围再取一次"
        return _dump(ok(payload, cost_ms=cost_ms, truncated=True))
    return _dump(ok(payload, cost_ms=cost_ms))


@register_tool(AGENT_NAME, 3)
@tool("code__appearance")
def code_appearance(app_id: str) -> str:
    """
    (工具优先级：3)
    看作业代码的总体样貌 —— 不读全文就知道这是个什么作业

    Args:
        app_id: YARN 应用 ID

    Returns:
        data: {app_id, app_type, total_lines, entry, imports, hierarchy}
              （Java 还多一个 package）
        其中：
            - hierarchy 是**定义嵌套树**（谁写在谁里面），**不是调用关系**；
              类节点用 classes 键、函数节点用 name 键，有装饰器才给 decorators；
              Java 的匿名类没有名字，不进这棵树
            - hierarchy 是 [] 只说明这份代码里没有类/函数（脚本式作业就是这样）；
              **解析不了的代码不给空树**，而是 error + kind=unsupported —— 两者别混
            - imports 是 {绑定名: 来源模块}，来源给的是完整模块路径；
              Java 里名字不在 imports 里**不等于找不到**（同包、同类不用 import）
            - entry 是入口的行号：Python 认 `if __name__ == "__main__"`，Java 认 static main；
              **它是 None 不代表没有入口**，只代表没认出来
            - package 是 Java 文件自己的包名，拿它把栈帧里的全名对回文件里的短名
    """
    source, app_type, err = _fetch_job_code(app_id)
    if err:
        return _dump(err)

    analyzer = _analyzer_of(source)
    if isinstance(analyzer, ToolResult):
        return _dump(analyzer)

    payload = {
        "app_id": app_id,
        "app_type": app_type,
        "total_lines": len(source.splitlines()),
        "entry": analyzer.entry(),
        "imports": {name: info["module"] for name, info in analyzer.imports().items()},
        "hierarchy": analyzer.class_hierarchy(),
    }
    package = getattr(analyzer, "package", None)
    if package:
        payload["package"] = package
    return _dump_capped(payload, "hierarchy")


@register_tool(AGENT_NAME, 3)
@tool("code__read")
def code_read(app_id: str, qualname: str = None, start_line: int = None,
              end_line: int = None) -> str:
    """
    (工具优先级：3)
    取作业代码里的一段 —— 要么按名字取整个函数/类，要么按行号取几行

    Args:
        app_id: YARN 应用 ID
        qualname: 限定名，**从文件的类往下写全**，如 "Job.process"；模块级的函数用裸名。
                  Java 可以直接抄栈帧里的写法（com.ops.fault.FaultJob$RateSource.run），
                  包名和 $ 会被归一
        start_line: 起始行（从 1 开始）
        end_line: 结束行；不给就只取 start_line 那一行

    Returns:
        data: {app_id, qualname, start_line, end_line, code}，Java 碰到同名方法时多一个 overloads
        其中：
            - 给了 qualname 就**以它为准，行号参数被忽略**；code 是那个函数/类的完整源码段
            - qualname 回的是**解析出来的全名**，写了包名或 $ 的会被归一成它
            - overloads 是 Java 同名方法（重载）的清单，每项 {signature, start_line, end_line}；
              code 给的是**行号最靠前**的那个 —— 手里的栈帧有行号时，先拿它和 overloads 对一下
            - 按行取时 code 每行带 `行号: ` 前缀，方便接着定位
            - not_found 时 message 会写明**错在哪一层、那一层有什么**，照着重试
    """
    source, app_type, err = _fetch_job_code(app_id)
    if err:
        return _dump(err)

    analyzer = _analyzer_of(source)
    if isinstance(analyzer, ToolResult):
        return _dump(analyzer)

    if qualname:
        got = analyzer.code_by_name(qualname)
        if got is None:
            return _dump(not_found(
                analyzer.lookup_hint(qualname) or
                f"这份代码里没有 {qualname!r}；类里的方法要用限定名（如 Job.process），"
                f"先 code__appearance 看有哪些名字"))
        payload = {"app_id": app_id, "qualname": got["qualname"],
                   "start_line": got["lineno"], "end_line": got["end_lineno"],
                   "code": got["code"]}
        if got.get("overloads"):
            payload["overloads"] = [
                {"signature": o["signature"], "start_line": o["lineno"], "end_line": o["end_lineno"]}
                for o in got["overloads"]]
        return _dump_capped(payload, "code")

    if start_line is None:
        return _dump(fail(ERR_UNKNOWN, "qualname 和 start_line 至少给一个"))

    lines = source.splitlines()
    lo = max(int(start_line) - 1, 0)
    hi = min(int(end_line) if end_line else int(start_line), len(lines))
    if lo >= len(lines):
        return _dump(not_found(f"这个文件只有 {len(lines)} 行，没有第 {start_line} 行"))
    payload = {"app_id": app_id, "qualname": None, "start_line": lo + 1, "end_line": hi,
               "code": "\n".join(f"{i + 1}: {lines[i]}" for i in range(lo, hi))}
    return _dump_capped(payload, "code")


@register_tool(AGENT_NAME, 3)
@tool("code__grep")
def code_grep(app_id: str, pattern: str, start_line: int = None, end_line: int = None,
              is_regex: bool = False, exclude_docstring: bool = False) -> str:
    """
    (工具优先级：3)
    在作业代码里按行搜文本 —— 注释、字符串、配置键都能搜到

    Args:
        app_id: YARN 应用 ID
        pattern: 要找的内容；**默认按字面量匹配**（不当正则），搜代码片段和配置键不会踩元字符
        start_line: 起始行（从 1 开始）；不给则从第 1 行开始
        end_line: 结束行；**不给就只看 start_line 那一行**，此时 is_regex 和 exclude_docstring 都不生效
        is_regex: True 才把 pattern 当正则
        exclude_docstring: True 时剔除落在注释里的命中（Python 是 docstring，Java 是 // 和 /* */）

    Returns:
        data: {app_id, pattern, matched, hits: [{lineno, text, scope}]}
        其中：
            - scope 是这一行所在函数/方法的限定名，不在任何类/函数里就是 "模块级"
              （Java 的匿名类没有名字，那里的行报到外面那层）
            - matched=0 是「这份代码里确实没有」，**不是「没取到」**
            - 搜的是源码文本，所以**注释和字符串也会命中** —— 这正是它区别于按名字查的地方
    """
    source, app_type, err = _fetch_job_code(app_id)
    if err:
        return _dump(err)

    analyzer = _analyzer_of(source)
    if isinstance(analyzer, ToolResult):
        return _dump(analyzer)

    hits = analyzer.grep(pattern, start_line=start_line, end_line=end_line,
                         is_regex=is_regex, exclude_docstring=exclude_docstring)
    payload = {"app_id": app_id, "pattern": pattern, "matched": len(hits), "hits": hits}
    if not hits:
        payload["note"] = "这份代码里没有匹配的行"
    return _dump_capped(payload, "hits")


# ---------------------------------------------------------------------------
# job__
# ---------------------------------------------------------------------------

def _client_host() -> str:
    """提交命令在哪台机器上跑 —— 就是 RM 所在的那台客户端节点"""
    from urllib.parse import urlparse
    return urlparse(manager).hostname or ""


@register_tool(AGENT_NAME, 4)
@tool("job__restart")
def job_restart(app_id: str, dry_run: bool = True, app_name: str | None = None) -> str:
    """
    (工具优先级：4)
    重提一个作业：把当初提交它的那条命令原样再跑一次。

    命令取自 HDFS 上按应用名存档的 `{startup_cmd_dir}/{spark|flink}/cmd/{name}.cmd`。

    Args:
        app_id: YARN 应用 ID；内部会用 YARN 返回的 name 去找命令文件
        dry_run: True（默认）只把命令取回来，**不执行**；False 才真的提交
        app_name: YARN 应用名称；不给则用 app_id 去找(可选)

    Returns:
        data: {app_id, app_name, app_type, cmd_path, command, executed, output}
        其中：
            - app_name 是 YARN 返回的应用名称（也是 cmd 文件名）
            - status = not_found 表示这个应用没有存档命令 —— 只能人工重提。
        （Spark 可以用 log__read 读它的 launch_container.sh，里面有 AM 当时的实际命令行，
        但那是 ApplicationMaster 的形态，和 spark-submit 的那条不是一回事。）
    """
    app_id = (app_id or "").strip()
    if not app_id:
        return _dump(fail(ERR_UNKNOWN, "app_name 不能为空"))

    app, app_type, err, cost = _job_app(app_id, "启动命令")
    if err:
        return _dump(err)

    host = _client_host()
    if not host:
        return _dump(fail(ERR_UNKNOWN, f"从 manager 配置里解析不出客户端主机: {manager}",
                          cost_ms=cost))

    # app_name 传的是 app_id；命令文件按 YARN 的 name 存
    resolved_name = ((app or {}).get("name") or app_name).strip()
    cmd_path = f"{STARTUP_CMD_DIR}/{app_type}/cmd/{resolved_name}.cmd"
    res = node_ssh.run(host, f'hdfs dfs -cat {shlex.quote(cmd_path)} 2>&1')
    if not res.ok:
        return _dump(fail(res.error_kind, res.message, cost_ms=res.cost_ms))
    command = res.stdout.strip()
    if not command or "No such file or directory" in command or command.startswith("cat: "):
        return _dump(not_found(
            f"没有这个应用的启动命令存档: hdfs://{cmd_path} —— 只能人工重提",
            cost_ms=res.cost_ms))

    payload = {"app_id": app_name, "app_name": resolved_name, "app_type": app_type,
               "cmd_path": cmd_path, "command": command, "executed": False, "output": ""}
    if dry_run:
        payload["note"] = "dry_run=True，只回显命令没有执行"
        return _dump(ok(payload, cost_ms=res.cost_ms))

    # 写操作：真提交。非幂等，网关那一侧不要让这条自动重试
    run_res = node_ssh.run(host, f"{command} 2>&1 | tail -n 40", timeout=120)
    if not run_res.ok:
        return _dump(fail(run_res.error_kind, run_res.message, cost_ms=run_res.cost_ms))
    payload["executed"] = True
    payload["output"] = run_res.stdout[-4000:]
    payload["exit_code"] = run_res.rc
    if run_res.rc != 0:
        payload["note"] = f"提交命令退出码 {run_res.rc}，看 output"
    return _dump(ok(payload, cost_ms=run_res.cost_ms))


# ---------------------------------------------------------------------------
# db__
# ---------------------------------------------------------------------------

def _check_data_of(error_id) -> Tuple[Any, Optional[ToolResult]]:
    """按 id 取一条错误记录的 check_data；取不到时第二个返回值是失败信封"""
    try:
        eid = int(error_id)
    except (TypeError, ValueError):
        return None, fail(ERR_UNKNOWN, f"id 必须是整数，收到 {error_id!r}")
    t0 = time.time()
    try:
        with SessionLocal() as session:
            row = session.query(CheckLog.id, CheckLog.check_data).filter(CheckLog.id == eid).first()
    except OperationalError as e:
        return None, fail(ERR_REFUSED, f"连不上数据库: {e}", cost_ms=int((time.time() - t0) * 1000))
    except SQLAlchemyError as e:
        return None, fail(ERR_UNKNOWN, f"{type(e).__name__}: {e}", cost_ms=int((time.time() - t0) * 1000))
    cost = int((time.time() - t0) * 1000)
    if row is None:
        return None, not_found(f"没有 id={eid} 的检查记录（可能已被清理）",
                               cost_ms=cost)
    return row.check_data, None


def _shape_of(data: Any) -> dict:
    """check_data 的结构 —— list 型是常态，它没有「键」，所以给元素键的并集"""
    if isinstance(data, dict):
        return {"type": "dict", "keys": list(data.keys())}
    if isinstance(data, list):
        item_keys = sorted({k for item in data if isinstance(item, dict) for k in item})
        return {"type": "list", "count": len(data), "item_keys": item_keys}
    return {"type": type(data).__name__}


@register_tool(AGENT_NAME, 1)
@tool("db__get_check_keys")
def db_get_check_keys(checker_id: int) -> str:
    """
    (工具优先级：1)
    查一条检查记录的 check_data 里有哪些字段。

    由于只给了错误的摘要，且错误数据可能很大，所以明细要按需来取 —— **先调它看清结构，再用
    db__get_check_value 取需要的字段**，不要一上来就把整份明细拉出来。

    Args:
        checker_id: 检查记录的 id，来自本次待处理错误列表里的 id

    Returns:
        data: dict 型 -> {type: "dict", keys: [...]}
              list 型 -> {type: "list", count: N, item_keys: [...]}
        注意：
              list 型是常态（每个失败的任务/stage/子任务一条），取字段时
              db__get_check_value 会返回一整列。
    """
    data, err = _check_data_of(checker_id)
    if err is not None:
        return _dump(err)
    return _dump(ok(_shape_of(data)))


@register_tool(AGENT_NAME, 2)
@tool("db__get_check_value")
def db_get_check_value(checker_id: int, key: str = "") -> str:
    """
    (工具优先级：2)
    从一条检查记录的 check_data 里按字段名取值。

    Args:
        checker_id: 检查记录的 id
        key: 字段名，先用 db__get_check_keys 看有哪些。
             可以逗号分隔一次取多个，比如 "stage_id,attempt_id"。
             **留空则返回整份 check_data**（明细可能很大，非必要别这么用）

    Returns:
        data: {fields: {字段名: 值}}
        注意：
              list 型的 check_data 会把该字段取成一整列（例如 task_id 一次给出全部），
              同时给出 found_in / total —— **found_in 比 total 小说明只有部分元素有
              这个字段**，别当成全部。
              字段不存在时该字段为 null 并在 missing 里列出来。
    """
    data, err = _check_data_of(checker_id)
    if err is not None:
        return _dump(err)

    keys = [k.strip() for k in (key or "").split(",") if k.strip()]
    if not keys:
        payload = {"fields": data, "missing": [], "found_in": None, "total": None,
                   "note": "key 留空，返回的是整份 check_data"}
        text = json.dumps(payload, ensure_ascii=False)
        if len(text) > _MAX_CHARS:
            payload["fields"] = text[:4000]
            payload["note"] = "整份 check_data 过大已截断，改用具体字段名取"
            return _dump(ok(payload, truncated=True))
        return _dump(ok(payload))

    fields, missing, found_in, total = {}, [], None, None
    for k in keys:
        if isinstance(data, dict):
            if k in data:
                fields[k] = data[k]
            else:
                fields[k] = None
                missing.append(k)
        elif isinstance(data, list):
            total = len(data)
            present = [item for item in data if isinstance(item, dict) and k in item]
            found_in = len(present)
            if present:
                fields[k] = [item[k] for item in present]
            else:
                fields[k] = None
                missing.append(k)
        else:
            fields[k] = None
            missing.append(k)

    return _dump(ok({"fields": fields, "missing": missing,
                     "found_in": found_in, "total": total}))


# ---------------------------------------------------------------------------
# cluster__  —— app_id 为空串的那类检查（磁盘/句柄/队列）用这一组
# ---------------------------------------------------------------------------

@register_tool(AGENT_NAME, 1)
@tool("cluster__get_metrics")
def cluster_get_metrics() -> str:
    """
    (工具优先级：1)
    取集群总量指标：应用数、容器数、内存和 vcore 的已用/可用/总量。

    判「是资源真的不够，还是只是排队」时用。

    Args: 无

    Returns:
        data: {apps_submitted, apps_running, apps_pending, apps_completed,
               containers_allocated, containers_pending, containers_reserved,
               memory_total, memory_available, memory_allocated,
               vcores_total, vcores_available, vcores_allocated,
               active_nodes, unhealthy_nodes}
        pending 大但 available 也大 = 队列在压着；available 接近 0 = 资源真的不够。
    """
    data, err, cost = _get_json(f"{manager}/ws/v1/cluster/metrics", what="集群指标 ")
    if err:
        return _dump(err)
    m = (data or {}).get("clusterMetrics") or {}
    if not m:
        return _dump(empty("RM 没返回集群指标", cost_ms=cost))
    return _dump(ok({
        "apps_submitted": m.get("appsSubmitted"),
        "apps_running": m.get("appsRunning"),
        "apps_pending": m.get("appsPending"),
        "apps_completed": m.get("appsCompleted"),
        "containers_allocated": m.get("containersAllocated"),
        "containers_pending": m.get("containersPending"),
        "containers_reserved": m.get("containersReserved"),
        "memory_total": m.get("totalMB"),
        "memory_available": m.get("availableMB"),
        "memory_allocated": m.get("allocatedMB"),
        "vcores_total": m.get("totalVirtualCores"),
        "vcores_available": m.get("availableVirtualCores"),
        "vcores_allocated": m.get("allocatedVirtualCores"),
        "active_nodes": m.get("activeNodes"),
        "unhealthy_nodes": m.get("unhealthyNodes"),
    }, cost_ms=cost))


def _leaf_queues(node: dict) -> list:
    """
    递归摊平出叶子队列 —— 判据是节点自己的 `type`，不靠层数

    和 check_resource_abnormal.get_all_queue_info 同一个思路（按 type 认叶子），
    但那边只认 `capacitySchedulerLeafQueueInfo`、且从 `queues.queue` 起步，
    **在本集群（fairScheduler）会一条都取不到**：叶子是 `fairSchedulerLeafQueueInfo`，
    根在 `schedulerInfo.rootQueue`，子节点在 `childQueues.queue`（也是 dict 包一层）。
    这里两种调度器都认，判据统一成「type 以 LeafQueueInfo 结尾」。
    """
    kids = (((node or {}).get("queues") or {}).get("queue")
            or ((node or {}).get("childQueues") or {}).get("queue")
            or [])
    if str((node or {}).get("type", "")).endswith("LeafQueueInfo"):
        return [node]
    if not kids:
        # 既没有 type 又没有子节点：当叶子兜底，总比整棵树都丢掉好
        return [node] if (node or {}).get("queueName") else []
    out = []
    for child in kids:
        out.extend(_leaf_queues(child))
    return out


def _scheduler_root(info: dict) -> dict:
    """容量调度器的根就是 schedulerInfo 自己，公平调度器的根在 rootQueue 里"""
    return (info or {}).get("rootQueue") or info or {}


@register_tool(AGENT_NAME, 1)
@tool("cluster__get_queue")
def cluster_get_queue(queue: str = "") -> str:
    """
    (工具优先级：1)
    取YARN队列的容量与占用。

    判「应用排不进去是不是队列满了」时用。

    Args:
        queue: 队列名（如 root.hadoop）。留空则返回全部叶子队列

    Returns:
        data: {scheduler_type, queues: [...]}
        注意：
              scheduler_type 是 capacityScheduler 或 fairScheduler —— **两者的字段不一样**，
              本集群实测是 fairScheduler。
              每个队列: {queue_name, state, used_memory_mb, max_memory_mb, used_ratio,
                        pending_containers, allocated_containers, max_apps,
                        num_active_applications, num_pending_applications}
              **取不到的字段是 null**（公平调度器不报应用数、容量调度器不报容器数）
              —— null 表示这个调度器不提供，不是 0。
    """
    data, err, cost = _get_json(f"{manager}/ws/v1/cluster/scheduler", what="调度器队列 ")
    if err:
        return _dump(err)
    info = ((data or {}).get("scheduler") or {}).get("schedulerInfo") or {}
    if not info:
        return _dump(empty("RM 没返回调度器信息", cost_ms=cost))

    leaves = _leaf_queues(_scheduler_root(info))
    if queue.strip():
        want = queue.strip().rstrip(".")
        leaves = [q for q in leaves if (q.get("queueName") or "").rstrip(".") == want]
        if not leaves:
            return _dump(not_found(f"没有名为 {queue!r} 的叶子队列", cost_ms=cost))
    if not leaves:
        return _dump(empty(f"调度器（{info.get('type')}）没报出任何叶子队列",
                           cost_ms=cost))

    queues = []
    for q in leaves:
        used = (q.get("usedResources") or {}).get("memory")
        maximum = (q.get("maxResources") or {}).get("memory")
        ratio = round(used / maximum, 4) if isinstance(used, (int, float)) and maximum else None
        queues.append({
            "queue_name": q.get("queueName"),
            "state": q.get("state"),
            "used_memory_mb": used,
            "max_memory_mb": maximum,
            "used_ratio": ratio,
            "pending_containers": q.get("pendingContainers"),
            "allocated_containers": q.get("allocatedContainers"),
            "max_apps": q.get("maxApps"),
            "num_active_applications": q.get("numActiveApplications"),
            "num_pending_applications": q.get("numPendingApplications"),
        })
    return _dump(ok({"scheduler_type": info.get("type"), "queues": queues},
                    cost_ms=cost))


@register_tool(AGENT_NAME, 1)
@tool("cluster__get_jmx")
def cluster_get_jmx() -> str:
    """
    (工具优先级：1)
    取 ResourceManager 进程自身的操作系统级指标。

    文件句柄耗尽那类告警看这里（OpenFileDescriptorCount 逼近 MaxFileDescriptorCount）。

    Args: 无

    Returns:
        data: {open_file_descriptors, max_file_descriptors, system_load_average,
               free_physical_memory, free_swap_space, process_cpu_load,
               available_processors}
        **取的是 RM 那台机器的值**，不是全集群的。
    """
    bean = "java.lang:type=OperatingSystem"
    data, err, cost = _get_json(f"{manager}/jmx", what="RM 的 JMX ",
                                params={"qry": bean})
    if err:
        return _dump(err)
    beans = (data or {}).get("beans") or []
    if not beans:
        return _dump(empty(f"RM 的 /jmx 没有返回 {bean} 这个 bean", cost_ms=cost))
    b = beans[0]
    return _dump(ok({
        "open_file_descriptors": b.get("OpenFileDescriptorCount"),
        "max_file_descriptors": b.get("MaxFileDescriptorCount"),
        "system_load_average": b.get("SystemLoadAverage"),
        "free_physical_memory": b.get("FreePhysicalMemorySize"),
        "free_swap_space": b.get("FreeSwapSpaceSize"),
        "process_cpu_load": b.get("ProcessCpuLoad"),
        "available_processors": b.get("AvailableProcessors"),
    }, cost_ms=cost))


# ---------------------------------------------------------------------------
# spark__  —— Stage / Executor 指标
# ---------------------------------------------------------------------------

def _spark_base(app_id: str) -> str:
    return f"{proxy}/proxy/{app_id}/api/v1/applications/{app_id}"


def _spark_task_rollup(tasks: dict) -> dict:
    """把 task 级明细压成计数 —— 明细本身动辄上百条（实测单个 stage 最多 120 个 task）"""
    if not isinstance(tasks, dict):
        tasks = {}
    by_status: dict = {}
    for t in tasks.values():
        if isinstance(t, dict):
            by_status[t.get("status", "?")] = by_status.get(t.get("status", "?"), 0) + 1
    return {"task_count": len(tasks), "by_status": by_status}


@register_tool(AGENT_NAME, 2)
@tool("spark__get_jobs")
def spark_get_jobs(app_id: str) -> str:
    """
    (工具优先级：2)
    取 Spark 应用当前的 job 列表。

    「任务超时」那类告警要拿它看 job 是不是真的不动了：/jobs 是**新的在前**，
    列表第一个就是最后跑的那个 job。

    Args:
        app_id: YARN 应用 ID

    Returns:
        data: [{job_id, name, status, submission_time, completion_time,
                num_active_tasks, num_completed_tasks, num_failed_tasks}]
        注意：
            传进来的 job 一定会跟着 stage 一起返回，但**失败后重试成功的 stage 不在这里**，
            要看失败证据得用 spark__get_stage。
    """
    data, err, cost = _get_json(f"{_spark_base(app_id)}/jobs", what=f"应用 {app_id} 的 job 列表 ")
    if err:
        return _dump(err)
    if not isinstance(data, list):
        return _dump(empty(f"{app_id} 没有 job 记录（应用可能刚启动，或已结束）",
                           cost_ms=cost))
    return _dump(ok([{
        "job_id": j.get("jobId"),
        "name": j.get("name"),
        "status": j.get("status"),
        "submission_time": j.get("submissionTime"),
        "completion_time": j.get("completionTime"),
        "num_active_tasks": len(j.get("activeTasks") or []),
        "num_completed_tasks": len(j.get("completedTasks") or []),
        "num_failed_tasks": len(j.get("failedTasks") or []),
    } for j in data], cost_ms=cost))


@register_tool(AGENT_NAME, 2)
@tool("spark__get_stage")
def spark_get_stage(app_id: str, stage_id: int) -> str:
    """
    (工具优先级：2)
    取一个 Spark stage 的详细指标。

    审「数据倾斜」「网络超时」这类 stage 级告警时用。**同时给两次 attempt**：
    指标类判定只能看最近一次（失败那次的数据是残缺的），失败类判定必须看失败那次
    —— Spark 会重试 stage，重试成功后最新 attempt 就是 COMPLETE，只看最新会漏掉
    已经自愈的失败（实测有 stage attempt0 失败、attempt1 成功后 5 个 checker 全返回 None）。

    Args:
        app_id: YARN 应用 ID
        stage_id: stage ID

    Returns:
        data: {latest: {...}, latest_failed: {...}}
        其中：
              每个 attempt: {attempt_id, status, num_tasks, by_status,
                             submission_time, completion_time, failure_reason,
                             executor_run_time, shuffle_read_records, shuffle_write_records,
                             disk_bytes_spilled, peak_execution_memory}
              task 级明细被压成计数，不逐条返回。
    """
    latest, latest_failed = get_stage_summary(stage_id, app_id)
    if latest is None and latest_failed is None:
        return _dump(not_found(f"应用 {app_id} 里取不到 stage {stage_id}（stage 不存在，"
                               f"或应用已结束、代理回落成 HTML）"))

    def shape(info: Optional[dict]) -> Optional[dict]:
        # 不返回 {}：「从没失败过」和「失败过但字段都是空的」必须分得开
        if not info:
            return None
        return {
            "attempt_id": info.get("attemptId"),
            "status": info.get("status"),
            "submission_time": info.get("submissionTime"),
            "completion_time": info.get("completionTime"),
            "failure_reason": info.get("failureReason"),
            "executor_run_time": info.get("executorRunTime"),
            "shuffle_read_records": (info.get("shuffleReadMetrics") or {}).get("recordsRead"),
            "shuffle_write_records": (info.get("shuffleWriteMetrics") or {}).get("recordsWritten"),
            "disk_bytes_spilled": info.get("diskBytesSpilled"),
            "peak_execution_memory": (info.get("peakExecutionMemory")),
            **_spark_task_rollup(info.get("tasks")),
        }

    return _dump(ok({"latest": shape(latest), "latest_failed": shape(latest_failed)}))


@register_tool(AGENT_NAME, 2)
@tool("spark__get_executors")
def spark_get_executors(app_id: str) -> str:
    """
    (工具优先级：2)
    取 Spark 应用的 executor 列表与指标。

    审「OOM」「executor 被杀」这类告警时用 —— **死因原文在 remove_reason 里**，
    它比退出码有信息量：137 只说明"被 SIGKILL"，kill -9 / 抢占 / 缩容 / 内存超限
    全长这样（实测同一个 137 曾被误报成 72 条「内存不足」）。

    Args:
        app_id: YARN 应用 ID

    Returns:
        data: [{executor_id, host, is_active, remove_reason, remove_time,
                rdd_blocks, memory_used, max_memory, jvm_heap_memory, jvm_gc_time,
                peak_on_heap_execution_memory}]
        注意：
            **peak_on_heap_execution_memory 是按心跳采样的**（生产默认 10 秒一次），
            短任务读到 0 一般是「没采到」，不是「没超」——要和 stage 的 disk_bytes_spilled
            交叉验证。
    """
    data, err, cost = _get_json(f"{_spark_base(app_id)}/allexecutors", what=f"应用 {app_id} 的 executor 列表 ")
    if err:
        return _dump(err)
    if not isinstance(data, list) or not data:
        return _dump(empty(f"{app_id} 没有 executor（应用可能还没分配到资源）",
                           cost_ms=cost))
    out = []
    for e in data:
        if not isinstance(e, dict):
            continue
        peak = e.get("peakMemoryMetrics") or {}
        out.append({
            "executor_id": e.get("id"),
            "host": e.get("hostPort"),
            "is_active": e.get("isActive"),
            "remove_reason": e.get("removeReason"),
            "remove_time": e.get("removeTime"),
            "rdd_blocks": e.get("rddBlocks"),
            "memory_used": e.get("memoryUsed"),
            "max_memory": e.get("maxMemory"),
            "jvm_heap_memory": peak.get("JVMHeapMemory"),
            "jvm_gc_time": (e.get("totalGCTime")),
            "peak_on_heap_execution_memory": peak.get("OnHeapExecutionMemory"),
        })
    return _dump(ok(out, cost_ms=cost))


# ---------------------------------------------------------------------------
# flink__  —— Job / Vertex / Checkpoint / TaskManager 指标
# ---------------------------------------------------------------------------

def _flink_get(app_id: str, path: str, what: str) -> Tuple[Any, Optional[ToolResult], int]:
    return _get_json(f"{get_flink_rest_base(app_id)}{path}", what=f"{what} ", timeout=_LOG_HTTP_TIMEOUT)


def _flink_job_ids(app_id: str) -> Tuple[list, Optional[ToolResult], int]:
    """该应用当前在跑的 job id —— 代理地址是动态的，job id 只能问"""
    data, err, cost = _flink_get(app_id, "/jobs/overview", "Flink job 列表")
    if err:
        return [], err, cost
    jobs = (data or {}).get("jobs") or []
    running = [j.get("jid") for j in jobs if j.get("jid") and j.get("state") == "RUNNING"]
    if not running:
        return [], empty(f"{app_id} 当前没有 RUNNING 的 Flink job（可能已结束或正在重启）",
                         cost_ms=cost), cost
    return running, None, cost


@register_tool(AGENT_NAME, 2)
@tool("flink__get_job")
def flink_get_job(app_id: str, job_id: str = "") -> str:
    """
    (工具优先级：2)
    取 Flink job 的概览，以及它的算子（vertex）列表。

    **这是 Flink 侧的第一个调用** —— 它给出 job_id 和 vertex_id，后面
    flink__get_vertex / flink__get_health 都要这两个值。

    Args:
        app_id: YARN 应用 ID
        job_id: job ID。留空则自动取该应用当前 RUNNING 的那个（通常只有一个）

    Returns:
        data: {job_id, name, state, start_time, duration, vertices: [...]}
        其中：
              每个 vertex: {vertex_id, name, parallelism, status, duration,
                           read_records, write_records}
              各 vertex 之间 read_records 差得远 = 数据倾斜的信号。
    """
    jid = (job_id or "").strip()
    if not jid:
        ids, err, _ = _flink_job_ids(app_id)
        if err:
            return _dump(err)
        jid = ids[0]

    data, err, cost = _flink_get(app_id, f"/jobs/{jid}", f"Flink job {jid}")
    if err:
        return _dump(err)
    if not data:
        return _dump(not_found(f"应用 {app_id} 里没有 job {jid}", cost_ms=cost))

    return _dump(ok({
        "job_id": data.get("jid"),
        "name": data.get("name"),
        "state": data.get("state"),
        "start_time": data.get("start-time"),
        "duration": data.get("duration"),
        "vertices": [{
            "vertex_id": v.get("id"),
            "name": v.get("name"),
            "parallelism": v.get("parallelism"),
            "status": v.get("status"),
            "duration": v.get("duration"),
            "read_records": ((v.get("metrics") or {}).get("read-records")),
            "write_records": ((v.get("metrics") or {}).get("write-records")),
        } for v in (data.get("vertices") or [])],
    }, cost_ms=cost))


@register_tool(AGENT_NAME, 2)
@tool("flink__get_vertex")
def flink_get_vertex(app_id: str, vertex_id: str, job_id: str = "") -> str:
    """
    (工具优先级：2)
    取一个算子的子任务级指标、水位线和背压。

    **数据倾斜看这里** —— 倾斜是子任务之间的量差，只有子任务级看得到。
    水位线和背压一并取，取不到时在 partial 里说明，不影响主数据。

    Args:
        app_id: YARN 应用 ID
        vertex_id: 算子 ID，取自 flink__get_job 的 vertices
        job_id: job ID，留空则自动取当前 RUNNING 的

    Returns:
        data: {vertex_id, name, parallelism, subtasks: [...],
               watermarks: [...], backpressure: [...]}
        其中：
              每个 subtask: {subtask_id, host, duration, status, read_records,
                             write_records, read_bytes, write_bytes}
              **状态在 subtask 级，算子级没有 status 这个字段**（实测详情里就没有）。
              **watermarks / backpressure 取失败时是 null 并在 partial 里列出原因**
              —— 别把 null 当成"没有异常"。
    """
    jid = (job_id or "").strip()
    if not jid:
        ids, err, _ = _flink_job_ids(app_id)
        if err:
            return _dump(err)
        jid = ids[0]

    base = f"/jobs/{jid}/vertices/{vertex_id}"
    detail, err, cost = _flink_get(app_id, base, f"算子 {vertex_id}")
    if err:
        return _dump(err)
    if not detail:
        return _dump(not_found(f"job {jid} 里没有算子 {vertex_id}",
                               cost_ms=cost))

    payload = {
        "job_id": jid,
        "vertex_id": detail.get("id"),
        "name": detail.get("name"),
        "parallelism": detail.get("parallelism"),
        # 算子级没有 status（实测详情里就没这个键），状态只在 subtask 上
        "subtasks": [{
            "subtask_id": s.get("subtask"),
            "host": s.get("host"),
            "status": s.get("status"),
            "duration": s.get("duration"),
            "read_records": ((s.get("metrics") or {}).get("read-records")),
            "write_records": ((s.get("metrics") or {}).get("write-records")),
            "read_bytes": ((s.get("metrics") or {}).get("read-bytes")),
            "write_bytes": ((s.get("metrics") or {}).get("write-bytes")),
        } for s in (detail.get("subtasks") or [])],
    }

    partial = []
    for path, field in ((f"{base}/watermarks", "watermarks"),
                        (f"{base}/backpressure", "backpressure")):
        sub, suberr, _ = _flink_get(app_id, path, field)
        if suberr is not None:
            payload[field] = None
            partial.append(f"{field}: {suberr.message}")
        else:
            payload[field] = sub
    if partial:
        payload["partial"] = partial
    return _dump(ok(payload, cost_ms=cost))


@register_tool(AGENT_NAME, 2)
@tool("flink__get_health")
def flink_get_health(app_id: str, job_id: str = "") -> str:
    """
    (工具优先级：2)
    取 Flink job 的 checkpoint 记录和异常历史。

    「检查点异常」「任务挂起」「其他异常」这几类告警的信息源。
    **exceptions 里的 stacktrace 是定因的关键** —— 类冲突、数据源、权限、格式都在里面。

    Args:
        app_id: YARN 应用 ID
        job_id: job ID，留空则自动取当前 RUNNING 的

    Returns:
        data: {job_id, checkpoints: {...}, exceptions: [...]}
        其中：
              exceptions 每项: {exception_name, stacktrace, task_name,
                                timestamp, taskmanager_id}
              **checkpoint 一条都没有**（counts 全 0）和「checkpoint 取失败」不是一回事，
              后者会在 partial 里列出来。
    """
    jid = (job_id or "").strip()
    if not jid:
        ids, err, _ = _flink_job_ids(app_id)
        if err:
            return _dump(err)
        jid = ids[0]

    payload, partial = {"job_id": jid}, []
    ckpt, cerr, cost = _flink_get(app_id, f"/jobs/{jid}/checkpoints", "checkpoint")
    if cerr is not None:
        payload["checkpoints"] = None
        partial.append(f"checkpoints: {cerr.message}")
    else:
        payload["checkpoints"] = {
            "counts": (ckpt or {}).get("counts") or {},
            "latest_completed": ((ckpt or {}).get("latest") or {}).get("completed"),
            "latest_failed": ((ckpt or {}).get("latest") or {}).get("failed"),
            "history": ((ckpt or {}).get("history") or [])[:10],
        }

    exc, eerr, _ = _flink_get(app_id, f"/jobs/{jid}/exceptions", "异常历史")
    if eerr is not None:
        payload["exceptions"] = None
        partial.append(f"exceptions: {eerr.message}")
    else:
        payload["exceptions"] = [{
            "exception_name": e.get("exceptionName"),
            "stacktrace": e.get("stacktrace"),
            "task_name": e.get("taskName"),
            "timestamp": e.get("timestamp"),
            "taskmanager_id": e.get("taskManagerId") or e.get("taskmanagerId"),
        } for e in (((exc or {}).get("exceptionHistory") or {}).get("entries") or [])]

    if partial:
        payload["partial"] = partial
    # 实测：checkpoint 20/20 全失败时 /exceptions 仍然是空的 —— 不点破的话
    # Agent 会把「exceptions=[]」读成「没问题」
    failed_ckpt = ((payload.get("checkpoints") or {}).get("counts") or {}).get("failed")
    if not payload.get("exceptions") and failed_ckpt:
        payload["note"] = (f"有 {failed_ckpt} 次 checkpoint 失败，但 /exceptions 是空的 —— "
                           f"checkpoint 失败不一定记进异常历史，别把 exceptions=[] 当成没问题")
    return _dump(ok(payload, cost_ms=cost))


@register_tool(AGENT_NAME, 2)
@tool("flink__get_taskmanagers")
def flink_get_taskmanagers(app_id: str) -> str:
    """
    (工具优先级：2)
    取 Flink 应用的 TaskManager 列表与堆内存水位。

    「TaskManager OOM 水位」那类告警用。**日志和详情的入口也在返回里**
    （detail_path / log_url），不用自己拼地址 —— JobManager 落在哪台机器是动态的。

    Args:
        app_id: YARN 应用 ID

    Returns:
        data: {taskmanagers: [{taskmanager_id, host, slots_number, free_slots,
                heap_used, heap_max, heap_usage_ratio, detail_path, log_url}]}
        注意：
              heap_usage_ratio 高不等于已经 OOM，**OOM 的直接证据在 taskmanager.err 里**
              （用 log__list / log__read 读它）。
              某个 TM 的详情取不到时它的堆字段是 null，并在 partial 里给出原因
              —— null 是「没取到」，不是「没占用」。
    """
    base = get_flink_rest_base(app_id)
    data, err, cost = _flink_get(app_id, "/taskmanagers", "TaskManager 列表")
    if err:
        return _dump(err)
    tms = (data or {}).get("taskmanagers") or []
    if not tms:
        return _dump(empty(f"{app_id} 没有 TaskManager（可能还没分配到资源）",
                           cost_ms=cost))

    out, partial = [], []
    for tm in tms:
        tm_id = tm.get("id", "")
        # 堆内存**只在 detail 端点上**（实测 /taskmanagers 列表的 metrics 是子集，
        # 没有 heapUsed/heapMax）—— 一个 TM 一次请求，TM 数量本来就只有几个
        metrics = tm.get("metrics") or {}
        if tm_id:
            detail, derr, _ = _flink_get(app_id, f"/taskmanagers/{tm_id}", f"TaskManager {tm_id}")
            if derr is not None:
                partial.append(f"{tm_id} 详情取不到（堆内存为 null）: {derr.message}")
            else:
                metrics = (detail or {}).get("metrics") or metrics
        used, maximum = metrics.get("heapUsed"), metrics.get("heapMax")
        ratio = None
        if isinstance(used, (int, float)) and isinstance(maximum, (int, float)) and maximum:
            ratio = round(used / maximum, 4)
        out.append({
            "taskmanager_id": tm_id,
            "host": tm.get("path"),
            "slots_number": tm.get("slotsNumber"),
            "free_slots": tm.get("freeSlots"),
            "heap_used": used,
            "heap_max": maximum,
            "heap_usage_ratio": ratio,
            "detail_path": f"{base}/taskmanagers/{tm_id}" if tm_id else "",
            "log_url": f"{base}/taskmanagers/{tm_id}/logs" if tm_id else "",
        })
    payload = {"taskmanagers": out}
    if partial:
        payload["partial"] = partial
    return _dump(ok(payload, cost_ms=cost))
