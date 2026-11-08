"""用例执行引擎：真实 HTTP 对话、人工确认/后台、集群核验，以及运维链路。"""
from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from string import Template
from typing import Any

import psycopg
import yaml

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.system_eval import config_env  # noqa: E402
from tests.system_eval.api_client import ApiClient, ApiError  # noqa: E402
from tests.system_eval.cluster import Cluster  # noqa: E402
from tests.system_eval.recorder import CaseRecorder, jsonable, now_iso  # noqa: E402
from tests.system_eval.sse import iter_sse  # noqa: E402

CASES_DIR = Path(__file__).resolve().parent / "cases"


# ---------------------------------------------------------------------------
# case spec
# ---------------------------------------------------------------------------

@dataclass
class CaseSpec:
    path: Path
    meta: dict[str, Any]
    prompt_template: str

    @property
    def id(self) -> str:
        return str(self.meta["id"])

    @property
    def stage(self) -> str:
        return str(self.meta["stage"])

    @property
    def submit_policy(self) -> str:
        value = self.meta.get("submit", "no")
        if isinstance(value, bool):
            return "yes" if value else "no"
        return str(value).lower()

    @property
    def timeout_s(self) -> float:
        return float(self.meta.get("timeout_s") or 2400)

    def render_prompt(self, context: dict[str, Any]) -> str:
        return Template(self.prompt_template).safe_substitute(context).strip()

    @classmethod
    def load(cls, path: Path) -> "CaseSpec":
        text = path.read_text(encoding="utf-8")
        m = re.match(r"^---\s*\n(.*?)\n---\s*\n(.*)$", text, re.S)
        if not m:
            raise ValueError(f"{path} 缺少 YAML frontmatter")
        meta = yaml.safe_load(m.group(1)) or {}
        body = m.group(2)
        pm = re.search(r"^##\s*用户输入[^\n]*\n(.*?)(?=^##\s|\Z)", body, re.S | re.M)
        prompt = (pm.group(1) if pm else body).strip()
        return cls(path=path, meta=meta, prompt_template=prompt)


def load_specs(*, stages: set[str] | None = None, ids: set[str] | None = None) -> list[CaseSpec]:
    specs = []
    for path in sorted(CASES_DIR.glob("*.md")):
        spec = CaseSpec.load(path)
        if stages and spec.stage not in stages:
            continue
        if ids and spec.id not in ids:
            continue
        specs.append(spec)
    order = {"analysis": 0, "develop": 1, "ops": 2}
    specs.sort(key=lambda s: (order.get(s.stage, 9), s.id))
    return specs


def _topics(run_id: str) -> dict[str, str]:
    return {
        "run_id": run_id,
        "words_topic": f"eval_{run_id}_words",
        "events_topic": f"eval_{run_id}_events",
        "topn_topic": f"eval_{run_id}_topn",
        "bad_topic": f"eval_{run_id}_bad",
    }


def _produce_for_case(spec: CaseSpec, topics: dict[str, str], rec: CaseRecorder) -> dict[str, Any] | None:
    """topic 必须在作业提交前存在；events/topn 的具体数据等应用 RUNNING 后再发。

    作业可能用 earliest 或 latest。先清空 topic、应用启动后再发，两种 offset 策略
    都能消费到同一批数据；如果提前发，latest 会直接跳过（B2h2 实测无输出）。
    """
    mode = spec.meta.get("producer")
    if not mode:
        return None
    try:
        from tests.system_eval import producer as producer_mod
        if mode == "words":
            info = producer_mod.produce_words(topics["words_topic"])
        elif mode == "events":
            info = producer_mod.produce_events(topics["events_topic"], send=False)
        elif mode == "topn":
            info = producer_mod.produce_events_topn(topics["topn_topic"], send=False)
        elif mode == "bad":
            info = producer_mod.produce_bad_events(topics["bad_topic"])
        else:
            info = {"error": f"unknown producer mode: {mode}"}
        rec.write_json("producer.json", info)
        return info
    except Exception as exc:  # noqa: BLE001
        info = {"error": f"{type(exc).__name__}: {exc}"}
        rec.write_json("producer.json", info)
        return info


# ---------------------------------------------------------------------------
# db / redis helpers
# ---------------------------------------------------------------------------

def _db_query(sql: str, params: tuple | None = None) -> list[dict[str, Any]]:
    with config_env._connect(config_env.EVAL_DB, autocommit=False) as conn:
        cur = conn.execute(sql, params or ())
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _db_execute(sql: str, params: tuple | None = None) -> int:
    with config_env._connect(config_env.EVAL_DB) as conn:
        cur = conn.execute(sql, params or ())
        return int(cur.rowcount or 0)


def _redis_queue_remove(app_id: str) -> None:
    try:
        import redis
        cfg = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
        r = redis.Redis(
            host=cfg["redis"]["host"], port=cfg["redis"]["port"],
            password=cfg["redis"]["password"], db=cfg["redis"]["db"],
        )
        key = cfg["hadoop"]["checker"]["error_queue"]["error_app_redis_key"]
        r.zrem(key, app_id)
    except Exception:
        pass


def _upsert_app_file(app_type: str, app_name: str, source_dir: str, commit_cmd: str) -> None:
    _db_execute(
        """
        insert into code.app_file_idx
            (app_type, app_name, source_type, source_dir, commit_cmd, create_time, update_time)
        values (%s, %s, 'hdfs', %s, %s, now(), now())
        on conflict (app_type, app_name) do update set
            source_type='hdfs', source_dir=excluded.source_dir,
            commit_cmd=excluded.commit_cmd, update_time=now()
        """,
        (app_type.upper(), app_name, source_dir, commit_cmd),
    )


# ---------------------------------------------------------------------------
# polling thread
# ---------------------------------------------------------------------------

class Poller(threading.Thread):
    def __init__(self, api: ApiClient, rec: CaseRecorder, holder: dict[str, str | None]):
        super().__init__(daemon=True)
        self.api = api
        self.rec = rec
        self.holder = holder
        self.stop_event = threading.Event()

    def run(self) -> None:
        while not self.stop_event.is_set():
            tid = self.holder.get("thread_id")
            if tid:
                try:
                    v = self.api.verification(tid)
                    if v:
                        self.rec.verification(v)
                except Exception:
                    pass
                try:
                    self.rec.background(self.api.backgrounds(tid))
                except Exception:
                    pass
                try:
                    self.rec.tool_traces(self.api.tool_traces(tid))
                except Exception:
                    pass
            self.stop_event.wait(5)

    def stop(self) -> None:
        self.stop_event.set()
        self.join(timeout=10)


# ---------------------------------------------------------------------------
# interrupt policy
# ---------------------------------------------------------------------------

@dataclass
class InterruptState:
    submit_answered: bool = False
    resumes: list[dict[str, Any]] = field(default_factory=list)


def _interrupt_value(interrupts: Any) -> Any:
    if isinstance(interrupts, list) and interrupts:
        item = interrupts[0]
        if isinstance(item, dict) and "value" in item:
            return item.get("value")
        return item
    if isinstance(interrupts, dict) and "value" in interrupts:
        return interrupts.get("value")
    return interrupts


def _answer_for(value: Any, spec: CaseSpec, state: InterruptState) -> Any:
    if isinstance(value, str):
        if state.submit_answered:
            return "no"
        state.submit_answered = True
        return spec.submit_policy
    if isinstance(value, dict):
        if value.get("kind") == "job_restart":
            return {"type": "no", "msg": "system_eval：只记录建议，本次不重启"}
        if value.get("type") == "error":
            return {"pass": False, "msg": "system_eval：验证阶段报错，终止", "force_terminate": True}
        return {"pass": True, "msg": "system_eval：自动通过"}
    return {"pass": False, "msg": "system_eval：未知 interrupt 类型", "force_terminate": True}


# ---------------------------------------------------------------------------
# project / trace helpers
# ---------------------------------------------------------------------------

_TMP_CODE_RE = re.compile(r"([A-Za-z]:[\\/][^\n\"']*?tmp_code[\\/][^\n\"']*|[^\s\"']*?/tmp_code/[^\s\"']*)")


def _trace_args(trace: dict[str, Any]) -> dict[str, Any]:
    args = trace.get("args")
    if isinstance(args, dict):
        return args
    if isinstance(args, str):
        try:
            parsed = json.loads(args)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _all_trace_text(traces: list[dict[str, Any]]) -> str:
    return "\n".join(json.dumps(t, ensure_ascii=False, default=str) for t in traces)


def _find_project_dir(traces: list[dict[str, Any]], history: dict[str, Any], app_type: str) -> Path | None:
    text = _all_trace_text(traces) + "\n" + json.dumps(history, ensure_ascii=False, default=str)
    for m in _TMP_CODE_RE.finditer(text):
        raw = m.group(1).replace("\\\\", "\\").replace("%20", " ").rstrip(".,;)]}\"'")
        p = Path(raw)
        if p.is_dir():
            return p
    project_name = (history.get("metadata") or {}).get("project_name")
    if not project_name:
        for trace in reversed(traces):
            if trace.get("tool") == "initialize_project":
                project_name = _trace_args(trace).get("project_name")
                if project_name:
                    break
            if trace.get("tool") == "register_submit_command":
                # register 的参数里没有项目名，但可能带 app_name；最后再兜底
                pass
    if project_name:
        base = ROOT / "tmp_code" / (app_type or "")
        for p in base.glob(f"*/{project_name}"):
            if p.is_dir():
                return p
        for p in (ROOT / "tmp_code").rglob(str(project_name)):
            if p.is_dir():
                return p
    return None


def _copy_project_artifacts(case_dir: Path, project: Path | None) -> dict[str, Any]:
    if project is None or not project.is_dir():
        return {"project_dir": None, "files": []}
    out = case_dir / "artifacts"
    out.mkdir(parents=True, exist_ok=True)
    files = []
    count = 0
    for path in sorted(project.rglob("*")):
        if not path.is_file() or count >= 60:
            continue
        rel = path.relative_to(project)
        if any(part in {"target", "__pycache__", ".git", "venv", ".venv", "node_modules"} for part in rel.parts):
            continue
        if path.suffix.lower() not in {".py", ".java", ".xml", ".md", ".txt", ".json", ".yaml", ".yml", ".properties"}:
            continue
        if path.stat().st_size > 2_000_000:
            continue
        dest = out / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, dest)
        files.append({"path": rel.as_posix(), "size": path.stat().st_size})
        count += 1
    return {"project_dir": str(project), "files": files}


def _extract_app_name(traces: list[dict[str, Any]], history: dict[str, Any]) -> str | None:
    for trace in reversed(traces):
        if trace.get("tool") != "register_submit_command":
            continue
        args = _trace_args(trace)
        name = args.get("app_name")
        if name:
            return str(name)
    blob = json.dumps(history, ensure_ascii=False, default=str)
    m = re.search(r"app_name\s*[=:：]\s*([A-Za-z0-9_.-]+)", blob)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# chat case
# ---------------------------------------------------------------------------

def _wait_ready(api: ApiClient, timeout: float = 180) -> None:
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            api.login(config_env.read_eval_creds()["email"], config_env.read_eval_creds()["password"])
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(3)
    raise RuntimeError(f"服务未就绪: {last}")


def _handle_stream(rec: CaseRecorder, resp, holder: dict[str, Any]) -> dict[str, Any]:
    ctx: dict[str, Any] = {
        "done": None, "error": None, "interrupt": None,
        "thread_switch": None, "events": 0,
    }
    try:
        for event, data in iter_sse(resp):
            t = time.perf_counter()
            rec.event(event, data, recv_perf=t)
            ctx["events"] += 1
            if event == "meta":
                holder["thread_id"] = data.get("thread_id")
            elif event == "thread_switch":
                ctx["thread_switch"] = data
                holder["thread_id"] = data.get("thread_id")
            elif event == "interrupt":
                ctx["interrupt"] = data.get("interrupt")
            elif event == "done":
                ctx["done"] = data
            elif event == "error":
                ctx["error"] = data
    finally:
        try:
            resp.close()
        except Exception:
            pass
    return ctx


def _resume_loop(
    api: ApiClient, rec: CaseRecorder, spec: CaseSpec,
    thread_id: str, interrupts: Any, state: InterruptState,
) -> list[dict[str, Any]]:
    current = interrupts
    while current:
        value = _interrupt_value(current)
        answer = _answer_for(value, spec, state)
        entry = {"interrupt": jsonable(current), "value": jsonable(value), "answer": jsonable(answer)}
        started = time.perf_counter()
        try:
            data = api.resume(thread_id, answer, timeout=spec.timeout_s)
            entry["response"] = jsonable(data)
            entry["duration_ms"] = round((time.perf_counter() - started) * 1000, 1)
            state.resumes.append(entry)
            rec.append_jsonl("resume.jsonl", entry)
            if not data.get("interrupted"):
                break
            current = data.get("interrupt") or []
        except Exception as exc:  # noqa: BLE001
            entry["error"] = f"{type(exc).__name__}: {exc}"
            entry["duration_ms"] = round((time.perf_counter() - started) * 1000, 1)
            state.resumes.append(entry)
            rec.append_jsonl("resume.jsonl", entry)
            break
    return state.resumes


def _wait_background(
    api: ApiClient, rec: CaseRecorder, spec: CaseSpec,
    thread_id: str, state: InterruptState,
) -> list[dict[str, Any]]:
    """深层测试转后台后：等记录终态；若后台续跑又 interrupt，继续 resume。"""
    deadline = time.monotonic() + spec.timeout_s
    started = time.monotonic()
    resumed: set[str] = set()
    while time.monotonic() < deadline:
        try:
            records = api.backgrounds(thread_id)
        except Exception:
            records = []
        rec.background(records)
        active = [r for r in records if r.get("status") in ("registered", "running")]
        paused = []
        for record in records:
            if not record.get("interrupt"):
                continue
            key = f"{record.get('task_id')}:{record.get('interrupt')}"
            if key not in resumed:
                paused.append((key, record))
        if paused:
            key, record = paused[-1]
            resumed.add(key)
            _resume_loop(api, rec, spec, thread_id, record.get("interrupt"), state)
            continue
        if not active:
            if records:
                return records
            # 刚 done 时 registry 可能还没建；给它 15 秒
            if time.monotonic() - started >= 15:
                return records
        time.sleep(2 if not records else 5)
    return []


def run_chat_case(spec: CaseSpec, ctx: dict[str, Any]) -> dict[str, Any]:
    api: ApiClient = ctx["api"]
    run_dir: Path = ctx["run_dir"]
    case_dir = run_dir / "cases" / spec.id
    rec = CaseRecorder(case_dir)
    topics = _topics(ctx["run_id"])
    prompt = spec.render_prompt(topics)
    (case_dir / "prompt.md").write_text(prompt, encoding="utf-8")
    rec.write_json("case.json", {
        "id": spec.id, "stage": spec.stage, "difficulty": spec.meta.get("difficulty"),
        "submit": spec.submit_policy, "app_type": spec.meta.get("app_type"),
        "producer": spec.meta.get("producer"), "topics": topics,
        "expected": spec.meta.get("expected"), "prompt_template": spec.prompt_template,
        "prompt_rendered": prompt,
    })

    # 必须在对话/提交之前把 Kafka topic 建出来并灌入确定性数据
    producer_info = _produce_for_case(spec, topics, rec)

    holder: dict[str, Any] = {"thread_id": None}
    poller = Poller(api, rec, holder)
    poller.start()

    state = InterruptState()
    result: dict[str, Any] = {
        "case": spec.id, "prompt": prompt, "threads": [],
        "done": None, "error": None, "interrupts": [], "resumes": [],
        "timeline": {}, "verification": None, "tool_traces": [],
        "project": None, "cluster": None, "ops": None,
    }
    started_ms = int(time.time() * 1000)
    case_started = time.perf_counter()
    outcome_status = "completed"

    try:
        try:
            resp = api.chat_stream(prompt, read_timeout=spec.timeout_s)
        except Exception as exc:  # noqa: BLE001
            result["error"] = f"chat_stream 失败: {type(exc).__name__}: {exc}"
            outcome_status = "failed"
        else:
            stream_ctx = _handle_stream(rec, resp, holder)
            result["done"] = jsonable(stream_ctx["done"])
            result["error"] = jsonable(stream_ctx["error"])
            if stream_ctx["done"]:
                result["threads"].append(stream_ctx["done"].get("thread_id"))
            if holder.get("thread_id"):
                result["threads"].append(holder["thread_id"])
            if stream_ctx["interrupt"]:
                result["interrupts"].append(jsonable(stream_ctx["interrupt"]))
                _resume_loop(api, rec, spec, holder["thread_id"], stream_ctx["interrupt"], state)
            elif stream_ctx["done"]:
                _wait_background(api, rec, spec, holder["thread_id"], state)
            elif stream_ctx["error"]:
                outcome_status = "failed"

        # 主线程结束后，后台可能还有 result；统一再兜一次
        if holder.get("thread_id"):
            try:
                _wait_background(api, rec, spec, holder["thread_id"], state)
            except Exception:
                pass

        # 汇总线程历史/工具轨迹
        threads = list(dict.fromkeys([t for t in result["threads"] if t]))
        histories = {}
        traces: list[dict[str, Any]] = []
        for tid in threads:
            try:
                histories[tid] = api.history(tid)
            except Exception as exc:  # noqa: BLE001
                histories[tid] = {"error": f"{type(exc).__name__}: {exc}"}
            try:
                traces.extend(api.tool_traces(tid))
            except Exception:
                pass
        result["histories"] = jsonable(histories)
        result["threads"] = threads
        result["resumes"] = jsonable(state.resumes)
        result["tool_traces"] = jsonable(traces)
        rec.write_json("tool_traces.json", traces)
        try:
            rec.write_json("verification.json", api.verification(threads[-1]) if threads else None)
        except Exception:
            pass
        try:
            rec.write_json("background.json", api.backgrounds(threads[-1]) if threads else [])
        except Exception:
            pass

        app_type = str(spec.meta.get("app_type") or "")
        final_history = histories.get(threads[-1]) if threads else {}
        project = _find_project_dir(traces, final_history or {}, app_type)
        result["project"] = _copy_project_artifacts(case_dir, project)

        verify = spec.meta.get("verify")
        if verify in ("yarn", "running"):
            try:
                result["cluster"] = _verify_yarn(
                    spec, ctx, rec, traces, final_history or {}, started_ms, producer_info,
                )
            except Exception as exc:  # noqa: BLE001
                result["cluster"] = {"error": f"{type(exc).__name__}: {exc}", "pass": False}
                rec.write_json("cluster.json", result["cluster"])
        else:
            result["cluster"] = {"skipped": True, "reason": "verify=none (任务要求不提交)"}
            rec.write_json("cluster.json", result["cluster"])

    except Exception as exc:  # noqa: BLE001
        result["error"] = f"{type(exc).__name__}: {exc}"
        outcome_status = "failed"
    finally:
        poller.stop()

    result["status"] = outcome_status
    result["duration_ms"] = round((time.perf_counter() - case_started) * 1000, 1)
    rec.final(result, timeline={"status": outcome_status, "stage": spec.stage})
    from tests.system_eval.rubric import score_chat_case
    rubric = score_chat_case(spec, case_dir)
    rec.write_json("rubric.json", rubric)
    rec.write_json("outcome.json", {"id": spec.id, "status": outcome_status,
                                    "duration_ms": result["duration_ms"],
                                    "rubric": rubric.get("total")})
    return result


# ---------------------------------------------------------------------------
# yarn verification
# ---------------------------------------------------------------------------

def _known_app_ids(ctx: dict[str, Any]) -> set[str]:
    return ctx.setdefault("known_app_ids", set())


def _find_new_app(
    cluster: Cluster, *, app_name: str | None, project_name: str | None,
    started_ms: int, known: set[str], timeout: float = 120,
) -> dict[str, Any] | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        apps = cluster.yarn_apps(started_after_ms=max(0, started_ms - 90_000), size=200)
        candidates = [a for a in apps if a.get("id") not in known]
        if app_name:
            exact = [a for a in candidates if a.get("name") == app_name]
            if exact:
                return sorted(exact, key=lambda a: a.get("startedTime") or 0)[-1]
        if project_name:
            part = [a for a in candidates if project_name in (a.get("name") or "")]
            if part:
                return sorted(part, key=lambda a: a.get("startedTime") or 0)[-1]
        if candidates:
            return sorted(candidates, key=lambda a: a.get("startedTime") or 0)[-1]
        time.sleep(5)
    return None


def _verify_yarn(
    spec: CaseSpec, ctx: dict[str, Any], rec: CaseRecorder,
    traces: list[dict[str, Any]], history: dict[str, Any],
    started_ms: int, producer_info: dict[str, Any] | None,
) -> dict[str, Any]:
    cluster: Cluster = ctx["cluster"]
    final_text = str(history.get("output_msg") or "")
    blob = final_text + "\n" + json.dumps(history, ensure_ascii=False, default=str)
    if any(k in blob for k in (
        "作业没有进行提交", "没有进行提交", "未进行提交",
        "拒绝自动提交", "集群提交失败", "无法处理结果返回的消息",
    )):
        out = {
            "skipped": True,
            "reason": "final answer reports no submission / submit failure",
            "final_text": final_text[-2000:],
            "pass": False,
        }
        rec.write_json("cluster.json", out)
        return out
    known = _known_app_ids(ctx)
    app_name = _extract_app_name(traces, history)
    project_name = (history.get("metadata") or {}).get("project_name")
    app = _find_new_app(
        cluster, app_name=app_name, project_name=project_name,
        started_ms=started_ms, known=known,
    )
    if app is None:
        out = {"app_name": app_name, "error": "没有找到新提交的 YARN 应用", "pass": False}
        rec.write_json("cluster.json", out)
        return out
    app_id = str(app.get("id"))
    known.add(app_id)
    verify = spec.meta.get("verify")
    replay: dict[str, Any] | None = None
    if verify == "running":
        app = cluster.wait_app(app_id, states={"RUNNING", "FAILED", "FINISHED", "KILLED"}, timeout=300)
        # 应用 RUNNING 后再发数据：earliest/latest 两种 offset 策略都能消费到；
        # 先等一会儿，让 KafkaSource 完成分区发现/订阅，避免 latest 跳过刚发的数据。
        mode = spec.meta.get("producer")
        events = (producer_info or {}).get("events") or []
        if app and app.get("state") == "RUNNING" and mode in ("events", "topn") and events:
            try:
                from tests.system_eval import producer as producer_mod
                time.sleep(10)
                producer_mod.send_events(producer_info.get("topic") or "", events)
                replay = {"ok": True, "count": len(events)}
            except Exception as exc:  # noqa: BLE001
                replay = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
            rec.write_json("producer_replay.json", replay)
        # 给 Flink 消费/窗口 + checkpoint 留时间
        # Flink 应用从 RUNNING 到 Kafka 消费/窗口输出还有启动时间；B2 给 120s，B3 给 180s
        time.sleep(120 if spec.meta.get("producer") == "events" else 180)
        cluster.kill_app(app_id)
        app = cluster.wait_app(app_id, states={"FINISHED", "FAILED", "KILLED"}, timeout=180) or app
    else:
        app = cluster.wait_app(
            app_id, states={"FINISHED", "FAILED", "KILLED"},
            timeout=spec.timeout_s,
        ) or app

    log_text = cluster.yarn_logs(app_id)
    (rec.dir / "cluster.log").write_text(log_text[-2_000_000:], encoding="utf-8")
    checks = _check_log(spec, log_text, producer_info)
    out = {
        "app_id": app_id,
        "app_name": app.get("name"),
        "state": app.get("state"),
        "final_status": app.get("finalStatus"),
        "log_excerpt": log_text[-12000:],
        "checks": checks,
        "producer_replay": replay,
        "pass": bool(checks) and all(c.get("ok") for c in checks),
    }
    rec.write_json("cluster.json", out)
    return out


def _check_log(spec: CaseSpec, log_text: str, producer_info: dict[str, Any] | None) -> list[dict[str, Any]]:
    text = log_text or ""
    normalized = re.sub(r"[ \t]+", " ", text)
    checks: list[dict[str, Any]] = []
    expected = spec.meta.get("expected") or {}
    if spec.id == "A1":
        for date, count in (expected.get("daily_counts") or {}).items():
            needle = f"dt={date}"
            checks.append({
                "name": f"{date} count={count}", "ok": needle in text and str(count) in text,
                "evidence": needle,
            })
    elif spec.id == "A3":
        for date, uv in (expected.get("daily_uv") or {}).items():
            for key, value in (uv or {}).items():
                checks.append({
                    "name": f"{date} {key}_uv={value}",
                    "ok": f"dt={date}" in text and str(value) in text,
                    "evidence": f"dt={date}",
                })
    elif spec.id == "B2":
        exp = ((producer_info or {}).get("expected") or {})
        for event, pv in (exp.get("pv") or {}).items():
            uv = (exp.get("uv") or {}).get(event)
            pattern = rf"event[=:]\s*{re.escape(event)}.*pv[=:]\s*{pv}.*uv[=:]\s*{uv}"
            checks.append({
                "name": f"event={event} pv={pv} uv={uv}",
                "ok": bool(re.search(pattern, normalized, re.I | re.S)),
                "evidence": f"event={event}",
            })
    elif spec.id == "B3":
        exp = (producer_info or {}).get("expected") or []
        for window in exp:
            for rank, item in enumerate(window.get("top3") or [], 1):
                c = item.get("category")
                v = item.get("sales")
                pattern = rf"rank[=:]\s*{rank}.*category[=:]\s*{re.escape(str(c))}.*sales[=:]\s*{v}"
                checks.append({
                    "name": f"window {window.get('window')} rank={rank} {c}={v}",
                    "ok": bool(re.search(pattern, normalized, re.I | re.S)),
                    "evidence": f"category={c} sales={v}",
                })
    elif spec.id == "C2":
        needle = str(expected.get("fixed_output") or "TOTAL_AMOUNT=60")
        checks.append({"name": needle, "ok": needle in text, "evidence": needle})
    else:
        checks.append({"name": "app final state", "ok": True, "evidence": "no log-specific check"})
    return checks


# ---------------------------------------------------------------------------
# ops case
# ---------------------------------------------------------------------------

def _ops_snapshot(app_id: str, session_id: str | None = None) -> dict[str, Any]:
    snap: dict[str, Any] = {
        "checker": _db_query(
            "select id, app_id, app_type, status, check_type, error_type, check_describe, "
            "checker_error, repeat_count, is_solved, check_time, create_time, update_time "
            "from log.checker_log where app_id=%s order by id", (app_id,),
        ),
        "runs": _db_query(
            "select id, app_id, session_id, app_type, status, error_count, started_at, "
            "finished_at, out_msg, error from log.agent_runs where app_id=%s order by id",
            (app_id,),
        ),
        "leases": _db_query(
            "select app_id, session_id, acquired_at, heartbeat_at from public.app_leases where app_id=%s",
            (app_id,),
        ),
    }
    if session_id:
        snap["tool_traces"] = _db_query(
            "select agent, tool, status, duration_ms, started_at, args, output, error "
            "from log.tool_trace where thread_id=%s order by started_at", (f"ops:{session_id}",),
        )
    return jsonable(snap)


def _snapshot_hash(snap: dict[str, Any]) -> str:
    raw = json.dumps(snap, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _handle_dev_interrupt(
    api: ApiClient, spec: CaseSpec, session_id: str, interrupt: Any,
    rec: CaseRecorder, state: InterruptState,
) -> dict[str, Any]:
    value = _interrupt_value(interrupt)
    answer = _answer_for(value, spec, state)
    started = time.perf_counter()
    entry = {
        "thread": "ops:" + session_id,
        "interrupt": jsonable(interrupt), "value": jsonable(value),
        "answer": jsonable(answer),
    }
    try:
        data = api.ops_handover_resume(session_id, answer, timeout=spec.timeout_s)
        entry["response"] = jsonable(data)
    except Exception as exc:  # noqa: BLE001
        entry["error"] = f"{type(exc).__name__}: {exc}"
        data = {}
    entry["duration_ms"] = round((time.perf_counter() - started) * 1000, 1)
    state.resumes.append(entry)
    rec.append_jsonl("handover_resume.jsonl", entry)
    return entry


def _prepare_c1(cluster: Cluster, ctx: dict[str, Any], spec: CaseSpec, rec: CaseRecorder) -> dict[str, Any]:
    started = int(time.time() * 1000)
    out = cluster.submit_c1_oom()
    (rec.dir / "fault_submit.log").write_text(out, encoding="utf-8")
    app = _find_new_app(cluster, app_name="spark-fault-oom_limit", project_name=None,
                       started_ms=started, known=_known_app_ids(ctx), timeout=120)
    if app is None:
        raise RuntimeError("C1 提交后找不到 spark-fault-oom_limit 应用")
    _known_app_ids(ctx).add(app["id"])
    return {"app_id": app["id"], "app_name": app.get("name"), "submit_output": out[-4000:]}


def _prepare_c2(cluster: Cluster, ctx: dict[str, Any], spec: CaseSpec, rec: CaseRecorder) -> dict[str, Any]:
    app_name = str(spec.meta.get("app_name") or "eval-c2-spark-format")
    fixture = ROOT / "tests" / "system_eval" / str(spec.meta.get("fixture") or "fixtures/spark_fmt")
    date = datetime.now().strftime("%Y-%m-%d")
    source_dir = f"hdfs:///app/target/spark/{date}/{app_name}"
    cluster.hdfs_put_dir(fixture, source_dir.replace("hdfs://", ""))
    input_local = rec.dir / "input.csv"
    input_local.write_text("user,amount\nA,10\nB,20\nC,N/A\nD,30\n", encoding="utf-8")
    cluster.hdfs_put_file(input_local, "/app/eval/c2/input.csv")
    main_local = fixture / "src" / "main" / "python" / "faulty_etl.py"
    cluster.hdfs_put_file(main_local, "/app/eval/c2/faulty_etl.py")
    commit_cmd = (
        f"spark-submit --master yarn --deploy-mode cluster --name {app_name} "
        f"hdfs:///app/eval/c2/faulty_etl.py"
    )
    _upsert_app_file("SPARK", app_name, source_dir, commit_cmd)
    out = cluster.spark_submit_hdfs(
        "hdfs:///app/eval/c2/faulty_etl.py", app_name,
        executor_memory="512m", driver_memory="768m", num_executors=1,
    )
    (rec.dir / "fault_submit.log").write_text(out, encoding="utf-8")
    app = _find_new_app(cluster, app_name=app_name, project_name=None,
                        started_ms=int(time.time() * 1000) - 120_000,
                        known=_known_app_ids(ctx), timeout=180)
    if app is None:
        raise RuntimeError(f"C2 提交后找不到应用 {app_name}")
    _known_app_ids(ctx).add(app["id"])
    return {
        "app_id": app["id"], "app_name": app_name, "source_dir": source_dir,
        "input": "hdfs:///app/eval/c2/input.csv", "submit_output": out[-4000:],
    }


def _build_flink_fixture() -> Path:
    fixture = ROOT / "tests" / "system_eval" / "fixtures" / "flink_fmt"
    mvn = ROOT / "Maven" / "mvn-3.9.9" / "bin" / "mvn.cmd"
    argv = ["cmd", "/c", str(mvn), "-q", "-DskipTests", "package"] if mvn.exists() else [
        "mvn", "-q", "-DskipTests", "package",
    ]
    proc = subprocess.run(argv, cwd=str(fixture), capture_output=True, text=True, timeout=600,
                          encoding="utf-8", errors="replace")
    if proc.returncode != 0:
        raise RuntimeError(f"Flink fixture 构建失败: {proc.stdout[-1000:]} {proc.stderr[-2000:]}")
    jars = sorted((fixture / "target").glob("*.jar"))
    if not jars:
        raise RuntimeError("Flink fixture 构建后没有 jar")
    return jars[-1]


def _prepare_c3(cluster: Cluster, ctx: dict[str, Any], spec: CaseSpec, rec: CaseRecorder) -> dict[str, Any]:
    from tests.system_eval import producer as producer_mod
    app_name = str(spec.meta.get("app_name") or "eval-c3-flink-format")
    fixture = ROOT / "tests" / "system_eval" / "fixtures" / "flink_fmt"
    date = datetime.now().strftime("%Y-%m-%d")
    source_dir = f"hdfs:///app/target/flink/{date}/{app_name}"
    # 只上传源码，不带 target（构建产物由运维源码索引跳过）
    stage = rec.dir / "source_upload"
    if stage.exists():
        shutil.rmtree(stage, ignore_errors=True)
    (stage / "src").mkdir(parents=True, exist_ok=True)
    shutil.copy2(fixture / "pom.xml", stage / "pom.xml")
    if (fixture / "README.md").exists():
        shutil.copy2(fixture / "README.md", stage / "README.md")
    shutil.copytree(fixture / "src", stage / "src", dirs_exist_ok=True)
    cluster.hdfs_put_dir(stage, source_dir.replace("hdfs://", ""))
    jar = _build_flink_fixture()
    jar_remote = f"/app/flink/jar/{app_name}.jar"
    cluster.hdfs_put_file(jar, jar_remote)
    topic = _topics(ctx["run_id"])["bad_topic"]
    producer_info = producer_mod.produce_bad_events(topic)
    rec.write_json("producer.json", producer_info)
    commit_cmd = (
        f"flink run-application -t yarn-application -c com.fm.EventParseJob "
        f"hdfs:///app/flink/jar/{app_name}.jar --topic {topic} --bootstrap node1:9092"
    )
    _upsert_app_file("FLINK", app_name, source_dir, commit_cmd)
    out = cluster.flink_submit_hdfs(
        f"hdfs:///app/flink/jar/{app_name}.jar", app_name, "com.fm.EventParseJob",
        parallelism=1, jobmanager_memory=768, taskmanager_memory=1024, slots=1,
        args=["--topic", topic, "--bootstrap", "node1:9092",
              "--group", f"eval-c3-{ctx['run_id']}"],
    )
    (rec.dir / "fault_submit.log").write_text(out, encoding="utf-8")
    app = _find_new_app(cluster, app_name=app_name, project_name=None,
                        started_ms=int(time.time() * 1000) - 120_000,
                        known=_known_app_ids(ctx), timeout=180)
    if app is None:
        raise RuntimeError(f"C3 提交后找不到应用 {app_name}")
    _known_app_ids(ctx).add(app["id"])
    return {
        "app_id": app["id"], "app_name": app_name, "source_dir": source_dir,
        "topic": topic, "producer": producer_info, "submit_output": out[-4000:],
    }


def _cleanup_ops_case(app_ids: list[str], rec: CaseRecorder, cluster: Cluster) -> dict[str, Any]:
    cleanup: dict[str, Any] = {"killed": [], "pending_done": 0, "leases_deleted": 0}
    for app_id in app_ids:
        try:
            _redis_queue_remove(app_id)
            app = cluster.app_by_id(app_id) if cluster else None
            if app and app.get("state") in ("RUNNING", "ACCEPTED", "NEW"):
                cluster.kill_app(app_id)
                cleanup["killed"].append(app_id)
        except Exception:
            pass
        try:
            cleanup["pending_done"] += _db_execute(
                "update log.checker_log set status='DONE', is_solved=true "
                "where app_id=%s and status in ('PENDING','PROCESSING')", (app_id,)
            )
            cleanup["leases_deleted"] += _db_execute(
                "delete from public.app_leases where app_id=%s", (app_id,)
            )
        except Exception:
            pass
    return cleanup


def run_ops_case(spec: CaseSpec, ctx: dict[str, Any]) -> dict[str, Any]:
    api: ApiClient = ctx["api"]
    cluster: Cluster = ctx["cluster"]
    run_dir: Path = ctx["run_dir"]
    case_dir = run_dir / "cases" / spec.id
    rec = CaseRecorder(case_dir)
    rec._cluster = cluster  # type: ignore[attr-defined]
    (case_dir / "prompt.md").write_text(spec.prompt_template, encoding="utf-8")
    rec.write_json("case.json", {
        "id": spec.id, "stage": spec.stage, "difficulty": spec.meta.get("difficulty"),
        "fault": spec.meta.get("fault"), "submit": spec.submit_policy,
        "expected": spec.meta.get("expected"), "prompt": spec.prompt_template,
        "topics": _topics(ctx["run_id"]),
    })

    result: dict[str, Any] = {
        "case": spec.id, "fault": spec.meta.get("fault"), "submit": None,
        "ops": None, "cluster": None, "status": "completed",
    }
    started = time.perf_counter()
    app_ids: list[str] = []
    try:
        fault = spec.meta.get("fault")
        if fault == "c1":
            submit = _prepare_c1(cluster, ctx, spec, rec)
        elif fault == "c2":
            submit = _prepare_c2(cluster, ctx, spec, rec)
        elif fault == "c3":
            submit = _prepare_c3(cluster, ctx, spec, rec)
        else:
            raise RuntimeError(f"未知 fault: {fault}")
        result["submit"] = submit
        app_ids.append(submit["app_id"])
        rec.write_json("submit.json", submit)

        # 等 checker 行 / 运维 run
        deadline = time.monotonic() + spec.timeout_s
        session_id: str | None = None
        last_hash = ""
        ops_history: dict[str, Any] = {}
        handover_history: dict[str, Any] = {}
        new_apps: list[dict[str, Any]] = []
        dev_started_ms = int(time.time() * 1000)
        dev_state: InterruptState = InterruptState()

        while time.monotonic() < deadline:
            snap = _ops_snapshot(submit["app_id"], session_id)
            h = _snapshot_hash(snap)
            if h != last_hash:
                last_hash = h
                rec.append_jsonl("ops_timeline.jsonl", {
                    "ms": round((time.perf_counter() - started) * 1000, 1), **snap,
                })
            runs = snap.get("runs") or []
            if runs and not session_id:
                session_id = str(runs[0].get("session_id"))
                reneval = _ops_snapshot(submit["app_id"], session_id)
                rec.append_jsonl("ops_timeline.jsonl", {"ms": round((time.perf_counter() - started) * 1000, 1), **reneval})
            if session_id:
                try:
                    ops_history = api.ops_agent_history(session_id)
                    rec.write_json("ops_history.json", ops_history)
                except Exception:
                    pass
                if ops_history.get("interrupted"):
                    value = _interrupt_value(ops_history.get("interrupt"))
                    if isinstance(value, dict) and value.get("kind") == "job_restart":
                        entry = {"interrupt": ops_history.get("interrupt"),
                                 "answer": {"type": "no", "msg": "system_eval：只记录建议，不重启"}}
                        try:
                            entry["response"] = api.ops_resume(
                                session_id, entry["answer"], timeout=spec.timeout_s)
                        except Exception as exc:  # noqa: BLE001
                            entry["error"] = f"{type(exc).__name__}: {exc}"
                        rec.append_jsonl("ops_resume.jsonl", entry)
                    else:
                        rec.append_jsonl("ops_unhandled_interrupt.jsonl", ops_history)
                if ops_history.get("handover") or handover_history:
                    try:
                        handover_history = api.ops_handover_history(session_id)
                        rec.write_json("handover_history.json", handover_history)
                    except Exception:
                        pass
                    if handover_history.get("interrupted"):
                        _handle_dev_interrupt(
                            api, spec, session_id, handover_history.get("interrupt"),
                            rec, dev_state,
                        )
                    elif handover_history.get("metadata"):
                        # 开发流程已经 work_end；提交确认 yes 时在 verify 后扫新应用
                        if spec.id == "C2" and dev_state.submit_answered:
                            try:
                                app = _find_new_app(
                                    cluster, app_name=None, project_name=None,
                                    started_ms=dev_started_ms, known=_known_app_ids(ctx),
                                    timeout=30,
                                )
                                if app:
                                    new_apps.append(app)
                                    _known_app_ids(ctx).add(app["id"])
                            except Exception:
                                pass

            # 同一个 app 可能因为错误没被标记 DONE 而被反复 lease；只认第一次
            # 有实际结果的终态，避免把重复 handover 当成还没结束。
            terminal = next(
                (r for r in runs if str(r.get("status")) in ("DONE", "ERROR", "ABANDONED")
                 and str(r.get("out_msg") or "").strip()),
                None,
            )
            if terminal is not None:
                if session_id != str(terminal.get("session_id")):
                    session_id = str(terminal.get("session_id"))
                break
            if runs and str(runs[0].get("status")) in ("ERROR", "ABANDONED"):
                break
            time.sleep(5)

        # 最终再抓一次历史 / 轨迹
        if session_id:
            try:
                ops_history = api.ops_agent_history(session_id)
                rec.write_json("ops_history.json", ops_history)
            except Exception:
                pass
            try:
                handover_history = api.ops_handover_history(session_id)
                rec.write_json("handover_history.json", handover_history)
            except Exception:
                pass
        final_snap = _ops_snapshot(submit["app_id"], session_id)
        rec.write_json("ops_snapshot.json", final_snap)

        # C2：等待修复后新应用完成并核日志
        if spec.id == "C2" and new_apps:
            app = new_apps[-1]
            app_id = str(app.get("id"))
            app_ids.append(app_id)
            final = cluster.wait_app(
                app_id, states={"FINISHED", "FAILED", "KILLED"}, timeout=900) or app
            logs = cluster.yarn_logs(app_id)
            (rec.dir / "fixed_app.log").write_text(logs[-2_000_000:], encoding="utf-8")
            checks = _check_log(spec, logs, None)
            result["cluster"] = {
                "app_id": app_id, "app_name": app.get("name"),
                "state": final.get("state"), "final_status": final.get("finalStatus"),
                "checks": checks, "pass": all(c.get("ok") for c in checks),
                "log_excerpt": logs[-12000:],
            }
            rec.write_json("cluster.json", result["cluster"])
        else:
            result["cluster"] = {"skipped": True}

        # 杀掉原始故障应用；记录最终 checker/run 状态
        try:
            if cluster.app_by_id(submit["app_id"]):
                cluster.kill_app(submit["app_id"])
        except Exception:
            pass
        result["ops"] = {
            "session_id": session_id,
            "history": jsonable(ops_history),
            "handover_history": jsonable(handover_history),
            "snapshot": final_snap,
            "resumes": jsonable(dev_state.resumes),
            "handover": bool(handover_history or (ops_history or {}).get("handover")),
        }
        rec.write_json("ops.json", result["ops"])

    except Exception as exc:  # noqa: BLE001
        result["error"] = f"{type(exc).__name__}: {exc}"
        result["status"] = "failed"
    finally:
        cleanup = _cleanup_ops_case(app_ids, rec, cluster)
        result["cleanup"] = cleanup
        rec.write_json("cleanup.json", cleanup)

    result["duration_ms"] = round((time.perf_counter() - started) * 1000, 1)
    rec.final(result, timeline={"status": result["status"], "stage": "ops"})
    from tests.system_eval.rubric import score_ops_case
    rubric = score_ops_case(spec, case_dir)
    rec.write_json("rubric.json", rubric)
    rec.write_json("outcome.json", {"id": spec.id, "status": result["status"],
                                    "duration_ms": result["duration_ms"],
                                    "rubric": rubric.get("total")})
    return result
