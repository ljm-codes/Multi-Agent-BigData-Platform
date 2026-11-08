"""用例数据落盘：原始 SSE、时间线、工具调用、验证/后台事件、评分。"""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple, set)):
        return [jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


class CaseRecorder:
    def __init__(self, case_dir: Path):
        self.dir = case_dir
        self.dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.started_perf = time.perf_counter()
        self.started_at = now_iso()
        self.events = 0
        self.tool_names: dict[str, int] = {}
        self.status_events: list[dict[str, Any]] = []
        self.interrupts: list[dict[str, Any]] = []
        self.agent_text: dict[str, list[str]] = {}
        self.agent_times: dict[str, dict[str, float]] = {}
        self.first_token_ms: float | None = None
        self.first_final_ms: float | None = None
        self.last_token_ms: float | None = None
        self.ttfb_ms: float | None = None
        self._seen_verification: set[str] = set()
        self._seen_background: set[str] = set()
        self._seen_traces: set[str] = set()

    # ---------- basic io ----------
    def write_json(self, name: str, data: Any) -> None:
        with self._lock:
            (self.dir / name).write_text(
                json.dumps(jsonable(data), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    def append_jsonl(self, name: str, data: Any) -> None:
        with self._lock:
            with (self.dir / name).open("a", encoding="utf-8") as f:
                f.write(json.dumps(jsonable(data), ensure_ascii=False) + "\n")

    def elapsed_ms(self) -> float:
        return (time.perf_counter() - self.started_perf) * 1000

    # ---------- sse ----------
    def event(self, event: str, data: dict[str, Any], recv_perf: float | None = None) -> None:
        with self._lock:
            self.events += 1
            recv_ms = ((recv_perf or time.perf_counter()) - self.started_perf) * 1000
            if self.ttfb_ms is None:
                self.ttfb_ms = recv_ms
            self.append_jsonl("events.jsonl", {
                "seq": self.events,
                "recv_ms": round(recv_ms, 3),
                "recv_at": now_iso(),
                "event": event,
                "data": data,
            })
            if event == "token":
                content = data.get("content") or ""
                agent = data.get("agent") or ""
                node = data.get("node") or ""
                if content and self.first_token_ms is None:
                    self.first_token_ms = recv_ms
                if content:
                    self.last_token_ms = recv_ms
                if node == "final":
                    if self.first_final_ms is None:
                        self.first_final_ms = recv_ms
                elif agent:
                    self.agent_text.setdefault(agent, []).append(content)
                    times = self.agent_times.setdefault(agent, {"first_ms": recv_ms, "last_ms": recv_ms})
                    times["last_ms"] = recv_ms
            elif event == "status":
                self.status_events.append({"recv_ms": round(recv_ms, 3), **data})
                name = (data.get("text") or "").replace("正在调用工具：", "").strip()
                if name:
                    self.tool_names[name] = self.tool_names.get(name, 0) + 1
            elif event == "interrupt":
                self.interrupts.append({"recv_ms": round(recv_ms, 3), **data})
            elif event == "update":
                if data.get("phase") == "llm_start":
                    self.agent_times.setdefault(data.get("agent") or "", {
                        "first_ms": recv_ms, "last_ms": recv_ms,
                    })

    # ---------- polls ----------
    def verification(self, record: Any) -> None:
        if not isinstance(record, dict):
            return
        key = json.dumps(record, sort_keys=True, ensure_ascii=False, default=str)
        with self._lock:
            if key in self._seen_verification:
                return
            self._seen_verification.add(key)
            self.append_jsonl("verification.jsonl", {"recv_ms": round(self.elapsed_ms(), 3), "record": record})

    def background(self, records: list[dict[str, Any]]) -> None:
        for record in records or []:
            key = json.dumps(record, sort_keys=True, ensure_ascii=False, default=str)
            with self._lock:
                if key in self._seen_background:
                    continue
                self._seen_background.add(key)
                self.append_jsonl("background.jsonl", {"recv_ms": round(self.elapsed_ms(), 3), "record": record})

    def tool_traces(self, traces: list[dict[str, Any]]) -> None:
        for trace in traces or []:
            trace_id = str(trace.get("trace_id") or "")
            key = f"{trace_id}:{trace.get('status')}"
            with self._lock:
                if key in self._seen_traces:
                    continue
                self._seen_traces.add(key)
                self.append_jsonl("tool_traces.jsonl", {"recv_ms": round(self.elapsed_ms(), 3), "record": trace})

    # ---------- finish ----------
    def final(self, result: dict[str, Any], timeline: dict[str, Any] | None = None) -> None:
        timeline = timeline or {}
        total_ms = self.elapsed_ms()
        payload = {
            "started_at": self.started_at,
            "finished_at": now_iso(),
            "total_ms": round(total_ms, 3),
            "ttfb_ms": round(self.ttfb_ms, 3) if self.ttfb_ms is not None else None,
            "first_token_ms": round(self.first_token_ms, 3) if self.first_token_ms is not None else None,
            "first_final_token_ms": round(self.first_final_ms, 3) if self.first_final_ms is not None else None,
            "last_token_ms": round(self.last_token_ms, 3) if self.last_token_ms is not None else None,
            "event_count": self.events,
            "interrupt_count": len(self.interrupts),
            "tool_status": self.tool_names,
            "agent_text": {k: "".join(v) for k, v in self.agent_text.items()},
            "agent_times": self.agent_times,
            **timeline,
        }
        self.write_json("timeline.json", payload)
        self.write_json("result.json", result)
