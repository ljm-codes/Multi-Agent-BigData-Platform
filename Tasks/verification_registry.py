"""
验证阶段进度事件 + 最终报告（内存态）。

过程事件走 WebSocket 实时推送给前端；最终报告随 WorkEndState.metadata 回传。
不落库：验证进度是短生命周期的 UI 状态，工具轨迹才需要 PostgreSQL 持久化。
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Literal

from SPO.main_agent_state_model import TaskOutcome

VerificationPhase = Literal[
    "started", "test_done", "heartbeat", "handed_off",
    "completed", "timeout", "failed",
]

_PHASE_BY_STATUS: dict[str, VerificationPhase] = {
    "completed": "completed",
    "background_completed": "completed",
    "handed_off": "handed_off",
    "timeout": "timeout",
    "background_timeout": "timeout",
    "error": "failed",
    "background_error": "failed",
}

_SUBSCRIBER_QUEUE_SIZE = 200
_MAX_FAILURES = 20
_MAX_OUTPUT_CHARS = 2000


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    try:
        import json
        return json.dumps(value, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return str(value)


def build_stage_metrics(stage: str, outcome: TaskOutcome) -> dict[str, Any]:
    """从 TaskOutcome.result 直接统计，不解析任何文本。"""
    items = outcome.result if isinstance(outcome.result, list) else (
        outcome.partial if isinstance(outcome.partial, list) else []
    )
    passed = 0
    failed = 0
    failures: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        status = item.get("status")
        if status == "passed":
            passed += 1
        elif status in ("failed", "error"):
            failed += 1
            if len(failures) < _MAX_FAILURES:
                failures.append({
                    "name": item.get("name"),
                    "test_file": item.get("test_file"),
                    "status": status,
                    "message": (item.get("message") or "")[:1000],
                    "time": item.get("time"),
                })
    return {
        "stage": stage,
        "task_id": outcome.task_id,
        "status": outcome.status,
        "total": len(items),
        "passed": passed,
        "failed": failed,
        "failures": failures,
    }


class VerificationRegistry:
    """按 thread 保存最新事件/各阶段报告，并广播给 WS 订阅者。"""

    def __init__(self):
        self._latest: dict[str, dict[str, Any]] = {}
        self._reports: dict[str, dict[str, dict[str, Any]]] = {}
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()

    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=_SUBSCRIBER_QUEUE_SIZE)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self._subscribers.discard(queue)

    def publish(self, event: dict[str, Any]) -> None:
        thread_id = event.get("thread_id") or ""
        if thread_id:
            self._latest[thread_id] = event
            report = event.get("report")
            if isinstance(report, dict) and report.get("stage"):
                self._reports.setdefault(thread_id, {})[report["stage"]] = report
        payload = {"type": "verification", "record": event}
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    queue.put_nowait(payload)
                except asyncio.QueueFull:
                    pass

    def get(self, thread_id: str) -> dict[str, Any] | None:
        return self._latest.get(thread_id)

    def get_report(self, thread_id: str) -> dict[str, Any] | None:
        stages = self._reports.get(thread_id) or {}
        if not stages:
            return None
        shallow = stages.get("shallow")
        deep = stages.get("deep")
        if deep and deep.get("status") in ("background_completed", "completed"):
            status = "completed"
        elif deep and deep.get("status") in ("background_timeout", "timeout"):
            status = "timeout"
        elif deep and deep.get("status") in ("background_error", "error"):
            status = "failed"
        elif shallow and shallow.get("status") == "completed":
            status = "completed"
        else:
            status = "running"
        passed = all(
            stage.get("failed", 0) == 0
            for stage in (shallow, deep)
            if isinstance(stage, dict)
        )
        return {
            "thread_id": thread_id,
            "status": status,
            "passed": passed,
            "shallow": shallow,
            "deep": deep,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    def clear(self, thread_id: str) -> None:
        self._latest.pop(thread_id, None)
        self._reports.pop(thread_id, None)


class VerificationReporter:
    """一个 stage 的进度采集器；结果直接来自 TaskOutcome/测试结果。"""

    def __init__(
            self,
            *,
            thread_id: str,
            stage: Literal["shallow", "deep"],
            total: int,
            project_name: str = "",
            app_type: str = "",
            prior: dict[str, Any] | None = None,
    ):
        self.thread_id = thread_id
        self.stage = stage
        self.total = max(0, int(total))
        self.project_name = project_name
        self.app_type = app_type
        self.prior = prior
        self.completed = 0
        self.passed = 0
        self.failed = 0
        self.failures: list[dict[str, Any]] = []
        self.current: dict[str, Any] | None = None
        self.last_output = ""
        self.started_at = time.monotonic()
        self.started_iso = datetime.now(timezone.utc).isoformat()

    def _event(self, phase: VerificationPhase, **extra: Any) -> dict[str, Any]:
        event = {
            "thread_id": self.thread_id,
            "stage": self.stage,
            "phase": phase,
            "project_name": self.project_name,
            "app_type": self.app_type,
            "total": self.total,
            "completed": self.completed,
            "passed": self.passed,
            "failed": self.failed,
            "failures": self.failures[-_MAX_FAILURES:],
            "current": self.current,
            "last_output": self.last_output[-_MAX_OUTPUT_CHARS:],
            "elapsed_s": round(time.monotonic() - self.started_at, 1),
            "started_at": self.started_iso,
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        if self.prior:
            event["prior"] = self.prior
        event.update(extra)
        return event

    def start(self) -> None:
        verification_registry.publish(self._event("started"))

    def result(self, item: dict[str, Any]) -> None:
        """_run_*_test_command 每个 command/用例完成的回调。"""
        self.completed += 1
        # 初始化时 total 可能是命令数，而 result 是按用例回调的；至少保证分母不落后
        if self.completed > self.total:
            self.total = self.completed
        status = item.get("status")
        if status == "passed":
            self.passed += 1
        elif status in ("failed", "error"):
            self.failed += 1
            if len(self.failures) < _MAX_FAILURES:
                self.failures.append({
                    "name": item.get("name"),
                    "test_file": item.get("test_file"),
                    "status": status,
                    "message": (item.get("message") or "")[:1000],
                    "time": item.get("time"),
                })
        self.current = {
            "name": item.get("name"),
            "test_file": item.get("test_file"),
            "status": status,
        }
        verification_registry.publish(self._event("test_done", current=self.current))

    def heartbeat(self, last_output: Any = "") -> None:
        text = _to_text(last_output)
        if text:
            self.last_output = text
        verification_registry.publish(self._event("heartbeat"))

    def finish(self, outcome: TaskOutcome) -> dict[str, Any]:
        report = build_stage_metrics(self.stage, outcome)
        if not report.get("total"):
            # handed_off / timeout 时 TaskOutcome.result 可能为空，用过程中累计的进度兜底
            report.update({
                "total": self.total,
                "passed": self.passed,
                "failed": self.failed,
                "failures": self.failures[-_MAX_FAILURES:],
            })
        phase = _PHASE_BY_STATUS.get(outcome.status, "failed")
        verification_registry.publish(self._event(
            phase,
            report=report,
            task_id=outcome.task_id,
            message=outcome.message,
            error=outcome.error,
        ))
        return report

    def report(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "task_id": "",
            "status": "running",
            "total": self.total,
            "passed": self.passed,
            "failed": self.failed,
            "failures": self.failures[-_MAX_FAILURES:],
        }


def publish_outcome(outcome: TaskOutcome) -> None:
    """后台任务完成/超时/失败时，把最终状态推给验证卡片。"""
    thread_id = (outcome.origin or {}).get("thread_id") or ""
    if not thread_id:
        return
    stage = outcome.ver_status if outcome.ver_status in ("shallow", "deep") else "deep"
    report = build_stage_metrics(stage, outcome)
    event = {
        "thread_id": thread_id,
        "stage": stage,
        "phase": _PHASE_BY_STATUS.get(outcome.status, "failed"),
        "project_name": "",
        "app_type": "",
        "total": report["total"],
        "completed": report["total"],
        "passed": report["passed"],
        "failed": report["failed"],
        "failures": report["failures"],
        "current": None,
        "last_output": "",
        "elapsed_s": None,
        "started_at": None,
        "ts": datetime.now(timezone.utc).isoformat(),
        "report": report,
        "task_id": outcome.task_id,
        "message": outcome.message,
        "error": outcome.error,
    }
    verification_registry.publish(event)


verification_registry = VerificationRegistry()
