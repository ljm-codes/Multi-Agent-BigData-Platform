"""
工具调用轨迹的持久化与实时分发。

设计原则：
    - 内存只保留正在运行（running）的 trace，工具结束后立刻从 _active 移除；
    - 已完成的 trace 落 PostgreSQL log.tool_trace，历史查询直接走库；
    - 数据库不可用/表未建时退化为有界内存队列，只用于本地调试；
    - 本模块只做 UI 投影，完整审计仍在 LangGraph messages 中。
"""
from __future__ import annotations

import asyncio
import json
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from sqlalchemy import delete, select, update, and_
from sqlalchemy.dialects.postgresql import insert as pg_insert

from SPO.postgresql_model import ToolTraceInfo
from Tools.db import AsyncSessionLocal
from Tools.log_settings import LogSetting
from load_config.config import config

logger = LogSetting.create(__name__)

TraceStatus = Literal["running", "success", "error"]

_MAX_OUTPUT_CHARS = 200_000
_RETENTION_DAYS = 30
_FALLBACK_LIMIT = 200
_SUBSCRIBER_QUEUE_SIZE = 200

_pg_conf = (config.get("databases") or {}).get("postgres") or {}
_DB_ENABLED = bool(_pg_conf.get("enabled", True))


def _jsonable(value: Any) -> Any:
    """把工具参数转成 jsonb 可写入的对象；不可序列化时退化为字符串。"""
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return json.loads(json.dumps(value, ensure_ascii=False, default=str))


def _to_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return str(value)


@dataclass(slots=True)
class ToolTrace:
    trace_id: str
    thread_id: str
    agent: str
    tool: str
    args: dict[str, Any] = field(default_factory=dict)
    output: str | None = None
    output_truncated: bool = False
    status: TraceStatus = "running"
    error: str | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime | None = None
    duration_ms: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "thread_id": self.thread_id,
            "agent": self.agent,
            "tool": self.tool,
            "args": self.args,
            "output": self.output,
            "output_truncated": self.output_truncated,
            "status": self.status,
            "error": self.error,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "duration_ms": self.duration_ms,
        }


class ToolTraceStore:
    """PostgreSQL 持久化 + 内存 active 表 + WebSocket 订阅者。"""

    def __init__(self):
        self._active: dict[tuple[str, str], ToolTrace] = {}
        self._fallback: deque[ToolTrace] = deque(maxlen=_FALLBACK_LIMIT)
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self._db_enabled = _DB_ENABLED
        self._db_error_logged = False

    # ---------- live subscribers ----------
    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=_SUBSCRIBER_QUEUE_SIZE)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self._subscribers.discard(queue)

    def _notify(self, trace: ToolTrace) -> None:
        payload = {"type": "tool_trace", "record": trace.to_dict()}
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                # 慢消费者：丢最旧的一条，保证最新状态能到
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    queue.put_nowait(payload)
                except asyncio.QueueFull:
                    pass

    # ---------- producer ----------
    async def start(
            self,
            *,
            thread_id: str,
            agent: str,
            tool: str,
            trace_id: str | None = None,
            args: Any = None,
    ) -> ToolTrace:
        trace = ToolTrace(
            trace_id=str(trace_id) if trace_id else uuid.uuid4().hex,
            thread_id=str(thread_id or ""),
            agent=agent,
            tool=tool,
            args=_jsonable(args if args is not None else {}),
        )
        if not isinstance(trace.args, dict):
            trace.args = {"args": trace.args}
        self._active[(trace.thread_id, trace.trace_id)] = trace
        await self._insert(trace)
        self._notify(trace)
        return trace

    async def finish(
            self,
            trace: ToolTrace | None,
            *,
            output: Any = None,
            status: TraceStatus = "success",
            error: str | None = None,
    ) -> ToolTrace | None:
        if trace is None:
            return None
        text = _to_text(output) if output is not None else None
        truncated = bool(text and len(text) > _MAX_OUTPUT_CHARS)
        if truncated:
            text = text[:_MAX_OUTPUT_CHARS] + "\n...[output truncated]"

        trace.output = text
        trace.output_truncated = truncated
        trace.status = status
        trace.error = error
        trace.finished_at = datetime.now(timezone.utc)
        trace.duration_ms = max(
            0, int((trace.finished_at - trace.started_at).total_seconds() * 1000)
        )
        self._active.pop((trace.thread_id, trace.trace_id), None)
        self._fallback.append(trace)
        await self._update(trace)
        self._notify(trace)
        return trace

    # ---------- consumer ----------
    async def list(self, thread_id: str, *, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        offset = max(0, int(offset))
        if self._db_enabled:
            try:
                async with AsyncSessionLocal() as session:
                    stmt = (
                        select(ToolTraceInfo)
                        .where(ToolTraceInfo.thread_id == thread_id)
                        .order_by(ToolTraceInfo.started_at.desc())
                        .limit(limit)
                        .offset(offset)
                    )
                    rows = (await session.scalars(stmt)).all()
                    return [self._row_to_dict(row) for row in rows]
            except Exception as exc:  # noqa: BLE001
                self._log_db_error(exc)

        items = [t for t in self._active.values() if t.thread_id == thread_id]
        items.extend(t for t in reversed(self._fallback) if t.thread_id == thread_id)
        return [t.to_dict() for t in items[offset:offset + limit]]

    async def cleanup(self, days: int = _RETENTION_DAYS) -> int:
        if not self._db_enabled:
            return 0
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    delete(ToolTraceInfo).where(ToolTraceInfo.started_at < cutoff)
                )
                await session.commit()
                return int(result.rowcount or 0)
        except Exception as exc:  # noqa: BLE001
            self._log_db_error(exc)
            return 0

    # ---------- db helpers ----------
    async def _insert(self, trace: ToolTrace) -> None:
        if not self._db_enabled:
            return
        values = {
            "trace_id": trace.trace_id,
            "thread_id": trace.thread_id,
            "agent": trace.agent,
            "tool": trace.tool,
            "args": trace.args,
            "output": None,
            "output_truncated": False,
            "status": "running",
            "error": None,
            "started_at": trace.started_at,
            "finished_at": None,
            "duration_ms": None,
            "created_at": trace.started_at,
        }
        try:
            async with AsyncSessionLocal() as session:
                stmt = pg_insert(ToolTraceInfo).values(**values)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["thread_id", "trace_id"],
                    set_={
                        "agent": trace.agent,
                        "tool": trace.tool,
                        "args": trace.args,
                        "output": None,
                        "output_truncated": False,
                        "status": "running",
                        "error": None,
                        "started_at": trace.started_at,
                        "finished_at": None,
                        "duration_ms": None,
                    },
                )
                await session.execute(stmt)
                await session.commit()
        except Exception as exc:  # noqa: BLE001
            self._log_db_error(exc)

    async def _update(self, trace: ToolTrace) -> None:
        if not self._db_enabled:
            return
        try:
            async with AsyncSessionLocal() as session:
                stmt = (
                    update(ToolTraceInfo)
                    .where(
                        and_(
                            ToolTraceInfo.thread_id == trace.thread_id,
                            ToolTraceInfo.trace_id == trace.trace_id,
                        )
                    )
                    .values(
                        output=trace.output,
                        output_truncated=trace.output_truncated,
                        status=trace.status,
                        error=trace.error,
                        finished_at=trace.finished_at,
                        duration_ms=trace.duration_ms,
                    )
                )
                await session.execute(stmt)
                await session.commit()
        except Exception as exc:  # noqa: BLE001
            self._log_db_error(exc)

    @staticmethod
    def _row_to_dict(row: ToolTraceInfo) -> dict[str, Any]:
        return {
            "trace_id": row.trace_id,
            "thread_id": row.thread_id,
            "agent": row.agent,
            "tool": row.tool,
            "args": row.args or {},
            "output": row.output,
            "output_truncated": row.output_truncated,
            "status": row.status,
            "error": row.error,
            "started_at": row.started_at.isoformat() if row.started_at else None,
            "finished_at": row.finished_at.isoformat() if row.finished_at else None,
            "duration_ms": row.duration_ms,
        }

    def _log_db_error(self, exc: Exception) -> None:
        if self._db_error_logged:
            return
        self._db_error_logged = True
        logger.warning(
            "tool_trace 持久化失败，已退化为内存模式；"
            "请确认 log.tool_trace 表已创建：%s: %s",
            type(exc).__name__, exc,
        )


tool_trace_store = ToolTraceStore()
