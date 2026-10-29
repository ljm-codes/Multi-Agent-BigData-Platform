"""
后台任务消息总线（两级线程模型）

数据：pending[thread_id] 存 TaskOutcome，一个线程的结果不会堵另一个线程。
唤醒：全局 asyncio.Event 只负责叫醒 perceptron，不携带 payload。
占用：occupancy[thread_id] = active / paused；由运行入口维护，perceptron 只读。

两个消费方：
  * PerceptionBackground（middleware）：CVA 模型调用时，按当前 thread_id + task_id 取自己的结果；
  * QueuePerceptron：thread 空闲时兜底处理 pending，交给回调启动后台续跑/通知。
"""
from __future__ import annotations

import asyncio
import json
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, Literal

from langchain_core.messages import AIMessage

from SPO.main_agent_state_model import TaskOutcome
from Tools import LogSetting

logger = LogSetting.create(__name__)

Occupancy = Literal["active", "paused"]
BgTaskStatus = Literal["registered", "running", "completed", "interrupted", "failed"]
# “临时锁”：只有这两个状态会阻止同一个 thread 开新任务；消费成功后自动解锁
_LOCKED_STATUS: tuple[BgTaskStatus, ...] = ("registered", "running")

# 后台结果状态 → registry 记录状态；中间件和 perceptron handler 共用
REGISTRY_STATUS: dict[str, BgTaskStatus] = {
    "background_completed": "completed",
    "background_timeout": "interrupted",
    "background_error": "failed",
}


def background_thread_id(main_thread_id: str, task_id: str) -> str:
    """后台任务独立 thread_id；主线程和后台任务各自持有自己的 checkpoint"""
    return f"{main_thread_id}:bg:{task_id}"


def _to_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return str(value)


def outcome_notice(outcome: TaskOutcome) -> AIMessage:
    """后台结果转成模型可读的消息；中间件和 perceptron handler 共用，保证格式一致"""
    if outcome.status == "background_completed":
        content = f"后台任务（task_id={outcome.task_id}）已完成，结果为：{_to_text(outcome.result)}"
    elif outcome.status == "background_timeout":
        content = (f"后台任务（task_id={outcome.task_id}）已超时中断：{outcome.message}；"
                   f"中断前的结果为：{_to_text(outcome.partial)}")
    elif outcome.status == "background_error":
        content = (f"后台任务（task_id={outcome.task_id}）执行异常："
                   f"{outcome.error or outcome.message}")
    else:
        content = f"后台任务（task_id={outcome.task_id}）状态：{outcome.status}；{outcome.message}"
    return AIMessage(content=content, name="deep_command:background")


class BackgroundBroker:
    """TaskSupervisor 的 sink 可以直接指向本对象（需要 async put）"""

    def __init__(self):
        self._pending: dict[str, deque[TaskOutcome]] = {}
        self._occupancy: dict[str, Occupancy] = {}
        self._event = asyncio.Event()

    # ---------- producer ----------
    async def put(self, outcome: TaskOutcome) -> None:
        """TaskSupervisor._watch_background 调用：await sink.put(outcome)"""
        thread_id = (outcome.origin or {}).get("thread_id")
        if not thread_id:
            logger.error("后台结果缺少 origin.thread_id，无法路由: task_id=%s", outcome.task_id)
            return
        self.publish(thread_id, outcome)

    def publish(self, thread_id: str, outcome: TaskOutcome) -> None:
        self._pending.setdefault(thread_id, deque()).append(outcome)
        self._event.set()

    # ---------- consumer ----------
    def pending_threads(self) -> list[str]:
        return [tid for tid, dq in self._pending.items() if dq]

    def pending_count(self, thread_id: str) -> int:
        return len(self._pending.get(thread_id) or ())

    def drain(self, thread_id: str, task_id: str | None = None) -> list[TaskOutcome]:
        """
        middleware 用：只取当前线程（可选只取当前 task_id）的结果，其它留在桶里
        """
        dq = self._pending.get(thread_id)
        if not dq:
            return []
        if task_id is None:
            items = list(dq)
            dq.clear()
        else:
            items = [item for item in dq if item.task_id == task_id]
            keep = [item for item in dq if item.task_id != task_id]
            dq.clear()
            dq.extend(keep)
        if not dq:
            self._pending.pop(thread_id, None)
        return items

    def take_all(self, thread_id: str) -> list[TaskOutcome]:
        """perceptron 用：线程空闲时把该线程的 pending 全部取走"""
        return self.drain(thread_id)

    async def wait(self) -> None:
        """等待下一次 publish/occupancy 变化；返回后由调用方重新扫描"""
        await self._event.wait()
        self._event.clear()

    # ---------- occupancy ----------
    def occupancy(self, thread_id: str) -> Occupancy | None:
        return self._occupancy.get(thread_id)

    def is_occupied(self, thread_id: str) -> bool:
        return self._occupancy.get(thread_id) in ("active", "paused")

    def set_active(self, thread_id: str) -> None:
        self._occupancy[thread_id] = "active"
        self._event.set()

    def set_paused(self, thread_id: str) -> None:
        self._occupancy[thread_id] = "paused"
        self._event.set()

    def set_idle(self, thread_id: str) -> None:
        self._occupancy.pop(thread_id, None)
        self._event.set()


class QueuePerceptron:
    """
    线程空闲时兜底消费 pending

    handler(thread_id, outcomes) 由调用方提供：
      * 自动续跑：启动后台图（bg thread），完成时 set_idle，interrupt 时 set_paused；
      * 只通知：写 registry / 推送前端，然后 set_idle。
    perceptron 调用 handler 前会 set_active；**handler 负责最终把状态置为 idle/paused**。
    如果 handler 启动的是异步任务并立即返回，活跃状态会保留，直到该任务自己结束。
    """

    def __init__(
            self,
            broker: BackgroundBroker,
            handler: Callable[[str, list[TaskOutcome]], Awaitable[None]],
    ):
        self._broker = broker
        self._handler = handler
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="queue-perceptron")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def _run(self) -> None:
        while True:
            await self._broker.wait()
            for thread_id in self._broker.pending_threads():
                if self._broker.is_occupied(thread_id):
                    # 正在跑/暂停：保留 pending，等运行结束 set_idle/set_paused 再唤醒
                    continue
                outcomes = self._broker.take_all(thread_id)
                if not outcomes:
                    continue
                self._broker.set_active(thread_id)
                try:
                    await self._handler(thread_id, outcomes)
                except asyncio.CancelledError:
                    self._broker.set_idle(thread_id)
                    raise
                except Exception:
                    logger.exception("queue_perceptron handler 失败 thread_id=%s", thread_id)
                    self._broker.set_idle(thread_id)


@dataclass
class BackgroundTaskRecord:
    task_id: str
    user_id: str
    main_thread_id: str
    bg_thread_id: str
    status: BgTaskStatus = "registered"
    summary: str = ""
    # 后台续跑完成后的输出；前端读记录时直接拿，不用翻图状态
    result: str = ""
    interrupt: Any = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))


class BackgroundTaskRegistry:
    """
    用户 thread 列表的内存态；后续可换 Postgres/Redis
    前端只读记录、状态、摘要和中断，不需要理解 LangGraph 内部 state
    """

    def __init__(self):
        self._records: dict[str, BackgroundTaskRecord] = {}
        # WebSocket 订阅者：register/update 时把最新记录推出去
        self._subscribers: set[asyncio.Queue[BackgroundTaskRecord]] = set()

    def subscribe(self) -> asyncio.Queue[BackgroundTaskRecord]:
        queue: asyncio.Queue[BackgroundTaskRecord] = asyncio.Queue()
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[BackgroundTaskRecord]) -> None:
        self._subscribers.discard(queue)

    def _notify(self, record: BackgroundTaskRecord) -> None:
        for queue in list(self._subscribers):
            queue.put_nowait(record)

    def register(self, *, task_id: str, user_id: str, main_thread_id: str,
                 status: BgTaskStatus = "registered", summary: str = "") -> BackgroundTaskRecord:
        record = BackgroundTaskRecord(
            task_id=task_id,
            user_id=user_id,
            main_thread_id=main_thread_id,
            bg_thread_id=background_thread_id(main_thread_id, task_id),
            status=status,
            summary=summary,
        )
        self._records[task_id] = record
        self._notify(record)
        return record

    def update(self, task_id: str, **fields: Any) -> BackgroundTaskRecord | None:
        record = self._records.get(task_id)
        if record is None:
            return None
        for key, value in fields.items():
            if hasattr(record, key):
                setattr(record, key, value)
        record.updated_at = datetime.now().isoformat(timespec="seconds")
        self._notify(record)
        return record

    def get(self, task_id: str) -> BackgroundTaskRecord | None:
        return self._records.get(task_id)

    def list_for_user(self, user_id: str) -> list[BackgroundTaskRecord]:
        return [record for record in self._records.values() if record.user_id == user_id]

    def list_for_thread(self, main_thread_id: str) -> list[BackgroundTaskRecord]:
        return [record for record in self._records.values() if record.main_thread_id == main_thread_id]

    def locked_records(self, main_thread_id: str) -> list[BackgroundTaskRecord]:
        """还没被消费掉的后台任务：有它们时，同一个 thread 不能开新任务"""
        return [record for record in self.list_for_thread(main_thread_id)
                if record.status in _LOCKED_STATUS]

    def is_locked(self, main_thread_id: str) -> bool:
        return bool(self.locked_records(main_thread_id))

    def remove_thread(self, main_thread_id: str) -> int:
        """删掉某会话的全部后台任务记录，返回删掉的条数（会话被删时调用）"""
        task_ids = [record.task_id for record in self.list_for_thread(main_thread_id)]
        for task_id in task_ids:
            self._records.pop(task_id, None)
        return len(task_ids)


# 进程内单例：主图、CVA middleware、perceptron 共用同一份
background_broker = BackgroundBroker()
background_registry = BackgroundTaskRegistry()
