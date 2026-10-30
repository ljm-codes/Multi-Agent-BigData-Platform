from __future__ import annotations

import asyncio
from typing import Callable, Literal, Awaitable, Any, ClassVar

from SPO.main_agent_state_model import TaskOutcome
from Tools.log_settings import LogSetting

OutcomeStatus = Literal[
    "completed",
    "timeout",
    "handed_off",
    "background_completed",
    "background_timeout",
    "error",
    "background_error",
]

TimeoutAction = Literal["cancel", "handoff"]
logger = LogSetting.create(__name__)


class TaskSupervisor:
    """
    异步任务管理器
    - 场景一：action="cancel" , first_timeout=X -> 超时即取消，返回部分结果。
    - 场景二：action="handoff" , first_timeout=X, second_timeout=Y
                -> 第一阶段超时后任务不被取消，移交后台；后台超时后取消。
    """
    _background_tasks: ClassVar[set[asyncio.Task]] = set()

    def __init__(
            self,
            *,
            task_id: str,
            origin: dict[str, Any],
            first_timeout: float,
            action: Literal["cancel", "handoff"] = "cancel",
            second_timeout: float | None = None,
            sink: Any | None = None,
            context: Any = None,
            on_handoff: Callable[[asyncio.Task, Any], Awaitable[None]] | None = None,
    ):
        if first_timeout <= 0:
            raise ValueError("first_timeout 必须 > 0")
        if action == "handoff":
            if second_timeout is None or second_timeout <= 0:
                raise ValueError("handoff 模式必须提供正的 second_timeout")
            if sink is None or not hasattr(sink, "put"):
                raise ValueError("handoff 模式必须提供带 put() 的 sink")

        self.task_id = task_id
        self.origin = origin
        self.first_timeout = first_timeout
        self.action = action
        self.second_timeout = second_timeout
        self.sink = sink
        self.context = context
        self.on_handoff = on_handoff

    """ public """
    async def run(self, coro: Awaitable[Any]) -> TaskOutcome:
        """
        执行协程，并根据配置施加超时策略。
        返回一个 TaskOutcome 实例，描述本次执行的结果状态。
        """
        task = asyncio.create_task(coro, name=f"TaskSupervisor[{self.task_id}]-body")
        if self.action == "cancel":
            return await self._run_cancel_mode(task)
        return await self._run_handoff_mode(task)

    """ cancel mode """
    async def _run_cancel_mode(self, task: asyncio.Task) -> TaskOutcome:
        """场景一执行器"""
        try:
            result = await asyncio.wait_for(task, timeout=self.first_timeout)
            return self._outcome("completed", result=result)
        except asyncio.TimeoutError:
            # wait_for 已取消并等待 task 结束；task 处于 cancelled 状态
            return self._outcome(
                "timeout",
                partial=self._extract_snapshot(),
                message=f"任务超过 {self.first_timeout}s，已中断。",
            )
        except asyncio.CancelledError:
            # 外层（节点）被取消：task 已被 wait_for 取消，继续向上传播
            raise
        except Exception as e:  # noqa: BLE001
            logger.exception("cancel 模式业务异常 task_id=%s", self.task_id)
            return self._outcome("error", error=repr(e), message="任务执行异常。")

    """ handoff mode """
    async def _run_handoff_mode(self, task: asyncio.Task) -> TaskOutcome:
        """场景二执行器"""
        try:
            result = await asyncio.wait_for(
                asyncio.shield(task), timeout=self.first_timeout
            )
            return self._outcome("completed", result=result)
        except asyncio.TimeoutError:
            # shield 保护 task，使其不被取消；此刻 task 仍在运行
            snapshot = self._extract_snapshot()
            if self.on_handoff is not None:
                try:
                    await self.on_handoff(task, self.context)
                except Exception:  # noqa: BLE001
                    logger.exception("on_handoff 失败 task_id=%s", self.task_id)

            watcher = asyncio.create_task(
                self._watch_background(task),
                name=f"TaskSupervisor[{self.task_id}]-watch",
            )
            self._background_tasks.add(watcher)
            watcher.add_done_callback(self._background_tasks.discard)

            return self._outcome(
                "handed_off",
                snapshot=snapshot,
                message=(
                    f"任务超过 {self.first_timeout}s，已转后台继续执行；"
                    f"后台上限 {self.second_timeout}s。task_id={self.task_id}"
                ),
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            logger.exception("handoff 模式业务异常 task_id=%s", self.task_id)
            return self._outcome("error", error=repr(e), message="任务执行异常。")

    async def _watch_background(self, task: asyncio.Task) -> None:
        """监控后台任务"""
        try:
            result = await asyncio.wait_for(
                asyncio.shield(task), timeout=self.second_timeout
            )
            outcome = self._outcome("background_completed", result=result)
        except asyncio.TimeoutError:
            # 后台超过第二时长：这一次允许取消
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001
                logger.exception("后台任务取消时抛异常 task_id=%s", self.task_id)
            outcome = self._outcome(
                "background_timeout",
                partial=self._extract_snapshot(),
                message=f"后台执行超过 {self.second_timeout}s，已中断。",
            )
        except Exception as e:  # noqa: BLE001
            logger.exception("后台任务执行异常 task_id=%s", self.task_id)
            outcome = self._outcome(
                "background_error", error=repr(e), message="后台任务执行异常。"
            )

        try:
            await self.sink.put(outcome)  # type: ignore[union-attr]
        except Exception:  # noqa: BLE001
            logger.exception("sink.put 失败 task_id=%s", self.task_id)

    """ utils """
    def _outcome(self, status: OutcomeStatus, **kwargs: Any) -> TaskOutcome:
        return TaskOutcome(
            task_id=self.task_id, origin=self.origin, status=status, **kwargs
        )

    def _extract_snapshot(self) -> Any:
        ctx = self.context
        if ctx is None:
            return None
        buf = ctx.get("buffer") if isinstance(ctx, dict) else getattr(ctx, "buffer", None)
        if buf is None:
            return None
        snap = getattr(buf, "snapshot", None)
        if callable(snap):
            try:
                return snap()
            except Exception:  # noqa: BLE001
                logger.exception("buffer.snapshot() 失败 task_id=%s", self.task_id)
                return None
        for attr in ("partial", "partial_result"):
            if hasattr(buf, attr):
                return getattr(buf, attr)
        return None
