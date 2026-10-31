"""
后台任务续跑 handler（方案 A）

QueuePerceptron 先 take_all 再调 handler，所以这里直接拿 outcomes 拼主图输入、调用
runner 起图，不再 publish 回桶——同一份 TaskOutcome 只会被消费一次（broker 同步取走）。

runner 约定：
    async def runner(thread_id: str, user_id: str, input_state: dict) -> None
- 用 context=MainStateContext(thread_id=..., user_id=...) 调用主图；
- 必须等到这次图运行结束才返回，否则 occupancy 会提前释放；
- 返回图的结果；结果里带 `__interrupt__` 时本 handler 会置 paused，不放锁。
- input_state = {"input_msg": 后台通知, "deep_task_id": task_id, "is_background": True}。
"""
from __future__ import annotations

from typing import Any, Awaitable, Callable

from SPO.main_agent_state_model import TaskOutcome
from Tasks.background_broker import (
    REGISTRY_STATUS,
    BackgroundBroker,
    BackgroundTaskRegistry,
    QueuePerceptron,
    background_broker,
    background_registry,
    outcome_notice,
)
from Tools.log_settings import LogSetting

logger = LogSetting.create(__name__)

Runner = Callable[[str, str, dict[str, Any]], Awaitable[Any]]


class BackgroundResumeHandler:
    """perceptron 的 handler：消费 outcome → 起图 → 按结果状态解锁"""

    def __init__(
            self,
            runner: Runner,
            *,
            broker: BackgroundBroker = background_broker,
            registry: BackgroundTaskRegistry = background_registry,
    ):
        self._runner = runner
        self._broker = broker
        self._registry = registry

    async def __call__(self, thread_id: str, outcomes: list[TaskOutcome]) -> None:
        paused = False
        try:
            for index, outcome in enumerate(outcomes):
                record = self._registry.get(outcome.task_id)
                user_id = record.user_id if record else ""
                notice = outcome_notice(outcome)
                input_state = {
                    "input_msg": notice.content if isinstance(notice.content, str) else str(notice.content),
                    "deep_task_id": outcome.task_id,
                    "is_background": True,
                }
                try:
                    graph_result = await self._runner(thread_id, user_id, input_state)
                except Exception:  # noqa: BLE001
                    # 不 publish 回桶：避免 perceptron 被立刻叫醒造成重试风暴；记录失败即可
                    logger.exception("后台续跑失败 thread_id=%s task_id=%s", thread_id, outcome.task_id)
                    self._registry.update(outcome.task_id, status="failed")
                    continue

                if isinstance(graph_result, dict) and graph_result.get("__interrupt__"):
                    # 图停在 interrupt：不判完成、不放锁；interrupt 暴露给前端，等 resume
                    self._registry.update(outcome.task_id, interrupt=graph_result["__interrupt__"])
                    paused = True
                    for remaining in outcomes[index + 1:]:
                        self._broker.publish(thread_id, remaining)
                    break

                fields: dict[str, Any] = {}
                if registry_status := REGISTRY_STATUS.get(outcome.status):
                    fields["status"] = registry_status
                if isinstance(graph_result, dict) and (output_msg := graph_result.get("output_msg")):
                    fields["result"] = output_msg
                if fields:
                    self._registry.update(outcome.task_id, **fields)
        finally:
            # perceptron 调 handler 前 set_active；handler 负责最终释放
            if paused:
                self._broker.set_paused(thread_id)
            else:
                self._broker.set_idle(thread_id)


def start_perceptron(
        runner: Runner,
        *,
        broker: BackgroundBroker = background_broker,
        registry: BackgroundTaskRegistry = background_registry,
) -> QueuePerceptron:
    """
    应用启动时调用（需要已运行的事件循环）；返回对象用于 shutdown 时 stop()。
    runner 传入真正的“用主图跑一次后台续跑”函数。
    """
    perceptron = QueuePerceptron(
        broker,
        BackgroundResumeHandler(runner, broker=broker, registry=registry),
    )
    perceptron.start()
    return perceptron
