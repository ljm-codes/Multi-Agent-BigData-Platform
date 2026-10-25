from dataclasses import replace
from typing import Callable, Awaitable

from langchain.agents.middleware import AgentMiddleware, ModelRequest, ModelResponse, ExtendedModelResponse
from langchain.agents.middleware.types import ResponseT
from langchain_core.messages import AIMessage
from langgraph.typing import ContextT
from typing_extensions import override

from Tasks.background_broker import (
    REGISTRY_STATUS,
    BackgroundBroker,
    background_broker,
    background_registry,
    outcome_notice,
)


def _with_notices(
        response: ModelResponse | AIMessage | ExtendedModelResponse,
        notices: list[AIMessage],
) -> ModelResponse | AIMessage | ExtendedModelResponse:
    """通知放在模型输出之前，保证 state 里最后一条 AIMessage 仍是模型输出"""
    if isinstance(response, ExtendedModelResponse):
        return ExtendedModelResponse(
            model_response=replace(
                response.model_response,
                result=[*notices, *response.model_response.result],
            ),
            command=response.command,
        )
    if isinstance(response, ModelResponse):
        return replace(response, result=[*notices, *response.result])
    return ModelResponse(result=[*notices, response])


class PerceptionBackground(AgentMiddleware):
    """
    感知后台任务的结果

    CVA 每次调用模型前，按当前 thread_id + state.deep_task_id 从 broker 取走
    属于这个后台任务的 TaskOutcome，拼成消息交给本次模型调用；不等待、不阻塞。
    没有 deep_task_id 或没有结果时原样放行。
    """

    def __init__(self, broker: BackgroundBroker | None = None):
        super().__init__()
        self.broker = broker or background_broker

    @override
    async def awrap_model_call(
            self,
            request: ModelRequest[ContextT],
            handler: Callable[[ModelRequest[ContextT]], Awaitable[ModelResponse[ResponseT]]],
    ) -> ModelResponse[ResponseT] | AIMessage | ExtendedModelResponse[ResponseT]:
        deep_task_id = (request.state or {}).get("deep_task_id")
        if not deep_task_id:
            return await handler(request)

        thread_id = request.runtime.context.thread_id
        outcomes = self.broker.drain(thread_id, task_id=deep_task_id)
        if not outcomes:
            return await handler(request)

        notices = [outcome_notice(outcome) for outcome in outcomes]
        try:
            response = await handler(request.override(messages=[*request.messages, *notices]))
        except BaseException:
            # 模型调用失败/被取消：把结果放回桶里，等下次感知；不改 registry 状态
            for outcome in outcomes:
                self.broker.publish(thread_id, outcome)
            raise

        for outcome in outcomes:
            registry_status = REGISTRY_STATUS.get(outcome.status)
            if registry_status:
                background_registry.update(outcome.task_id, status=registry_status)

        return _with_notices(response, notices)
