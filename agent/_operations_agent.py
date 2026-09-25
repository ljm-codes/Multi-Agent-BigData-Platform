from typing import Literal, List

from langchain.agents.middleware import ModelRequest, ModelResponse
from langchain.chat_models import init_chat_model
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langgraph.prebuilt import ToolNode
from langgraph.types import Command, Send
from langgraph.constants import START, END
from langgraph.graph import StateGraph
from langgraph.runtime import Runtime

from SPO.agent_state_model import (OperationsAndReconcileMainState,
                                   OperationsAndReconcileInputState,
                                   OperationsAndReconcileOutputState)
from Tools.operations_agent_tools import AGENT_NAME as OM_AGENT
from Tools.registry import get_tools
from load_config.config import config
from middleware.compose import run_before_model, chain_model_call_wrappers, ComposedModelResult, run_after_model, \
    chain_tool_call_wrappers

model_config = config.get("model")

model = init_chat_model(
    model=model_config.get("name"),
    **model_config.get("params")
)

operations_middleware = []
model_chain = chain_model_call_wrappers(operations_middleware)

operations_tools = get_tools(OM_AGENT)


def input_handler(state: OperationsAndReconcileInputState) -> OperationsAndReconcileMainState:
    """
    将 输入状态 整改成 可以给大模型理解的文本
    """
    if state.get("is_cluster", False):
        # 若为集群问题：{错误ID} 来自 {check_type}，级别为 {error_type}，重复次数为 {repeat_count}，错误描述为 {error_describe}
        error_list = state.get("error_list", [])
        error_str = "\n".join(
            [
                f"{error['id']} 来自 {error['check_type']}，级别为 {error['error_type']}，重复次数为 {error['repeat_count']}，"
                f"错误描述为 {error['error_describe']}"
                for error in error_list
            ]
        )

        nl_text = f"""
        大数据集群出现了问题，以下是问题详情：
        {error_str}
        """
        check_msg = HumanMessage(content=nl_text)
        return {
            "messages": [check_msg]
        }
    if not state.get("is_cluster", False):
        # 若为应用问题：{错误ID} 来自 {check_type}，级别为 {error_type}，重复次数为 {repeat_count}，错误描述为 {error_describe}
        error_list = state.get("error_list", [])
        error_str = "\n".join(
            [
                f"{error['id']} 来自 {error['check_type']}，级别为 {error['error_type']}，重复次数为 {error['repeat_count']}，"
                f"错误描述为 {error['error_describe']}"
                for error in error_list
            ]
        )
        nl_text = f"""
        应用 {state.get("app_id", "")} 出现了问题，他的应用类型为 {state.get("app_type", "")}，以下是问题详情：
        {error_str}
        """

        check_msg = HumanMessage(content=nl_text)
        return {
            "messages": [check_msg]
        }

    return {
        "type": "error",
        "error_desc_text": "输入状态格式错误",
        "messages": []
    }


def filter_msg_handler_node(state: dict) -> Literal["end", "chat"]:
    if state.get("type", "") or state.get("error_desc_text", ""):
        return "end"
    return "chat"


async def operations_chat(state: OperationsAndReconcileMainState, runtime: Runtime) -> List[Command]:
    """
    运维 Agent 主对话节点
    """
    # 处理中间件的 调用前 abefore_agent 方法
    pre_update = await run_before_model(operations_middleware, state, runtime)

    # 用于 方便处理 中间件的 wrap_model_call 方法
    request = ModelRequest(
        model=model,
        messages=[*state["messages"]],
        system_message=SystemMessage(content=""),
        tools=operations_tools,
        state=state,
        runtime=runtime,
    )

    # 底层内部
    async def execute_model(req: ModelRequest) -> ModelResponse:
        mags = ([req.system_message] if req.system_message else []) + req.messages
        tools = [*req.tools] if req.tools else None
        model_ = req.model.bind_tools(tools, tool_choice=req.tool_choice) if tools else req.model

        # 它直接返回的是 AIMessage
        response = await model_.ainvoke(mags)

        return ModelResponse(result=[response])

    if model_chain is not None:
        # 底层构造 A -> B -> C -> ... -> execute_model 调用链
        result = await model_chain(request, execute_model)
    else:
        result = ComposedModelResult(model_response=await execute_model(request))

    commands: list[Command] = [Command(update={"messages": result.model_response.result, **pre_update})]
    commands.extend(result.commands)

    # 处理中间件的 调用后 after_agent 方法
    await run_after_model(operations_middleware, {**state, 'messages': result.model_response.result}, runtime)

    return commands


def forward_node(state: OperationsAndReconcileMainState) -> Literal["tool_gateway", "end"]:
    last_ai_msg = None
    for msg in reversed(state['messages']):
        if isinstance(msg, AIMessage):
            last_ai_msg = msg
            break

    if last_ai_msg and last_ai_msg.tool_calls:
        return "tool_gateway"

    return "end"


def tool_gateway(state: dict) -> Literal["tool", "chat", "end"] | List[Send]:
    pass


_builder = StateGraph(
    OperationsAndReconcileMainState,  # type: ignore
    input_schema=OperationsAndReconcileInputState,  # type: ignore
    output_schema=OperationsAndReconcileOutputState,  # type: ignore
)

_builder.add_node("input", input_handler)
_builder.add_node("chat", operations_chat)
_builder.add_node("tool_gateway", tool_gateway)
_builder.add_node("tool", ToolNode(
    tools=operations_tools,
    awrap_tool_call=chain_tool_call_wrappers(operations_middleware)
))

_builder.add_edge(START, "input")
_builder.add_conditional_edges(
    "input",
    filter_msg_handler_node,
    ["end", "chat"]
)

_builder.add_conditional_edges(
    "chat",
    forward_node,
    ["tool_gateway", "end"]
)
_builder.add_edge("tool_gateway", "tool")
_builder.add_edge("tool", "tool_gateway")

_builder.add_edge("end", END)

operations_agent = _builder.compile()
