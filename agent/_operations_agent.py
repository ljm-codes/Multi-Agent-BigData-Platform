import json
import re
import uuid
from datetime import datetime, timezone
from typing import Literal, List, TypedDict, NotRequired

from langchain.agents.middleware import ModelRequest, ModelResponse
from langchain.chat_models import init_chat_model
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage, ToolCall, RemoveMessage, BaseMessage, \
    ToolMessage
from langgraph.prebuilt import ToolNode
from langgraph.types import Command, Send
from langgraph.constants import START, END
from langgraph.graph import StateGraph
from langgraph.runtime import Runtime
from pydantic import BaseModel, Field
from sqlalchemy import select, delete, and_

from SPO.agent_state_model import (OperationsAndReconcileMainState,
                                   OperationsAndReconcileInputState,
                                   OperationsAndReconcileOutputState, ErrorInfo)
from SPO.postgresql_model import CheckLog, app_lease
from Tools import OM as OM_AGENT, LogSetting
from Tools.alarm_sender import send_warning_msg
from Tools.db import SessionLocal, AsyncSessionLocal
from Tools.registry import get_tools, get_priority_tools
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

tool_categories = get_priority_tools(OM_AGENT)
tool_to_category = {
    name: cat for cat, names in tool_categories.items() for name in names
}
maximum_category = max(tool_categories.keys())
_tool_progress_savepoint: dict[str, int] = {}
logger = LogSetting.create(__name__)

with open("./prompt/operations_agent.md", "r", encoding="utf-8") as f:
    operations_rule_manual = f.read()

_operations_system_prompt = f"""
你现在是一个大数据运维助手，现在有一个 调度者（一个整理错误信息的助手） 给你提供对应的 大数据应用或者集群的错误信息。你需要根据这些错误信息，与你的能力，尽量做好运维的工作
下面是对应的运维工作手册：
{operations_rule_manual}
"""


class HandoverMsgModel(TypedDict):
    """当出现代码错误时，或者需要调优代码时，需要填写的参数"""
    # 错误详细描述
    error_describe: str
    # 建议
    suggest: str


class SummaryMsgModel(TypedDict):
    """总结本次操作的结果"""
    # 已解决错误ID列表
    solve_error_ids: List[int]
    # 未解决错误ID列表
    unsolvable_error_ids: NotRequired[List[int]]
    # 应用ID
    app_id: NotRequired[str]
    # 未解决错误时 需要填写的简述
    unsolvable_description: NotRequired[str]


class SendMsgModel(TypedDict):
    """发送消息"""
    # 发送的目的地
    node: Literal["summary", "handover"]
    # 当为 summary 时，必须要填写的参数Model
    summary_info: NotRequired[SummaryMsgModel]
    # 当为 handover 时，必须要填写的参数Model
    handover_info: NotRequired[HandoverMsgModel]


class OperationsAndMaintenanceAgentOutModel(BaseModel):
    nodes: List[SendMsgModel] = Field(
        default_factory=list,
        description="本次要将所处理后的结果发送到指定结果处理节点"
                    "填入的是一个列表，说明一个列表运行有多个元素，每个元素表示一个节点，我们将会并发处理每个你所填入的元素。"
                    "每个元素的node表示要发送到的节点，当你填入了 summary 时，必须要填入 summary_info 参数，"
                    "其中，summary_info 参数中，solve_error_ids 是你已经解决了对应错误的ID列表，"
                    "unsolvable_error_ids 是可选的，是你无法解决的错误ID列表，"
                    "app_id 是可选的，当错误是非集群问题时，就需要填写这个参数，"
                    "unsolvable_description 是可选的，是对你无法解决的错误的简单描述；"
                    "当你填入 handover 时，必须要填入 handover_info 参数，"
                    "其中，handover_info 参数中，error_describe 是错误详细描述，"
                    "suggest 是建议。"
    )


def input_handler(state: OperationsAndReconcileInputState) -> OperationsAndReconcileMainState:
    """
    将 输入状态 整改成 可以给大模型理解的文本
    """
    if state.get("is_cluster", False):
        # 若为集群问题：{错误ID} 来自 {check_type}，级别为 {error_type}，重复次数为 {repeat_count}，错误描述为 {error_describe}
        error_list = state.get("error_list", [])
        error_str = "\n".join(
            [
                f"错误ID: {error['id']} 来自 {error['check_type']}，级别为 {error['error_type']}，重复次数为 {error['repeat_count']}，"
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
            "messages": [check_msg],
            "error_id_list": [error['id'] for error in error_list]
        }
    if not state.get("is_cluster", False):
        # 若为应用问题：{错误ID} 来自 {check_type}，级别为 {error_type}，重复次数为 {repeat_count}，错误描述为 {error_describe}
        error_list = state.get("error_list", [])
        error_str = "\n".join(
            [
                f"错误ID: {error['id']} 来自 {error['check_type']}，级别为 {error['error_type']}，重复次数为 {error['repeat_count']}，"
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
            "messages": [check_msg],
            "error_id_list": [error['id'] for error in error_list]
        }

    return {
        "type": "error",
        "error_desc_text": "输入状态格式错误",
        "messages": []
    }


def filter_msg_handler_node(state: dict) -> Literal["chat_end", "chat"]:
    if state.get("type", "") or state.get("error_desc_text", ""):
        return "chat_end"
    return "chat"


def _extract_text_content(message: BaseMessage) -> str:
    """提取消息文本：兼容 str 与 OpenAI 风格内容块列表（[{'type': 'text', 'text': ...}, ...]）。"""
    content = message.content
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and block.get('type') == 'text':
                parts.append(str(block.get('text', '')))
        return ''.join(parts)
    return ''


def _strip_json_wrappers(text: str) -> str:
    """剥离常见 JSON 包装，返回最内层候选 JSON 文本（失败返回原文）：
    ```json 代码块、首尾引号包裹。复杂注入下模型可能产出包装过的 JSON，
    直接 model_validate_json 会失败，剥一层再试可避免被当作自然语言兜底。
    """
    s = text.strip()
    match = re.match(r'^```(?:json)?\s*(.*?)\s*```$', s, re.S)
    if match:
        s = match.group(1).strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ('"', "'"):
        s = s[1:-1].strip()
    return s


def _parse_route_message(message: BaseMessage) -> OperationsAndMaintenanceAgentOutModel | None:
    """
    将 大模型输出的 AIMessage 解析为 OperationsAndMaintenanceAgentOutModel 类型
    若无法解析 则返回 None
    """
    # 理想情况
    if isinstance(message, AIMessage) and message.tool_calls:
        tool_call = message.tool_calls[0]
        if tool_call['name'] == OperationsAndMaintenanceAgentOutModel.__name__:
            try:
                route = OperationsAndMaintenanceAgentOutModel.model_validate(tool_call["args"])
                return route
            except Exception as e:
                logger.error(f"进行解析时出现了异常{type(e).__name__}: {e}")
                return None
        # 当 大模型调用工具时 进行返回 None
        return None
    # 避免出现大模型 将 消息 写在 content 中，所以需要进行解析
    content = _extract_text_content(message)
    if content.strip():
        try:
            # 若为简单的 JSON 字符串 则直接解析
            route = OperationsAndMaintenanceAgentOutModel.model_validate_json(content)
            return route
        except Exception as e:
            logger.debug(f"进行解析时出现了异常{type(e).__name__}: {e}")
            # 尝试另一种方法：解包装后 再解析
            stripped = _strip_json_wrappers(content)
            if content != stripped.strip():
                try:
                    route = OperationsAndMaintenanceAgentOutModel.model_validate_json(stripped)
                    return route
                except Exception as e:
                    logger.error(f"进行解析时出现了异常{type(e).__name__}: {e}")
                    pass

            match = re.search(r'\{.*}', content, re.S)
            if match:
                try:
                    route = OperationsAndMaintenanceAgentOutModel.model_validate_json(match.group(0))
                    return route
                except Exception as e:
                    logger.error(f"进行解析时出现了异常{type(e).__name__}: {e}")
                    pass
        # 实在没招了，走 告警 吧
        logger.warning(f"无法解析消息 {content} 为 OperationsAndMaintenanceAgentOutModel 类型")
    return None


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
        system_message=SystemMessage(content=_operations_system_prompt),
        tools=operations_tools,
        tool_choice="any",
        state=state,  # type: ignore[arg-type]
        runtime=runtime,
    )
    error_id_list = state.get("error_id_list", [])
    app_id = runtime.context.app_id

    # 底层内部
    async def execute_model(req: ModelRequest) -> ModelResponse:
        mags = ([req.system_message] if req.system_message else []) + req.messages
        tools = [OperationsAndMaintenanceAgentOutModel, *req.tools] if req.tools else None
        model_ = req.model.bind_tools(tools, tool_choice=req.tool_choice) if tools else req.model

        # 它直接返回的是 AIMessage
        response = await model_.ainvoke(mags)

        # 将 大模型输出的 AIMessage 进行解析
        route = _parse_route_message(response) if isinstance(response, BaseMessage) else None
        if route is None:
            if isinstance(response, AIMessage) and response.tool_calls:
                response = response.model_copy(
                    update={"additional_kwargs": {**response.additional_kwargs, 'stream_visible': False}})
                return ModelResponse(result=[response])
            # 无法解析 -> 告警
            fallback = AIMessage(
                content="",
                additional_kwargs={
                    "route": ErrorInfo(
                        type="error",
                        error_from="_parse_route_message",
                        error_info={
                            "error_id": error_id_list,
                            "app_id": app_id,
                            "description": "出现无法解析消息为 OperationsAndMaintenanceAgentOutModel 类型"
                        }
                    )
                }
            )
            return ModelResponse(result=[fallback])

        # 正常处理 结构化信息 将结构化信息存放于 additional_kwargs 中
        route_dict = route.model_dump()
        ai_msg = AIMessage(
            content="",
            additional_kwargs={
                "route": route_dict,
            }
        )

        return ModelResponse(result=[ai_msg])

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


def forward_node(
        state: OperationsAndReconcileMainState
) -> Literal["tool_gateway", "summary", "handover"] | Send | List[Send]:
    last_ai_msg = None
    for msg in reversed(state['messages']):
        if isinstance(msg, AIMessage):
            last_ai_msg = msg
            break
    if not last_ai_msg:
        return Send("chat_end", {"type": "error", "error_desc_text": "forward_node节点异常"})

    route = last_ai_msg.additional_kwargs.get("route")
    # 工具调用
    if route is None:
        if last_ai_msg and last_ai_msg.tool_calls:
            return "tool_gateway"
        return Send("chat_end",
                    {"type": "error", "error_desc_text": "由 operations_chat 解析导致的 forward_node节点异常"})

    # 结束情况
    if route.get("type") == "error" and route.get("error_from") == "_parse_route_message":
        return Send("chat_end", {"type": "error", "error_desc_text": json.dumps(route)})
    nodes = route.get("nodes")  # 理应来说是 List[SendMsgModel]
    if not nodes:
        # 都没有，就是出现了解析异常
        return Send("chat_end", {"type": "error", "error_desc_text": "forward_node节点异常"})
    send_list = []
    vy_type_dict = {node.get("node", ""): node for node in nodes}  # 类型->节点信息 （自动去重，并保留最后一个）
    for node in list(vy_type_dict.values()):
        node_name = node.get("node", "")
        if not node_name:
            return Send("chat_end", {"type": "error", "error_desc_text": json.dumps(route)})
        if node_name == "summary":
            send_list.append(Send(
                "summary",
                node.get("summary_info", {}),
            ))
        if node_name == "handover":
            send_list.append(Send(
                "handover",
                node.get("handover_info", {}),
            ))
    return send_list


def app_summary(state: SummaryMsgModel) -> Literal["chat_end"] | Send:
    if not state:
        return Send("chat_end", {"type": "error", "error_desc_text": "app_solve节点异常"})
    solve_error_ids = state.get("solve_error_ids", [])
    unsolvable_error_ids = state.get("unsolvable_error_ids", [])
    app_id = state.get("app_id", "")
    unsolvable_desc = state.get("unsolvable_description", "")
    if unsolvable_error_ids:
        is_success, msg = send_warning_msg(error_id=unsolvable_error_ids, app_id=app_id, description=unsolvable_desc)
        if not is_success:
            return Send("chat_end", {"type": "error", "error_desc_text": msg})
    try:
        with SessionLocal() as session:
            stmt = select(CheckLog).where(CheckLog.id.in_(solve_error_ids) and CheckLog.app_id == app_id)
            check_logs = session.scalar(stmt).all()
            for check_log in check_logs:
                check_log.is_solved = True
                check_log.status = "DONE"
                check_log.update_time = datetime.now(timezone.utc)

            session.commit()
        return "chat_end"
    except Exception as e:
        return Send("chat_end", {"type": "error", "error_desc_text": f"修改数据库时出错 {type(e).__name__}: {e}"})


async def app_handover(state: HandoverMsgModel) -> Literal["chat_end"] | Send:
    # 暂时不写，因为 对应的开发 Agent 还没实现
    pass


def _annotate_and_sort(tool_calls: list[ToolCall]) -> list[dict]:
    """
    展开 tool_calls 并进行排序
    """
    annotated = [
        {**tc, "_category": tool_to_category.get(tc["name"], -1)}
        for tc in tool_calls
    ]

    def key(tc):
        cat = tc["_category"]
        if cat == -1:
            return float("inf")
        return cat

    return sorted(annotated, key=key, reverse=False)


def _skip_tool_msg_encapsulation(remaining: list[dict], permission: int) -> list[ToolMessage]:
    rejected_tool_calls = []
    for unfinished in remaining:
        for un in unfinished:
            rejected_tool_calls.append(
                ToolMessage(
                    content=f"工具越级调用被拒，请先调用等级为{permission}的工具",
                    name=un.get("name", ""),
                    tool_chall_id=un.get("id", ""),
                    id=f"rejected-{str(uuid.uuid4())}"
                )
            )
    return rejected_tool_calls


def tool_gateway(state: OperationsAndReconcileMainState, runtime: Runtime) -> Command:
    """
    工具网关
    控制 Agent 工具调用的流程
    同一级别允许并行调用， 若不同级别则顺序串行调用
    """
    # 本来打算采用 ToolNode.ainvoke + for 来控制 串行 与 并行
    # 结论：行不通，因为 ToolNode.ainvoke实际返回的是一个list[Command | list[ToolMessage] | dict[str, list[ToolMessage]]]
    #      其中 Command 取决于 工具中间件是否会返回 Command 然而 工具网关 对处理 Command 不方便
    #      除非，手动实现 Langgraph 底层 对于 Command 的处理。
    #      但是即便 不传 Command 也需要解嵌套，复杂度可能为O(n^2)
    # 查阅的代码：
    #       - langgraph._internal._runnable.py 中的 RunnableCallable 类 中的 ainvoke 方法
    #           -> 通过查阅 ToolNode 发现实际只会走 ret = await self.afunc(*args, **kwargs)
    #       - langgraph.prebuilt.tool_node.py 中的 ToolNode 类 中的 _afunc 与 _combine_tool_outputs
    # TODO: 未能实现最大重试次数
    remaining = state.get("remaining_tool_calls") or []
    temp_ids = state.get("temp_ai_ids") or []

    # 初始化
    if not remaining and not temp_ids:
        last_ai_msg = None
        for msg in reversed(state['messages']):
            if isinstance(msg, AIMessage):
                last_ai_msg = msg
                break
        if not last_ai_msg.tool_calls:
            return Command(
                goto="chat"
            )
        remaining = _annotate_and_sort(last_ai_msg.tool_calls)

    # 轮询 调度节点 完毕 -> 无工具可再运行
    if not remaining:
        return Command(
            goto="chat",
            update={
                "messages": [RemoveMessage(id=i) for i in temp_ids],
                "temp_ai_ids": []
            }
        )

    # 调度 工具
    sid = runtime.context.session_id
    old_progress = _tool_progress_savepoint.get(sid, 0)
    next_cat = remaining[0]["_category"]
    if next_cat == -1 and old_progress != maximum_category:
        # 当获取到的工具等级为 -1（即最后执行）且之前所执行的进度不是最大等级时，说明为越级调用，直接拒绝
        rejected_tool_calls = _skip_tool_msg_encapsulation(remaining, maximum_category)
        return Command(
            goto="chat",
            update={
                "messages": [RemoveMessage(id=i) for i in temp_ids] + rejected_tool_calls,
                "temp_ai_ids": []
            }
        )
    elif next_cat - old_progress > 1:
        # 当现有工具等级与之前所执行的进度之差大于1时，说明为越级调用，直接拒绝
        rejected_tool_calls = _skip_tool_msg_encapsulation(remaining, old_progress + 1)
        return Command(
            goto="chat",
            update={
                "messages": [RemoveMessage(id=i) for i in temp_ids] + rejected_tool_calls,
                "temp_ai_ids": []
            }
        )
    # 当下一个状态大于之前的进度 才会更新进度点
    if next_cat > old_progress:
        _tool_progress_savepoint[sid] = next_cat
    calls_this_round = [tc for tc in remaining if tc["_category"] == next_cat]
    rest = [tc for tc in remaining if tc["_category"] != next_cat]

    clean_calls = [
        {k: v for k, v in tc.items() if not k.startswith("_")}
        for tc in calls_this_round
    ]
    temp_ai = AIMessage(content="", tools=clean_calls, id=f"temp-{str(uuid.uuid4())}")

    return Command(
        goto="tool",
        update={
            "messages": [temp_ai],
            "remaining_tool_calls": rest,
            "temp_ai_ids": temp_ids + [temp_ai.id]
        }
    )


async def chat_end(
        state: OperationsAndReconcileMainState,
        runtime: Runtime
) -> OperationsAndReconcileOutputState:
    # 无论如何都要解锁
    _tool_progress_savepoint.pop(runtime.context.session_id, None)
    try:
        async with AsyncSessionLocal() as session:
            stmt = delete(app_lease).where(
                and_(
                    app_lease.app_id == runtime.context.app_id,
                    app_lease.session_id == runtime.context.session_id
                )
            )
            await session.execute(stmt)
    except Exception as e:
        logger.exception(f"在尝试解锁应用占用锁时出错 {type(e).__name__}: {e}")
        return OperationsAndReconcileOutputState(
            type="error",
            out_msg=f"解锁应用占用锁时出错 {type(e).__name__}: {e}"
        )

    if state.get("type", "") == "error":
        return OperationsAndReconcileOutputState(
            type="error",
            out_msg=state.get("error_desc_text", "")
        )
    return OperationsAndReconcileOutputState(
        type="info",
        out_msg=state.get("messages")[-1].content
    )


_builder = StateGraph(
    OperationsAndReconcileMainState,  # type: ignore
    input_schema=OperationsAndReconcileInputState,  # type: ignore
    output_schema=OperationsAndReconcileOutputState,  # type: ignore
)

_builder.add_node("input", input_handler)  # type: ignore[arg-type]
_builder.add_node("chat", operations_chat)  # type: ignore[arg-type]
_builder.add_node("summary", app_summary)  # type: ignore[arg-type]
_builder.add_node("handover", app_handover)  # type: ignore[arg-type]
_builder.add_node("tool_gateway", tool_gateway)  # type: ignore[arg-type]
_builder.add_node("tool", ToolNode(
    tools=operations_tools,
    awrap_tool_call=chain_tool_call_wrappers(operations_middleware)
))
_builder.add_node("chat_end", chat_end)  # type: ignore

_builder.add_edge(START, "input")
_builder.add_conditional_edges(
    "input",
    filter_msg_handler_node,
    ["chat_end", "chat"]
)

_builder.add_conditional_edges(
    "chat",
    forward_node,
    ["tool_gateway", "summary", "handover", "chat_end"]
)
# _builder.add_edge("tool_gateway", "tool")
_builder.add_edge("tool", "tool_gateway")

_builder.add_edge("summary", "chat_end")
_builder.add_edge("handover", "chat_end")
_builder.add_edge("chat_end", END)

operations_agent = _builder.compile()

if __name__ == "__main__":
    graph_png = operations_agent.get_graph(xray=True).draw_mermaid_png()
    # print(graph_png)
    with open("graph.png", "wb") as f:
        f.write(graph_png)
