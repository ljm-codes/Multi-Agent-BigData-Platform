from datetime import datetime
from pathlib import Path
from typing import cast, List, Literal

from langchain.agents.middleware import ModelRequest, ModelResponse
from langchain.chat_models import init_chat_model
from langchain_core.messages import HumanMessage, SystemMessage, BaseMessage, AIMessage
from langgraph.constants import START, END
from langgraph.graph import StateGraph
from langgraph.runtime import Runtime
from langgraph.types import Command, Send
from pydantic import BaseModel, Field

from SPO.main_agent_state_model import DevelopAgentMainState, DevelopAgentOutputState, DevelopAgentInputState, \
    DevelopGraphErrorState, AgentDevelopInputState, WorkEndState, MainStateContext, CodeVerifyAgentInputState
from Tasks.background_broker import background_registry
from Tools import LogSetting
from Tools.struct_analy import parse_route_message
from agent.code_verify_agent import verify_agent
from agent.data_analysis_agent import big_data_analysis_code_agent, preliminary_verify as preliminary_analysis_verify
from agent.data_develop_agent import big_data_develop_code_agent, preliminary_verify
from load_config.config import config, get_model_params
from middleware.compose import chain_model_call_wrappers, run_before_model, ComposedModelResult, run_after_model

model_config = config.get("model")
logger = LogSetting.create(__name__)

with open(Path(__file__).parent / "prompt" / "main_agent.md", "r", encoding="utf-8") as f:
    _dispatch_system_prompt = f.read()

_dispatch_model = init_chat_model(
    model=model_config.get("name"),
    **get_model_params(),
)

dispatch_middleware = []
dispatch_model_chain = chain_model_call_wrappers(dispatch_middleware)


# Agent 与 app_type 的硬约束：data_develop 只做 Java Flink，data_analysis 只做 Python Spark
_AGENT_APP_TYPE = {"data_develop": "flink", "data_analysis": "spark"}
_APP_TYPE_AGENT = {app_type: agent for agent, app_type in _AGENT_APP_TYPE.items()}


class SendInfo(BaseModel):
    send_to: Literal["data_develop", "data_analysis"] = Field(
        default="data_develop",
        description="要将任务发送给对应的Agent：data_develop（只处理 Flink/Java）、data_analysis（只处理 Spark/Python）"
    )
    app_type: Literal["spark", "flink"] = Field(
        default="flink",
        description="应用类型；data_develop 必须填 flink，data_analysis 必须填 spark"
    )
    task_list: List[str] = Field(description="要给对应的Agent发送的任务列表，每个任务是一个字符串")


class DevelopAgentOutState(BaseModel):
    thread_action: Literal["continue", "new_task"] = Field(
        default="continue",
        description="判断这次用户消息是继续当前任务/聊天（continue），还是明显无关的新任务（new_task）。"
                    "只有当前 thread 存在后台任务时才需要特别判断；拿不准填 continue。"
    )
    thread_title: str = Field(
        default="",
        description="当 thread_action=new_task 时，给新会话窗口一个简短标题；否则留空。"
    )
    send_info_list: list[SendInfo] = Field(
        default_factory=list,
        description="当需要派送任务时，进行填写（允许同时发布多个Agent与对应的多个任务）")
    out_to_user: str = Field(
        default="",
        description="要返回给用户的的消息，当这个字段有值时，将会忽略send_info_list；new_task 时可填写说明")


def _locked_background_records(thread_id: str):
    """还没被消费掉的后台任务；有它们时同一个 thread 不能开新任务"""
    return background_registry.locked_records(thread_id)


def _background_status_text(thread_id: str) -> str:
    """只有当前 thread 有后台任务时才返回状态文本；没有就返回空串，dispatch 保持原样"""
    records = _locked_background_records(thread_id)
    if not records:
        return ""
    lines = ["当前 thread 的后台任务（仅在判断是否新任务时参考）："]
    for record in records:
        summary = (record.summary or "").strip()
        lines.append(f"- task_id={record.task_id}, status={record.status}, "
                     f"summary={summary or '（无）'}")
    lines.append("如果用户这次是在继续当前任务、查进度、追问或闲聊，thread_action 填 continue；"
                 "只有明显是无关的新任务才填 new_task，并给 thread_title；拿不准填 continue。")
    return "\n".join(lines)


def develop_msg_handler(state: DevelopAgentInputState) -> DevelopAgentMainState:
    # TODO: 目前仅仅只是将输入的消息添加到消息列表中，
    #    未来将会加上对应的注入风险检测 与 运维Agent 传入的消息进行处理
    background = bool(state.get("is_background"))
    update = {
        "messages": [HumanMessage(content=state["input_msg"])],
        # 普通消息显式写 False，避免上一轮后台回流留下的状态把这次误判成后台续跑
        "is_background": background,
    }
    if background:
        if deep_task_id := state.get("deep_task_id"):
            update["deep_task_id"] = deep_task_id
        if source := state.get("source"):
            update["source"] = source
    return update


def _last_human_text(state: DevelopAgentMainState) -> str:
    for message in reversed(state.get("messages", [])):
        if isinstance(message, HumanMessage):
            return message.content if isinstance(message.content, str) else str(message.content)
    return ""


def _route_after_handler(state: DevelopAgentMainState, runtime: Runtime) -> str | Send:
    """普通消息进 dispatch；后台回流直接进 CVA 子图"""
    if not state.get("is_background"):
        return "dis_chat"

    deep_task_id = state.get("deep_task_id")
    app_type = state.get("app_type")
    date = state.get("date")
    project_name = state.get("project_name")
    if not deep_task_id or not app_type or not date or not project_name:
        return Send(
            "work_end",
            WorkEndState(
                type="error",
                desc="后台续跑缺少任务上下文（deep_task_id/app_type/date/project_name）",
                from_="handler",
                thread_id=runtime.context.thread_id,
            ),
        )

    background_msg = _last_human_text(state)
    return Send(
        "code_verify",
        CodeVerifyAgentInputState(
            source=state.get("source") or "data_develop_agent",
            source_msg=background_msg,
            background_msg=background_msg,
            app_type=app_type,
            project_name=project_name,
            date=date,
            original_task_list=state.get("original_task_list") or [],
            deep_task_id=deep_task_id,
        ),
    )


def _parse_route_message(message: BaseMessage) -> DevelopAgentOutState | None:
    """
    将 大模型输出的 AIMessage 解析为 DevelopAgentOutState 类型
    若无法解析 则返回 None
    """
    return parse_route_message(message, DevelopAgentOutState)


async def dispatch_chat(state: DevelopAgentMainState, runtime: Runtime) -> List[Command]:
    dis_update = await run_before_model(dispatch_middleware, state, runtime)

    alloc_error_msg_list: list[BaseMessage] = []
    if allocate_error_info := state.get("error_info"):
        score = allocate_error_info.get("from_")
        alloc_error_msg_list.append(AIMessage(
            content=f"分配错误：{allocate_error_info['desc']}",
            name=score,
        ))

    background_status = _background_status_text(runtime.context.thread_id)
    system_content = (f"{_dispatch_system_prompt}\n\n{background_status}"
                      if background_status else _dispatch_system_prompt)

    dis_request = ModelRequest(
        model=_dispatch_model,
        messages=[*state["messages"], *alloc_error_msg_list],
        system_message=SystemMessage(content=system_content),
        tools=[],
        state=state,  # type: ignore[arg-type]
        runtime=runtime,
    )

    async def dis_execute(dis_req: ModelRequest) -> ModelResponse:
        mags = ([dis_req.system_message] if dis_req.system_message else []) + dis_req.messages
        tools = [DevelopAgentOutState, *dis_req.tools]
        # 主调度没有其它工具时强制走结构化输出，避免模型直接自由文本回答
        tool_choice = dis_req.tool_choice or ("any" if not dis_req.tools else None)
        model_ = dis_req.model.bind_tools(tools, tool_choice=tool_choice)

        dis_res = await model_.ainvoke(mags)
        route = _parse_route_message(dis_res)
        if route is None:
            if isinstance(dis_res, AIMessage) and dis_res.tool_calls:
                response = dis_res.model_copy(
                    update={"additional_kwargs": {**dis_res.additional_kwargs}}
                )
                return ModelResponse(result=[response])
            fallback = AIMessage(
                content="",
                additional_kwargs={
                    "error": DevelopGraphErrorState(
                        from_="dis_execute",
                        thread_id=runtime.context.thread_id,
                        desc=f"无法处理结果返回的消息：{dis_res}",
                        goto_="work_end",
                        original_task_list=[],
                    )
                },
            )
            return ModelResponse(result=[fallback])
        send_dict = route.model_dump()
        out_ai_msg = AIMessage(
            content="",
            additional_kwargs={
                "send": send_dict
            }
        )
        return ModelResponse(result=[out_ai_msg])

    if dispatch_model_chain is not None:
        dis_result = await dispatch_model_chain(dis_request, dis_execute)
    else:
        dis_result = ComposedModelResult(model_response=await dis_execute(dis_request))

    commands: list[Command] = [Command(update={"messages": dis_result.model_response.result, **dis_update})]
    commands.extend(dis_result.commands)

    await run_after_model(dispatch_middleware, {**state, 'messages': dis_result.model_response.result}, runtime)

    return commands


def distribute_task_to_agent(state: DevelopAgentMainState, runtime: Runtime) -> List[Send] | Literal["tool"] | Send:
    last_ai_msg = None
    for msg in reversed(state["messages"]):
        if isinstance(msg, AIMessage):
            last_ai_msg = msg
            break

    if last_ai_msg is None:
        return Send(
            "work_end",
            WorkEndState(
                type="error",
                desc="无法解析结果返回的消息",
                from_="distribute_task_to_agent",
                thread_id=runtime.context.thread_id,
            )
        )
    error_info = last_ai_msg.additional_kwargs.get("error")
    send_info = last_ai_msg.additional_kwargs.get("send") or {}
    # 暂时没有任何工具
    # if last_ai_msg and last_ai_msg.tool_calls:
    #     return "tool"
    if error_info:
        return Send(
            "work_end",
            WorkEndState(
                type="error",
                from_=error_info.get("from_"),
                desc=error_info.get("desc"),
                thread_id=runtime.context.thread_id,
            )
        )

    # 只在当前 thread 有后台任务时才接受 new_task；否则视为 continue
    if send_info.get("thread_action") == "new_task" and _locked_background_records(runtime.context.thread_id):
        request_text = ""
        for message in reversed(state["messages"]):
            if isinstance(message, HumanMessage):
                request_text = message.content
                break
        return Send(
            "work_end",
            WorkEndState(
                type="info",
                from_="dis_chat",
                desc=send_info.get("out_to_user") or "检测到新任务，将在新的会话中处理",
                thread_id=runtime.context.thread_id,
                thread_action="new_task",
                thread_title=send_info.get("thread_title", ""),
                request_text=request_text,
            )
        )

    send_info_list = send_info.get("send_info_list", [])
    to_user_msg = send_info.get("out_to_user", "")
    if to_user_msg:
        return Send(
            "work_end",
            WorkEndState(
                type="info",
                from_="dis_chat",
                desc=to_user_msg,
                thread_id=runtime.context.thread_id,
            )
        )
    send_list = []
    send_info_dict = {send_i.get("send_to"): send_i for send_i in send_info_list}
    for send_info in list(send_info_dict.values()):
        send_to = send_info.get("send_to")
        expect_app_type = _AGENT_APP_TYPE.get(send_to)
        app_type = send_info.get("app_type", expect_app_type)
        if expect_app_type is None or app_type != expect_app_type:
            right_agent = _APP_TYPE_AGENT.get(app_type, "对应的 Agent")
            return Send(
                "work_end",
                WorkEndState(
                    type="error",
                    from_="distribute_task_to_agent",
                    desc=(f"任务路由错误：{send_to} 只接受 {expect_app_type or '未知'} 任务，"
                          f"收到的是 {app_type}；{app_type} 任务请交给 {right_agent}。"),
                    thread_id=runtime.context.thread_id,
                )
            )
        task_list = send_info.get("task_list", [])
        send_list.append(
            Send(
                send_to,
                AgentDevelopInputState(
                    is_repair=False,
                    task_list=task_list,
                    task_num=len(task_list),
                    app_type=app_type,
                ),
            )
        )
    if not send_list:
        return Send(
            "work_end",
            WorkEndState(
                type="info",
                from_="dis_chat",
                desc="没有可执行的任务，请补充需求后重试。",
                thread_id=runtime.context.thread_id,
            )
        )
    return send_list


def work_end(state: WorkEndState) -> DevelopAgentOutputState:
    desc_msg = state.get("desc", "")
    metadata_dict = {
        "type": state.get("type"),
        "from_": state.get("from_"),
        "thread_id": state.get("thread_id"),
    }
    if app_type := state.get("app_type"):
        metadata_dict["app_type"] = app_type
    if date := state.get("date"):
        metadata_dict["date"] = date
    if p_name := state.get("project_name"):
        metadata_dict["project_name"] = p_name
    if thread_action := state.get("thread_action"):
        metadata_dict["thread_action"] = thread_action
    if thread_title := state.get("thread_title"):
        metadata_dict["thread_title"] = thread_title
    if request_text := state.get("request_text"):
        metadata_dict["request_text"] = request_text
    return DevelopAgentOutputState(
        output_msg=desc_msg,
        out_time=datetime.now(),
        metadata=metadata_dict,
    )


builder = StateGraph(
    cast("StateT", DevelopAgentMainState),
    context_schema=cast("ContextT", MainStateContext),
    input_schema=cast("InputT", DevelopAgentInputState),
    output_schema=cast("OutputT", DevelopAgentOutputState),
)

builder.add_node("handler", cast("StateNode", develop_msg_handler))
builder.add_node("dis_chat", cast("StateNode", dispatch_chat))
builder.add_node("data_develop", cast("StateNode", big_data_develop_code_agent))
builder.add_node("data_analysis", cast("StateNode", big_data_analysis_code_agent))
# 直接挂编译好的 CVA 子图：子图里的 Command(graph=PARENT) / interrupt 才会生效
builder.add_node("code_verify", cast("StateNode", verify_agent))
builder.add_node("work_end", cast("StateNode", work_end))

builder.add_edge(START, "handler")
builder.add_conditional_edges(
    "handler",
    _route_after_handler,
    ["dis_chat", "code_verify", "work_end"]
)
builder.add_conditional_edges(
    "dis_chat",
    distribute_task_to_agent,
    ["data_develop", "data_analysis", "work_end"]
)
builder.add_conditional_edges(
    "data_develop",
    preliminary_verify,
    ["dis_chat", "data_develop", "code_verify"]
)
builder.add_conditional_edges(
    "data_analysis",
    preliminary_analysis_verify,
    ["work_end", "data_analysis", "code_verify"]
)
builder.add_edge("work_end", END)
