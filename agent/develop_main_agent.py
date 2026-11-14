import re
from datetime import datetime
from pathlib import Path
from typing import Any, cast, List, Literal
from uuid import uuid4

from langchain.agents.middleware import ModelRequest, ModelResponse
from langchain.chat_models import init_chat_model
from langchain_core.messages import HumanMessage, SystemMessage, BaseMessage, AIMessage
from langgraph.constants import START, END
from langgraph.graph import StateGraph
from langgraph.prebuilt import ToolNode
from langgraph.runtime import Runtime
from langgraph.types import Command, Send
from pydantic import BaseModel, Field

from SPO.main_agent_state_model import DevelopAgentMainState, DevelopAgentOutputState, DevelopAgentInputState, \
    DevelopGraphErrorState, AgentDevelopInputState, AllocateErrorInfo, WorkEndState, MainStateContext, \
    CodeVerifyAgentInputState, agent_label, content_text
from Tasks.background_broker import background_registry
from Tasks.project_registry import admin_id_of, check_project_dir, project_registry
from Tools.log_settings import LogSetting
from Tools.main_agent_tools import AGENT_NAME as MA
from Tools.registry import get_tools
from Tools.struct_analy import (
    extract_text_content, parse_route_message, parse_tool_call,
    repair_invalid_tool_calls, strip_tool_call,
)
from Tools.usage_metrics import merge_raw_usage
from agent.code_verify_agent import verify_agent
from agent.data_analysis_agent import big_data_analysis_code_agent, preliminary_verify as preliminary_analysis_verify
from agent.data_develop_agent import big_data_develop_code_agent, preliminary_verify
from load_config.config import config, get_model_params
from middleware.compose import chain_model_call_wrappers, run_before_model, ComposedModelResult, run_after_model, \
    chain_tool_call_wrappers
from middleware.tool_trace_middleware import ToolTraceMiddleware

model_config = config.get("model")
logger = LogSetting.create(__name__)

with open(Path(__file__).parent / "prompt" / "main_agent.md", "r", encoding="utf-8") as f:
    _dispatch_system_prompt = f.read()

_dispatch_model = init_chat_model(
    model=model_config.get("name"),
    max_tokens=16384,  # 框架默认不传，服务端按 8192 截断工具参数
    **get_model_params(),
)
main_agent_tools = get_tools(MA)

dispatch_middleware = [ToolTraceMiddleware("dis_chat")]
dispatch_model_chain = chain_model_call_wrappers(dispatch_middleware)

# Agent 与 app_type 的硬约束：data_develop 只做 Java Flink，data_analysis 只做 Python Spark
_AGENT_APP_TYPE = {"data_develop": "flink", "data_analysis": "spark"}
_APP_TYPE_AGENT = {app_type: agent for agent, app_type in _AGENT_APP_TYPE.items()}


class SendInfo(BaseModel):
    send_to: Literal["data_develop", "data_analysis", "code_verify"] = Field(
        default="data_develop",
        description="要将任务发送给对应的Agent：data_develop（只处理 Flink/Java）、"
                    "data_analysis（只处理 Spark/Python）、code_verify（只对当前已有项目做重新验证，不写代码）"
    )
    app_type: Literal["spark", "flink"] = Field(
        default="flink",
        description="应用类型；data_develop 必须填 flink，data_analysis 必须填 spark；"
                    "code_verify 填当前项目的类型"
    )
    task_list: List[str] = Field(
        default_factory=list,
        description="要给对应的Agent发送的任务清单，里面记载的是该任务的详细描述，以及你所能想到的一些注意事项；"
                    "code_verify 时写这次要验证的范围")
    is_submit: bool = Field(default=False,
                            description="该任务是否需要提交作业到集群，false 表示不提交作业到集群，true 表示提交作业到集群")


class DevelopAgentOutState(BaseModel):
    thread_action: Literal["continue", "new_task"] = Field(
        default="continue",
        description="判断这次用户消息是继续当前任务/项目（continue），还是明显无关的新任务/新项目（new_task）。"
                    "后台任务由系统管理，不要因为有没有后台任务而改变这个判断；拿不准填 continue。"
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
    # ---- 项目登记：跨会话回到旧项目 / 给新项目留档 ----
    project_id: str | None = Field(
        default=None,
        description="用户要回到/继续某个已登记的历史项目时，填 list_projects 返回的 project_id；"
                    "新项目、不确定、当前会话内的普通继续都留空。不要自己编 project_id。"
    )
    project_title: str = Field(
        default="",
        description="新项目的短标题（仅 thread_action=new_task 且要派开发/分析任务时填写，一行以内）"
    )
    project_desc: str = Field(
        default="",
        description="新项目的 1~3 句说明，告诉调度这个项目是做什么的（仅新建项目时填写）"
    )
    project_summary: str = Field(
        default="",
        description="本轮任务的 3~5 句结果摘要，收尾时填写；给项目表和后续 RAG 用"
    )


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
        # 显式给 id：项目登记表要用 HumanMessage.id 作为对话区间起点
        "messages": [HumanMessage(content=state["input_msg"], id=str(uuid4()))],
        # 普通消息显式写 False，避免上一轮后台回流留下的状态把这次误判成后台续跑
        "is_background": background,
        # 每轮先清空项目对话起点和摘要；调度确认要派任务/收尾时再填
        "project_turn_start_id": "",
        "project_summary": "",
    }
    if background:
        if deep_task_id := state.get("deep_task_id"):
            update["deep_task_id"] = deep_task_id
        if source := state.get("source"):
            update["source"] = source
        if status := state.get("status"):
            update["status"] = status
    if state.get("is_ops"):
        update["is_ops"] = True
        if handover := state.get("ops_handover"):
            update["ops_handover"] = handover
    return update


_NEGATED_SUBMIT = re.compile(
    r"(?:不要|不用|不需要|无需|不必|别|勿|禁止|不允许|不可|切勿|不)"
    r"[^。；;，,\n]{0,12}?(?:提交|部署上线)"
)


def _wants_submit(text: str) -> bool:
    """用户原话是否明确要求提交到集群；否定表达优先。

    “最后请不要进行提交到集群”这种中间夹了字的否定也要识别，不能只匹配“不要提交”。
    """
    text = (text or "").lower()
    if _NEGATED_SUBMIT.search(text):
        return False
    return any(k in text for k in (
        "提交到集群", "提交到 yarn", "提交到yarn", "提交集群",
        "提交运行", "提交到集群运行", "部署上线",
    ))


def _llm_messages(messages: list[BaseMessage]) -> list[BaseMessage]:
    """过滤掉零正文的用量台账消息，避免污染调度模型的上下文。"""
    return [
        message for message in messages
        if not (isinstance(message, AIMessage) and (message.additional_kwargs or {}).get("usage_ledger"))
    ]


def _last_human_text(state: DevelopAgentMainState) -> str:
    for message in reversed(state.get("messages", [])):
        if isinstance(message, HumanMessage):
            return message.content if isinstance(message.content, str) else str(message.content)
    return ""


def _last_human_message(state: DevelopAgentMainState) -> HumanMessage | None:
    for message in reversed(state.get("messages", [])):
        if isinstance(message, HumanMessage):
            return message
    return None


async def _prepare_project(
        dis: DevelopAgentOutState,
        state: DevelopAgentMainState,
        runtime: Runtime,
) -> tuple[dict[str, Any], str]:
    """解析 project_id / 新项目登记，返回父图 state 更新与错误信息。

    - 模型给了 project_id：校验归属和目录，回填坐标，切换当前项目；
    - new_task 且要派开发/分析：登记一条新项目（坐标等 initialize_project 回填）；
    - 普通继续：只记录本轮对话起点和摘要。
    """
    admin_id = admin_id_of(runtime.context.user_id)
    if not admin_id:
        return {}, ""
    thread_id = runtime.context.thread_id
    human = _last_human_message(state)
    start_id = str(getattr(human, "id", "") or "")
    has_sends = bool(dis.send_info_list)
    project_id = str(dis.project_id or "").strip()
    update: dict[str, Any] = {}
    if start_id and (has_sends or project_id):
        update["project_turn_start_id"] = start_id
    # 只接受“给用户的最终答复”这一轮的摘要，避免派任务时的计划摘要覆盖真实结果
    if dis.out_to_user.strip() and dis.project_summary.strip():
        update["project_summary"] = dis.project_summary.strip()[:1500]

    if project_id:
        row = await project_registry.get_project(project_id, admin_id)
        if row is None:
            return {}, "找不到该项目，或它不属于当前用户。"
        if not row.get("exists"):
            await project_registry.mark_missing(project_id, admin_id)
            return {}, "该项目的目录已不存在（可能被删除或移动）；请先告诉用户，由用户决定是否重新创建。"
        update.update({
            "project_id": row["project_id"],
            "project_title": row["title"],
            "project_desc": row["description"],
            "project_name": row["project_name"],
            "date": row["date"],
            "app_type": row["app_type"],
        })
        if start_id:
            await project_registry.append_conversation(project_id, thread_id, start_id)
        return update, ""

    # 新任务：只有真正派给开发/分析时才登记；纯聊天不建项目。
    # 首次派任务时 thread_action 可能是默认的 continue，因此“当前还没有 project_id”也按新项目处理。
    dev_sends = [item for item in (dis.send_info_list or [])
                 if item.send_to in ("data_develop", "data_analysis")]
    if dev_sends and (dis.thread_action == "new_task" or not state.get("project_id")):
        # 有后台任务时入口会换新 thread，新项目由新 thread 的调度登记
        if _locked_background_records(thread_id):
            return update, ""
        app_type = dev_sends[0].app_type
        tasks = dev_sends[0].task_list or []
        title = dis.project_title.strip() or (tasks[0][:60] if tasks else "")
        desc = dis.project_desc.strip() or "\n".join(tasks)[:1000]
        row = await project_registry.create_project(
            admin_id=admin_id,
            thread_id=thread_id,
            app_type=app_type,
            title=title or "未命名项目",
            description=desc,
            start_message_id=start_id or None,
        )
        if row is None:
            return update, ""
        existing_name = state.get("project_name")
        existing_date = state.get("date")
        is_legacy_reuse = (
                dis.thread_action != "new_task"
                and existing_name and existing_date
                and state.get("app_type") == app_type
        )
        if is_legacy_reuse:
            # 老线程还没登记过：把现有坐标补登记，绝不能当成新项目清空
            await project_registry.set_coordinates(
                row["project_id"], admin_id=admin_id, app_type=app_type,
                project_date=str(existing_date), project_name=str(existing_name),
            )
        if start_id:
            await project_registry.append_conversation(row["project_id"], thread_id, start_id)
        update.update({
            "project_id": row["project_id"],
            "project_title": row["title"],
            "project_desc": row["description"],
            "project_name": str(existing_name) if is_legacy_reuse else None,
            "date": str(existing_date) if is_legacy_reuse else None,
            "app_type": app_type,
        })
        return update, ""

    # 继续当前项目：只补对话起点
    if state.get("project_id") and start_id and has_sends:
        await project_registry.append_conversation(str(state["project_id"]), thread_id, start_id)
    return update, ""


def _route_after_handler(state: DevelopAgentMainState, runtime: Runtime) -> str | Send:
    """普通消息进 dispatch；运维转交直进开发节点；后台回流直接进 CVA 子图"""
    handover = state.get("ops_handover")
    if state.get("is_ops") and handover:
        app_type = handover.get("app_type")
        if app_type not in ("spark", "flink"):
            return Send(
                "work_end",
                WorkEndState(
                    type="error",
                    desc=f"运维转交的应用类型不支持：{app_type!r}",
                    from_="handler",
                    thread_id=runtime.context.thread_id,
                ),
            )
        task_list = [str(item) for item in (handover.get("task_list") or [])]
        return Send(
            "data_develop" if app_type == "flink" else "data_analysis",
            AgentDevelopInputState(
                is_repair=True,  # 项目已搬进 tmp_code，不允许再初始化
                task_list=task_list,
                task_num=len(task_list),
                app_type=app_type,
                date=handover.get("date"),
                project_name=handover.get("project_name"),
                is_submit=True,  # 运维转交的修复完成后强制走提交（人工仍可拒绝）
                is_ops=True,
                ops_handover=handover,
            ),
        )

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
            status=state.get("status", "preliminary"),
            is_submit=state.get("is_submit", False),
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
        messages=[*_llm_messages(state["messages"]), *alloc_error_msg_list],
        system_message=SystemMessage(content=system_content),
        tools=main_agent_tools,
        state=state,  # type: ignore[arg-type]
        runtime=runtime,
    )

    project_update: dict[str, Any] = {}
    project_prepared = False

    async def dis_execute(dis_req: ModelRequest) -> ModelResponse:
        nonlocal project_update, project_prepared
        logger.debug("埋点：调度本轮可用工具：%s",
                     [getattr(tool, "name", str(tool)) for tool in (dis_req.tools or [])])
        mags: list[BaseMessage] = ([dis_req.system_message] if dis_req.system_message else []) + dis_req.messages

        # 结构化 schema 与业务工具一起绑定；any 保证模型不会回自由文本
        bound_model = dis_req.model.bind_tools(
            [DevelopAgentOutState, *(dis_req.tools or [])],
            tool_choice=dis_req.tool_choice or "any",
        )

        # 结构化解析失败/工具信息超限会重试，所有真实发生的调用都要计入
        call_usages: list[dict[str, Any]] = []

        async def _ainvoke(messages: list[BaseMessage]) -> tuple[
            str, BaseMessage, DevelopAgentOutState | None, Exception | None
        ]:
            raw_msg = await bound_model.ainvoke(messages, config={"tags": ["agent:dis_chat"]})
            if isinstance(raw_msg, AIMessage) and getattr(raw_msg, "usage_metadata", None):
                call_usages.append(raw_msg.usage_metadata)
            logger.debug(
                f"缓存信息埋点：{raw_msg.usage_metadata if raw_msg and hasattr(raw_msg, 'usage_metadata') else ''}")
            logger.debug(f"缓存信息埋点：{raw_msg.response_metadata}")

            # 和 CVA 一样：模型偶尔把 true/false/null 写成 Python 字面量，LangChain 放进
            # invalid_tool_calls；先修复，不要直接结束整轮。
            if isinstance(raw_msg, AIMessage) and getattr(raw_msg, "invalid_tool_calls", None):
                repair_status, repaired = repair_invalid_tool_calls(raw_msg)
                if repair_status == "ok" and repaired:
                    raw_msg = repaired
                if repair_status == "error" and repaired:
                    # 当 error且存在 repaired时，说明工具信息超限，允许重试一次
                    return "retry", repaired, None, None

            parsed_obj, parsing_err = parse_tool_call(raw_msg, DevelopAgentOutState)
            return "continue", raw_msg, parsed_obj, parsing_err

        status, dis_res_raw, dis_parsed, parsing_err = await _ainvoke(mags)

        def _real_tool_call(msg: BaseMessage) -> bool:
            """真实工具调用（list_projects / info_inquiry 等）必须交给 ToolNode。"""
            if not isinstance(msg, AIMessage):
                return False
            for call in (msg.tool_calls or []):
                name = call.get("name") if isinstance(call, dict) else getattr(call, "name", None)
                if name and name != DevelopAgentOutState.__name__:
                    return True
            return False

        def _tool_response(msg: BaseMessage) -> ModelResponse:
            """交给 ToolNode：剔除结构化调用，避免它被当业务工具执行。"""
            logger.debug("埋点：调度模型调用了真实工具，交给 ToolNode 执行")
            return ModelResponse(result=[strip_tool_call(msg, DevelopAgentOutState.__name__)])

        if _real_tool_call(dis_res_raw):
            return _tool_response(dis_res_raw)

        if parsing_err and not dis_parsed:
            # 结构化解析异常
            mags.append(AIMessage(
                content=f"输出的结构化数据异常，错误信息：{parsing_err}，请确保你调用的`DevelopAgentOutState`工具参数没问题下再次尝试",
            ))
            status, dis_res_raw, dis_parsed, parsing_err = await _ainvoke(mags)
        elif status == "retry":
            mags.append(dis_res_raw)
            status, dis_res_raw, dis_parsed, parsing_err = await _ainvoke(mags)

        if _real_tool_call(dis_res_raw):
            return _tool_response(dis_res_raw)

        usage_metadata = merge_raw_usage(*call_usages) or getattr(dis_res_raw, "usage_metadata", None)
        response_metadata = getattr(dis_res_raw, "response_metadata", {}) or {}
        dis_res_id = getattr(dis_res_raw, "id", None)

        if dis_parsed is None:
            # 两次都没拿到结构化：把模型最后的内容塞进 out_to_user，不终止会话
            text = extract_text_content(dis_res_raw).strip() if isinstance(dis_res_raw, BaseMessage) else ""
            dis_parsed = DevelopAgentOutState(
                out_to_user=text or "抱歉，本轮没有生成有效回复，请再试一次。")

        # 成功拿到结构化输出：先解析项目上下文，再挂到消息上
        send_dict = dis_parsed.model_dump()
        if not project_prepared:
            # 模型链重试时只解析一次，避免重复建项目
            project_prepared = True
            project_update, project_error = await _prepare_project(dis_parsed, state, runtime)
            if project_error:
                send_dict["__project_error__"] = project_error
        out_ai_msg = AIMessage(
            content="",
            additional_kwargs={"send": send_dict},
            usage_metadata=usage_metadata,
            response_metadata=response_metadata,
            id=dis_res_id,
        )
        return ModelResponse(result=[out_ai_msg])

    if dispatch_model_chain is not None:
        dis_result = await dispatch_model_chain(dis_request, dis_execute)
    else:
        dis_result = ComposedModelResult(model_response=await dis_execute(dis_request))

    commands: list[Command] = [Command(update={
        "messages": dis_result.model_response.result,
        **project_update,
        **dis_update,
    })]
    commands.extend(dis_result.commands)

    await run_after_model(dispatch_middleware, {**state, 'messages': dis_result.model_response.result}, runtime)

    return commands


def _ver_submit_footer(messages: list[BaseMessage]) -> str:
    """从当前用户轮次之后的 ver_end 消息里提取确定性的提交状态。"""
    last_human = -1
    for i, message in enumerate(messages):
        if isinstance(message, HumanMessage):
            last_human = i
    for message in reversed(messages[last_human + 1:]):
        if not isinstance(message, AIMessage):
            continue
        if not str(getattr(message, "name", "") or "").startswith("ver:"):
            continue
        text = content_text(message.content)
        if "作业没有进行提交" in text:
            return "提交状态：未提交（CVA 返回：作业没有进行提交）"
        if "对于提交返回的结果" in text:
            matched = re.search(
                r"类型为\s*(info|warn|error)\s*[，,]\s*该类型的详细描述为：\s*(.*)",
                text, re.S,
            )
            if matched:
                submit_type, desc = matched.group(1), matched.group(2).strip()
                if submit_type == "error":
                    return f"提交状态：提交失败（{desc}）"
                return f"提交状态：已提交（{desc}）"
            return "提交状态：提交结果见上方 CVA 返回"
    return ""


def _append_submit_footer(
        text: str, messages: list[BaseMessage]
) -> str:
    footer = _ver_submit_footer(messages)
    if not footer:
        return text
    conflict = footer.startswith("提交状态：未提交") and any(
        keyword in text for keyword in ("已安排提交", "已提交", "提交成功")
    )
    prefix = "注意：正文与验证结果不一致，以下方提交状态为准。\n" if conflict else ""
    return f"{prefix}{text.rstrip()}\n\n{footer}"


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
    usage_metadata = getattr(last_ai_msg, "usage_metadata", None)
    response_metadata = getattr(last_ai_msg, "response_metadata", None) or {}
    dis_res_id = getattr(last_ai_msg, "id", None)
    # 暂时没有任何工具
    if last_ai_msg and last_ai_msg.tool_calls:
        return "tool"

    def back_to_dispatcher(desc: str) -> Send:
        """项目类错误让调度自己向用户解释；不要绕过调度直接 work_end。"""
        return Send(
            "dis_chat",
            {
                **state,
                "error_info": AllocateErrorInfo(
                    from_="distribute_task_to_agent",
                    desc=f"{desc}\n请直接用 out_to_user 向用户说明原因并询问下一步；本轮不要再次派发同一任务。",
                ),
            },
        )

    if project_error := send_info.get("__project_error__"):
        return back_to_dispatcher(str(project_error))
    if error_info:
        return Send(
            "work_end",
            WorkEndState(
                type="error",
                from_=error_info.get("from_"),
                desc=error_info.get("desc"),
                thread_id=runtime.context.thread_id,
                id=dis_res_id,
                usage_metadata=usage_metadata,
                response_metadata=response_metadata,
            )
        )

    is_new_task = send_info.get("thread_action") == "new_task"
    # 后台任务由系统管理：只有“新任务 + 该 thread 仍有后台任务”时才换新会话。
    # 没有后台任务时留在当前会话，但项目上下文按新任务重置。
    if is_new_task and _locked_background_records(runtime.context.thread_id):
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

    send_list = []
    user_text = _last_human_text(state)
    send_info_dict = {send_i.get("send_to"): send_i for send_i in send_info_list}
    for send_info in list(send_info_dict.values()):
        send_to = send_info.get("send_to")
        submit_flag = bool(send_info.get("is_submit", False))
        if not submit_flag and _wants_submit(user_text):
            # 主模型偶尔忽略“提交到 YARN”的指令；这里做确定性兜底，
            # 仍会经过 CVA 的提交确认，不会绕过人工。
            submit_flag = True
            logger.info("用户明确要求提交，自动将 is_submit 置 true")

        if send_to == "code_verify":
            # 直连验证：不再先派开发 Agent 重新初始化项目，直接用父图里的项目坐标
            project_name = state.get("project_name")
            project_date = state.get("date")
            project_type = state.get("app_type")
            if not (project_name and project_date and project_type):
                return back_to_dispatcher(
                    "当前会话没有可验证的项目；请先完成一次开发任务，或让用户明确要验证哪个项目。"
                )
            project_check = check_project_dir(project_type, project_date, project_name)
            if not project_check["exists"]:
                return back_to_dispatcher(
                    "当前项目的目录不存在（可能已被删除或移动）；请告诉用户并确认是否重新创建。"
                )
            verify_type = send_info.get("app_type", project_type)
            if verify_type and verify_type != project_type:
                return back_to_dispatcher(
                    f"验证类型与当前项目不一致：项目是 {project_type}，请求的是 {verify_type}。"
                )
            verify_tasks = send_info.get("task_list") or [user_text]
            # background_msg 会覆盖 ver_handler 的标准消息，所以验证范围必须一起带进来
            verify_scope = "\n".join(f"- {item}" for item in verify_tasks) or "- 重新执行浅层/深层验证"
            send_list.append(
                Send(
                    "code_verify",
                    CodeVerifyAgentInputState(
                        source="dis_chat",
                        source_msg=user_text,
                        background_msg=(
                            f"用户要求对现有项目重新进行代码验证（不需要重新开发）。\n"
                            f"项目：{project_name}（{project_type}/{project_date}）。\n"
                            f"本次验证范围：\n{verify_scope}\n"
                            f"用户原话：{user_text}"
                        ),
                        app_type=project_type,
                        project_name=project_name,
                        date=project_date,
                        project_id=state.get("project_id"),
                        original_task_list=verify_tasks,
                        status="preliminary",
                        is_repair=True,
                        is_submit=submit_flag,
                    ),
                )
            )
            continue

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
        if not task_list:
            return Send(
                "work_end",
                WorkEndState(
                    type="error",
                    from_="distribute_task_to_agent",
                    desc=f"派给 {send_to} 的任务清单不能为空。",
                    thread_id=runtime.context.thread_id,
                )
            )
        # 继续当前任务：把父图里的项目坐标传下去，工具才会落到原目录；
        # 新任务则不带，交给开发/分析 Agent 新建项目。
        reuse_project = bool(
            not is_new_task
            and state.get("project_name")
            and state.get("date")
            and state.get("app_type") == app_type
        )
        project_check = None
        if reuse_project:
            # 先把目录事实告诉 Agent，避免它先调一次工具才发现目录没了
            project_check = check_project_dir(state.get("app_type"), state.get("date"), state.get("project_name"))
            if not project_check["exists"]:
                return back_to_dispatcher(
                    "当前项目的目录不存在（可能已被删除或移动）；请告诉用户并确认是否重新创建，"
                    "不要直接新建同名项目。"
                )
        dev_input = AgentDevelopInputState(
            is_repair=reuse_project,
            task_list=task_list,
            task_num=len(task_list),
            app_type=app_type,
            is_submit=submit_flag,
        )
        if state.get("project_id"):
            dev_input["project_id"] = str(state["project_id"])
        if reuse_project:
            dev_input["date"] = state.get("date")
            dev_input["project_name"] = state.get("project_name")
            if project_check:
                dev_input["project_check"] = project_check
        send_list.append(Send(send_to, dev_input))
    # 放在这是为了防止出现“已安排提交”但实际上没有提交的情况 -> 不是模型问题，而是原本的路由有问题
    if to_user_msg:
        # CVA 的 submit_desc 是事实；主模型不能自己编“已安排提交”
        to_user_msg = _append_submit_footer(to_user_msg, state.get("messages", []))
        send_list.append(Send(
            "work_end",
            WorkEndState(
                type="info",
                from_="dis_chat",
                desc=to_user_msg,
                thread_id=runtime.context.thread_id,
                usage_metadata=usage_metadata,
                response_metadata=response_metadata,
                id=dis_res_id,
            )
        ))
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


def work_end(state: WorkEndState) -> dict[str, Any]:
    desc_msg = state.get("desc") or ""
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
    # 验证子图回传的结构化报告；前端用它在最终消息里展示状态
    if verification := state.get("verification"):
        metadata_dict["verification"] = verification
    result = {
        "output_msg": desc_msg,
        "out_time": datetime.now(),
        "metadata": metadata_dict,
    }
    if desc_msg.strip():
        # 用户可见的最终回复写回父图 messages；output_schema 只影响 ainvoke 的返回值
        result["messages"] = [
            AIMessage(content=desc_msg, name=agent_label(state.get("from_")), id=state.get("id", ""),
                      usage_metadata=state.get("usage_metadata"), response_metadata=state.get("response_metadata") or {}),
        ]
    return result


builder = StateGraph(
    cast("StateT", DevelopAgentMainState),
    context_schema=cast("ContextT", MainStateContext),
    input_schema=cast("InputT", DevelopAgentInputState),
    output_schema=cast("OutputT", DevelopAgentOutputState),
)

builder.add_node("handler", cast("StateNode", develop_msg_handler))
builder.add_node("dis_chat", cast("StateNode", dispatch_chat))
builder.add_node("tool", ToolNode(
    tools=main_agent_tools,
    awrap_tool_call=chain_tool_call_wrappers(dispatch_middleware)
))
builder.add_node("data_develop", cast("StateNode", big_data_develop_code_agent))
builder.add_node("data_analysis", cast("StateNode", big_data_analysis_code_agent))
# 直接挂编译好的 CVA 子图：子图里的 Command(graph=PARENT) / interrupt 才会生效
builder.add_node("code_verify", cast("StateNode", verify_agent))
builder.add_node("work_end", cast("StateNode", work_end))

builder.add_edge(START, "handler")
builder.add_conditional_edges(
    "handler",
    _route_after_handler,
    ["dis_chat", "code_verify", "work_end", "data_develop", "data_analysis"]
)
builder.add_conditional_edges(
    "dis_chat",
    distribute_task_to_agent,
    ["data_develop", "data_analysis", "work_end", "tool"]
)
builder.add_edge("tool", "dis_chat")
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
