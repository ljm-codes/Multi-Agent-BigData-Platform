"""
大数据分析Agent

只面向 spark + python：写完代码后做一次语法检查，失败打回本节点（至多 3 次修复），
通过后交给 code_verify。
"""
import tokenize
from datetime import datetime
from pathlib import Path
from typing import Any, cast, NotRequired
from uuid import uuid4

from langchain.agents import create_agent, AgentState
from langchain.chat_models import init_chat_model
from langchain_core.messages import HumanMessage, AIMessage
from langgraph.runtime import Runtime
from langgraph.types import Send

from SPO.main_agent_state_model import AgentDevelopInputState, MainStateContext, CodeVerifyAgentInputState, \
    DevelopGraphErrorState, WorkEndState, content_text
from Tasks.project_registry import admin_id_of, project_registry
from Tools.app_name import validate_app_name
from Tools.log_settings import LogSetting
from Tools.data_analysis_agent_tools import AGENT_NAME as ANALYSIS_AGENT, analysis_business_files
from Tools.registry import get_tools
from Tools.share_check_tool import redis_con
from Tools.usage_metrics import last_response_metadata, merged_raw_usage_of_messages, usage_ledger_message
from load_config.config import config, ROOT_BASE_DIR_PATH, get_model_params
from middleware.tool_call_guard import ToolCallHistoryGuard
from middleware.tool_trace_middleware import ToolTraceMiddleware

model_config = config.get("model")
params = get_model_params(thinking={"type": "enabled"})
# 数据分析Agent同样采用思考模式
data_analysis_model = init_chat_model(
    model=model_config.get("name"),
    **params
)
data_analysis_tools = get_tools(ANALYSIS_AGENT)
logger = LogSetting.create(__name__)

with open(Path(__file__).parent / "prompt" / "data_analysis_agent.md", "r", encoding="utf-8") as f:
    _analysis_system_prompt = f.read()


class CodeAnalysisState(AgentState):
    project_name: str
    project_id: NotRequired[str]
    is_repair: bool
    repair_num: NotRequired[int]


def _has_python_code(app_path: Path) -> bool:
    try:
        files, _ = analysis_business_files(app_path)
        return bool(files)
    except Exception:
        return False


async def big_data_analysis_code_agent(
        state: AgentDevelopInputState,
        runtime: Runtime
) -> DevelopGraphErrorState | dict[str, Any] | CodeVerifyAgentInputState:
    task_list = state.get("task_list") or []
    task_num = state.get("task_num")

    thread_id = runtime.context.thread_id
    user_id = runtime.context.user_id

    if task_num == 0:
        return DevelopGraphErrorState(
            from_='data_analysis_agent',
            thread_id=thread_id,
            desc='任务列表不能为空',
            goto_="work_end",
            original_task_list=task_list
        )
    if error_msg := state.get("error_msg"):
        # 基础语法校验失败
        handle_msg = HumanMessage(
            content=f"整体代码验证失败，对应的错误消息为：{error_msg}"
        )
    else:
        handle_msg = HumanMessage(
            content=f"请根据以下任务列表，开发 spark 数据分析代码（python）：{'\n'.join(task_list)}\n"
                    f"{state.get('human_suggestion', '')}"
        )
    if state.get("is_ops"):
        tasks = "\n".join(f"- {item}" for item in (task_list or []))
        handle_msg = HumanMessage(
            content=f"这是运维 Agent 转交的现网应用修复任务，项目已经存在（{state.get('project_name')}），"
                    f"不要重新初始化项目，直接定位并修改代码。\n修复清单：\n{tasks}"
        )
    if human_suggestion := state.get("human_suggestion"):
        handle_msg = HumanMessage(
            content=f"经人工核对，代码开发不符合业务需求（系统已自动锁定该项目）。人工建议为：\n{human_suggestion}"
        )
    if check := state.get("project_check"):
        handle_msg = HumanMessage(content=(
            f"项目目录状态：exists={bool(check.get('exists'))}，"
            f"has_code={bool(check.get('has_code'))}。\n{handle_msg.content}"
        ))
    in_state = {
        "messages": [handle_msg],
        "project_name": state.get("project_name") or str(uuid4()),
        "is_repair": state.get("is_repair", False),
    }
    if project_id := state.get("project_id"):
        in_state["project_id"] = project_id
    repair_num = state.get("repair_num")
    if repair_num and repair_num >= 4:
        return DevelopGraphErrorState(
            from_='data_analysis_agent',
            thread_id=thread_id,
            desc="修复次数已经达到上限，现在强制终止该任务",
            goto_="work_end",
            original_task_list=task_list
        )
    if repair_num:
        in_state["repair_num"] = repair_num
    code_analysis_agent = create_agent(
        model=data_analysis_model,
        tools=data_analysis_tools,
        middleware=cast("AgentMiddleware", [
            ToolTraceMiddleware("data_analysis"),
            ToolCallHistoryGuard("data_analysis"),
        ]),
        system_prompt=_analysis_system_prompt,
        context_schema=cast("ContextT", MainStateContext),
        state_schema=cast("StateT", CodeAnalysisState),
    )
    date = state.get("date") or datetime.now().strftime('%Y-%m-%d')

    result = await code_analysis_agent.ainvoke(
        cast(
            "InputT",
            in_state
        ),
        config={
            "configurable": {"thread_id": runtime.context.thread_id},
            "tags": ["agent:data_analysis"],
            "max_concurrency": 1,
        },
        context=cast(
            "ContextT",
            MainStateContext(
                thread_id=thread_id,
                user_id=user_id,
                app_type="spark",
                date=date,
            )
        ),
    )
    # 模型有时只回一句“我先初始化…”就结束（没有 tool_call），必须做业务代码后置校验。
    project_dir = result.get("project_name") or in_state["project_name"]
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / "spark" / date / project_dir
    if not _has_python_code(app_path):
        logger.warning(f"分析 Agent 未写 Python 代码，重试一次: {app_path}")
        retry_text = (
            f"你还没有写任何业务代码：{app_path / 'src' / 'main' / 'python'} 下没有 .py 文件。"
            "项目已经初始化，不要再次调用 initialize_project。请立即调用 write_analysis_app_code "
            "写出完整实现，然后调用 verify_analysis_app_code；不要只回复计划。"
        )
        retry_state = {
            "messages": [*result.get("messages", []), HumanMessage(content=retry_text)],
            "project_name": project_dir,
            "is_repair": True,
        }
        if project_id := state.get("project_id"):
            retry_state["project_id"] = project_id
        result = await code_analysis_agent.ainvoke(
            cast("InputT", retry_state),
            config={
                "configurable": {"thread_id": runtime.context.thread_id},
                "tags": ["agent:data_analysis"],
                "max_concurrency": 1,
            },
            context=cast(
                "ContextT",
                MainStateContext(thread_id=thread_id, user_id=user_id,
                                 app_type="spark", date=date),
            ),
        )
        project_dir = result.get("project_name") or project_dir
        app_path = ROOT_BASE_DIR_PATH / "tmp_code" / "spark" / date / project_dir
        if not _has_python_code(app_path):
            # 报错终止也要把两次分析 run 的消耗记进父图 checkpoint
            ledger = usage_ledger_message(
                "data_analysis",
                merged_raw_usage_of_messages(result.get("messages", [])),
                last_response_metadata(result.get("messages", [])),
            )
            error = DevelopGraphErrorState(
                from_="data_analysis_agent",
                thread_id=thread_id,
                desc=(f"项目 {app_path} 下仍然没有业务 Python 代码；"
                      f"分析 Agent 连续两次只初始化/未写代码，终止。"),
                goto_="work_end",
                original_task_list=task_list,
            )
            return {**error, **({"messages": [ledger]} if ledger else {})}
    # initialize_project 没被调用时也能回填坐标；set_coordinates 本身幂等
    project_id = state.get("project_id")
    admin_id = admin_id_of(user_id)
    if project_id and admin_id:
        await project_registry.set_coordinates(
            str(project_id), admin_id=admin_id, app_type="spark",
            project_date=str(date or ""), project_name=str(project_dir or ""),
        )
    # logger.info(f"数据分析Agent的完整回复为：{result}")
    final_text = ""
    # 分析 Agent 内部可能跑了很多轮工具调用；整个 run 的用量合并后一次性回传父图
    usage_metadata = merged_raw_usage_of_messages(result.get("messages", []))
    response_metadata = None
    analysis_id = None
    for message in reversed(result.get("messages", [])):
        if isinstance(message, AIMessage):
            text = content_text(message.content)
            if text.strip():
                final_text = text
                response_metadata = message.response_metadata or {}
                analysis_id = message.id
                break
    code_verify_in_state = CodeVerifyAgentInputState(
        source='data_analysis_agent',
        source_msg=final_text or content_text(result["messages"][-1].content),
        app_type="spark",
        project_name=project_dir,
        date=date,
        original_task_list=task_list,
        is_repair=state.get("is_repair", False),
        is_submit=state.get("is_submit", False),
        is_ops=state.get("is_ops", False),
    )
    if repair_num:
        code_verify_in_state["repair_num"] = repair_num
    if project_id := state.get("project_id"):
        code_verify_in_state["project_id"] = project_id
    if final_text.strip():
        # 把用户可见的最终回复写回父图 messages；Send 的临时输入不受影响
        return {
            **code_verify_in_state,
            "messages": [AIMessage(
                content=f"来自数据分析Agent的回复：\n{final_text}", name="data_analysis",
                id=analysis_id, usage_metadata=usage_metadata, response_metadata=response_metadata)],
        }
    # 没有正文也要记账，否则这次 run 的消耗到了验证 Agent 就看不见了
    ledger = usage_ledger_message(
        "data_analysis", usage_metadata,
        response_metadata or last_response_metadata(result.get("messages", [])),
    )
    return {**code_verify_in_state, **({"messages": [ledger]} if ledger else {})}


def _submit_registration_error(project_name: str | None, is_submit: bool) -> str:
    """data_analysis 的下一节点复查：注册了就必须是合法 app_name；提交任务必须有注册。"""
    if not project_name:
        return ""
    try:
        raw_name = redis_con.hget(f"DAA:{project_name}", "app_name")
    except Exception as e:  # noqa: BLE001
        return f"读取提交注册信息失败{type(e).__name__}: {e}"
    if isinstance(raw_name, bytes):
        raw_name = raw_name.decode("utf-8")
    if raw_name:
        error = validate_app_name(raw_name)
        return f"注册的 app_name 不合法：{error}" if error else ""
    if is_submit:
        return ("没有找到提交注册信息（register_submit_command 未调用或失败）；"
                "提交任务必须先注册合法的 app_name")
    return ""


def preliminary_verify(state: CodeVerifyAgentInputState | DevelopGraphErrorState) -> Send:
    """
    数据分析项目的初步验证（对 src/main/python 下所有业务 .py 做 compile 检查）
    """
    if state.get("from_"):
        return Send(
            node=cast(str, state.get("goto_")),
            arg=WorkEndState(
                type="error",
                desc=cast(str, state.get("desc")),
                from_=cast(str, state.get("from_")),
                thread_id=cast(str, state.get("thread_id")),
                app_type="spark",
            )
        )
    date = state.get("date")
    project_name = state.get("project_name")
    original_task_list = state.get("original_task_list") or []
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / "spark" / date / project_name
    repair_num = state.get("repair_num", 1)
    if repair_num > 3:
        return Send(
            "work_end",
            WorkEndState(
                type="error",
                from_="data_analysis_agent",
                desc=f"超过3次修复任然没有修复成功，强制终止该任务，该项目在{app_path}下，请手动修复",
                thread_id=cast(str, state.get("thread_id")),
                app_type="spark",
            )
        )
    registration_error = _submit_registration_error(
        project_name, bool(state.get("is_submit")),
    )
    if registration_error:
        return Send(
            "data_analysis",
            {
                "project_name": project_name,
                "date": date,
                "app_type": "spark",
                "error_msg": registration_error,
                "task_list": original_task_list,
                "task_num": len(original_task_list),
                "is_repair": True,
                "repair_num": 1 if not state.get("repair_num") else state.get("repair_num") + 1,
                "is_ops": state.get("is_ops", False),
                "ops_handover": state.get("ops_handover"),
                "project_id": state.get("project_id"),
            }
        )
    py_files, _ = analysis_business_files(app_path)
    errors: list[str] = []
    if not py_files:
        errors.append(f"项目 {app_path} 下没有找到任何业务 Python 代码（src/main/python）")
    for f in py_files:
        if len(errors) >= 20:
            errors.append("……错误过多，只列出前 20 条")
            break
        try:
            with tokenize.open(f) as fh:
                source = fh.read()
            # 不执行，只编译；用 compile 而不是 ast.parse —— 后者漏掉编译阶段的检查
            compile(source, str(f), "exec")
        except SyntaxError as e:
            location = f"第 {e.lineno} 行"
            if e.offset:
                location += f" 第 {e.offset} 列"
            errors.append(f"{f.relative_to(app_path)} {location}：{e.msg}")
        except Exception as e:
            errors.append(f"{f.relative_to(app_path)} 读取/编译失败：{type(e).__name__}: {e}")

    if errors:
        return Send(
            "data_analysis",
            {
                "project_name": project_name,
                "date": date,
                "app_type": "spark",
                "error_msg": "\n".join(errors),
                "task_list": original_task_list,
                "task_num": len(original_task_list),
                "is_repair": True,
                "repair_num": 1 if not state.get("repair_num") else state.get("repair_num") + 1,
                # 修复重入也要保留运维模式，否则验证结束会回到 dis_chat
                "is_ops": state.get("is_ops", False),
                "ops_handover": state.get("ops_handover"),
                "project_id": state.get("project_id"),
            }
        )
    return Send(
        "code_verify",
        # 新任务/修复后的验证重新从浅层开始，不能沿用上一次后台回流留下的 status
        {**state, "status": "preliminary"}
    )
