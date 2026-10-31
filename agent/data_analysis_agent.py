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
from Tools.log_settings import LogSetting
from Tools.data_analysis_agent_tools import AGENT_NAME as ANALYSIS_AGENT, analysis_business_files
from Tools.registry import get_tools
from load_config.config import config, ROOT_BASE_DIR_PATH, get_model_params

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
    is_repair: bool
    repair_num: NotRequired[int]


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
    if human_suggestion := state.get("human_suggestion"):
        handle_msg = HumanMessage(
            content=f"经人工核对，代码开发不符合业务需求（系统已自动锁定该项目）。人工建议为：\n{human_suggestion}"
        )
    in_state = {
        "messages": [handle_msg],
        "project_name": state.get("project_name") or str(uuid4()),
        "is_repair": state.get("is_repair", False),
    }
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
        middleware=[],
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
    # logger.info(f"数据分析Agent的完整回复为：{result}")
    final_text = ""
    for message in reversed(result.get("messages", [])):
        if isinstance(message, AIMessage):
            text = content_text(message.content)
            if text.strip():
                final_text = text
                break
    code_verify_in_state = CodeVerifyAgentInputState(
        source='data_analysis_agent',
        source_msg=final_text or content_text(result["messages"][-1].content),
        app_type="spark",
        project_name=result.get("project_name"),
        date=date,
        original_task_list=task_list,
        is_repair=state.get("is_repair", False),
    )
    if repair_num:
        code_verify_in_state["repair_num"] = repair_num
    if final_text.strip():
        # 把用户可见的最终回复写回父图 messages；Send 的临时输入不受影响
        return {
            **code_verify_in_state,
            "messages": [AIMessage(content=final_text, name="data_analysis")],
        }
    return code_verify_in_state


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
            }
        )
    return Send(
        "code_verify",
        state
    )
