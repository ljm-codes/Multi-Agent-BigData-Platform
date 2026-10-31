"""
大数据开发Agent
"""
from datetime import datetime
from pathlib import Path
from typing import Any, cast, NotRequired
from uuid import uuid4

from langchain.agents import create_agent, AgentState
from langchain.chat_models import init_chat_model
from langchain_core.messages import HumanMessage, AIMessage
from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.runtime import Runtime
from langgraph.types import Send

from SPO.main_agent_state_model import AgentDevelopInputState, MainStateContext, CodeVerifyAgentInputState, \
    DevelopGraphErrorState, AllocateErrorInfo, content_text
from Tools.log_settings import LogSetting
from Tools.data_develop_agent_tools import AGENT_NAME as DEVELOP_AGENT, run_mvn_command
from Tools.registry import get_tools
from load_config.config import config, ROOT_BASE_DIR_PATH, get_model_params

model_config = config.get("model")
params = get_model_params(thinking={"type": "enabled"})
mcp_config = config.get("mcp")
maven_config = mcp_config.get("maven")
# 代码开发Agent将会采用思考模式
#   ==> 注意：若你使用的是deepseek模型，那么请参照tests/verify_ds_think_model.py中的思考案例
code_develop_model = init_chat_model(
    model=model_config.get("name"),
    **params
)
data_develop_tools = get_tools(DEVELOP_AGENT)
logger = LogSetting.create(__name__)

with open(Path(__file__).parent / "prompt" / "data_develop_agent.md", "r", encoding="utf-8") as f:
    _code_develop_system_prompt = f.read()


class CodeDevelopState(AgentState):
    project_name: str
    is_repair: bool
    repair_num: NotRequired[int]


async def big_data_develop_code_agent(
        state: AgentDevelopInputState,
        runtime: Runtime
) -> CodeVerifyAgentInputState | DevelopGraphErrorState | dict[str, Any]:
    task_list = state.get("task_list")
    task_num = state.get("task_num")
    app_type = state.get("app_type")

    thread_id = runtime.context.thread_id
    user_id = runtime.context.user_id

    if task_num == 0:
        return DevelopGraphErrorState(
            from_='data_develop_agent',
            thread_id=thread_id,
            desc='任务列表不能为空',
            goto_="dis_chat",
            original_task_list=task_list
        )
    if error_msg := state.get("error_msg"):
        # 基础语法校验失败
        handle_msg = HumanMessage(
            content=f"整体代码验证失败，对应的错误消息为：{error_msg}"
        )
    else:
        handle_msg = HumanMessage(
            content=f"请根据以下任务列表，开发代码：{'\n'.join(task_list)}\n{state.get('human_suggestion', '')}"
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
            from_='data_develop_agent',
            thread_id=thread_id,
            desc="修复次数已经达到上限，现在强制终止该任务",
            goto_="work_end",
            original_task_list=task_list
        )
    if repair_num:
        in_state["repair_num"] = repair_num
    mcp_servers = (maven_config or {}).get("mcpServers") or {}
    if not mcp_servers:
        return DevelopGraphErrorState(
            from_="data_develop_agent",
            thread_id=thread_id,
            desc="Maven MCP 未配置（config.mcp.maven.mcpServers 为空）",
            goto_="work_end",
            original_task_list=task_list,
        )
    connections = {
        name: {**cfg, "transport": "stdio"}
        for name, cfg in mcp_servers.items()
    }
    try:
        mcp_client = MultiServerMCPClient(connections)
        maven_tool = await mcp_client.get_tools()
    except Exception as e:  # noqa: BLE001
        logger.exception("启动 Maven MCP 失败")
        return DevelopGraphErrorState(
            from_="data_develop_agent",
            thread_id=thread_id,
            desc=f"启动 Maven MCP 失败：{type(e).__name__}: {e}",
            goto_="work_end",
            original_task_list=task_list,
        )
    # 该Agent 专门写java代码
    code_develop_agent = create_agent(
        model=code_develop_model,
        tools=[*maven_tool, *data_develop_tools],
        middleware=[],
        system_prompt=_code_develop_system_prompt,
        context_schema=cast("ContextT", MainStateContext),
        state_schema=cast("StateT", CodeDevelopState),
    )
    date = state.get("date") or datetime.now().strftime('%Y-%m-%d')

    result = await code_develop_agent.ainvoke(
        cast(
            "InputT",
            in_state
        ),
        config={
            "configurable": {"thread_id": runtime.context.thread_id},
            "tags": ["agent:data_develop"],
        },
        context=cast(
            "ContextT",
            MainStateContext(
                thread_id=thread_id,
                user_id=user_id,
                app_type=app_type,
                date=date,
            )
        ),
    )
    # logger.info(f"大数据开发Agent的完整回复为：{result}")
    final_text = ""
    for message in reversed(result.get("messages", [])):
        if isinstance(message, AIMessage):
            text = content_text(message.content)
            if text.strip():
                final_text = text
                break
    code_verify_in_state = CodeVerifyAgentInputState(
        source='data_develop_agent',
        source_msg=final_text or content_text(result["messages"][-1].content),
        app_type=app_type,
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
            "messages": [AIMessage(content=final_text, name="data_develop")],
        }
    return code_verify_in_state


def preliminary_verify(state: CodeVerifyAgentInputState | DevelopGraphErrorState) -> Send:
    """
    整体的项目的初步验证（语法验证）
    """
    if state.get("from_"):
        return Send(
            node=cast(str, state.get("goto_")),
            arg=AllocateErrorInfo(
                from_=cast(str, state.get("from_")),
                desc=cast(str, state.get("desc"))
            )
        )
    # 进行语法验证
    app_type = state.get("app_type")
    project_name = state.get("project_name")
    date = state.get("date")
    app_path = ROOT_BASE_DIR_PATH / "tmp_code" / app_type / date / project_name
    res_code, res_out = run_mvn_command("mvn clean compile", path=str(app_path))
    if res_code != 0:
        return Send(
            "data_develop",
            {
                "project_name": project_name,
                "date": date,
                "app_type": app_type,
                "error_msg": res_out,
                "task_list": state.get("original_task_list"),
                "is_repair": True,
                "repair_num": 1 if not state.get("repair_num") else state.get("repair_num") + 1,
            }
        )
    return Send(
        "code_verify",
        state
    )
