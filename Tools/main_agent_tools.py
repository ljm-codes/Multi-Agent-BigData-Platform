import json

from langchain_core.tools import tool
from langgraph.prebuilt import ToolRuntime
from langgraph.types import interrupt

from SPO.main_agent_state_model import NeedAddInfo
from Tasks.project_registry import admin_id_of, project_registry
from Tools.log_settings import LogSetting
from Tools.registry import register_tool

AGENT_NAME = "MA"

logger = LogSetting.create(__name__)


@register_tool(AGENT_NAME)
@tool
def info_inquiry(need_add_info: NeedAddInfo) -> str:
    """
    当用户描述了一些信息时，但是用户没有提供足够的信息，那么将会调用该工具进行追问，来获取更多的信息
    参数：
        need_add_info: 需要追问的信息
    参数详解：
        - title：整体追问的标题（可选）比如说：追问一些详细的数据
        - description：整体追问的描述，比如说：相关的kafka、流程不详细，现在需要进行追问
        - info_inquiry：详细需要追加的信息(一个列表，可以一次询问一个或多个问题，每个元素是一个InfoInquiry对象，包含question、suggestion、suggestion_index)
            - question：需要向用户追问的问题，比如说：kafka的topic名称
            - suggestion：该问题的建议（可选，可以提出多个建议，但是不得超过3个建议），比如说：整体采用滚动窗口，整个窗口7天，每次滚动3天
            - suggestion_index：你建议采用哪个建议（可选），比如说：0，1，2，不建议采用其他索引
    注意：
        - 该工具是为了让用户的体验感更好，且更加直观，而不是一堆问题直接“甩”到用户上
        - 该工具允许用户自主提出做法，所以建议不用太多
    原理： 通过`中断`，将信息传给用户进行处理
    返回： 对应问题的答复
    """
    out_dict = {"title": need_add_info.title, "description": need_add_info.description}
    inquiry = need_add_info.info_inquiry
    inquiry_list = []
    for info in inquiry:
        len_suggestion = len(info.suggestion)
        if len_suggestion > 3:
            suggestion = info.suggestion[:3]
        else:
            suggestion = info.suggestion
        idx = info.suggestion_index
        if idx not in (0, 1, 2):
            idx = None
        inquiry_list.append(
            {
                "question": info.question,
                "suggestion": suggestion,
                "suggestion_index": idx
            }
        )
    out_dict["info_inquiry"] = inquiry_list
    answer = interrupt(out_dict)  # 用户将会返回 list({"question", "reply"})
    if not isinstance(answer, list):
        return "用户返回了错误的信息，请再次调用该工具，或者直接调用`DevelopAgentOutState`工具进行询问"

    human_answer = []
    for an in answer:
        question = an.get("question")
        reply = an.get("reply")
        if not question or not reply:
            continue
        human_answer.append(f"问题：{question}，对应的人工答复为：{reply}")
    return "\n".join(human_answer)


@register_tool(AGENT_NAME)
@tool
async def list_projects(runtime: ToolRuntime, keyword: str = "", limit: int = 20) -> str:
    """
    作用：列出当前用户登记过的历史项目/任务，用来定位用户说的“之前那个项目/最开始的任务”。
    参数：
        keyword：可选，按标题、项目名或描述模糊过滤；
        limit：最多返回条数，默认 20，最大 20。
    返回：JSON 字符串；每项含 project_id/title/description/app_type/date/project_name/
          status/verify_status/updated_at/exists/has_code。
          只有 exists=true 的项目才能直接复用；exists=false 时先告诉用户目录已不存在。
    注意：把选中的 project_id 填进 DevelopAgentOutState.project_id，不要自己编 project_id，否则会找不到项目。
    """
    admin_id = admin_id_of(runtime.context.user_id)
    if not admin_id:
        return "无法识别当前用户，不能查询历史项目"
    projects = await project_registry.list_projects(admin_id, keyword=keyword, limit=limit)
    if not projects:
        return "当前用户还没有登记过的历史项目"
    items = [
        {
            "project_id": item["project_id"],
            "title": item["title"],
            "description": (item.get("description") or "")[:300],
            "app_type": item["app_type"],
            "date": item["date"],
            "project_name": item["project_name"],
            "status": item["status"],
            "verify_status": item["verify_status"],
            "updated_at": item["updated_at"],
            "exists": item["exists"],
            "has_code": item["has_code"],
        }
        for item in projects
    ]
    return json.dumps({"projects": items}, ensure_ascii=False)
