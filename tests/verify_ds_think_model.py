"""
这个测试的出现是由于为了解决 langchain-deepseek 的 ChatDeepSeek 类 的 _get_request_payload 方法
中并不支持 reasoning_content 返回参数，导致 大模型无法进行推理
"""
from langchain.agents import create_agent
from langchain.chat_models import init_chat_model
from langchain_deepseek import ChatDeepSeek
from langchain_core.messages import AIMessage, HumanMessage, BaseMessage, SystemMessage

if not getattr(ChatDeepSeek, "_reasoning_content_patched", False):
    _original_get_request_payload = ChatDeepSeek._get_request_payload


    def _patched_get_request_payload(self, input_, *, stop=None, **kwargs):
        payload = _original_get_request_payload(self, input_, stop=stop, **kwargs)

        try:
            messages = self._convert_input(input_).to_messages()
        except Exception:
            return payload

        payload_messages = payload.get("messages", [])
        # 优先按消息 id 匹配，退化为按索引匹配
        id_to_payload = {}
        for i, m in enumerate(payload_messages):
            mid = m.get("id") or m.get("tool_call_id")
            if mid:
                id_to_payload[mid] = m

        for i, msg in enumerate(messages):
            if not isinstance(msg, AIMessage):
                continue
            reasoning = msg.additional_kwargs.get("reasoning_content")
            if not reasoning:
                continue

            target = None
            if msg.id and msg.id in id_to_payload:
                target = id_to_payload[msg.id]
            elif i < len(payload_messages):
                target = payload_messages[i]

            if target is not None and target.get("role") == "assistant":
                target["reasoning_content"] = reasoning

        # print(f"patched payload: {payload}")
        return payload


    ChatDeepSeek._get_request_payload = _patched_get_request_payload
    ChatDeepSeek._reasoning_content_patched = True

from pydantic import BaseModel, Field
from typing import List
from load_config.config import config
from langchain_core.tools import tool


class Output(BaseModel):
    out_msg: str = Field(default="", description="输出的结果内容")
    user_name: str = Field(default="", description="用户姓名")
    user_hobby: str = Field(default="", description="用户爱好")
    user_age: int = Field(default=0, description="用户年龄")
    user_gender: str = Field(default="", description="用户性别")


model_config = config.get("model")

print(model_config.get("params"))

# 禁用 思考模式
params = model_config.get("params").copy()

params["extra_body"] = {
    "thinking": {
        "type": "disabled"
    }
}

print(params)


@tool
def get_weather(city: str) -> str:
    """
    获取城市的天气
    参数：
        city: 城市名称
    返回：
        该城市的天气情况
    """
    return f"{city}的天气是晴朗的"


ds_model = init_chat_model(
    model=model_config.get("name"),
    **params
)  # .bind_tools([Output, get_weather], tool_choice="any")

test_agent = create_agent(
    model=ds_model,
    tools=[get_weather]
)

if __name__ == '__main__':
    """
    测试结果：
        通过！！
    """
    history: List[BaseMessage] = []
    # schema_hint = Output.model_json_schema()
    # system_prompt = f"""你必须严格输出 JSON 格式，结构如下：
    # {schema_hint}
    # 只输出 JSON，不要包含任何其他文本。
    # """
    # history.append(SystemMessage(content=system_prompt))
    while True:
        user_input = input("请输入：")
        if user_input == "exit":
            break
        response = test_agent.invoke(
            {"messages": [HumanMessage(content=user_input)]},
            config={"configurable": {"thread_id": "123"}},
        )
        print(response)
        """
        测试结果：ds 思考模式下，不支持 with_structured_output 即 tool_choice 为 'Any' 的情况
            只允许通过 系统提示词 + "json_object" 来实现结构化输出的格式
            (来源于 deepseek 官方的 局限， 并不是来源于 langchain/langgraph 架构 的问题)
        
        结论：
            若使用思考模式：则放弃 结构化 或 在 系统提示词 中指定的 JSON 格式，并在 invoke 中指定 response_format={"type": "json_object"} 来实现结构化输出的格式
        """
        # history.append(HumanMessage(content=user_input))
        # # 尝试非工具 非agent 直接用 init_chat_model 调用
        # response = ds_model.invoke(
        #     history,
        #     config={"configurable": {"thread_id": "123"}},
        #     # response_format={"type": "json_object"}
        # )
        # history.append(response)
        # print(f"类型：{type(response)}")  # 原始调用 直接输出 AIMessage 类
        # print(response)
        # print(f"{Output.model_validate_json(response.content)}")

    # print(history)
    # print(test_agent.get_state({"configurable": {"thread_id": "123"}}).values)
