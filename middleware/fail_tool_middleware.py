from langchain.agents.middleware import AgentMiddleware


class ToolFailMarkMiddleware(AgentMiddleware):
    """
    用于处理工具失败时的处理中间件
    做法：
        解析工具返回的json格式，判断是否有对应的失败标记

    """
    def __init__(self):
        super().__init__()
