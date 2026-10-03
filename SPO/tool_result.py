"""
Agent 工具的统一返回信封 —— 网关只认这个，不认工具内部抛的异常

存在的理由和 CheckOutcome 一样：**「取不到」和「没有」必须分开**。这三种实测形态
在 HTTP 层完全同形：
  * NM 的 containerlogs 对「文件不存在」也返回 200 + HTML（和空文件只差一个 <pre>）；
  * 应用结束后 YARN 代理回落成 HTML，状态码仍是 200；
  * ssh 的「没权限」和「grep 没匹配」都只表现为输出为空。
工具层不把它们分开，网关就只能一律当成功，Agent 也分不出「集群健康」和「没测到」。
"""
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

# 失败分类
ERR_TIMEOUT = "timeout"          # 可重试
ERR_REFUSED = "refused"          # 服务没起
ERR_AUTH = "auth"                # 凭据不对
ERR_PERMISSION = "permission"    # 连上了但没权限
ERR_PARSE = "parse"              # 拿到了但解析不了（多半是代理回落的 HTML）
ERR_UNSUPPORTED = "unsupported"  # 本环境这条路不成立
ERR_UNKNOWN = "unknown"

# 只有这几个类别值得重试
#   -> 来自 参数 网络问题 等基本错误，并非来自业务逻辑 代码 本身的错误 因此允许大模型整改后重试
RETRYABLE = {ERR_TIMEOUT, ERR_UNKNOWN}


class ToolResult(BaseModel):
    """
    status 的四种取值，含义互斥，**不许用空 data 表示 empty**：
        ok        拿到了
        empty     问到了，结果确实是空的（集群上真的没有运行中的应用）
        not_found 对象不存在（应用没了 / 容器没了 / 文件没了）
        error     没问到（超时 / 拒绝 / 权限 / 解析失败）
    """
    status: Literal["ok", "empty", "not_found", "error"] = Field(description="四态之一")
    data: Optional[Any] = Field(default=None, description="只有 ok 时有值")
    error_kind: str = Field(default="", description="只有 error 时有值，见 ERR_*")
    message: str = Field(default="", description="错误原因；error/not_found 时必填")
    retryable: bool = Field(default=False, description="可否能重试")
    truncated: bool = Field(default=False, description="返回体被截断了，不是全部")
    cost_ms: int = Field(default=0, description="本次耗时(毫秒) 非必需字段")


def ok(data: Any, *, cost_ms: int = 0, truncated: bool = False) -> ToolResult:
    return ToolResult(status="ok", data=data, cost_ms=cost_ms, truncated=truncated)


def empty(message: str, *, cost_ms: int = 0) -> ToolResult:
    return ToolResult(status="empty", message=message, cost_ms=cost_ms)


def not_found(message: str, *, cost_ms: int = 0) -> ToolResult:
    return ToolResult(status="not_found", message=message, cost_ms=cost_ms)


def fail(kind: str, message: str, *, cost_ms: int = 0) -> ToolResult:
    return ToolResult(status="error", error_kind=kind, message=message,
                      retryable=kind in RETRYABLE, cost_ms=cost_ms)
