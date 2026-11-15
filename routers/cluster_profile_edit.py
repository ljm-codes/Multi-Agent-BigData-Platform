"""集群 profile 直接编辑入口：按单个文件文本更新，不负责上传/契约/apply。"""
from fastapi import APIRouter, Request
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from SPO.route_results import ResultCode, RouteResponse
from Tools.log_settings import LogSetting
from services.cluster_profile_store import ProfileStoreError, profile_store

route = APIRouter()
logger = LogSetting.create(__name__)


class ClusterProfileFileEdit(BaseModel):
    content: str = Field(description="文件完整文本")
    expected_sha256: str = Field(default="", description="当前文件 sha256，用于乐观锁")
    reason: str = Field(default="", description="修改原因")


def _admin_id(request: Request) -> int | None:
    info = getattr(request.state, "info", None)
    if not isinstance(info, dict):
        return None
    try:
        return int(info.get("id"))
    except (TypeError, ValueError):
        return None


def _store_error(exc: ProfileStoreError) -> RouteResponse:
    code = {
        "not_found": ResultCode.NOT_FOUND,
        "forbidden": ResultCode.PERMISSION_ERROR,
        "conflict": ResultCode.INVALID_VALUE_ERROR,
        "invalid": ResultCode.INVALID_VALUE_ERROR,
        "too_large": ResultCode.UPLOAD_LIMIT_ERROR,
        "db": ResultCode.DB_ERROR,
    }.get(exc.code, ResultCode.SERVER_ERROR)
    return RouteResponse.error(code=code, msg=str(exc))


@route.put("/profiles/{profile_id}/files/{filename}")
async def edit_profile_file(
        request: Request,
        profile_id: int,
        filename: str,
        body: ClusterProfileFileEdit,
) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    content = body.content.encode("utf-8")
    if len(content) > profile_store.max_file_bytes:
        return RouteResponse.error(
            code=ResultCode.UPLOAD_LIMIT_ERROR, msg=f"{filename} 超过单文件大小限制"
        )
    try:
        data = await run_in_threadpool(
            profile_store.write_file, admin_id, profile_id, filename, content,
            body.expected_sha256 or None, body.reason,
        )
    except ProfileStoreError as exc:
        return _store_error(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("直接编辑 profile 文件失败: %s", exc)
        return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")
    return RouteResponse.ok(data=data)
