"""集群 profile 上传入口：只负责 multipart 接收与边界校验。

文件校验、sha256、revision、落盘和审计统一在 cluster_profile_store。
"""
from fastapi import APIRouter, File, Form, Request, UploadFile
from starlette.concurrency import run_in_threadpool

from SPO.route_results import ResultCode, RouteResponse
from Tools.log_settings import LogSetting
from services.cluster_profile_store import ProfileStoreError, profile_store

route = APIRouter()
logger = LogSetting.create(__name__)


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


@route.post("/profiles/{profile_id}/files/upload")
async def upload_profile_files(
        request: Request,
        profile_id: int,
        files: list[UploadFile] = File(description="XML / flink-conf.yaml / server.properties"),
        expected_revision: int | None = Form(default=None, description="乐观锁：当前 draft revision"),
        reason: str = Form(default="", description="修改原因"),
) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    if not files:
        return RouteResponse.error(code=ResultCode.NOT_VALUES_ERROR, msg="至少上传一个文件")

    payload: list[tuple[str, bytes]] = []
    total = 0
    for upload in files:
        content = await upload.read()
        if len(content) > profile_store.max_file_bytes:
            return RouteResponse.error(
                code=ResultCode.UPLOAD_LIMIT_ERROR,
                msg=f"{upload.filename} 超过单文件大小限制",
            )
        total += len(content)
        payload.append((upload.filename or "", content))

    try:
        data = await run_in_threadpool(
            profile_store.upload_files, admin_id, profile_id, payload,
            expected_revision, reason,
        )
    except ProfileStoreError as exc:
        return _store_error(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("上传 profile 文件失败: %s", exc)
        return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")

    return RouteResponse.ok(code=ResultCode.UPLOAD_SUCCESS, data=data)
