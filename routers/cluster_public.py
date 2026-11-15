"""公有集群资源配置：读取/scan/rollback。

公有资源修改（scan/rollback）只允许 permission == 2 的管理员。
"""
from fastapi import APIRouter, File, Form, Request, UploadFile
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from SPO.route_results import ResultCode, RouteResponse
from Tools.log_settings import LogSetting
from services.cluster_profile_store import ProfileStoreError
from services.cluster_materialize import check_public, resolve_public_drift
from services.cluster_snapshot_store import snapshot_store

route = APIRouter()
logger = LogSetting.create(__name__)


class PublicScanRequest(BaseModel):
    reason: str = Field(default="", description="scan 原因")


class PublicRollbackRequest(BaseModel):
    snapshot_id: int = Field(description="要回滚的 public snapshot id")
    reason: str = Field(default="", description="回滚原因")


class PublicDriftResolveRequest(BaseModel):
    drift_id: int = Field(description="config_drift id")
    decision: str = Field(description="keep / rollback")
    note: str = Field(default="")


class PublicFileEdit(BaseModel):
    content: str = Field(description="文件完整文本")
    reason: str = Field(default="")


def _info(request: Request) -> dict | None:
    info = getattr(request.state, "info", None)
    return info if isinstance(info, dict) else None


def _admin_id(request: Request) -> int | None:
    info = _info(request)
    try:
        return int(info.get("id")) if info else None
    except (TypeError, ValueError):
        return None


def _is_admin_2(request: Request) -> bool:
    info = _info(request)
    try:
        return info is not None and int(info.get("permission")) == 2
    except (TypeError, ValueError):
        return False


def _error(exc: ProfileStoreError) -> RouteResponse:
    code = {
        "not_found": ResultCode.NOT_FOUND,
        "conflict": ResultCode.INVALID_VALUE_ERROR,
        "invalid": ResultCode.INVALID_VALUE_ERROR,
        "too_large": ResultCode.UPLOAD_LIMIT_ERROR,
        "db": ResultCode.DB_ERROR,
    }.get(exc.code, ResultCode.SERVER_ERROR)
    return RouteResponse.error(code=code, msg=str(exc))


@route.get("/public")
async def get_public(request: Request) -> RouteResponse:
    if _admin_id(request) is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    try:
        data = await run_in_threadpool(snapshot_store.get_public, True)
    except Exception as exc:  # noqa: BLE001
        logger.exception("读取公有资源失败: %s", exc)
        return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")
    return RouteResponse.ok(data=data)


@route.get("/public/snapshots")
async def public_snapshots(request: Request) -> RouteResponse:
    if _admin_id(request) is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    if not _is_admin_2(request):
        return RouteResponse.error(code=ResultCode.PERMISSION_ERROR,
                                   msg="只有权限等级 2 的管理员才能查看公有快照")
    try:
        data = await run_in_threadpool(snapshot_store.list_public_snapshots)
    except Exception as exc:  # noqa: BLE001
        logger.exception("读取公有快照失败: %s", exc)
        return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")
    return RouteResponse.ok(data=data)


@route.put("/public/files/{filename}")
async def edit_public_file(request: Request, filename: str,
                           body: PublicFileEdit) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    if not _is_admin_2(request):
        return RouteResponse.error(code=ResultCode.PERMISSION_ERROR,
                                   msg="只有权限等级 2 的管理员才能修改公有资源")
    content = body.content.encode("utf-8")
    if len(content) > snapshot_store.max_file_bytes:
        return RouteResponse.error(code=ResultCode.UPLOAD_LIMIT_ERROR,
                                   msg=f"{filename} 超过单文件大小限制")
    try:
        data = await run_in_threadpool(
            snapshot_store.write_public_file, admin_id, filename, content, body.reason
        )
    except ProfileStoreError as exc:
        return _error(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("编辑公有配置文件失败: %s", exc)
        return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")
    return RouteResponse.ok(data=data)


@route.post("/public/files/upload")
async def upload_public_files(
        request: Request,
        files: list[UploadFile] = File(description="allowlist 配置文件"),
        reason: str = Form(default=""),
) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    if not _is_admin_2(request):
        return RouteResponse.error(code=ResultCode.PERMISSION_ERROR,
                                   msg="只有权限等级 2 的管理员才能修改公有资源")
    if not files:
        return RouteResponse.error(code=ResultCode.NOT_VALUES_ERROR, msg="至少上传一个文件")
    payload: list[tuple[str, bytes]] = []
    for upload in files:
        content = await upload.read()
        if len(content) > snapshot_store.max_file_bytes:
            return RouteResponse.error(code=ResultCode.UPLOAD_LIMIT_ERROR,
                                       msg=f"{upload.filename} 超过单文件大小限制")
        payload.append((upload.filename or "", content))
    try:
        data = await run_in_threadpool(
            snapshot_store.upload_public_files, admin_id, payload, reason
        )
    except ProfileStoreError as exc:
        return _error(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("上传公有配置文件失败: %s", exc)
        return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")
    return RouteResponse.ok(data=data)


@route.delete("/public/snapshots/{snapshot_id}")
async def delete_public_snapshot(request: Request, snapshot_id: int,
                                 reason: str = "") -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    if not _is_admin_2(request):
        return RouteResponse.error(code=ResultCode.PERMISSION_ERROR,
                                   msg="只有权限等级 2 的管理员才能删除公有快照")
    try:
        data = await run_in_threadpool(
            snapshot_store.delete_public_snapshot, admin_id, snapshot_id, reason
        )
    except ProfileStoreError as exc:
        return _error(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("删除公有快照失败: %s", exc)
        return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")
    return RouteResponse.ok(data=data)


@route.post("/public/scan")
async def scan_public(request: Request, body: PublicScanRequest) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    if not _is_admin_2(request):
        return RouteResponse.error(code=ResultCode.PERMISSION_ERROR,
                                   msg="只有权限等级 2 的管理员才能修改公有资源")
    try:
        data = await run_in_threadpool(snapshot_store.scan_public, admin_id, body.reason)
    except ProfileStoreError as exc:
        return _error(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("scan 公有资源失败: %s", exc)
        return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")
    return RouteResponse.ok(data=data)


@route.post("/public/rollback")
async def rollback_public(request: Request, body: PublicRollbackRequest) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    if not _is_admin_2(request):
        return RouteResponse.error(code=ResultCode.PERMISSION_ERROR,
                                   msg="只有权限等级 2 的管理员才能回滚公有资源")
    try:
        data = await run_in_threadpool(
            snapshot_store.rollback_public, admin_id, body.snapshot_id, body.reason
        )
    except ProfileStoreError as exc:
        return _error(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("回滚公有资源失败: %s", exc)
        return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")
    return RouteResponse.ok(data=data)


@route.get("/public/config-drift")
async def public_drift(request: Request) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    if not _is_admin_2(request):
        return RouteResponse.error(code=ResultCode.PERMISSION_ERROR,
                                   msg="只有权限等级 2 的管理员才能检查公有资源")
    try:
        data = await run_in_threadpool(check_public, admin_id)
    except ProfileStoreError as exc:
        return _error(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("检查公有资源 drift 失败: %s", exc)
        return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")
    return RouteResponse.ok(data=data)


@route.post("/public/config-drift/resolve")
async def public_drift_resolve(request: Request,
                               body: PublicDriftResolveRequest) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    if not _is_admin_2(request):
        return RouteResponse.error(code=ResultCode.PERMISSION_ERROR,
                                   msg="只有权限等级 2 的管理员才能处理公有资源")
    try:
        data = await run_in_threadpool(
            resolve_public_drift, admin_id, body.drift_id, body.decision, body.note
        )
    except ProfileStoreError as exc:
        return _error(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("处理公有资源 drift 失败: %s", exc)
        return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")
    return RouteResponse.ok(data=data)
