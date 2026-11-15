"""快照归档管理员接口：列表/下载/删除/prune/restore/GC。

全部要求 permission == 2。
"""
from datetime import datetime

from fastapi import APIRouter, Query, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from SPO.route_results import ResultCode, RouteResponse
from Tools.log_settings import LogSetting
from services.cluster_profile_store import ProfileStoreError
from services.cluster_snapshot_store import snapshot_store

route = APIRouter()
logger = LogSetting.create(__name__)


class PruneRequest(BaseModel):
    before: datetime = Field(description="删除该时间之前的归档")
    dry_run: bool = Field(default=True)


class RestoreRequest(BaseModel):
    reason: str = Field(default="", description="恢复原因")


class GCRequest(BaseModel):
    dry_run: bool = Field(default=True)
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


def _deny() -> RouteResponse:
    return RouteResponse.error(code=ResultCode.PERMISSION_ERROR,
                               msg="只有权限等级 2 的管理员才能操作归档")


def _error(exc: ProfileStoreError) -> RouteResponse:
    code = {
        "not_found": ResultCode.NOT_FOUND,
        "conflict": ResultCode.INVALID_VALUE_ERROR,
        "invalid": ResultCode.INVALID_VALUE_ERROR,
        "too_large": ResultCode.UPLOAD_LIMIT_ERROR,
        "db": ResultCode.DB_ERROR,
    }.get(exc.code, ResultCode.SERVER_ERROR)
    return RouteResponse.error(code=code, msg=str(exc))


@route.get("/archives")
async def list_archives(request: Request,
                        include_deleted: bool = Query(default=False)) -> RouteResponse:
    if _admin_id(request) is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    if not _is_admin_2(request):
        return _deny()
    try:
        data = await run_in_threadpool(
            snapshot_store.list_archives, include_deleted
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("读取归档列表失败: %s", exc)
        return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")
    return RouteResponse.ok(data=data)


@route.get("/archives/{archive_id}/download")
async def download_archive(request: Request, archive_id: int):
    if _admin_id(request) is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    if not _is_admin_2(request):
        return _deny()
    try:
        path = await run_in_threadpool(snapshot_store.archive_path, archive_id)
    except ProfileStoreError as exc:
        return _error(exc)
    return FileResponse(path, media_type="application/gzip", filename=path.name)


@route.delete("/archives/{archive_id}")
async def delete_archive(request: Request, archive_id: int,
                         note: str = Query(default="")) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    if not _is_admin_2(request):
        return _deny()
    try:
        data = await run_in_threadpool(snapshot_store.delete_archive, admin_id, archive_id, note)
    except ProfileStoreError as exc:
        return _error(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("删除归档失败: %s", exc)
        return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")
    return RouteResponse.ok(data=data)


@route.post("/archives/prune")
async def prune_archives(request: Request, body: PruneRequest) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    if not _is_admin_2(request):
        return _deny()
    try:
        data = await run_in_threadpool(
            snapshot_store.prune_archives, admin_id, body.before, body.dry_run
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("prune 归档失败: %s", exc)
        return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")
    return RouteResponse.ok(data=data)


@route.post("/archives/{archive_id}/restore")
async def restore_archive(request: Request, archive_id: int,
                          body: RestoreRequest) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    if not _is_admin_2(request):
        return _deny()
    try:
        data = await run_in_threadpool(
            snapshot_store.restore_archive, admin_id, archive_id, body.reason
        )
    except ProfileStoreError as exc:
        return _error(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("恢复归档失败: %s", exc)
        return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")
    return RouteResponse.ok(data=data)


@route.post("/archives/gc")
async def run_gc(request: Request, body: GCRequest) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    if not _is_admin_2(request):
        return _deny()
    try:
        data = await run_in_threadpool(
            snapshot_store.run_gc, admin_id, body.dry_run, body.reason
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("快照 GC 失败: %s", exc)
        return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")
    return RouteResponse.ok(data=data)
