"""集群 profile 生命周期：创建/查询/文件读取。

上传和直接编辑分别在 cluster_profile_upload.py / cluster_profile_edit.py。
"""
from fastapi import APIRouter, Query, Request
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from SPO.route_results import ResultCode, RouteResponse
from Tools.log_settings import LogSetting
from services.cluster_profile_store import ProfileStoreError, profile_store

route = APIRouter()
logger = LogSetting.create(__name__)

_CODE_MAP = {
    "not_found": ResultCode.NOT_FOUND,
    "forbidden": ResultCode.PERMISSION_ERROR,
    "conflict": ResultCode.INVALID_VALUE_ERROR,
    "invalid": ResultCode.INVALID_VALUE_ERROR,
    "too_large": ResultCode.UPLOAD_LIMIT_ERROR,
    "db": ResultCode.DB_ERROR,
}


class ClusterProfileCreate(BaseModel):
    name: str = Field(description="profile 名称")
    description: str = Field(default="", description="描述")
    cluster_name: str = Field(default="", description="集群名称，默认同 profile 名称")
    fs_default_fs: str = Field(default="", description="fs.defaultFS，可选")
    hive_metastore_uris: str = Field(default="", description="hive.metastore.uris，可选")
    kafka_bootstrap: str = Field(default="", description="Kafka bootstrap，可选")


def _admin_id(request: Request) -> int | None:
    info = getattr(request.state, "info", None)
    if not isinstance(info, dict):
        return None
    try:
        return int(info.get("id"))
    except (TypeError, ValueError):
        return None


def _store_error(exc: ProfileStoreError) -> RouteResponse:
    return RouteResponse.error(code=_CODE_MAP.get(exc.code, ResultCode.SERVER_ERROR), msg=str(exc))


def _server_error(exc: Exception) -> RouteResponse:
    logger.exception("cluster profile 操作失败: %s", exc)
    return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")


@route.post("/profiles")
async def create_profile(request: Request, body: ClusterProfileCreate) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    try:
        data = await run_in_threadpool(
            profile_store.create_profile, admin_id, body.name, body.description,
            body.cluster_name, body.fs_default_fs, body.hive_metastore_uris,
            body.kafka_bootstrap,
        )
    except ProfileStoreError as exc:
        return _store_error(exc)
    except Exception as exc:  # noqa: BLE001
        return _server_error(exc)
    return RouteResponse.ok(data=data)


@route.get("/profiles")
async def list_profiles(request: Request) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    try:
        data = await run_in_threadpool(profile_store.list_profiles, admin_id)
    except Exception as exc:  # noqa: BLE001
        return _server_error(exc)
    return RouteResponse.ok(data=data)


@route.get("/profiles/{profile_id}")
async def get_profile(request: Request, profile_id: int) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    try:
        data = await run_in_threadpool(profile_store.get_profile, admin_id, profile_id)
    except ProfileStoreError as exc:
        return _store_error(exc)
    except Exception as exc:  # noqa: BLE001
        return _server_error(exc)
    return RouteResponse.ok(data=data)


@route.delete("/profiles/{profile_id}")
async def delete_profile(request: Request, profile_id: int,
                         reason: str = Query(default="")) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    try:
        data = await run_in_threadpool(
            profile_store.delete_profile, admin_id, profile_id, reason
        )
    except ProfileStoreError as exc:
        return _store_error(exc)
    except Exception as exc:  # noqa: BLE001
        return _server_error(exc)
    return RouteResponse.ok(data=data)


@route.get("/profiles/{profile_id}/files")
async def list_files(request: Request, profile_id: int,
                     revision: int | None = Query(default=None)) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    try:
        data = await run_in_threadpool(profile_store.list_files, admin_id, profile_id, revision)
    except ProfileStoreError as exc:
        return _store_error(exc)
    except Exception as exc:  # noqa: BLE001
        return _server_error(exc)
    return RouteResponse.ok(data=data)


@route.get("/profiles/{profile_id}/files/{filename}")
async def read_file(request: Request, profile_id: int, filename: str,
                    revision: int | None = Query(default=None)) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    try:
        data = await run_in_threadpool(
            profile_store.read_file, admin_id, profile_id, filename, revision
        )
    except ProfileStoreError as exc:
        return _store_error(exc)
    except Exception as exc:  # noqa: BLE001
        return _server_error(exc)
    return RouteResponse.ok(data=data)
