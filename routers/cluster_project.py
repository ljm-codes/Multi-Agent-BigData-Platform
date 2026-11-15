"""项目资源 materialize / 项目级 config drift / 前端替换。"""
from fastapi import APIRouter, File, Form, Request, UploadFile
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from SPO.route_results import ResultCode, RouteResponse
from Tools.log_settings import LogSetting
from services.cluster_materialize import (
    check_project,
    materialize_project,
    project_resource_info,
    replace_project_resource,
    resolve_project_drift,
)
from services.cluster_profile_store import ProfileStoreError

route = APIRouter()
logger = LogSetting.create(__name__)


class MaterializeRequest(BaseModel):
    profile_id: int | None = Field(default=None)
    profile_revision: int | None = Field(default=None)
    public_snapshot_id: int | None = Field(default=None)
    reason: str = Field(default="")


class ProjectDriftResolveRequest(BaseModel):
    drift_id: int = Field(description="config_drift id")
    decision: str = Field(description="keep / rollback")
    note: str = Field(default="")


def _admin_id(request: Request) -> int | None:
    info = getattr(request.state, "info", None)
    if not isinstance(info, dict):
        return None
    try:
        return int(info.get("id"))
    except (TypeError, ValueError):
        return None


def _error(exc: ProfileStoreError) -> RouteResponse:
    code = {
        "not_found": ResultCode.NOT_FOUND,
        "forbidden": ResultCode.PERMISSION_ERROR,
        "conflict": ResultCode.INVALID_VALUE_ERROR,
        "invalid": ResultCode.INVALID_VALUE_ERROR,
        "too_large": ResultCode.UPLOAD_LIMIT_ERROR,
        "db": ResultCode.DB_ERROR,
    }.get(exc.code, ResultCode.SERVER_ERROR)
    return RouteResponse.error(code=code, msg=str(exc))


@route.post("/projects/{project_id}/materialize")
async def materialize(request: Request, project_id: str,
                      body: MaterializeRequest) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    try:
        data = await run_in_threadpool(
            materialize_project, admin_id, project_id,
            profile_id=body.profile_id,
            profile_revision=body.profile_revision,
            public_snapshot_id=body.public_snapshot_id,
            reason=body.reason,
        )
    except ProfileStoreError as exc:
        return _error(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("materialize 项目资源失败: %s", exc)
        return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")
    return RouteResponse.ok(data=data)


@route.get("/projects/{project_id}/resources")
async def project_resources(request: Request, project_id: str) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    try:
        data = await run_in_threadpool(project_resource_info, admin_id, project_id)
    except ProfileStoreError as exc:
        return _error(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("读取项目资源失败: %s", exc)
        return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")
    return RouteResponse.ok(data=data)


@route.post("/projects/{project_id}/resources/replace")
async def replace_resource(
        request: Request,
        project_id: str,
        path: str = Form(description="src/main/resources 下的相对路径"),
        delete: bool = Form(default=False),
        reason: str = Form(default=""),
        file: UploadFile | None = File(default=None),
) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    content = None
    if not delete:
        if file is None:
            return RouteResponse.error(code=ResultCode.NOT_VALUES_ERROR, msg="缺少文件")
        content = await file.read()
    try:
        data = await run_in_threadpool(
            replace_project_resource, admin_id, project_id, path,
            content=content, delete=delete, reason=reason,
        )
    except ProfileStoreError as exc:
        return _error(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("替换项目资源失败: %s", exc)
        return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")
    return RouteResponse.ok(data=data)


@route.get("/projects/{project_id}/config-drift")
async def project_drift(request: Request, project_id: str) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    try:
        data = await run_in_threadpool(check_project, admin_id, project_id)
    except ProfileStoreError as exc:
        return _error(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("检查项目 drift 失败: %s", exc)
        return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")
    return RouteResponse.ok(data=data)


@route.post("/projects/{project_id}/config-drift/resolve")
async def project_drift_resolve(request: Request, project_id: str,
                                body: ProjectDriftResolveRequest) -> RouteResponse:
    admin_id = _admin_id(request)
    if admin_id is None:
        return RouteResponse.error(code=ResultCode.UNAUTHORIZED, msg="未授权")
    try:
        data = await run_in_threadpool(
            resolve_project_drift, admin_id, body.drift_id, body.decision, body.note
        )
    except ProfileStoreError as exc:
        return _error(exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("处理项目 drift 失败: %s", exc)
        return RouteResponse.error(code=ResultCode.SERVER_ERROR, msg="服务器错误")
    return RouteResponse.ok(data=data)
