import asyncio
import posixpath
import shlex
from datetime import datetime, timezone
from typing import Literal
from urllib.parse import urlparse

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import select, func, and_, update, delete

from SPO.postgresql_model import AdminProject, AdminProjectConversation, AdminThread, AppFileIndex
from SPO.route_results import RouteResponse, ResultCode
from Tools import node_ssh
from Tools.db import SessionLocal
from Tools.job_command import split, validate_structure
from Tools.log_settings import LogSetting
from load_config.config import config

route = APIRouter()
logger = LogSetting.create(__name__)
CONNECT_TO_CLUSTER = bool((config.get("app") or {}).get("connect_to_cluster", True))


@route.get("/info")
async def get_project_info(
        request: Request, limit: int = 10, offset: int = 0,
        app_type: Literal["spark", "flink"] | None = None,
        status: Literal["active", "completed", "failed", "missing", "archived"] | None = None,
        verify_status: Literal['unknown', 'passed', 'failed', 'error'] | None = None,
        start_time: datetime | None = None, end_time: datetime | None = None,
) -> RouteResponse:
    """获取项目信息；筛选条件为空时返回全部。"""
    token_info = request.state.info
    if not token_info or not isinstance(token_info, dict):
        return RouteResponse.error(msg="token 信息错误，请重新登录获取", code=ResultCode.INVALID_VALUE_ERROR)
    admin_id = token_info.get("id")
    admin_name = token_info.get("username")
    permission = token_info.get("permission")
    if not admin_id or not admin_name or not permission:
        return RouteResponse.error(msg="token 信息错误，请重新登录获取", code=ResultCode.INVALID_VALUE_ERROR)
    try:
        admin_id = int(admin_id)
    except (ValueError, TypeError):
        return RouteResponse.error(msg="token 信息错误，请重新登录获取", code=ResultCode.INVALID_VALUE_ERROR)
    payload = {
        "limit": limit,
        "offset": offset,
    }
    filters = [AdminProject.admin_id == admin_id]
    if app_type:
        payload["app_type"] = app_type
        filters.append(AdminProject.app_type == app_type)
    if status:
        payload["status"] = status
        filters.append(AdminProject.status == status)
    if verify_status:
        payload["verify_status"] = verify_status
        filters.append(AdminProject.verify_status == verify_status)
    if start_time and end_time:
        payload["start_time"] = start_time
        payload["end_time"] = end_time
        filters.append(AdminProject.created_at.between(start_time, end_time))

    stmt = (select(AdminProject).where(*filters)
            .order_by(AdminProject.updated_at.desc())
            .limit(limit).offset(offset))
    count_stmt = select(func.count()).select_from(AdminProject).where(*filters)
    try:
        with SessionLocal() as session:
            result = session.execute(stmt).scalars().all()
            total = session.execute(count_stmt).scalar_one()
        payload["log_data"] = [
            {
                "id": row.id,
                "project_id": row.project_id,
                "admin_id": row.admin_id,
                "thread_id": row.thread_id,
                "title": row.title,
                "description": row.description,
                "summary": row.summary,
                "app_type": row.app_type,
                "project_date": row.project_date,
                "project_name": row.project_name,
                "start_message_id": row.start_message_id,
                "end_message_id": row.end_message_id,
                "status": row.status,
                "verify_status": row.verify_status,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None,
            }
            for row in result
        ]
        payload["total"] = len(result)
        payload["total_count"] = int(total or 0)
        return RouteResponse.ok(data=payload, msg="获取项目信息成功")
    except Exception as e:
        return RouteResponse.error(msg=f"在获取项目信息时出错({type(e).__name__})：{e}", code=ResultCode.DB_ERROR)


@route.get("/command/{project_id}")
async def get_command(
        request: Request, project_id: str,
) -> RouteResponse:
    """获取项目命令。"""
    token_info = request.state.info
    if not token_info or not isinstance(token_info, dict):
        return RouteResponse.error(msg="token 信息错误，请重新登录获取", code=ResultCode.INVALID_VALUE_ERROR)
    admin_id = token_info.get("id")
    admin_name = token_info.get("username")
    permission = token_info.get("permission")
    if not admin_id or not admin_name or not permission:
        return RouteResponse.error(msg="token 信息错误，请重新登录获取", code=ResultCode.INVALID_VALUE_ERROR)
    try:
        admin_id = int(admin_id)
    except (ValueError, TypeError):
        return RouteResponse.error(msg="token 信息错误，请重新登录获取", code=ResultCode.INVALID_VALUE_ERROR)
    # 先确认项目归属，再取最近一次登记的命令
    try:
        with SessionLocal() as session:
            owned = session.execute(
                select(AdminProject.id).where(
                    and_(
                        AdminProject.admin_id == admin_id,
                        AdminProject.project_id == project_id,
                    )
                )
            ).first()
            if not owned:
                return RouteResponse.error(msg="项目不存在或您没有权限访问", code=ResultCode.NOT_FOUND)
            row = session.execute(
                select(AppFileIndex)
                .where(AppFileIndex.project_id == project_id)
                .order_by(AppFileIndex.update_time.desc(), AppFileIndex.id.desc())
                .limit(1)
            ).scalar_one_or_none()
    except Exception as e:
        return RouteResponse.error(msg=f"在获取项目命令时出错({type(e).__name__})：{e}", code=ResultCode.DB_ERROR)
    if row is None or not (row.commit_cmd or "").strip():
        return RouteResponse.error(msg="该项目还没有登记提交命令", code=ResultCode.NOT_FOUND)
    return RouteResponse.ok(data={
        "project_id": project_id,
        "app_name": row.app_name,
        "app_type": (row.app_type or "").lower(),
        "command": row.commit_cmd,
        "idx_type": row.idx_type,
        "updated_at": row.update_time.isoformat() if row.update_time else None,
    }, msg="获取项目命令成功")


class CommandEditBody(BaseModel):
    command: str


def _require_admin(request: Request) -> tuple[int | None, RouteResponse | None]:
    token_info = request.state.info
    if not token_info or not isinstance(token_info, dict):
        return None, RouteResponse.error(msg="token 信息错误，请重新登录获取", code=ResultCode.INVALID_VALUE_ERROR)
    admin_id = token_info.get("id")
    admin_name = token_info.get("username")
    permission = token_info.get("permission")
    if not admin_id or not admin_name or not permission:
        return None, RouteResponse.error(msg="token 信息错误，请重新登录获取", code=ResultCode.INVALID_VALUE_ERROR)
    try:
        return int(admin_id), None
    except (ValueError, TypeError):
        return None, RouteResponse.error(msg="token 信息错误，请重新登录获取", code=ResultCode.INVALID_VALUE_ERROR)


def _project_of(session, admin_id: int, project_id: str) -> AdminProject | None:
    return session.execute(
        select(AdminProject).where(
            and_(
                AdminProject.admin_id == admin_id,
                AdminProject.project_id == project_id,
            )
        )
    ).scalar_one_or_none()


def _latest_command_of(session, project_id: str) -> AppFileIndex | None:
    return session.execute(
        select(AppFileIndex)
        .where(AppFileIndex.project_id == project_id)
        .order_by(AppFileIndex.update_time.desc(), AppFileIndex.id.desc())
        .limit(1)
    ).scalar_one_or_none()


def _expected_program(app_type: str) -> str:
    return "spark-submit" if app_type == "spark" else "flink"


def _client_host() -> str:
    manager = str(((config.get("hadoop") or {}).get("yarn") or {}).get("manager") or "").strip()
    if not manager:
        return ""
    if "://" not in manager:
        manager = f"http://{manager}"
    return urlparse(manager).hostname or ""


@route.put("/command/{project_id}/edit")
async def edit_command(
        request: Request, project_id: str, body: CommandEditBody,
) -> RouteResponse:
    """手动修改项目命令；保存前只校验程序名和 shell 控制符。"""
    admin_id, error = _require_admin(request)
    if error:
        return error
    try:
        with SessionLocal() as session:
            project = _project_of(session, admin_id, project_id)
            if project is None:
                return RouteResponse.error(msg="项目不存在或您没有权限访问", code=ResultCode.NOT_FOUND)
            row = _latest_command_of(session, project_id)
            if row is None or not (row.commit_cmd or "").strip():
                return RouteResponse.error(msg="该项目还没有登记提交命令", code=ResultCode.NOT_FOUND)
            app_type = project.app_type
            row_id = row.id
    except Exception as e:
        return RouteResponse.error(msg=f"在修改项目命令时出错({type(e).__name__})：{e}", code=ResultCode.DB_ERROR)

    try:
        tokens = split(body.command)
    except ValueError as e:
        return RouteResponse.error(msg=str(e), code=ResultCode.INVALID_VALUE_ERROR)
    expected = _expected_program(app_type)
    if posixpath.basename(tokens[0]) != expected:
        return RouteResponse.error(msg=f"{app_type} 项目的命令必须以 {expected} 开头",
                                   code=ResultCode.INVALID_VALUE_ERROR)
    structure_error = validate_structure(tokens, allow_placeholder=True)
    if structure_error:
        return RouteResponse.error(msg=structure_error, code=ResultCode.INVALID_VALUE_ERROR)
    command = shlex.join(tokens)
    try:
        with SessionLocal() as session:
            session.execute(
                update(AppFileIndex)
                .where(AppFileIndex.id == row_id)
                .values(commit_cmd=command, idx_type="tmp", update_time=datetime.now(timezone.utc))
            )
            session.commit()
    except Exception as e:
        return RouteResponse.error(msg=f"保存项目命令失败({type(e).__name__})：{e}", code=ResultCode.DB_ERROR)
    return RouteResponse.ok(data={
        "project_id": project_id,
        "command": command,
        "idx_type": "tmp",
    }, msg="命令已保存")


@route.post("/command/{project_id}/submit")
async def submit_command(request: Request, project_id: str) -> RouteResponse:
    """直接执行已保存的项目命令（SSH 到 RM 客户端节点）。"""
    admin_id, error = _require_admin(request)
    if error:
        return error
    try:
        with SessionLocal() as session:
            project = _project_of(session, admin_id, project_id)
            if project is None:
                return RouteResponse.error(msg="项目不存在或您没有权限访问", code=ResultCode.NOT_FOUND)
            row = _latest_command_of(session, project_id)
            if row is None or not (row.commit_cmd or "").strip():
                return RouteResponse.error(msg="该项目还没有登记提交命令", code=ResultCode.NOT_FOUND)
            app_type = project.app_type
            command = row.commit_cmd
            row_id = row.id
    except Exception as e:
        return RouteResponse.error(msg=f"在获取项目命令时出错({type(e).__name__})：{e}", code=ResultCode.DB_ERROR)

    if not CONNECT_TO_CLUSTER:
        return RouteResponse.error(msg="集群被禁用，无法提交命令", code=ResultCode.SERVER_ERROR)
    try:
        tokens = split(command)
    except ValueError as e:
        return RouteResponse.error(msg=str(e), code=ResultCode.INVALID_VALUE_ERROR)
    expected = _expected_program(app_type)
    if posixpath.basename(tokens[0]) != expected:
        return RouteResponse.error(msg=f"{app_type} 项目的命令必须以 {expected} 开头",
                                   code=ResultCode.INVALID_VALUE_ERROR)
    if any("<jar>" in tok for tok in tokens):
        return RouteResponse.error(msg="命令里的 jar 名还是 <jar>，请先打包或手动改成实际 jar 名",
                                   code=ResultCode.INVALID_VALUE_ERROR)
    structure_error = validate_structure(tokens)
    if structure_error:
        return RouteResponse.error(msg=structure_error, code=ResultCode.INVALID_VALUE_ERROR)
    command = shlex.join(tokens)

    host = _client_host()
    if not host:
        return RouteResponse.error(msg="无法从 hadoop.yarn.manager 解析提交主机", code=ResultCode.SERVER_ERROR)
    shell_command = f"{command} 2>&1 | tail -n 40"
    try:
        res = await asyncio.to_thread(
            node_ssh.run, host,
            f"bash -o pipefail -c {shlex.quote(shell_command)}",
            timeout=120,
        )
    except Exception as e:
        return RouteResponse.error(msg=f"执行命令失败({type(e).__name__})：{e}", code=ResultCode.SERVER_ERROR)
    if not res.ok:
        return RouteResponse.error(msg=f"SSH 执行失败：{res.message}", code=ResultCode.SERVER_ERROR,
                                   data={"project_id": project_id, "command": command})

    try:
        with SessionLocal() as session:
            session.execute(
                update(AppFileIndex)
                .where(AppFileIndex.id == row_id)
                .values(commit_cmd=command, idx_type="brief", update_time=datetime.now(timezone.utc))
            )
            session.commit()
    except Exception as e:
        logger.error(f"更新项目命令状态失败: {type(e).__name__}: {e}")

    data = {
        "project_id": project_id,
        "command": command,
        "idx_type": "brief",
        "executed": True,
        "exit_code": res.rc,
        "output": (res.stdout or "")[-4000:],
    }
    if res.rc != 0:
        return RouteResponse.error(msg=f"提交命令退出码 {res.rc}", code=ResultCode.SERVER_ERROR, data=data)
    return RouteResponse.ok(data=data, msg="命令已提交")


@route.delete("/{project_id}")
async def delete_project(request: Request, project_id: str) -> RouteResponse:
    """删除项目记录：相关会话和会话片段都为空才允许。"""
    admin_id, error = _require_admin(request)
    if error:
        return error
    try:
        with SessionLocal() as session:
            project = _project_of(session, admin_id, project_id)
            if project is None:
                return RouteResponse.error(msg="项目不存在或您没有权限访问", code=ResultCode.NOT_FOUND)

            thread_ids = {project.thread_id} if project.thread_id else set()
            thread_ids.update(
                t for t in session.execute(
                    select(AdminProjectConversation.thread_id).where(
                        AdminProjectConversation.project_id == project_id
                    )
                ).scalars().all() if t
            )
            live_threads = session.execute(
                select(AdminThread.thread_id).where(
                    AdminThread.thread_id.in_(list(thread_ids))
                )
            ).scalars().all() if thread_ids else []
            segment_count = session.scalar(
                select(func.count()).select_from(AdminProjectConversation).where(
                    AdminProjectConversation.project_id == project_id
                )
            ) or 0
            if live_threads or segment_count:
                return RouteResponse.error(
                    msg=(f"该项目还有 {len(live_threads)} 个相关会话、"
                         f"{int(segment_count)} 个会话片段，请先删除所有相关会话"),
                    code=ResultCode.INVALID_VALUE_ERROR,
                    data={
                        "project_id": project_id,
                        "thread_ids": sorted(live_threads),
                        "conversation_count": int(segment_count),
                    },
                )

            deleted_commands = session.execute(
                delete(AppFileIndex).where(AppFileIndex.project_id == project_id)
            ).rowcount or 0
            deleted_segments = session.execute(
                delete(AdminProjectConversation).where(
                    AdminProjectConversation.project_id == project_id
                )
            ).rowcount or 0
            session.execute(
                delete(AdminProject).where(AdminProject.project_id == project_id)
            )
            session.commit()
    except Exception as e:
        return RouteResponse.error(msg=f"删除项目失败({type(e).__name__})：{e}", code=ResultCode.DB_ERROR)
    return RouteResponse.ok(data={
        "project_id": project_id,
        "deleted_commands": int(deleted_commands),
        "deleted_conversations": int(deleted_segments),
    }, msg="项目已删除")
