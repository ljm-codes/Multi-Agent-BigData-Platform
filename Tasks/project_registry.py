"""
项目/任务登记表。

约定：
    - 模型只接触 project_id；真实目录由 app_type/project_date/project_name 拼装并校验；
    - 写操作全部按 admin_id 过滤，禁止跨用户；
    - 所有 DB 操作走 AsyncSessionLocal，避免在事件循环里做同步 IO；
    - 任何 DB 异常只记日志、不打断对话。
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import and_, or_, select
from sqlalchemy.exc import IntegrityError

from SPO.postgresql_model import AdminProject, AdminProjectConversation
from Tools.db import AsyncSessionLocal
from Tools.log_settings import LogSetting
from load_config.config import ROOT_BASE_DIR_PATH

logger = LogSetting.create(__name__)

_MAX_TITLE = 128
_MAX_DESC = 1000
_MAX_SUMMARY = 1500
_MAX_LIMIT = 20


def admin_id_of(user_id: Any) -> int | None:
    try:
        return int(user_id)
    except (TypeError, ValueError):
        return None


def project_path(app_type: Any, project_date: Any, project_name: Any) -> Path | None:
    """由项目坐标拼出真实目录；任何非法坐标返回 None。"""
    app_type = str(app_type or "")
    project_date = str(project_date or "")
    project_name = str(project_name or "")
    if app_type not in ("spark", "flink") or not project_date or not project_name:
        return None
    if "/" in project_name or "\\" in project_name or ".." in project_name:
        return None
    root = (ROOT_BASE_DIR_PATH / "tmp_code").resolve()
    path = (root / app_type / project_date / project_name).resolve()
    return path if root in path.parents else None


def check_project_dir(app_type: Any, project_date: Any, project_name: Any) -> dict[str, Any]:
    """返回目录当前状态：exists / has_code；供调度和回填前校验。"""
    path = project_path(app_type, project_date, project_name)
    if path is None or not path.is_dir():
        return {"exists": False, "has_code": False}
    has_code = any(path.glob("src/main/java/**/*.java")) or any(path.glob("src/main/python/**/*.py"))
    return {"exists": True, "has_code": has_code}


def _clip(value: Any, limit: int) -> str:
    text = str(value or "").strip()
    return text[:limit]


def _row_dict(row: AdminProject, *, with_check: bool = True) -> dict[str, Any]:
    data = {
        "project_id": row.project_id,
        "title": row.title,
        "description": row.description or "",
        "summary": row.summary or "",
        "app_type": row.app_type,
        "date": row.project_date,
        "project_name": row.project_name,
        "thread_id": row.thread_id,
        "start_message_id": row.start_message_id,
        "end_message_id": row.end_message_id,
        "status": row.status,
        "verify_status": row.verify_status,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }
    if with_check:
        data.update(check_project_dir(row.app_type, row.project_date, row.project_name))
    return data


class ProjectRegistry:
    """项目登记、坐标回填、会话区间与目录校验。"""

    # ---------- 写 ----------
    @staticmethod
    async def create_project(
            *,
            admin_id: int,
            thread_id: str,
            app_type: str,
            title: str,
            description: str,
            start_message_id: str | None = None,
    ) -> dict[str, Any] | None:
        if app_type not in ("spark", "flink") or not admin_id:
            return None
        project_id = str(uuid.uuid4())
        try:
            async with AsyncSessionLocal() as session:
                row = AdminProject(
                    project_id=project_id,
                    admin_id=int(admin_id),
                    thread_id=str(thread_id or ""),
                    title=_clip(title, _MAX_TITLE) or "未命名项目",
                    description=_clip(description, _MAX_DESC),
                    app_type=app_type,
                    start_message_id=_clip(start_message_id, 64) or None,
                    status="active",
                    verify_status="unknown",
                )
                session.add(row)
                await session.commit()
                await session.refresh(row)
                return _row_dict(row, with_check=False)
        except Exception as exc:  # noqa: BLE001
            logger.warning("创建项目登记失败：%s: %s", type(exc).__name__, exc)
            return None

    @staticmethod
    async def set_coordinates(
            project_id: str,
            *,
            admin_id: int,
            app_type: str,
            project_date: str,
            project_name: str,
    ) -> bool:
        """initialize_project 真正建目录后回填坐标；按用户归属过滤。"""
        if not project_id or not admin_id or app_type not in ("spark", "flink"):
            return False
        try:
            async with AsyncSessionLocal() as session:
                row = await session.scalar(select(AdminProject).where(
                    and_(
                        AdminProject.project_id == project_id,
                        AdminProject.admin_id == int(admin_id),
                    )
                ))
                if row is None:
                    return False
                row.app_type = app_type
                row.project_date = str(project_date or "")
                row.project_name = str(project_name or "")
                row.updated_at = datetime.now(timezone.utc)
                await session.commit()
                return True
        except IntegrityError:
            # 同一用户下相同坐标已有一条记录：保留原行，不覆盖
            logger.warning("项目坐标已存在，跳过回填：project_id=%s %s/%s/%s",
                           project_id, app_type, project_date, project_name)
            return False
        except Exception as exc:  # noqa: BLE001
            logger.warning("回填项目坐标失败：%s: %s", type(exc).__name__, exc)
            return False

    @staticmethod
    async def update_meta(
            project_id: str,
            *,
            admin_id: int,
            title: str | None = None,
            description: str | None = None,
            summary: str | None = None,
    ) -> bool:
        if not project_id or not admin_id:
            return False
        try:
            async with AsyncSessionLocal() as session:
                row = await session.scalar(select(AdminProject).where(
                    and_(
                        AdminProject.project_id == project_id,
                        AdminProject.admin_id == int(admin_id),
                    )
                ))
                if row is None:
                    return False
                if title is not None and title.strip():
                    row.title = _clip(title, _MAX_TITLE)
                if description is not None and description.strip():
                    row.description = _clip(description, _MAX_DESC)
                if summary is not None and summary.strip():
                    row.summary = _clip(summary, _MAX_SUMMARY)
                row.updated_at = datetime.now(timezone.utc)
                await session.commit()
                return True
        except Exception as exc:  # noqa: BLE001
            logger.warning("更新项目元信息失败：%s: %s", type(exc).__name__, exc)
            return False

    @staticmethod
    async def update_verification(
            project_id: str,
            *,
            admin_id: int,
            verify_status: str,
            status: str | None = None,
    ) -> bool:
        """CVA 验证结束后更新 verify_status；可选同步项目生命周期状态。"""
        if not project_id or not admin_id:
            return False
        try:
            async with AsyncSessionLocal() as session:
                row = await session.scalar(select(AdminProject).where(
                    and_(
                        AdminProject.project_id == project_id,
                        AdminProject.admin_id == int(admin_id),
                    )
                ))
                if row is None:
                    return False
                if verify_status in ("unknown", "passed", "failed", "error"):
                    row.verify_status = verify_status
                if status in ("active", "completed", "failed", "missing", "archived"):
                    row.status = status
                row.updated_at = datetime.now(timezone.utc)
                await session.commit()
                return True
        except Exception as exc:  # noqa: BLE001
            logger.warning("更新项目验证状态失败：%s: %s", type(exc).__name__, exc)
            return False

    @staticmethod
    async def append_conversation(project_id: str, thread_id: str, start_message_id: str) -> bool:
        """记录项目在某个会话里的区间起点；同一段重复调用只刷新时间。"""
        if not project_id or not thread_id or not start_message_id:
            return False
        try:
            async with AsyncSessionLocal() as session:
                row = await session.scalar(select(AdminProjectConversation).where(
                    and_(
                        AdminProjectConversation.project_id == project_id,
                        AdminProjectConversation.thread_id == thread_id,
                        AdminProjectConversation.start_message_id == start_message_id,
                    )
                ))
                if row is None:
                    session.add(AdminProjectConversation(
                        project_id=project_id,
                        thread_id=thread_id,
                        start_message_id=start_message_id,
                    ))
                else:
                    row.updated_at = datetime.now(timezone.utc)
                project = await session.scalar(select(AdminProject).where(
                    AdminProject.project_id == project_id
                ))
                if project is not None:
                    # 新一轮开始：刷新最近使用时间，已完成/失败的项目重新置为 active
                    project.updated_at = datetime.now(timezone.utc)
                    if project.status in ("completed", "failed"):
                        project.status = "active"
                await session.commit()
                return True
        except Exception as exc:  # noqa: BLE001
            logger.warning("记录项目对话区间失败：%s: %s", type(exc).__name__, exc)
            return False

    @staticmethod
    async def finalize_turn(
            *,
            project_id: str,
            admin_id: int,
            thread_id: str,
            start_message_id: str,
            end_message_id: str | None,
            summary: str = "",
            status: str | None = None,
            verify_status: str | None = None,
    ) -> bool:
        """一轮结束时更新项目状态、摘要与对话区间终点。"""
        if not project_id or not admin_id:
            return False
        try:
            async with AsyncSessionLocal() as session:
                row = await session.scalar(select(AdminProject).where(
                    and_(
                        AdminProject.project_id == project_id,
                        AdminProject.admin_id == int(admin_id),
                    )
                ))
                if row is not None:
                    if end_message_id:
                        row.end_message_id = _clip(end_message_id, 64)
                    if summary.strip():
                        row.summary = _clip(summary, _MAX_SUMMARY)
                    if status in ("active", "completed", "failed", "missing", "archived"):
                        row.status = status
                    if verify_status in ("unknown", "passed", "failed", "error"):
                        row.verify_status = verify_status
                    row.updated_at = datetime.now(timezone.utc)
                if thread_id and start_message_id:
                    segment = await session.scalar(select(AdminProjectConversation).where(
                        and_(
                            AdminProjectConversation.project_id == project_id,
                            AdminProjectConversation.thread_id == thread_id,
                            AdminProjectConversation.start_message_id == start_message_id,
                        )
                    ))
                    if segment is None:
                        segment = AdminProjectConversation(
                            project_id=project_id,
                            thread_id=thread_id,
                            start_message_id=start_message_id,
                            end_message_id=_clip(end_message_id, 64) or None,
                        )
                        session.add(segment)
                    else:
                        if end_message_id:
                            segment.end_message_id = _clip(end_message_id, 64)
                        segment.updated_at = datetime.now(timezone.utc)
                await session.commit()
                return row is not None
        except Exception as exc:  # noqa: BLE001
            logger.warning("项目收尾落库失败：%s: %s", type(exc).__name__, exc)
            return False

    @staticmethod
    async def mark_missing(project_id: str, admin_id: int) -> None:
        try:
            async with AsyncSessionLocal() as session:
                row = await session.scalar(select(AdminProject).where(
                    and_(
                        AdminProject.project_id == project_id,
                        AdminProject.admin_id == int(admin_id),
                    )
                ))
                if row is not None:
                    row.status = "missing"
                    row.updated_at = datetime.now(timezone.utc)
                    await session.commit()
        except Exception as exc:  # noqa: BLE001
            logger.warning("标记项目缺失失败：%s: %s", type(exc).__name__, exc)

    # ---------- 读 ----------
    @staticmethod
    async def get_project(project_id: str, admin_id: int) -> dict[str, Any] | None:
        if not project_id or not admin_id:
            return None
        try:
            async with AsyncSessionLocal() as session:
                row = await session.scalar(select(AdminProject).where(
                    and_(
                        AdminProject.project_id == project_id,
                        AdminProject.admin_id == int(admin_id),
                    )
                ))
                return _row_dict(row) if row is not None else None
        except Exception as exc:  # noqa: BLE001
            logger.warning("查询项目失败：%s: %s", type(exc).__name__, exc)
            return None

    @staticmethod
    async def list_projects(
            admin_id: int,
            *,
            keyword: str = "",
            limit: int = 20,
    ) -> list[dict[str, Any]]:
        if not admin_id:
            return []
        size = max(1, min(int(limit or 20), _MAX_LIMIT))
        try:
            async with AsyncSessionLocal() as session:
                stmt = select(AdminProject).where(AdminProject.admin_id == int(admin_id))
                keyword = str(keyword or "").strip()
                if keyword:
                    like = f"%{keyword}%"
                    stmt = stmt.where(or_(
                        AdminProject.title.ilike(like),
                        AdminProject.project_name.ilike(like),
                        AdminProject.description.ilike(like),
                    ))
                stmt = stmt.order_by(AdminProject.updated_at.desc()).limit(size)
                rows = (await session.scalars(stmt)).all()
                return [_row_dict(row) for row in rows]
        except Exception as exc:  # noqa: BLE001
            logger.warning("查询项目列表失败：%s: %s", type(exc).__name__, exc)
            return []

    @staticmethod
    async def conversations(project_id: str, admin_id: int) -> list[dict[str, Any]]:
        """项目登记的所有对话区间；供 RAG/前端排查。"""
        if not project_id or not admin_id:
            return []
        try:
            async with AsyncSessionLocal() as session:
                owned = await session.scalar(select(AdminProject.id).where(
                    and_(
                        AdminProject.project_id == project_id,
                        AdminProject.admin_id == int(admin_id),
                    )
                ))
                if owned is None:
                    return []
                rows = (await session.scalars(
                    select(AdminProjectConversation)
                    .where(AdminProjectConversation.project_id == project_id)
                    .order_by(AdminProjectConversation.created_at.asc())
                )).all()
                return [
                    {
                        "thread_id": row.thread_id,
                        "start_message_id": row.start_message_id,
                        "end_message_id": row.end_message_id,
                        "created_at": row.created_at.isoformat() if row.created_at else None,
                    }
                    for row in rows
                ]
        except Exception as exc:  # noqa: BLE001
            logger.warning("查询项目对话区间失败：%s: %s", type(exc).__name__, exc)
            return []


project_registry = ProjectRegistry()
