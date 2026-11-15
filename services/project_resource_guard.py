"""项目 resources 的每次工具调用守卫：廉价签名 -> 内容 hash 二次判定。"""
from __future__ import annotations

import threading
from pathlib import Path

from sqlalchemy import select, and_

from SPO.cluster_env_model import ProjectResource
from SPO.postgresql_model import AdminProject
from Tools.db import SessionLocal
from Tools.log_settings import LogSetting
from services.cluster_materialize import check_project, _project_target
from services.cluster_profile_store import ALLOWED_FILES, sha256_bytes

logger = LogSetting.create(__name__)

_cache: dict[str, tuple[tuple, dict]] = {}
_lock = threading.Lock()
_IGNORED = {".DS_Store", "Thumbs.db"}


def _signature(target: Path) -> tuple:
    if not target.is_dir():
        return ()
    items = []
    for path in sorted(target.rglob("*")):
        if not path.is_file():
            continue
        name = path.name
        if name in _IGNORED or name.endswith(".tmp"):
            continue
        stat = path.stat()
        items.append((path.relative_to(target).as_posix(), stat.st_size,
                      stat.st_mtime_ns))
    return tuple(items)


def _target_and_baseline(admin_id: int, project_id: str):
    with SessionLocal() as session:
        project = session.execute(
            select(AdminProject).where(
                and_(
                    AdminProject.project_id == project_id,
                    AdminProject.admin_id == int(admin_id),
                )
            )
        ).scalar_one_or_none()
        if project is None:
            return None, None
        resource = session.get(ProjectResource, project_id)
        if resource is None:
            return None, None
        if not project.project_date or not project.project_name:
            return None, None
        _, target = _project_target(project)
        return target, dict(resource.managed_files or {})


def _config_hashes(target: Path) -> dict[str, str]:
    result = {}
    if not target.is_dir():
        return result
    for name in ALLOWED_FILES:
        path = target / name
        if path.is_file():
            result[name] = sha256_bytes(path.read_bytes())
    return result


def check_project_fast(admin_id: int, project_id: str) -> dict:
    target, baseline = _target_and_baseline(admin_id, project_id)
    if target is None:
        return {"clean": True, "skipped": True}
    signature = _signature(target)
    # 第二道闸：配置文件每次工具调用都做内容 hash，防止 mtime/size 被伪造。
    expected_config = {name: sha for name, sha in baseline.items()
                       if name in ALLOWED_FILES}
    config_changed = _config_hashes(target) != expected_config
    with _lock:
        cached = _cache.get(project_id)
    if not config_changed and cached is not None and cached[0] == signature:
        return cached[1]
    result = check_project(admin_id, project_id)
    with _lock:
        _cache[project_id] = (signature, result)
    return result


def invalidate_project_guard(project_id: str) -> None:
    with _lock:
        _cache.pop(project_id, None)
