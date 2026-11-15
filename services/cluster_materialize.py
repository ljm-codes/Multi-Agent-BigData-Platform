"""项目资源配置：cluster_resources + profile -> src/main/resources。

拷贝源始终优先文件目录；DB 快照只保存基线和回滚依据。
项目 resources 允许任意相对路径文件；超过阈值的内容不进 DB。
"""
from __future__ import annotations

import hashlib
import posixpath
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from sqlalchemy import select, and_

from SPO.cluster_env_model import (
    ClusterProfile,
    ConfigDrift,
    ConfigSnapshot,
    EnvEvent,
    ProjectResource,
    PublicConfig,
)
from SPO.postgresql_model import AdminProject
from Tools.db import SessionLocal
from Tools.log_settings import LogSetting
from load_config.config import ROOT_BASE_DIR_PATH
from services.cluster_profile_store import (
    ALLOWED_FILES,
    ProfileStoreError,
    bundle_hash,
    normalize_relative_path,
    profile_store,
    sha256_bytes,
    validate_content,
)
from services.cluster_snapshot_store import snapshot_store

logger = LogSetting.create(__name__)

_IGNORED_NAMES = {".DS_Store", "Thumbs.db"}


def _project(session, project_id: str) -> AdminProject:
    project = session.execute(
        select(AdminProject).where(AdminProject.project_id == project_id)
    ).scalar_one_or_none()
    if project is None:
        raise ProfileStoreError("项目不存在", "not_found")
    return project


def _project_target(project: AdminProject) -> tuple[str, Path]:
    if not project.app_type or not project.project_date or not project.project_name:
        raise ProfileStoreError("项目缺少 app_type/project_date/project_name", "invalid")
    relative = (Path("tmp_code") / project.app_type / project.project_date /
                project.project_name / "src/main/resources")
    return relative.as_posix(), (ROOT_BASE_DIR_PATH / relative).resolve()


def _public_snapshot(session, public_snapshot_id: int) -> ConfigSnapshot:
    snapshot = session.get(ConfigSnapshot, int(public_snapshot_id))
    if snapshot is None or snapshot.scope != "public":
        raise ProfileStoreError("public snapshot 不存在", "not_found")
    return snapshot


def _public_state(admin_id: int, public_snapshot_id: Optional[int] = None):
    """返回 (public_snapshot, files)；文件读取优先 cluster_resources 目录。"""
    if public_snapshot_id is not None:
        with SessionLocal() as session:
            snapshot = _public_snapshot(session, public_snapshot_id)
            payload = snapshot_store.snapshot_payload(session, snapshot, include_content=False)
            files = snapshot_store.snapshot_files(session, snapshot)
            if any(content is None for content in files.values()):
                raise ProfileStoreError("public snapshot 内容已清空，请从归档恢复", "conflict")
            return payload, {name: content for name, content in files.items() if content}
    try:
        # 首次导入是系统 bootstrap，不把普通用户记成公有资源修改者
        snapshot_store.ensure_public(0, "materialize 前导入 cluster_resources")
    except ProfileStoreError as exc:
        logger.warning("cluster_resources 首次导入失败：%s", exc)
        raise
    drift = check_public(admin_id)
    if not drift.get("clean"):
        raise ProfileStoreError(
            "cluster_resources 与公有快照不一致，需要先处理 public drift", "conflict")
    public = snapshot_store.get_public(include_content=False)
    files = snapshot_store.read_public_files()
    return public, files


def _profile_files(admin_id: int, profile_id: int, revision: int) -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    for row in profile_store.list_files(admin_id, int(profile_id), int(revision)):
        data = profile_store.read_file(admin_id, int(profile_id), row["filename"], int(revision))
        files[row["filename"]] = data["content"].encode("utf-8")
    return files


def _write_env_files(target: Path, files: dict[str, bytes]) -> None:
    """只管理 allowlist 环境配置；项目里用户自己加的资源文件不动。"""
    target.mkdir(parents=True, exist_ok=True)
    existing = {path.name for path in target.iterdir()
                if path.is_file() and path.name in ALLOWED_FILES}
    for name in existing - set(files):
        (target / name).unlink(missing_ok=True)
    for name, content in files.items():
        validate_content(name, content, profile_store.max_file_bytes)
        tmp = target / f".{name}.tmp"
        tmp.write_bytes(content)
        tmp.replace(target / name)


def _iter_project_files(target: Path):
    if not target.is_dir():
        return
    for path in sorted(target.rglob("*")):
        if not path.is_file():
            continue
        name = path.name
        if name in _IGNORED_NAMES or name.endswith(".tmp"):
            continue
        yield path


def _scan_hashes(target: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for path in _iter_project_files(target):
        rel = path.relative_to(target).as_posix()
        hasher = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        result[rel] = hasher.hexdigest()
    return result


def _scan_project_files(target: Path) -> tuple[dict[str, bytes], dict[str, dict], dict[str, str]]:
    """返回 (小文件内容, 大文件元数据, 全部 hash)。大文件不读进内存。"""
    contents: dict[str, bytes] = {}
    extra: dict[str, dict] = {}
    hashes: dict[str, str] = {}
    limit = profile_store.max_file_bytes
    for path in _iter_project_files(target):
        rel = path.relative_to(target).as_posix()
        size = path.stat().st_size
        if size <= limit:
            content = path.read_bytes()
            if rel in ALLOWED_FILES:
                validate_content(rel, content, limit)
            contents[rel] = content
            hashes[rel] = sha256_bytes(content)
        else:
            hasher = hashlib.sha256()
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    hasher.update(chunk)
            sha = hasher.hexdigest()
            hashes[rel] = sha
            extra[rel] = {"sha256": sha, "size_bytes": size,
                          "mime_type": "application/octet-stream"}
    return contents, extra, hashes


def local_project_resources(admin_id: int, project_id: str) -> tuple[str, dict[str, bytes]]:
    """读项目当前 resources 里的小文件；只读，不写库、不调 SSH。"""
    with SessionLocal() as session:
        project = _project(session, project_id)
        if int(project.admin_id) != int(admin_id):
            raise ProfileStoreError("无权读取该项目的环境配置", "forbidden")
        target_rel, target = _project_target(project)
    contents, _, _ = _scan_project_files(target)
    return target_rel, contents


def _refresh_baseline(session, project: AdminProject, resource: ProjectResource, *,
                      source: str, reason: str, actor: int) -> dict:
    """用户显式替换/删除资源后，重算整个项目 resources 的基线。"""
    target_rel, target = _project_target(project)
    contents, extra, hashes = _scan_project_files(target)
    previous = session.get(ConfigSnapshot, resource.materialized_snapshot_id) \
        if resource.materialized_snapshot_id else None
    previous_files = ((previous.manifest or {}).get("files") or {}) if previous else {}
    sources = {
        name: ((previous_files.get(name) or {}).get("source") or "project")
        if (previous_files.get(name) or {}).get("sha256") == sha else "project"
        for name, sha in hashes.items()
    }
    if hashes:
        snapshot = snapshot_store.create_snapshot(
            session, scope="project", scope_key=f"project:{project.project_id}",
            files=contents, extra_files=extra, file_sources=sources,
            public_config_id=previous.public_config_id if previous else None,
            profile_id=resource.profile_id, project_id=project.project_id,
            source=source, parent=previous, actor=actor, reason=reason, relaxed=True,
        )
        resource.materialized_snapshot_id = snapshot.id
        resource.effective_hash = snapshot.bundle_hash
        resource.managed_files = {
            name: meta["sha256"]
            for name, meta in (snapshot.manifest.get("files") or {}).items()
        }
        snapshot_id = snapshot.id
    else:
        resource.effective_hash = bundle_hash({})
        resource.managed_files = {}
        snapshot_id = None
    resource.target_path = target_rel
    project.verify_status = "stale"
    session.add(EnvEvent(
        event_type="keep", project_id=project.project_id, actor=actor, reason=reason,
        diff={"kind": source, "snapshot_id": snapshot_id, "files": sorted(hashes)},
    ))
    return {"snapshot_id": snapshot_id, "files": sorted(hashes),
            "effective_hash": resource.effective_hash,
            "content_omitted": sorted(extra), "target_path": target_rel}


def replace_project_resource(admin_id: int, project_id: str, path: str, *,
                             content: Optional[bytes] = None, delete: bool = False,
                             reason: str = "") -> dict:
    """前端替换按钮：只改项目 src/main/resources，不碰 cluster_resources/。"""
    rel = normalize_relative_path(path)
    with SessionLocal() as session:
        try:
            project = _project(session, project_id)
            if int(project.admin_id) != int(admin_id):
                raise ProfileStoreError("无权修改该项目的资源", "forbidden")
            resource = session.get(ProjectResource, project_id)
            if resource is None:
                raise ProfileStoreError("项目尚未 materialize，不能替换资源", "not_found")
            _, target = _project_target(project)
            dst = (target / rel).resolve()
            if not dst.is_relative_to(target.resolve()):
                raise ProfileStoreError("非法资源路径")
            if delete:
                if not dst.is_file():
                    raise ProfileStoreError("文件不存在", "not_found")
                dst.unlink()
            else:
                if content is None:
                    raise ProfileStoreError("缺少文件内容")
                if len(content) > snapshot_store.max_total_bytes:
                    raise ProfileStoreError("单文件超过项目资源大小限制", "too_large")
                name = posixpath.basename(rel)
                if name in ALLOWED_FILES:
                    validate_content(name, content, profile_store.max_file_bytes)
                else:
                    from services.cluster_profile_store import validate_project_content
                    validate_project_content(rel, content, profile_store.max_file_bytes)
                dst.parent.mkdir(parents=True, exist_ok=True)
                tmp = dst.with_name(f".{dst.name}.tmp")
                tmp.write_bytes(content)
                tmp.replace(dst)
            data = _refresh_baseline(session, project, resource, source="user_replace",
                                     reason=reason or f"replace {rel}", actor=admin_id)
            session.commit()
            data.update({"project_id": project_id, "path": rel,
                         "verify_status": project.verify_status})
            try:
                from services.project_resource_guard import invalidate_project_guard
                invalidate_project_guard(project_id)
            except Exception:  # noqa: BLE001
                pass
            return data
        except Exception:
            session.rollback()
            raise


def archive_project(admin_id: int, project_id: str, reason: str = "") -> dict:
    """删会话归档项目：保留项目和 tmp_code 目录，只标 archived + 归档配置快照。"""
    snapshot_id = None
    project_dir = None
    with SessionLocal() as session:
        try:
            project = _project(session, project_id)
            if int(project.admin_id) != int(admin_id):
                raise ProfileStoreError("无权归档该项目", "forbidden")
            resource = session.get(ProjectResource, project_id)
            snapshot_id = resource.materialized_snapshot_id if resource else None
            project.status = "archived"
            if project.project_date and project.project_name:
                project_dir = str(Path("tmp_code") / project.app_type /
                                  project.project_date / project.project_name)
            session.add(EnvEvent(
                event_type="archive", project_id=project_id, actor=admin_id,
                reason=reason or "delete last conversation",
                diff={"snapshot_id": snapshot_id, "project_dir": project_dir},
            ))
            session.commit()
        except Exception:
            session.rollback()
            raise
    if snapshot_id:
        try:
            snapshot_store.archive_snapshot(admin_id, snapshot_id,
                                            reason or "archive project resources")
        except ProfileStoreError as exc:
            logger.warning("项目快照归档跳过：%s", exc)
    return {"project_id": project_id, "status": "archived",
            "project_dir": project_dir, "snapshot_id": snapshot_id}


def project_resource_info(admin_id: int, project_id: str) -> dict:
    with SessionLocal() as session:
        project = _project(session, project_id)
        if int(project.admin_id) != int(admin_id):
            raise ProfileStoreError("无权读取该项目", "forbidden")
        resource = session.get(ProjectResource, project_id)
        if project.project_date and project.project_name:
            target_rel, target = _project_target(project)
        else:
            target_rel, target = "", None
        contents, extra, hashes = _scan_project_files(target) if target is not None \
            else ({}, {}, {})
        text_contents: dict[str, str] = {}
        for rel, content in contents.items():
            try:
                text_contents[rel] = content.decode("utf-8")
            except UnicodeDecodeError:
                continue
        return {
            "project_id": project_id,
            "status": project.status,
            "verify_status": project.verify_status,
            "project_dir": ((Path("tmp_code") / project.app_type / project.project_date /
                             project.project_name).as_posix() if project.project_date and
                            project.project_name else None),
            "target_path": target_rel,
            "files": sorted(hashes),
            "contents": text_contents,
            "content_omitted": sorted(extra),
            "effective_hash": resource.effective_hash if resource else None,
            "snapshots": {
                "public_snapshot_id": resource.public_snapshot_id if resource else None,
                "profile_snapshot_id": resource.profile_snapshot_id if resource else None,
                "materialized_snapshot_id": resource.materialized_snapshot_id if resource else None,
            } if resource else None,
        }


def materialize_project(admin_id: int, project_id: str, *,
                        profile_id: Optional[int] = None,
                        profile_revision: Optional[int] = None,
                        public_snapshot_id: Optional[int] = None,
                        reason: str = "") -> dict:
    with SessionLocal() as session:
        try:
            project = _project(session, project_id)
            if int(project.admin_id) != int(admin_id):
                raise ProfileStoreError("无权操作该项目的环境配置", "forbidden")

            profile = None
            profile_files: dict[str, bytes] = {}
            profile_revision = int(profile_revision or 0)
            if profile_id is not None:
                profile = session.get(ClusterProfile, int(profile_id))
                if profile is None or int(profile.owner_admin_id) != int(admin_id):
                    raise ProfileStoreError("profile 不存在或无权访问", "not_found")
                profile_revision = int(profile_revision or profile.draft_revision)
                profile_files = _profile_files(admin_id, int(profile_id), profile_revision)

            public, public_files = _public_state(admin_id, public_snapshot_id)
            effective_env = {**public_files, **profile_files}
            target_rel, target = _project_target(project)
            if effective_env:
                _write_env_files(target, effective_env)

            contents, extra, hashes = _scan_project_files(target)
            if not hashes:
                raise ProfileStoreError("public/profile 都没有可 materialize 的配置", "invalid")

            sources: dict[str, str] = {}
            for name in hashes:
                if name in profile_files:
                    sources[name] = "profile"
                elif name in public_files:
                    sources[name] = "public"
                else:
                    sources[name] = "project"

            profile_snapshot = None
            if profile is not None and profile_files:
                parent_profile = session.get(ConfigSnapshot, profile.current_snapshot_id) \
                    if profile.current_snapshot_id else None
                profile_snapshot = snapshot_store.create_snapshot(
                    session, scope="profile", scope_key=f"profile:{profile.id}",
                    files=profile_files, profile_id=profile.id,
                    source="materialize", parent=parent_profile,
                    actor=admin_id, reason=reason,
                )
                profile.current_snapshot_id = profile_snapshot.id

            previous = session.get(ProjectResource, project.project_id)
            parent = session.get(ConfigSnapshot, previous.materialized_snapshot_id) \
                if previous is not None and previous.materialized_snapshot_id else None
            project_snapshot = snapshot_store.create_snapshot(
                session, scope="project", scope_key=f"project:{project_id}",
                files=contents, extra_files=extra,
                file_sources=sources,
                public_config_id=(public or {}).get("public_config_id"),
                profile_id=profile.id if profile is not None else None,
                project_id=project_id, source="materialize", parent=parent,
                actor=admin_id, reason=reason, relaxed=True,
            )
            resource = previous or ProjectResource(project_id=project_id)
            resource.profile_id = profile.id if profile is not None else None
            resource.profile_revision = profile_revision or None
            resource.public_snapshot_id = (public or {}).get("snapshot_id")
            resource.profile_snapshot_id = profile_snapshot.id if profile_snapshot else None
            resource.materialized_snapshot_id = project_snapshot.id
            resource.effective_hash = project_snapshot.bundle_hash
            resource.managed_files = {
                name: meta["sha256"]
                for name, meta in (project_snapshot.manifest.get("files") or {}).items()
            }
            resource.target_path = target_rel
            resource.created_by = resource.created_by or admin_id
            session.add(resource)
            project.verify_status = "unknown"
            session.add(EnvEvent(
                event_type="materialize", project_id=project_id,
                profile_id=profile.id if profile is not None else None,
                actor=admin_id, reason=reason,
                diff={"public_snapshot_id": resource.public_snapshot_id,
                      "profile_snapshot_id": resource.profile_snapshot_id,
                      "materialized_snapshot_id": project_snapshot.id,
                      "effective_hash": project_snapshot.bundle_hash,
                      "content_omitted": sorted(extra)},
            ))
            session.commit()
            try:
                from services.project_resource_guard import invalidate_project_guard
                invalidate_project_guard(project_id)
            except Exception:  # noqa: BLE001
                pass
            return {
                "project_id": project_id,
                "target_path": target_rel,
                "public_snapshot_id": resource.public_snapshot_id,
                "profile_snapshot_id": resource.profile_snapshot_id,
                "materialized_snapshot_id": project_snapshot.id,
                "effective_hash": project_snapshot.bundle_hash,
                "files": sorted(hashes),
                "content_omitted": sorted(extra),
            }
        except Exception:
            session.rollback()
            raise


def _public_drift_info(session, resource: ProjectResource) -> Optional[dict]:
    if resource.public_snapshot_id is None:
        return None
    snapshot = session.get(ConfigSnapshot, resource.public_snapshot_id)
    if snapshot is None:
        return None
    expected = {
        name: meta["sha256"]
        for name, meta in ((snapshot.manifest or {}).get("files") or {}).items()
    }
    try:
        current = snapshot_store.read_public_files()
    except ProfileStoreError as exc:
        return {"error": str(exc), "expected_snapshot_id": snapshot.id}
    actual = {name: sha256_bytes(content) for name, content in current.items()}
    if actual == expected:
        return None
    return {
        "scope": "public",
        "expected_snapshot_id": snapshot.id,
        "expected_hash": snapshot.bundle_hash,
        "actual_hash": snapshot_store.files_bundle(current),
        "actual_files": actual,
        "message": "cluster_resources 与项目记录的公有快照不一致",
    }


def _find_pending_project_drift(session, project_id: str,
                                actual_hash: str) -> Optional[ConfigDrift]:
    rows = session.execute(
        select(ConfigDrift)
        .where(
            and_(
                ConfigDrift.scope == "project",
                ConfigDrift.project_id == project_id,
                ConfigDrift.status == "pending")
        ).order_by(ConfigDrift.id.desc())
    ).scalars().all()
    for row in rows:
        if row.actual_hash == actual_hash:
            return row
    return None


def check_project(admin_id: int, project_id: str) -> dict:
    with SessionLocal() as session:
        try:
            project = _project(session, project_id)
            if int(project.admin_id) != int(admin_id):
                raise ProfileStoreError("无权读取该项目的环境配置", "forbidden")
            resource = session.get(ProjectResource, project_id)
            if resource is None:
                raise ProfileStoreError("项目尚未 materialize", "not_found")
            _, target = _project_target(project)
            actual_hashes = _scan_hashes(target)
            expected = resource.managed_files or {}
            public_drift = _public_drift_info(session, resource)
            project_clean = actual_hashes == expected
            if project_clean and public_drift is None:
                return {"clean": True, "project_id": project_id,
                        "effective_hash": resource.effective_hash}

            result = {
                "clean": False,
                "project_id": project_id,
                "target_path": resource.target_path,
                "expected_hash": resource.effective_hash,
                "public_drift": public_drift,
            }
            if not project_clean:
                actual_bundle = bundle_hash(actual_hashes)
                drift = _find_pending_project_drift(session, project_id, actual_bundle)
                if drift is None:
                    drift = ConfigDrift(
                        scope="project", project_id=project_id,
                        expected_snapshot_id=resource.materialized_snapshot_id,
                        expected_hash=resource.effective_hash,
                        actual_files=actual_hashes, actual_hash=actual_bundle,
                        status="pending",
                    )
                    session.add(drift)
                    session.flush()
                    session.add(EnvEvent(
                        event_type="drift", project_id=project_id, actor=admin_id,
                        reason="project resources changed",
                        diff={"drift_id": drift.id,
                              "expected_hash": resource.effective_hash,
                              "actual_hash": actual_bundle,
                              "files": sorted(actual_hashes)},
                    ))
                result.update({
                    "drift_id": drift.id,
                    "actual_hash": actual_bundle,
                    "actual_files": actual_hashes,
                })
            session.commit()
            return result
        except Exception:
            session.rollback()
            raise


def _restore_bytes(name: str, state: dict, resource: ProjectResource,
                   target: Path) -> Optional[bytes]:
    """按 source 优先从 profile revision / cluster_resources / DB / 当前项目文件恢复。"""
    name = normalize_relative_path(name)
    expected = state["sha256"]
    source = state.get("source")
    if source == "profile" and resource.profile_id and resource.profile_revision:
        path = profile_store.revision_dir(resource.profile_id, resource.profile_revision) / name
        if path.is_file():
            data = path.read_bytes()
            if sha256_bytes(data) == expected:
                return data
    if source == "public":
        path = snapshot_store.public_root / name
        if path.is_file():
            data = path.read_bytes()
            if sha256_bytes(data) == expected:
                return data
    content = state.get("content")
    if content is not None:
        return content
    path = target / name
    if path.is_file():
        data = path.read_bytes()
        if sha256_bytes(data) == expected:
            return data
    return None


def resolve_project_drift(admin_id: int, drift_id: int, decision: str,
                          note: str = "") -> dict:
    if decision not in {"keep", "rollback"}:
        raise ProfileStoreError("decision 只能是 keep 或 rollback")
    with SessionLocal() as session:
        try:
            drift = session.get(ConfigDrift, int(drift_id))
            if drift is None or drift.scope != "project" or drift.status != "pending":
                raise ProfileStoreError("drift 不存在或已处理", "not_found")
            project = _project(session, drift.project_id)
            if int(project.admin_id) != int(admin_id):
                raise ProfileStoreError("无权处理该项目的 drift", "forbidden")
            resource = session.get(ProjectResource, drift.project_id)
            if resource is None:
                raise ProfileStoreError("项目资源不存在", "not_found")
            _, target = _project_target(project)
            if decision == "keep":
                contents, extra, hashes = _scan_project_files(target)
                previous = session.get(ConfigSnapshot, resource.materialized_snapshot_id) \
                    if resource.materialized_snapshot_id else None
                previous_files = ((previous.manifest or {}).get("files") or {}) if previous else {}
                sources = {
                    name: (previous_files.get(name) or {}).get("source") or "project"
                    if (previous_files.get(name) or {}).get("sha256") == sha
                    else "project"
                    for name, sha in hashes.items()
                }
                if hashes:
                    snapshot = snapshot_store.create_snapshot(
                        session, scope="project", scope_key=f"project:{drift.project_id}",
                        files=contents, extra_files=extra, file_sources=sources,
                        public_config_id=previous.public_config_id if previous else None,
                        profile_id=resource.profile_id, project_id=drift.project_id,
                        source="drift_keep", parent=previous, actor=admin_id,
                        reason=note, relaxed=True,
                    )
                    resource.materialized_snapshot_id = snapshot.id
                    resource.effective_hash = snapshot.bundle_hash
                    resource.managed_files = {
                        name: meta["sha256"]
                        for name, meta in (snapshot.manifest.get("files") or {}).items()
                    }
                    snapshot_id = snapshot.id
                else:
                    resource.effective_hash = bundle_hash({})
                    resource.managed_files = {}
                    snapshot_id = None
                project.verify_status = "stale"
                drift.status = "kept"
                session.add(EnvEvent(
                    event_type="keep", project_id=drift.project_id, actor=admin_id,
                    reason=note, diff={"drift_id": drift.id, "snapshot_id": snapshot_id},
                ))
            else:
                snapshot = session.get(ConfigSnapshot, resource.materialized_snapshot_id) \
                    if resource.materialized_snapshot_id else None
                if snapshot is None:
                    raise ProfileStoreError("没有可回滚的 materialized snapshot", "not_found")
                states = snapshot_store.snapshot_file_states(session, snapshot)
                expected_names = set(states)
                for path in _iter_project_files(target):
                    rel = path.relative_to(target).as_posix()
                    if rel not in expected_names:
                        path.unlink(missing_ok=True)
                unrestorable: list[str] = []
                for name, state in states.items():
                    data = _restore_bytes(name, state, resource, target)
                    if data is None:
                        unrestorable.append(name)
                        continue
                    dst = target / normalize_relative_path(name)
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    tmp = dst.with_name(f".{dst.name}.tmp")
                    tmp.write_bytes(data)
                    tmp.replace(dst)
                project.verify_status = "unknown"
                drift.status = "rolled_back"
                session.add(EnvEvent(
                    event_type="rollback", project_id=drift.project_id, actor=admin_id,
                    reason=note, diff={"drift_id": drift.id,
                                       "snapshot_id": snapshot.id,
                                       "unrestorable": unrestorable},
                ))
                drift.resolved_by = admin_id
                drift.resolved_at = datetime_now()
                drift.note = note
                session.commit()
                try:
                    from services.project_resource_guard import invalidate_project_guard
                    invalidate_project_guard(drift.project_id)
                except Exception:  # noqa: BLE001
                    pass
                return {"drift_id": drift.id, "decision": decision,
                        "verify_status": project.verify_status,
                        "materialized_snapshot_id": resource.materialized_snapshot_id,
                        "unrestorable": unrestorable}
            drift.resolved_by = admin_id
            drift.resolved_at = datetime_now()
            drift.note = note
            session.commit()
            try:
                from services.project_resource_guard import invalidate_project_guard
                invalidate_project_guard(drift.project_id)
            except Exception:  # noqa: BLE001
                pass
            return {"drift_id": drift.id, "decision": decision,
                    "verify_status": project.verify_status,
                    "materialized_snapshot_id": resource.materialized_snapshot_id}
        except Exception:
            session.rollback()
            raise


def check_public(admin_id: int) -> dict:
    actual = snapshot_store.read_public_files()
    actual_hashes = {name: sha256_bytes(content) for name, content in actual.items()}
    actual_bundle = snapshot_store.files_bundle(actual)
    with SessionLocal() as session:
        try:
            public = session.execute(
                select(PublicConfig).where(PublicConfig.name == snapshot_store.public_config_name)
            ).scalar_one_or_none()
            current = session.get(ConfigSnapshot, public.current_snapshot_id) \
                if public is not None and public.current_snapshot_id else None
            if current is None:
                if not actual:
                    return {"clean": True, "public_config_id": public.id if public else None}
                drift = _find_pending_public_drift(session, public, actual_bundle)
                if drift is None:
                    drift = ConfigDrift(scope="public",
                                        public_config_id=public.id if public else None,
                                        actual_files=actual_hashes,
                                        actual_hash=actual_bundle,
                                        status="pending")
                    session.add(drift)
                    session.flush()
                    session.add(EnvEvent(
                        event_type="drift", actor=admin_id, reason="public resources changed",
                        diff={"drift_id": drift.id, "actual_hash": actual_bundle,
                              "files": sorted(actual_hashes)},
                    ))
                session.commit()
                return {"clean": False, "drift_id": drift.id,
                        "expected_hash": None, "actual_hash": actual_bundle,
                        "actual_files": actual_hashes}
            expected = {
                name: meta["sha256"]
                for name, meta in ((current.manifest or {}).get("files") or {}).items()
            }
            if actual_hashes == expected:
                return {"clean": True, "public_config_id": public.id,
                        "snapshot_id": current.id}
            drift = _find_pending_public_drift(session, public, actual_bundle)
            if drift is None:
                drift = ConfigDrift(scope="public", public_config_id=public.id,
                                    expected_snapshot_id=current.id,
                                    expected_hash=current.bundle_hash,
                                    actual_files=actual_hashes, actual_hash=actual_bundle,
                                    status="pending")
                session.add(drift)
                session.flush()
                session.add(EnvEvent(
                    event_type="drift", actor=admin_id, reason="public resources changed",
                    diff={"drift_id": drift.id, "expected_hash": current.bundle_hash,
                          "actual_hash": actual_bundle, "files": sorted(actual_hashes)},
                ))
            session.commit()
            return {"clean": False, "drift_id": drift.id,
                    "expected_hash": drift.expected_hash,
                    "actual_hash": actual_bundle,
                    "actual_files": actual_hashes}
        except Exception:
            session.rollback()
            raise


def _find_pending_public_drift(session, public: Optional[PublicConfig],
                               actual_hash: str) -> Optional[ConfigDrift]:
    stmt = select(ConfigDrift).where(
        and_(
            ConfigDrift.scope == "public",
            ConfigDrift.status == "pending",
            ConfigDrift.actual_hash == actual_hash,
        )
    )
    if public is not None:
        stmt = stmt.where(ConfigDrift.public_config_id == public.id)
    return session.execute(stmt.order_by(ConfigDrift.id.desc())).scalars().first()


def resolve_public_drift(admin_id: int, drift_id: int, decision: str,
                         note: str = "") -> dict:
    if decision not in {"keep", "rollback"}:
        raise ProfileStoreError("decision 只能是 keep 或 rollback")
    with SessionLocal() as session:
        drift = session.get(ConfigDrift, int(drift_id))
        if drift is None or drift.scope != "public" or drift.status != "pending":
            raise ProfileStoreError("drift 不存在或已处理", "not_found")
        if decision == "keep":
            snapshot_store.scan_public(admin_id, note)
        else:
            if drift.expected_snapshot_id is None:
                snapshot_store.write_public_files({})
            else:
                snapshot_store.rollback_public(admin_id, drift.expected_snapshot_id, note)
        with SessionLocal() as s:
            row = s.get(ConfigDrift, int(drift_id))
            row.status = "kept" if decision == "keep" else "rolled_back"
            row.resolved_by = admin_id
            row.resolved_at = datetime_now()
            row.note = note
            s.commit()
        return {"drift_id": drift_id, "decision": decision}


def bootstrap_cluster_env(admin_id: int = 0) -> dict:
    """应用启动：校验 cluster_resources；没有公有快照才初始导入，绝不覆盖已有快照。"""
    try:
        files = snapshot_store.read_public_files()
    except ProfileStoreError as exc:
        logger.error("cluster_resources 校验失败：%s", exc)
        return {"ok": False, "error": str(exc), "files": 0}
    if not files:
        return {"ok": True, "files": 0, "snapshot_id": None, "drift": None}
    try:
        snapshot_store.ensure_public(admin_id, "应用启动导入 cluster_resources")
        drift = check_public(admin_id)
        public = snapshot_store.get_public(include_content=False)
        return {"ok": True, "files": len(files),
                "snapshot_id": public["snapshot_id"] if public else None,
                "drift": drift if not drift.get("clean") else None}
    except ProfileStoreError as exc:
        logger.error("cluster_resources 初始化失败：%s", exc)
        return {"ok": False, "error": str(exc), "files": len(files)}


def datetime_now():
    return datetime.now(timezone.utc)
