"""配置快照 / 公有资源 / 归档 / GC。

快照内容以 DB 的 config_snapshot_file.content 为准；归档文件在独立目录，管理员可删。
"""
from __future__ import annotations

import io
import json
import os
import tarfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from sqlalchemy import func, select, and_, or_

from SPO.cluster_env_model import (
    ClusterProfile,
    ConfigDrift,
    ConfigSnapshot,
    ConfigSnapshotArchive,
    ConfigSnapshotFile,
    EnvEvent,
    ProjectEnv,
    ProjectResource,
    PublicConfig,
)
from Tools.db import SessionLocal
from Tools.log_settings import LogSetting
from load_config.config import ROOT_BASE_DIR_PATH, config
from services.cluster_profile_store import (
    ALLOWED_FILES,
    ProfileStoreError,
    bundle_hash,
    normalize_filename,
    profile_store,
    sha256_bytes,
    validate_content,
    validate_project_content,
)

logger = LogSetting.create(__name__)

_CFG = config.get("cluster_env") or {}
_PUBLIC_CONFIG_NAME = "default"
_IGNORED_PUBLIC = {"README.md", "README.txt", ".gitkeep"}
_MAX_TOTAL_BYTES = int(_CFG.get("max_total_bytes") or 10 * 1024 * 1024)


def _resolve(path_value: str) -> Path:
    path = Path(path_value)
    if not path.is_absolute():
        path = ROOT_BASE_DIR_PATH / path
    return path.resolve()


PUBLIC_ROOT = _resolve(_CFG.get("public_resources_root") or "cluster_resources")
ARCHIVE_ROOT = _resolve(_CFG.get("snapshot_archive_dir") or "cluster_env_archives")
KEEP_VERSIONS = int(_CFG.get("snapshot_keep_versions") or 10)
RETENTION_DAYS = int(_CFG.get("snapshot_retention_days") or 30)
ARCHIVE_AUTO_PRUNE_DAYS = int(_CFG.get("archive_auto_prune_days") or 0)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ClusterSnapshotStore:
    def __init__(self, session_factory=SessionLocal, public_root: Path = PUBLIC_ROOT,
                 archive_root: Path = ARCHIVE_ROOT,
                 max_file_bytes: int = None,
                 max_total_bytes: int = _MAX_TOTAL_BYTES,
                 keep_versions: int = KEEP_VERSIONS,
                 retention_days: int = RETENTION_DAYS,
                 archive_auto_prune_days: int = ARCHIVE_AUTO_PRUNE_DAYS,
                 public_config_name: str = _PUBLIC_CONFIG_NAME):
        self.session_factory = session_factory
        self.public_root = Path(public_root).resolve()
        self.archive_root = Path(archive_root).resolve()
        self.public_config_name = public_config_name
        self.max_file_bytes = int(max_file_bytes or profile_store.max_file_bytes)
        self.max_total_bytes = int(max_total_bytes)
        self.keep_versions = int(keep_versions)
        self.retention_days = int(retention_days)
        self.archive_auto_prune_days = int(archive_auto_prune_days)
        self.public_root.mkdir(parents=True, exist_ok=True)
        self.archive_root.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ public
    def get_public(self, include_content: bool = True) -> Optional[dict]:
        with self.session_factory() as session:
            public = self._get_public_config(session, create=False)
            if public is None or public.current_snapshot_id is None:
                return None
            snapshot = session.get(ConfigSnapshot, public.current_snapshot_id)
            if snapshot is None:
                return None
            return self._snapshot_payload(session, snapshot, include_content=include_content)

    def ensure_public(self, admin_id: int, reason: str = "") -> Optional[dict]:
        """首次使用时把用户预先放进 public_root 的配置导入成快照。

        已有快照时绝不覆盖：中途改文件属于 drift，必须由用户决定保留/回滚。
        """
        current = self.get_public(include_content=False)
        if current is not None:
            return current
        files = self._read_public_files()
        if not files:
            return None
        return self.scan_public(admin_id, reason or "首次导入公有资源")

    def scan_public(self, admin_id: int, reason: str = "") -> dict:
        files = self._read_public_files()
        if not files:
            raise ProfileStoreError("公有资源目录没有任何可导入的配置文件", "invalid")
        bundle = self._files_bundle(files)
        with self.session_factory() as session:
            try:
                public = self._get_public_config(session, create=True)
                current = session.get(ConfigSnapshot, public.current_snapshot_id) \
                    if public.current_snapshot_id else None
                if current is not None and current.bundle_hash == bundle and current.content_purged_at is None:
                    return self._snapshot_payload(session, current, include_content=True)
                if current is not None:
                    current.status = "superseded"
                scope_key = f"public:{public.id}"
                snapshot = self._create_snapshot(
                    session, scope="public", scope_key=scope_key, files=files,
                    public_config_id=public.id,
                    source="public_scan", parent=current,
                    actor=admin_id, reason=reason,
                )
                public.current_snapshot_id = snapshot.id
                session.add(EnvEvent(
                    event_type="public_scan", actor=admin_id, reason=reason,
                    diff={"public_config_id": public.id, "snapshot_id": snapshot.id,
                          "version": snapshot.version, "bundle_hash": bundle},
                ))
                session.commit()
                return self._snapshot_payload(session, snapshot, include_content=True)
            except Exception:
                session.rollback()
                raise

    def write_public_file(self, admin_id: int, filename: str, content: bytes,
                          reason: str = "") -> dict:
        """管理员直接改 cluster_resources 里的单个配置文件，并生成新快照。"""
        name = normalize_filename(filename)
        validate_content(name, content, self.max_file_bytes)
        files = self._read_public_files()
        files[name] = content
        if sum(len(c) for c in files.values()) > self.max_total_bytes:
            raise ProfileStoreError("公有资源总大小超限", "too_large")
        self._write_public_files(files)
        return self.scan_public(admin_id, reason or f"edit {name}")

    def upload_public_files(self, admin_id: int,
                            uploads: list[tuple[str, bytes]], reason: str = "") -> dict:
        if not uploads:
            raise ProfileStoreError("至少上传一个文件")
        files = self._read_public_files()
        for filename, content in uploads:
            name = normalize_filename(filename)
            validate_content(name, content, self.max_file_bytes)
            files[name] = content
        if sum(len(c) for c in files.values()) > self.max_total_bytes:
            raise ProfileStoreError("公有资源总大小超限", "too_large")
        self._write_public_files(files)
        return self.scan_public(admin_id, reason or "upload public files")

    def list_public_snapshots(self, limit: int = 50) -> list[dict]:
        with self.session_factory() as session:
            rows = session.execute(
                select(ConfigSnapshot)
                .where(ConfigSnapshot.scope == "public")
                .order_by(ConfigSnapshot.version.desc())
                .limit(max(1, min(int(limit or 50), 200)))
            ).scalars().all()
            return [self._snapshot_payload(session, row, include_content=False)
                    for row in rows]

    def delete_public_snapshot(self, admin_id: int, snapshot_id: int,
                               reason: str = "") -> dict:
        """删除一条非当前公有快照；至少保留当前快照。"""
        with self.session_factory() as session:
            try:
                snapshot = session.get(ConfigSnapshot, int(snapshot_id))
                if snapshot is None or snapshot.scope != "public":
                    raise ProfileStoreError("公有快照不存在", "not_found")
                public = self._get_public_config(session, create=False)
                if public is None or snapshot.public_config_id != public.id:
                    raise ProfileStoreError("快照不属于当前公有配置", "invalid")
                if public.current_snapshot_id == snapshot.id:
                    raise ProfileStoreError("当前公有快照不能删除", "conflict")
                total = session.execute(
                    select(func.count()).select_from(ConfigSnapshot).where(
                        ConfigSnapshot.scope == "public"
                    )
                ).scalar_one() or 0
                if total <= 1:
                    raise ProfileStoreError("至少要保留一个公有快照", "conflict")
                if snapshot.pinned:
                    raise ProfileStoreError("快照已 pin，不能删除", "conflict")
                refs = []
                for model, column in (
                    (ProjectResource, ProjectResource.public_snapshot_id),
                    (ProjectEnv, ProjectEnv.public_snapshot_id),
                ):
                    row = session.execute(
                        select(model).where(column == snapshot.id).limit(1)
                    ).scalar_one_or_none()
                    if row is not None:
                        refs.append(model.__tablename__)
                drift = session.execute(
                    select(ConfigDrift).where(
                        ConfigDrift.expected_snapshot_id == snapshot.id,
                        ConfigDrift.status == "pending",
                    ).limit(1)
                ).scalar_one_or_none()
                if drift is not None:
                    refs.append("config_drift")
                if refs:
                    raise ProfileStoreError(
                        f"快照仍被引用，不能删除：{', '.join(sorted(set(refs)))}",
                        "conflict",
                    )
                archive = session.execute(
                    select(ConfigSnapshotArchive).where(
                        ConfigSnapshotArchive.snapshot_id == snapshot.id
                    )
                ).scalar_one_or_none()
                if archive is not None:
                    path = self._resolve_archive(Path(archive.path))
                    path.unlink(missing_ok=True)
                    path.with_suffix(path.suffix + ".manifest.json").unlink(missing_ok=True)
                    session.delete(archive)
                session.execute(
                    ConfigSnapshotFile.__table__.delete().where(
                        ConfigSnapshotFile.snapshot_id == snapshot.id
                    )
                )
                session.delete(snapshot)
                session.add(EnvEvent(
                    event_type="gc", actor=admin_id, reason=reason or "delete public snapshot",
                    diff={"action": "delete_public_snapshot", "snapshot_id": snapshot.id,
                          "version": snapshot.version},
                ))
                session.commit()
                return {"snapshot_id": snapshot_id, "deleted": True}
            except Exception:
                session.rollback()
                raise

    def rollback_public(self, admin_id: int, snapshot_id: int, reason: str = "") -> dict:
        with self.session_factory() as session:
            try:
                snapshot = session.get(ConfigSnapshot, int(snapshot_id))
                if snapshot is None or snapshot.scope != "public":
                    raise ProfileStoreError("public snapshot 不存在", "not_found")
                public = self._get_public_config(session, create=True)
                if snapshot.public_config_id != public.id:
                    raise ProfileStoreError("snapshot 不属于当前 public config", "invalid")
                files = self._snapshot_files(session, snapshot)
                if any(content is None for content in files.values()):
                    raise ProfileStoreError("snapshot 内容已清空，请从归档恢复", "conflict")
                current = session.get(ConfigSnapshot, public.current_snapshot_id) \
                    if public.current_snapshot_id else None
                if current is not None and current.id != snapshot.id:
                    current.status = "superseded"
                snapshot.status = "active"
                public.current_snapshot_id = snapshot.id
                self._write_public_files(files)
                session.add(EnvEvent(
                    event_type="public_rollback", actor=admin_id, reason=reason,
                    diff={"public_config_id": public.id, "snapshot_id": snapshot.id,
                          "version": snapshot.version},
                ))
                session.commit()
                return self._snapshot_payload(session, snapshot, include_content=True)
            except Exception:
                session.rollback()
                raise

    # ------------------------------------------------------------------ snapshot core
    def _create_snapshot(self, session, *, scope: str, scope_key: str, files: dict[str, bytes],
                         public_config_id: Optional[int] = None, profile_id: Optional[int] = None,
                         project_id: Optional[str] = None, source: str,
                         parent: Optional[ConfigSnapshot] = None,
                         actor: Optional[int] = None, reason: str = "",
                         relaxed: bool = False,
                         file_sources: Optional[dict[str, str]] = None,
                         extra_files: Optional[dict[str, dict]] = None) -> ConfigSnapshot:
        """relaxed=True 用于项目 resources：允许任意相对路径，大文件只存 hash。

        extra_files: 只带元数据的大文件 {path: {sha256,size_bytes,mime_type}}。
        """
        extra_files = extra_files or {}
        if not files and not extra_files:
            raise ProfileStoreError("快照不能为空", "invalid")
        if not relaxed and sum(len(c) for c in files.values()) > self.max_total_bytes:
            raise ProfileStoreError("快照总大小超限", "too_large")
        version = (session.execute(
            select(func.max(ConfigSnapshot.version)).where(ConfigSnapshot.scope_key == scope_key)
        ).scalar_one_or_none() or 0) + 1
        file_meta = {}
        stored_bytes = 0
        for name in sorted(set(files) | set(extra_files)):
            if name in extra_files:
                extra = extra_files[name]
                file_meta[name] = {
                    "sha256": extra["sha256"], "size_bytes": int(extra["size_bytes"]),
                    "mime_type": extra.get("mime_type") or "application/octet-stream",
                    "content_omitted": True,
                    "source": (file_sources or {}).get(name),
                }
                continue
            content = files[name]
            if relaxed:
                # 已知配置仍严格校验；大文件不解析内容
                if name in ALLOWED_FILES or len(content) <= self.max_file_bytes:
                    mime = validate_project_content(name, content, self.max_file_bytes)
                else:
                    mime = "application/octet-stream"
                omit = (len(content) > self.max_file_bytes
                        or stored_bytes + len(content) > self.max_total_bytes)
            else:
                mime = validate_content(name, content, self.max_file_bytes)
                omit = False
            if not omit:
                stored_bytes += len(content)
            file_meta[name] = {"sha256": sha256_bytes(content), "size_bytes": len(content),
                               "mime_type": mime, "content_omitted": omit,
                               "source": (file_sources or {}).get(name)}
        bundle = bundle_hash({name: meta["sha256"] for name, meta in file_meta.items()})
        snapshot = ConfigSnapshot(
            scope=scope, scope_key=scope_key,
            public_config_id=public_config_id, profile_id=profile_id, project_id=project_id,
            version=version, bundle_hash=bundle,
            manifest={"files": file_meta, "source": source,
                      "parent_snapshot_id": parent.id if parent else None},
            source=source,
            parent_snapshot_id=parent.id if parent else None,
            actor=actor, reason=reason,
            retained_until=_utcnow() + timedelta(days=self.retention_days),
        )
        session.add(snapshot)
        session.flush()
        for name in sorted(file_meta):
            meta = file_meta[name]
            session.add(ConfigSnapshotFile(
                snapshot_id=snapshot.id, filename=name, sha256=meta["sha256"],
                size_bytes=meta["size_bytes"], mime_type=meta["mime_type"],
                content=None if meta["content_omitted"] else files[name],
                content_omitted=meta["content_omitted"],
            ))
        session.flush()
        return snapshot

    def _snapshot_file_states(self, session, snapshot: ConfigSnapshot) -> dict[str, dict]:
        rows = session.execute(
            select(ConfigSnapshotFile)
            .where(ConfigSnapshotFile.snapshot_id == snapshot.id)
            .order_by(ConfigSnapshotFile.filename)
        ).scalars().all()
        manifest_files = (snapshot.manifest or {}).get("files") or {}
        result: dict[str, dict] = {}
        for row in rows:
            content = row.content
            if content is not None and sha256_bytes(bytes(content)) != row.sha256:
                raise ProfileStoreError(f"快照文件哈希不一致: {row.filename}", "db")
            result[row.filename] = {
                "sha256": row.sha256,
                "size_bytes": row.size_bytes,
                "mime_type": row.mime_type,
                "content": bytes(content) if content is not None else None,
                "content_omitted": bool(row.content_omitted),
                "source": (manifest_files.get(row.filename) or {}).get("source"),
            }
        return result

    def _snapshot_files(self, session, snapshot: ConfigSnapshot) -> dict[str, Optional[bytes]]:
        states = self._snapshot_file_states(session, snapshot)
        return {name: state["content"] for name, state in states.items()}

    def _snapshot_payload(self, session, snapshot: ConfigSnapshot, *,
                          include_content: bool = False) -> dict:
        states = self._snapshot_file_states(session, snapshot)
        data = {
            "snapshot_id": snapshot.id,
            "scope": snapshot.scope,
            "public_config_id": snapshot.public_config_id,
            "profile_id": snapshot.profile_id,
            "project_id": snapshot.project_id,
            "version": snapshot.version,
            "bundle_hash": snapshot.bundle_hash,
            "source": snapshot.source,
            "status": snapshot.status,
            "pinned": snapshot.pinned,
            "created_at": snapshot.created_at.isoformat() if snapshot.created_at else None,
            "content_purged_at": snapshot.content_purged_at.isoformat()
            if snapshot.content_purged_at else None,
            "files": [
                {"filename": name, "size_bytes": state["size_bytes"],
                 "sha256": state["sha256"],
                 "content_omitted": state["content_omitted"],
                 "source": state["source"],
                 "purged": state["content"] is None and not state["content_omitted"]}
                for name, state in states.items()
            ],
        }
        if include_content:
            data["contents"] = {
                name: state["content"].decode("utf-8")
                if state["content"] is not None else None
                for name, state in states.items()
            }
        return data

    # ------------------------------------------------------------------ public files
    def _read_public_files(self) -> dict[str, bytes]:
        self.public_root.mkdir(parents=True, exist_ok=True)
        files: dict[str, bytes] = {}
        total = 0
        for path in sorted(self.public_root.iterdir()):
            if path.is_dir() or path.name.startswith(".") or path.name in _IGNORED_PUBLIC:
                continue
            name = normalize_filename(path.name)
            content = path.read_bytes()
            validate_content(name, content, self.max_file_bytes)
            total += len(content)
            if total > self.max_total_bytes:
                raise ProfileStoreError("公有资源总大小超限", "too_large")
            files[name] = content
        return files

    def _write_public_files(self, files: dict[str, bytes]) -> None:
        self.public_root.mkdir(parents=True, exist_ok=True)
        for path in sorted(self.public_root.iterdir()):
            if path.is_file() and path.name in ALLOWED_FILES and path.name not in files:
                path.unlink(missing_ok=True)
        for name, content in files.items():
            validate_content(name, content, self.max_file_bytes)
            target = self.public_root / name
            tmp = target.with_name(f".{name}.tmp")
            tmp.write_bytes(content)
            os.replace(tmp, target)

    @staticmethod
    def _files_bundle(files: dict[str, bytes]) -> str:
        return bundle_hash({name: sha256_bytes(content) for name, content in files.items()})

    # ------------------------------------------------------------------ archive
    def archive_snapshot(self, admin_id: int, snapshot_id: int, reason: str = "") -> dict:
        with self.session_factory() as session:
            try:
                snapshot = session.get(ConfigSnapshot, int(snapshot_id))
                if snapshot is None:
                    raise ProfileStoreError("snapshot 不存在", "not_found")
                data = self._archive_in_session(session, snapshot, admin_id, reason)
                session.commit()
                return data
            except Exception:
                session.rollback()
                raise

    def _archive_in_session(self, session, snapshot: ConfigSnapshot,
                            actor: Optional[int], reason: str) -> dict:
        existing = session.execute(
            select(ConfigSnapshotArchive).where(ConfigSnapshotArchive.snapshot_id == snapshot.id)
        ).scalar_one_or_none()
        if existing is not None and existing.status == "archived":
            return self._archive_payload(existing)
        states = self._snapshot_file_states(session, snapshot)
        missing = [name for name, state in states.items()
                   if state["content"] is None and not state["content_omitted"]]
        if missing:
            raise ProfileStoreError("快照内容已清空，无法再归档", "conflict")
        archived = {name: state["content"] for name, state in states.items()
                    if state["content"] is not None}
        omitted = [name for name, state in states.items() if state["content_omitted"]]
        now = _utcnow()
        relative = (Path(snapshot.scope) / f"{now:%Y}" / f"{now:%m}" /
                    f"snapshot_{snapshot.id}_{snapshot.bundle_hash}.tar.gz")
        target = self._resolve_archive(relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_id": snapshot.id, "scope": snapshot.scope,
            "scope_key": snapshot.scope_key, "version": snapshot.version,
            "bundle_hash": snapshot.bundle_hash, "source": snapshot.source,
            "parent_snapshot_id": snapshot.parent_snapshot_id,
            "created_at": snapshot.created_at.isoformat() if snapshot.created_at else None,
            "files": {name: state["sha256"] for name, state in states.items()},
            "archived_files": sorted(archived),
            "content_omitted": sorted(omitted),
        }
        tmp = target.with_name(f".{target.name}.tmp")
        try:
            with tarfile.open(tmp, "w:gz") as tar:
                manifest_bytes = json.dumps(manifest, ensure_ascii=False,
                                            sort_keys=True).encode("utf-8")
                info = tarfile.TarInfo("manifest.json")
                info.size = len(manifest_bytes)
                info.mtime = int(now.timestamp())
                tar.addfile(info, io.BytesIO(manifest_bytes))
                for name, content in archived.items():
                    info = tarfile.TarInfo(name)
                    info.size = len(content)
                    info.mtime = int(now.timestamp())
                    tar.addfile(info, io.BytesIO(content))
            with target.with_suffix(target.suffix + ".manifest.json").open("w", encoding="utf-8") as f:
                json.dump(manifest, f, ensure_ascii=False, sort_keys=True, indent=2)
            archive_bytes = tmp.read_bytes()
            archive_sha = sha256_bytes(archive_bytes)
            self._verify_archive(tmp, manifest)
            os.replace(tmp, target)
        except Exception:
            tmp.unlink(missing_ok=True)
            raise
        archive = existing or ConfigSnapshotArchive(snapshot_id=snapshot.id, scope=snapshot.scope,
                                                    path=relative.as_posix())
        archive.path = relative.as_posix()
        archive.sha256 = archive_sha
        archive.size_bytes = len(archive_bytes)
        archive.status = "archived"
        archive.created_by = actor
        archive.note = reason
        session.add(archive)
        session.add(EnvEvent(
            event_type="archive", actor=actor, reason=reason,
            diff={"snapshot_id": snapshot.id, "path": relative.as_posix(),
                  "sha256": archive_sha},
        ))
        session.flush()
        return self._archive_payload(archive)

    @staticmethod
    def _verify_archive(path: Path, manifest: dict) -> None:
        with tarfile.open(path, "r:gz") as tar:
            member = tar.extractfile("manifest.json")
            if member is None:
                raise ProfileStoreError("归档缺少 manifest.json", "db")
            got = json.loads(member.read().decode("utf-8"))
            if got.get("bundle_hash") != manifest.get("bundle_hash"):
                raise ProfileStoreError("归档 manifest 与快照不一致", "db")
            archived = set(manifest.get("archived_files") or [])
            for name, expected in (manifest.get("files") or {}).items():
                if archived and name not in archived:
                    continue
                if not archived and name in (manifest.get("content_omitted") or []):
                    continue
                handle = tar.extractfile(name)
                if handle is None:
                    raise ProfileStoreError(f"归档缺少文件: {name}", "db")
                if sha256_bytes(handle.read()) != expected:
                    raise ProfileStoreError(f"归档文件哈希不一致: {name}", "db")

    def purge_snapshot(self, admin_id: int, snapshot_id: int, reason: str = "") -> dict:
        with self.session_factory() as session:
            try:
                snapshot = session.get(ConfigSnapshot, int(snapshot_id))
                if snapshot is None:
                    raise ProfileStoreError("snapshot 不存在", "not_found")
                data = self._purge_in_session(session, snapshot, admin_id, reason)
                session.commit()
                return data
            except Exception:
                session.rollback()
                raise

    def _purge_in_session(self, session, snapshot: ConfigSnapshot,
                          actor: Optional[int], reason: str) -> dict:
        if snapshot.content_purged_at is not None:
            return {"snapshot_id": snapshot.id, "already_purged": True}
        if snapshot.pinned:
            raise ProfileStoreError("snapshot 已 pin，不能清理", "conflict")
        public = session.execute(
            select(PublicConfig).where(PublicConfig.current_snapshot_id == snapshot.id)
        ).scalar_one_or_none()
        profile = session.execute(
            select(ClusterProfile).where(ClusterProfile.current_snapshot_id == snapshot.id)
        ).scalar_one_or_none()
        project = session.execute(
            select(ProjectEnv).where(
                or_(
                    ProjectEnv.public_snapshot_id == snapshot.id,
                    ProjectEnv.profile_snapshot_id == snapshot.id,
                    ProjectEnv.materialized_snapshot_id == snapshot.id,
                )
            )
        ).scalar_one_or_none()
        resource = session.execute(
            select(ProjectResource).where(
                or_(
                    ProjectResource.public_snapshot_id == snapshot.id,
                    ProjectResource.profile_snapshot_id == snapshot.id,
                    ProjectResource.materialized_snapshot_id == snapshot.id,
                )
            )
        ).scalar_one_or_none()
        drift = session.execute(
            select(ConfigDrift).where(
                and_(
                    ConfigDrift.expected_snapshot_id == snapshot.id,
                    ConfigDrift.status == "pending",
                )
            )
        ).scalar_one_or_none()
        if public or profile or project or resource or drift:
            raise ProfileStoreError("snapshot 仍被引用，不能清理", "conflict")
        archive = session.execute(
            select(ConfigSnapshotArchive).where(
                and_(
                    ConfigSnapshotArchive.snapshot_id == snapshot.id,
                    ConfigSnapshotArchive.status == "archived",
                )
            )
        ).scalar_one_or_none()
        if archive is None:
            raise ProfileStoreError("snapshot 尚未归档，不能清理", "conflict")
        for row in session.execute(
                select(ConfigSnapshotFile).where(ConfigSnapshotFile.snapshot_id == snapshot.id)
        ).scalars().all():
            row.content = None
        snapshot.status = "purged"
        snapshot.content_purged_at = _utcnow()
        session.add(EnvEvent(
            event_type="purge", actor=actor, reason=reason,
            diff={"snapshot_id": snapshot.id, "archive_path": archive.path},
        ))
        session.flush()
        return {"snapshot_id": snapshot.id, "purged": True, "archive_path": archive.path}

    def restore_archive(self, admin_id: int, archive_id: int, reason: str = "") -> dict:
        with self.session_factory() as session:
            try:
                archive = session.get(ConfigSnapshotArchive, int(archive_id))
                if archive is None or archive.status != "archived":
                    raise ProfileStoreError("归档不存在或已删除", "not_found")
                target = self._resolve_archive(Path(archive.path))
                with tarfile.open(target, "r:gz") as tar:
                    manifest_handle = tar.extractfile("manifest.json")
                    if manifest_handle is None:
                        raise ProfileStoreError("归档缺少 manifest.json", "db")
                    manifest = json.loads(manifest_handle.read().decode("utf-8"))
                    archived = set(manifest.get("archived_files") or [])
                    omitted = set(manifest.get("content_omitted") or [])
                    names = archived or {
                        name for name in (manifest.get("files") or {})
                        if name not in omitted
                    }
                    files: dict[str, bytes] = {}
                    for name in names:
                        handle = tar.extractfile(name)
                        if handle is None:
                            raise ProfileStoreError(f"归档缺少文件: {name}", "db")
                        files[name] = handle.read()
                old = session.get(ConfigSnapshot, archive.snapshot_id)
                if old is None:
                    raise ProfileStoreError("归档对应的 snapshot 不存在", "not_found")
                scope_key = old.scope_key
                new_snapshot = self._create_snapshot(
                    session, scope=old.scope, scope_key=scope_key, files=files,
                    public_config_id=old.public_config_id, profile_id=old.profile_id,
                    project_id=old.project_id, source="restore", parent=old,
                    actor=admin_id, reason=reason, relaxed=(old.scope == "project"),
                )
                if old.scope == "public" and old.public_config_id:
                    public = self._get_public_config(session, create=True)
                    current = session.get(ConfigSnapshot, public.current_snapshot_id) \
                        if public.current_snapshot_id else None
                    if current is not None and current.id != new_snapshot.id:
                        current.status = "superseded"
                    public.current_snapshot_id = new_snapshot.id
                    self._write_public_files(files)
                session.add(EnvEvent(
                    event_type="restore", actor=admin_id, reason=reason,
                    diff={"archive_id": archive.id, "snapshot_id": new_snapshot.id,
                          "version": new_snapshot.version},
                ))
                session.commit()
                return self._snapshot_payload(session, new_snapshot, include_content=True)
            except Exception:
                session.rollback()
                raise

    # ------------------------------------------------------------------ archives api
    def list_archives(self, include_deleted: bool = False) -> list[dict]:
        with self.session_factory() as session:
            stmt = select(ConfigSnapshotArchive).order_by(
                ConfigSnapshotArchive.created_at.desc()
            )
            if not include_deleted:
                # deleted 只保留 DB tombstone/审计，不在管理员列表里重复出现
                stmt = stmt.where(ConfigSnapshotArchive.status != "deleted")
            rows = session.execute(stmt).scalars().all()
            return [self._archive_payload(row) for row in rows]

    def archive_path(self, archive_id: int) -> Path:
        with self.session_factory() as session:
            archive = session.get(ConfigSnapshotArchive, int(archive_id))
            if archive is None or archive.status != "archived":
                raise ProfileStoreError("归档不存在或已删除", "not_found")
            path = self._resolve_archive(Path(archive.path))
            if not path.is_file():
                raise ProfileStoreError("归档文件缺失", "db")
            return path

    def delete_archive(self, admin_id: int, archive_id: int, note: str = "") -> dict:
        with self.session_factory() as session:
            try:
                archive = session.get(ConfigSnapshotArchive, int(archive_id))
                if archive is None:
                    raise ProfileStoreError("归档不存在", "not_found")
                path = self._resolve_archive(Path(archive.path))
                path.unlink(missing_ok=True)
                path.with_suffix(path.suffix + ".manifest.json").unlink(missing_ok=True)
                archive.status = "deleted"
                archive.deleted_by = admin_id
                archive.deleted_at = _utcnow()
                archive.note = note or archive.note
                session.add(EnvEvent(
                    event_type="gc", actor=admin_id, reason=note or "delete archive",
                    diff={"archive_id": archive.id, "path": archive.path, "action": "delete"},
                ))
                session.commit()
                return self._archive_payload(archive)
            except Exception:
                session.rollback()
                raise

    def prune_archives(self, admin_id: int, before: datetime, dry_run: bool = False) -> dict:
        with self.session_factory() as session:
            try:
                rows = session.execute(
                    select(ConfigSnapshotArchive).where(
                        ConfigSnapshotArchive.status == "archived",
                        ConfigSnapshotArchive.created_at < before,
                    )
                ).scalars().all()
                result = [self._archive_payload(row) for row in rows]
                if not dry_run:
                    for archive in rows:
                        path = self._resolve_archive(Path(archive.path))
                        path.unlink(missing_ok=True)
                        path.with_suffix(path.suffix + ".manifest.json").unlink(missing_ok=True)
                        archive.status = "deleted"
                        archive.deleted_by = admin_id
                        archive.deleted_at = _utcnow()
                    session.add(EnvEvent(
                        event_type="gc", actor=admin_id, reason="prune archives",
                        diff={"count": len(rows), "before": before.isoformat()},
                    ))
                    session.commit()
                return {"dry_run": dry_run, "count": len(result), "archives": result}
            except Exception:
                session.rollback()
                raise

    # ------------------------------------------------------------------ gc
    def run_gc(self, admin_id: int, dry_run: bool = False, reason: str = "") -> dict:
        with self.session_factory() as session:
            try:
                now = _utcnow()
                cutoff = now - timedelta(days=self.retention_days)
                snapshots = session.execute(
                    select(ConfigSnapshot)
                    .where(ConfigSnapshot.content_purged_at.is_(None))
                    .order_by(ConfigSnapshot.scope_key, ConfigSnapshot.version.desc())
                ).scalars().all()
                public_current = {row.current_snapshot_id for row in session.execute(
                    select(PublicConfig).where(PublicConfig.current_snapshot_id.is_not(None))
                ).scalars().all()}
                profile_current = {row.current_snapshot_id for row in session.execute(
                    select(ClusterProfile).where(ClusterProfile.current_snapshot_id.is_not(None))
                ).scalars().all()}
                project_refs = set()
                for row in session.execute(
                        select(ProjectEnv).where(
                            (ProjectEnv.public_snapshot_id.is_not(None)) |
                            (ProjectEnv.profile_snapshot_id.is_not(None)) |
                            (ProjectEnv.materialized_snapshot_id.is_not(None))
                        )
                ).scalars().all():
                    project_refs.update(filter(None, [
                        row.public_snapshot_id, row.profile_snapshot_id,
                        row.materialized_snapshot_id,
                    ]))
                for row in session.execute(
                        select(ProjectResource).where(
                            (ProjectResource.public_snapshot_id.is_not(None)) |
                            (ProjectResource.profile_snapshot_id.is_not(None)) |
                            (ProjectResource.materialized_snapshot_id.is_not(None))
                        )
                ).scalars().all():
                    project_refs.update(filter(None, [
                        row.public_snapshot_id, row.profile_snapshot_id,
                        row.materialized_snapshot_id,
                    ]))
                for row in session.execute(
                        select(ConfigDrift).where(
                            ConfigDrift.expected_snapshot_id.is_not(None),
                            ConfigDrift.status == "pending",
                        )
                ).scalars().all():
                    project_refs.add(row.expected_snapshot_id)
                refs = public_current | profile_current | project_refs
                by_key: dict[str, list[ConfigSnapshot]] = {}
                for snapshot in snapshots:
                    by_key.setdefault(snapshot.scope_key, []).append(snapshot)
                candidates = []
                for items in by_key.values():
                    top = items[0].version
                    for snapshot in items:
                        if snapshot.pinned or snapshot.id in refs:
                            continue
                        outside_versions = snapshot.version <= top - self.keep_versions
                        outside_days = snapshot.created_at is not None and snapshot.created_at < cutoff
                        if outside_versions and outside_days:
                            candidates.append(snapshot)
                result = []
                for snapshot in candidates:
                    entry = {"snapshot_id": snapshot.id, "scope": snapshot.scope,
                             "version": snapshot.version, "scope_key": snapshot.scope_key}
                    if dry_run:
                        entry["action"] = "would_archive_purge"
                    else:
                        try:
                            self._archive_in_session(session, snapshot, admin_id, reason or "gc")
                            self._purge_in_session(session, snapshot, admin_id, reason or "gc")
                            entry["action"] = "archived_purged"
                        except ProfileStoreError as exc:
                            entry["action"] = "skipped"
                            entry["error"] = str(exc)
                    result.append(entry)
                session.add(EnvEvent(
                    event_type="gc", actor=admin_id, reason=reason or "snapshot gc",
                    diff={"dry_run": dry_run, "candidates": len(result),
                          "keep_versions": self.keep_versions,
                          "retention_days": self.retention_days},
                ))
                session.commit()
                if not dry_run and self.archive_auto_prune_days > 0:
                    self.prune_archives(
                        admin_id, _utcnow() - timedelta(days=self.archive_auto_prune_days),
                    )
                return {"dry_run": dry_run, "candidates": result}
            except Exception:
                session.rollback()
                raise

    # ------------------------------------------------------------------ public helpers
    def create_snapshot(self, session, **kwargs) -> ConfigSnapshot:
        return self._create_snapshot(session, **kwargs)

    def snapshot_files(self, session, snapshot: ConfigSnapshot) -> dict[str, Optional[bytes]]:
        return self._snapshot_files(session, snapshot)

    def snapshot_file_states(self, session, snapshot: ConfigSnapshot) -> dict[str, dict]:
        return self._snapshot_file_states(session, snapshot)

    def snapshot_payload(self, session, snapshot: ConfigSnapshot, *,
                         include_content: bool = True) -> dict:
        return self._snapshot_payload(session, snapshot, include_content=include_content)

    def read_public_files(self) -> dict[str, bytes]:
        return self._read_public_files()

    def write_public_files(self, files: dict[str, bytes]) -> None:
        self._write_public_files(files)

    def files_bundle(self, files: dict[str, bytes]) -> str:
        return self._files_bundle(files)

    # ------------------------------------------------------------------ helpers
    def _get_public_config(self, session, create: bool) -> Optional[PublicConfig]:
        public = session.execute(
            select(PublicConfig).where(PublicConfig.name == self.public_config_name)
        ).scalar_one_or_none()
        if public is None and create:
            public = PublicConfig(name=self.public_config_name)
            session.add(public)
            session.flush()
        return public

    @staticmethod
    def _archive_payload(archive: ConfigSnapshotArchive) -> dict:
        return {
            "archive_id": archive.id,
            "snapshot_id": archive.snapshot_id,
            "scope": archive.scope,
            "path": archive.path,
            "sha256": archive.sha256,
            "size_bytes": archive.size_bytes,
            "status": archive.status,
            "created_at": archive.created_at.isoformat() if archive.created_at else None,
            "deleted_at": archive.deleted_at.isoformat() if archive.deleted_at else None,
            "note": archive.note,
        }

    def _resolve_archive(self, relative: Path) -> Path:
        root = self.archive_root.resolve()
        path = (root / relative).resolve()
        if not path.is_relative_to(root):
            raise ProfileStoreError("非法归档路径", "invalid")
        return path


snapshot_store = ClusterSnapshotStore()
