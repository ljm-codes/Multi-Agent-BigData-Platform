"""集群 profile 文件存储：上传/直接编辑、revision 快照、sha256/ETag。

只写 draft revision，不碰 active contract；validate/apply 由后续服务负责。
"""
from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import posixpath
import re
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

import yaml
from sqlalchemy import select, and_, or_
from sqlalchemy.exc import IntegrityError

from SPO.cluster_env_model import (
    ClusterInfo,
    ClusterProfile,
    ConfigDrift,
    ConfigSnapshot,
    EnvEvent,
    ProfileFile,
    ProjectEnv,
    ProjectResource,
)
from Tools.db import SessionLocal
from Tools.log_settings import LogSetting
from load_config.config import ROOT_BASE_DIR_PATH, config

ALLOWED_FILES = frozenset({
    "core-site.xml",
    "hdfs-site.xml",
    "yarn-site.xml",
    "mapred-site.xml",
    "hive-site.xml",
    "hbase-site.xml",
    "flink-conf.yaml",
    "server.properties",
})
MAX_FILES = 20
MAX_REASON_LEN = 1000

_CFG = config.get("cluster_env") or {}
_MAX_FILE_BYTES = int(_CFG.get("max_file_bytes") or 1024 * 1024)
_MAX_TOTAL_BYTES = int(_CFG.get("max_total_bytes") or 10 * 1024 * 1024)
logger = LogSetting.create(__name__)


class ProfileStoreError(Exception):
    """code: not_found / conflict / invalid / forbidden / too_large / db"""

    def __init__(self, message: str, code: str = "invalid"):
        super().__init__(message)
        self.code = code


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def bundle_hash(file_hashes: dict[str, str]) -> str:
    h = hashlib.sha256()
    for name in sorted(file_hashes):
        h.update(f"{name}:{file_hashes[name]}\n".encode("utf-8"))
    return h.hexdigest()


def cluster_identity_hash(fs_default_fs: str, hive_metastore_uris: str,
                          kafka_bootstrap: str, fallback_name: str = "") -> str:
    payload = {
        "fs.defaultFS": (fs_default_fs or "").strip(),
        "hive.metastore.uris": (hive_metastore_uris or "").strip(),
        "kafka.bootstrap": (kafka_bootstrap or "").strip(),
    }
    if not any(payload.values()):
        payload = {"name": (fallback_name or "").strip()}
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha256_bytes(raw.encode("utf-8"))


def normalize_filename(filename: str) -> str:
    """浏览器可能带 C:\\fakepath\\；只取基名，并做 allowlist/字符校验。"""
    name = re.split(r"[\\/]", str(filename or ""))[-1].strip()
    if name in {"", ".", ".."} or name not in ALLOWED_FILES:
        raise ProfileStoreError(f"不允许的文件名: {filename!r}")
    if not re.fullmatch(r"[A-Za-z0-9._-]+", name):
        raise ProfileStoreError(f"文件名包含非法字符: {name!r}")
    return name


def normalize_relative_path(path: str) -> str:
    """项目 resources 用的相对路径：允许子目录，禁止绝对路径和 ..。"""
    raw = str(path or "").replace("\\", "/").strip()
    if not raw:
        raise ProfileStoreError("文件路径不能为空")
    if raw.startswith("/") or re.match(r"^[A-Za-z]:", raw):
        raise ProfileStoreError(f"不允许绝对路径: {path!r}")
    parts: list[str] = []
    for part in raw.split("/"):
        part = part.strip()
        if part in ("", "."):
            continue
        if part == "..":
            raise ProfileStoreError(f"路径不能包含 ..: {path!r}")
        if len(part) > 255:
            raise ProfileStoreError(f"路径片段过长: {part[:60]!r}")
        parts.append(part)
    if not parts:
        raise ProfileStoreError(f"非法路径: {path!r}")
    return "/".join(parts)


def validate_content(filename: str, content: bytes, max_file_bytes: int = _MAX_FILE_BYTES) -> str:
    """校验并返回 mime_type；XML 拒绝 DOCTYPE/ENTITY（XXE）。"""
    if not content:
        raise ProfileStoreError("文件内容不能为空")
    if len(content) > max_file_bytes:
        raise ProfileStoreError(f"{filename} 超过单文件大小限制", "too_large")
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ProfileStoreError(f"{filename} 不是 UTF-8 文本") from exc

    if filename.endswith(".xml"):
        upper = text.upper()
        if "<!DOCTYPE" in upper or "<!ENTITY" in upper:
            raise ProfileStoreError(f"{filename} 不允许 DOCTYPE/ENTITY")
        try:
            root = ET.fromstring(text)
        except ET.ParseError as exc:
            raise ProfileStoreError(f"{filename} XML 解析失败: {exc}") from exc
        if root.tag.split("}")[-1] != "configuration":
            raise ProfileStoreError(f"{filename} 根节点必须是 <configuration>")
        return "application/xml"

    if filename == "flink-conf.yaml":
        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            raise ProfileStoreError(f"{filename} YAML 解析失败: {exc}") from exc
        if data is not None and not isinstance(data, dict):
            raise ProfileStoreError(f"{filename} 顶层必须是 key: value")
        return "application/yaml"

    # server.properties
    entries = 0
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith(("#", "!")):
            continue
        if "=" not in stripped and ":" not in stripped:
            raise ProfileStoreError(f"{filename} 存在非 key=value 行: {stripped[:80]!r}")
        entries += 1
    if entries == 0:
        raise ProfileStoreError(f"{filename} 没有任何配置项")
    return "text/plain"


_SECRET_WORD = r"(?:password|passwd|secret|keytab|credential|token|accesskey|secretkey)"
_SECRET_XML_RE = re.compile(
    rf"(<property\b[^>]*>\s*<name>[^<]*{_SECRET_WORD}[^<]*</name>\s*<value>)(.*?)(</value>)",
    re.IGNORECASE | re.DOTALL,
)
_SECRET_LINE_RE = re.compile(
    rf"(?im)^([ \t]*[^#\s][^\n=:]*?{_SECRET_WORD}[^\n=:]*[ \t]*[=:][ \t]*)(.*)$"
)


def redact_config_text(text: str) -> str:
    """给 Agent 看之前隐藏密码类值；路径/非敏感键值原样保留。"""
    if not text:
        return text
    text = _SECRET_XML_RE.sub(r"\1***\3", text)
    if text.lstrip().startswith("<"):
        return text
    return _SECRET_LINE_RE.sub(r"\1***", text)


def validate_project_content(path: str, content: bytes,
                             max_file_bytes: int = _MAX_FILE_BYTES) -> str:
    """项目 resources 的宽松校验：已知环境配置仍严格，其它资源只做基本安全检查。"""
    name = posixpath.basename(path)
    if name in ALLOWED_FILES:
        return validate_content(name, content, max_file_bytes)
    if not content:
        return "application/octet-stream"
    if name.endswith(".xml"):
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            return mimetypes.guess_type(name)[0] or "application/octet-stream"
        upper = text.upper()
        if "<!DOCTYPE" in upper or "<!ENTITY" in upper:
            raise ProfileStoreError(f"{path} 不允许 DOCTYPE/ENTITY")
        return "application/xml"
    return mimetypes.guess_type(name)[0] or "application/octet-stream"


class ClusterProfileStore:
    def __init__(self, storage_root: Path, session_factory=SessionLocal,
                 max_file_bytes: int = _MAX_FILE_BYTES):
        self.storage_root = Path(storage_root).resolve()
        self.storage_root.mkdir(parents=True, exist_ok=True)
        self.session_factory = session_factory
        self.max_file_bytes = int(max_file_bytes)

    # ------------------------------------------------------------------ create/get
    def create_profile(self, admin_id: int, name: str, description: str = "",
                       cluster_name: str = "", fs_default_fs: str = "",
                       hive_metastore_uris: str = "", kafka_bootstrap: str = "") -> dict:
        name = (name or "").strip()
        if not name or len(name) > 128:
            raise ProfileStoreError("profile 名称不能为空且不超过 128 字符")
        cluster_name = (cluster_name or name).strip()
        identity = cluster_identity_hash(fs_default_fs, hive_metastore_uris,
                                         kafka_bootstrap, fallback_name=cluster_name)
        with self.session_factory() as session:
            try:
                cluster = session.execute(
                    select(ClusterInfo).where(ClusterInfo.identity_hash == identity)
                ).scalar_one_or_none()
                if cluster is None:
                    cluster = ClusterInfo(
                        name=cluster_name,
                        identity_hash=identity,
                        fs_default_fs=(fs_default_fs or "").strip() or None,
                        hive_metastore_uris=(hive_metastore_uris or "").strip() or None,
                        kafka_bootstrap=(kafka_bootstrap or "").strip() or None,
                        source="manual",
                        status="active",
                        description="",
                        created_by=admin_id,
                    )
                    session.add(cluster)
                    session.flush()
                profile = ClusterProfile(
                    cluster_id=cluster.id,
                    owner_admin_id=admin_id,
                    name=name,
                    source="manual",
                    status="draft",
                    current_revision=1,
                    draft_revision=1,
                    description=(description or "").strip(),
                    created_by=admin_id,
                )
                session.add(profile)
                session.flush()
                data = self._profile_dict(session, profile)
                session.add(EnvEvent(
                    profile_id=profile.id, event_type="init", actor=admin_id,
                    reason="create profile",
                    diff={"profile_id": profile.id, "cluster_id": cluster.id},
                ))
                session.commit()
                return data
            except IntegrityError as exc:
                session.rollback()
                existing = session.execute(
                    select(ClusterProfile).where(
                        ClusterProfile.owner_admin_id == int(admin_id),
                        ClusterProfile.name == name,
                    )
                ).scalar_one_or_none()
                if existing is not None:
                    raise ProfileStoreError(
                        f"profile 名称「{name}」已存在；请直接选择该 profile 上传文件，"
                        f"或换一个名称",
                        "conflict",
                    ) from exc
                raise ProfileStoreError("cluster 名称/身份已存在，请换一个 profile 名称",
                                        "conflict") from exc

    def list_profiles(self, admin_id: int) -> list[dict]:
        with self.session_factory() as session:
            profiles = session.execute(
                select(ClusterProfile)
                .where(
                    and_(
                        ClusterProfile.owner_admin_id == admin_id,
                        ClusterProfile.status != "deleted"
                    )
                ).order_by(ClusterProfile.updated_at.desc())
            ).scalars().all()
            return [self._profile_dict(session, p) for p in profiles]

    def delete_profile(self, admin_id: int, profile_id: int, note: str = "") -> dict:
        """只在没有任何项目引用时删除；先归档 revision，再删目录，行软删保留审计。"""
        with self.session_factory() as session:
            try:
                profile = self._owned_profile(session, admin_id, profile_id)
                snapshot_ids = [
                    row for row in session.execute(
                        select(ConfigSnapshot.id).where(ConfigSnapshot.profile_id == profile.id)
                    ).scalars().all()
                ]
                refs: set[str] = set()
                stmt = select(ProjectResource.project_id).where(
                    ProjectResource.profile_id == profile.id
                )
                if snapshot_ids:
                    stmt = select(ProjectResource.project_id).where(
                        or_(
                            ProjectResource.profile_id == profile.id,
                            ProjectResource.profile_snapshot_id.in_(snapshot_ids)
                        )
                    )
                refs.update(session.execute(stmt).scalars().all())
                stmt_env = select(ProjectEnv.project_id).where(
                    ProjectEnv.profile_id == profile.id
                )
                if snapshot_ids:
                    stmt_env = select(ProjectEnv.project_id).where(
                        or_(
                            ProjectEnv.profile_id == profile.id,
                            ProjectEnv.profile_snapshot_id.in_(snapshot_ids)
                        )
                    )
                refs.update(session.execute(stmt_env).scalars().all())
                if snapshot_ids:
                    refs.update(session.execute(
                        select(ConfigDrift.project_id).where(
                            ConfigDrift.expected_snapshot_id.in_(snapshot_ids),
                            ConfigDrift.status == "pending",
                            ConfigDrift.project_id.is_not(None),
                        )
                    ).scalars().all())
                refs.discard(None)
                if refs:
                    raise ProfileStoreError(
                        f"profile 仍被 {len(refs)} 个项目引用，禁止删除：{sorted(refs)[:10]}",
                        "conflict",
                    )

                rows = self._revision_rows(session, profile.id, profile.draft_revision)
                files = {
                    row.filename: self._read_storage(row.storage_path) for row in rows
                }
                archive_snapshot_id = profile.current_snapshot_id
                if files:
                    from services.cluster_snapshot_store import snapshot_store
                    parent = session.get(ConfigSnapshot, profile.current_snapshot_id) \
                        if profile.current_snapshot_id else None
                    snapshot = snapshot_store.create_snapshot(
                        session, scope="profile", scope_key=f"profile:{profile.id}",
                        files=files, profile_id=profile.id,
                        source="delete_archive", parent=parent,
                        actor=admin_id, reason=note or "delete profile",
                    )
                    archive_snapshot_id = snapshot.id
                    profile.current_snapshot_id = snapshot.id
                session.execute(
                    ProfileFile.__table__.delete().where(ProfileFile.profile_id == profile.id)
                )
                profile.status = "deleted"
                session.add(EnvEvent(
                    profile_id=profile.id, event_type="gc", actor=admin_id,
                    reason=note or "delete profile",
                    diff={"action": "delete_profile", "archive_snapshot_id": archive_snapshot_id,
                          "files": sorted(files)},
                ))
                session.commit()
            except Exception:
                session.rollback()
                raise
        if archive_snapshot_id:
            try:
                from services.cluster_snapshot_store import snapshot_store
                snapshot_store.archive_snapshot(admin_id, archive_snapshot_id,
                                                note or "delete profile")
            except ProfileStoreError as exc:
                logger.warning("profile %s 归档快照失败：%s", profile_id, exc)
        shutil.rmtree(self.storage_root / str(int(profile_id)), ignore_errors=True)
        return {"profile_id": profile_id, "deleted": True,
                "archive_snapshot_id": archive_snapshot_id}

    def get_profile(self, admin_id: int, profile_id: int) -> dict:
        with self.session_factory() as session:
            profile = self._owned_profile(session, admin_id, profile_id)
            return self._profile_dict(session, profile)

    def list_files(self, admin_id: int, profile_id: int,
                   revision: Optional[int] = None) -> list[dict]:
        with self.session_factory() as session:
            profile = self._owned_profile(session, admin_id, profile_id)
            revision = revision or profile.draft_revision
            rows = session.execute(
                select(ProfileFile)
                .where(
                    and_(ProfileFile.profile_id == profile.id,
                         ProfileFile.revision == revision
                         )
                ).order_by(ProfileFile.filename)
            ).scalars().all()
            return [self._file_dict(row) for row in rows]

    def read_file(self, admin_id: int, profile_id: int, filename: str,
                  revision: Optional[int] = None) -> dict:
        name = normalize_filename(filename)
        with self.session_factory() as session:
            profile = self._owned_profile(session, admin_id, profile_id)
            revision = revision or profile.draft_revision
            row = session.execute(
                select(ProfileFile).where(
                    and_(ProfileFile.profile_id == profile.id,
                         ProfileFile.revision == revision,
                         ProfileFile.filename == name)
                )
            ).scalar_one_or_none()
            if row is None:
                raise ProfileStoreError("文件不存在", "not_found")
            content = self._read_storage(row.storage_path)
            if sha256_bytes(content) != row.sha256:
                raise ProfileStoreError("文件哈希与数据库不一致", "db")
            try:
                text = content.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ProfileStoreError("文件不是 UTF-8 文本") from exc
            return {**self._file_dict(row), "content": text}

    # ------------------------------------------------------------------ upload/edit
    def upload_files(self, admin_id: int, profile_id: int,
                     files: list[tuple[str, bytes]],
                     expected_revision: Optional[int] = None,
                     reason: str = "") -> dict:
        if not files:
            raise ProfileStoreError("至少上传一个文件")
        if len(files) > MAX_FILES:
            raise ProfileStoreError(f"一次最多上传 {MAX_FILES} 个文件", "too_large")
        self._check_reason(reason)

        uploads: dict[str, bytes] = {}
        for filename, content in files:
            name = normalize_filename(filename)
            if name in uploads:
                raise ProfileStoreError(f"重复文件: {name}")
            validate_content(name, content, self.max_file_bytes)
            uploads[name] = content
        if sum(len(c) for c in uploads.values()) > _MAX_TOTAL_BYTES:
            raise ProfileStoreError("上传总大小超限", "too_large")

        with self.session_factory() as session:
            try:
                profile = self._owned_profile(session, admin_id, profile_id, for_update=True)
                if expected_revision is not None and expected_revision != profile.draft_revision:
                    raise ProfileStoreError("profile 已被修改，请刷新后重试", "conflict")
                base_rows = self._revision_rows(session, profile.id, profile.draft_revision)
                new_revision = profile.draft_revision if not base_rows else profile.draft_revision + 1
                result = self._write_revision(
                    session, profile, base_rows, uploads, new_revision,
                    admin_id, event_type="upload", reason=reason,
                )
                session.commit()
                return result
            except ProfileStoreError:
                session.rollback()
                raise
            except IntegrityError as exc:
                session.rollback()
                raise ProfileStoreError("写入 profile 文件失败（唯一约束冲突）", "conflict") from exc

    def write_file(self, admin_id: int, profile_id: int, filename: str,
                   content: bytes, expected_sha256: Optional[str] = None,
                   reason: str = "") -> dict:
        name = normalize_filename(filename)
        validate_content(name, content, self.max_file_bytes)
        self._check_reason(reason)
        with self.session_factory() as session:
            try:
                profile = self._owned_profile(session, admin_id, profile_id, for_update=True)
                base_rows = self._revision_rows(session, profile.id, profile.draft_revision)
                current = next((row for row in base_rows if row.filename == name), None)
                if current is None:
                    raise ProfileStoreError("文件不存在，不能编辑；请使用上传接口", "not_found")
                if expected_sha256 and expected_sha256 != current.sha256:
                    raise ProfileStoreError("文件已被修改，请刷新后重试", "conflict")
                new_revision = profile.draft_revision + 1
                result = self._write_revision(
                    session, profile, base_rows, {name: content}, new_revision,
                    admin_id, event_type="edit", reason=reason,
                )
                session.commit()
                return result
            except ProfileStoreError:
                session.rollback()
                raise
            except IntegrityError as exc:
                session.rollback()
                raise ProfileStoreError("写入 profile 文件失败（唯一约束冲突）", "conflict") from exc

    # ------------------------------------------------------------------ internals
    def _write_revision(self, session, profile: ClusterProfile,
                        base_rows: list[ProfileFile], uploads: dict[str, bytes],
                        new_revision: int, admin_id: int, event_type: str,
                        reason: str) -> dict:
        final: dict[str, bytes] = {}
        for row in base_rows:
            content = self._read_storage(row.storage_path)
            if sha256_bytes(content) != row.sha256:
                raise ProfileStoreError(f"历史文件哈希不一致: {row.filename}", "db")
            final[row.filename] = content
        final.update(uploads)
        if len(final) > MAX_FILES:
            raise ProfileStoreError(f"profile 文件数超过 {MAX_FILES}", "too_large")
        if sum(len(c) for c in final.values()) > _MAX_TOTAL_BYTES:
            raise ProfileStoreError("profile 文件总大小超限", "too_large")

        revision_dir = (self.storage_root / str(profile.id) / str(new_revision))
        revision_dir.mkdir(parents=True, exist_ok=True)
        file_hashes: dict[str, str] = {}
        try:
            for name in sorted(final):
                content = final[name]
                mime = validate_content(name, content, self.max_file_bytes)
                sha = sha256_bytes(content)
                relative = f"{profile.id}/{new_revision}/{name}"
                target = self._resolve_relative(relative)
                tmp = target.with_name(f".{name}.tmp")
                tmp.write_bytes(content)
                os.replace(tmp, target)
                session.add(ProfileFile(
                    profile_id=profile.id,
                    revision=new_revision,
                    filename=name,
                    sha256=sha,
                    size_bytes=len(content),
                    mime_type=mime,
                    storage_path=relative,
                    created_by=admin_id,
                ))
                file_hashes[name] = sha
        except Exception:
            shutil.rmtree(revision_dir, ignore_errors=True)
            raise

        bundle = bundle_hash(file_hashes)
        profile.draft_revision = new_revision
        profile.draft_bundle_hash = bundle
        profile.status = "draft"
        session.add(EnvEvent(
            profile_id=profile.id, event_type=event_type, actor=admin_id,
            reason=reason,
            diff={"revision": new_revision, "files": file_hashes},
        ))
        session.flush()
        return {
            "profile_id": profile.id,
            "revision": new_revision,
            "files": [{"filename": n, "sha256": s} for n, s in sorted(file_hashes.items())],
            "bundle_hash": bundle,
            "changed": True,
        }

    @staticmethod
    def _owned_profile(session, admin_id: int, profile_id: int,
                       for_update: bool = False) -> ClusterProfile:
        stmt = select(ClusterProfile).where(ClusterProfile.id == int(profile_id))
        if for_update:
            stmt = stmt.with_for_update()
        profile = session.execute(stmt).scalar_one_or_none()
        if profile is None or profile.status == "deleted":
            raise ProfileStoreError("profile 不存在", "not_found")
        if profile.owner_admin_id != int(admin_id):
            raise ProfileStoreError("无权访问该 profile", "forbidden")
        return profile

    @staticmethod
    def _revision_rows(session, profile_id: int, revision: int) -> list[ProfileFile]:
        return session.execute(
            select(ProfileFile).where(
                and_(ProfileFile.profile_id == profile_id,
                     ProfileFile.revision == revision)
            )
        ).scalars().all()

    @staticmethod
    def _check_reason(reason: str) -> None:
        if len(reason or "") > MAX_REASON_LEN:
            raise ProfileStoreError("reason 过长")

    def revision_dir(self, profile_id: int, revision: int) -> Path:
        """profile revision 的文件目录；回滚时优先从这里拷贝。"""
        return (self.storage_root / str(int(profile_id)) / str(int(revision))).resolve()

    def _resolve_relative(self, relative: str) -> Path:
        root = self.storage_root.resolve()
        path = (root / relative).resolve()
        if not path.is_relative_to(root):
            raise ProfileStoreError("非法文件路径")
        return path

    def _read_storage(self, relative: str) -> bytes:
        path = self._resolve_relative(relative)
        if not path.is_file():
            raise ProfileStoreError(f"文件存储缺失: {relative}", "db")
        return path.read_bytes()

    @staticmethod
    def _file_dict(row: ProfileFile) -> dict:
        return {
            "id": row.id,
            "profile_id": row.profile_id,
            "revision": row.revision,
            "filename": row.filename,
            "sha256": row.sha256,
            "size_bytes": row.size_bytes,
            "mime_type": row.mime_type,
        }

    @staticmethod
    def _profile_dict(session, profile: ClusterProfile) -> dict:
        cluster = session.get(ClusterInfo, profile.cluster_id)
        return {
            "id": profile.id,
            "name": profile.name,
            "description": profile.description,
            "status": profile.status,
            "source": profile.source,
            "owner_admin_id": profile.owner_admin_id,
            "cluster_id": profile.cluster_id,
            "cluster": None if cluster is None else {
                "id": cluster.id,
                "name": cluster.name,
                "identity_hash": cluster.identity_hash,
                "fs_default_fs": cluster.fs_default_fs,
                "hive_metastore_uris": cluster.hive_metastore_uris,
                "kafka_bootstrap": cluster.kafka_bootstrap,
            },
            "current_revision": profile.current_revision,
            "draft_revision": profile.draft_revision,
            "draft_bundle_hash": profile.draft_bundle_hash,
            "created_at": profile.created_at.isoformat() if profile.created_at else None,
            "updated_at": profile.updated_at.isoformat() if profile.updated_at else None,
        }


def _load_store() -> ClusterProfileStore:
    root = Path(_CFG.get("storage_root") or "cluster_profiles")
    if not root.is_absolute():
        root = ROOT_BASE_DIR_PATH / root
    return ClusterProfileStore(root, SessionLocal, _MAX_FILE_BYTES)


profile_store = _load_store()
