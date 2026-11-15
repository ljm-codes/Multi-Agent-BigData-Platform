"""cluster_env schema ORM 模型（表结构由 sql/cluster_env.sql 维护）。"""
from datetime import datetime
from typing import Optional

from sqlalchemy import BIGINT, INTEGER, Boolean, ForeignKey, LargeBinary, String, TEXT, TIMESTAMP, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from SPO.postgresql_model import Base

_SCHEMA = {"schema": "cluster_env"}
_NOW = text("now()")


class ClusterInfo(Base):
    __tablename__ = "cluster_info"
    __table_args__ = (_SCHEMA,)

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(TEXT, nullable=False)
    identity_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    fs_default_fs: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)
    hive_metastore_uris: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)
    kafka_bootstrap: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)
    source: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("'manual'"))
    status: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("'active'"))
    description: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("''"))
    created_by: Mapped[Optional[int]] = mapped_column(BIGINT, nullable=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=_NOW)
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=_NOW)


class ClusterProfile(Base):
    __tablename__ = "profile"
    __table_args__ = (_SCHEMA,)

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    cluster_id: Mapped[int] = mapped_column(
        BIGINT, ForeignKey("cluster_env.cluster_info.id", ondelete="RESTRICT"), nullable=False
    )
    owner_admin_id: Mapped[int] = mapped_column(BIGINT, nullable=False)
    name: Mapped[str] = mapped_column(TEXT, nullable=False)
    source: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("'upload'"))
    status: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("'draft'"))
    current_revision: Mapped[int] = mapped_column(INTEGER, nullable=False, server_default=text("1"))
    draft_revision: Mapped[int] = mapped_column(INTEGER, nullable=False, server_default=text("1"))
    draft_bundle_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    current_snapshot_id: Mapped[Optional[int]] = mapped_column(
        BIGINT, ForeignKey("cluster_env.config_snapshot.id", ondelete="SET NULL"), nullable=True
    )
    description: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("''"))
    created_by: Mapped[Optional[int]] = mapped_column(BIGINT, nullable=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=_NOW)
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=_NOW)


class ProfileFile(Base):
    __tablename__ = "profile_file"
    __table_args__ = (_SCHEMA,)

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    profile_id: Mapped[int] = mapped_column(
        BIGINT, ForeignKey("cluster_env.profile.id", ondelete="RESTRICT"), nullable=False
    )
    revision: Mapped[int] = mapped_column(INTEGER, nullable=False)
    filename: Mapped[str] = mapped_column(TEXT, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BIGINT, nullable=False)
    mime_type: Mapped[str] = mapped_column(
        TEXT, nullable=False, server_default=text("'application/octet-stream'")
    )
    storage_path: Mapped[str] = mapped_column(TEXT, nullable=False)
    created_by: Mapped[Optional[int]] = mapped_column(BIGINT, nullable=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=_NOW)


class Contract(Base):
    __tablename__ = "contract"
    __table_args__ = (_SCHEMA,)

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    profile_id: Mapped[int] = mapped_column(
        BIGINT, ForeignKey("cluster_env.profile.id", ondelete="RESTRICT"), nullable=False
    )
    cluster_id: Mapped[int] = mapped_column(
        BIGINT, ForeignKey("cluster_env.cluster_info.id", ondelete="RESTRICT"), nullable=False
    )
    profile_revision: Mapped[int] = mapped_column(INTEGER, nullable=False)
    profile_bundle_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    env_version: Mapped[int] = mapped_column(INTEGER, nullable=False)
    contract_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    contract_data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    provenance: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    conflicts: Mapped[list] = mapped_column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    unknowns: Mapped[list] = mapped_column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    status: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("'valid'"))
    probed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=_NOW)
    expires_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    created_by: Mapped[Optional[int]] = mapped_column(BIGINT, nullable=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=_NOW)


class ProjectEnv(Base):
    __tablename__ = "project_env"
    __table_args__ = (_SCHEMA,)

    project_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("public.admin_project.project_id", ondelete="CASCADE"),
        primary_key=True,
    )
    profile_id: Mapped[int] = mapped_column(
        BIGINT, ForeignKey("cluster_env.profile.id", ondelete="RESTRICT"), nullable=False
    )
    profile_revision: Mapped[int] = mapped_column(INTEGER, nullable=False)
    profile_bundle_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    active_contract_id: Mapped[int] = mapped_column(
        BIGINT, ForeignKey("cluster_env.contract.id", ondelete="RESTRICT"), nullable=False
    )
    active_env_version: Mapped[int] = mapped_column(INTEGER, nullable=False)
    status: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("'active'"))
    locked: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    lock_owner: Mapped[Optional[int]] = mapped_column(BIGINT, nullable=True)
    lock_expires_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    public_snapshot_id: Mapped[Optional[int]] = mapped_column(
        BIGINT, ForeignKey("cluster_env.config_snapshot.id", ondelete="SET NULL"), nullable=True
    )
    profile_snapshot_id: Mapped[Optional[int]] = mapped_column(
        BIGINT, ForeignKey("cluster_env.config_snapshot.id", ondelete="SET NULL"), nullable=True
    )
    materialized_snapshot_id: Mapped[Optional[int]] = mapped_column(
        BIGINT, ForeignKey("cluster_env.config_snapshot.id", ondelete="SET NULL"), nullable=True
    )
    activated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=_NOW)
    activated_by: Mapped[Optional[int]] = mapped_column(BIGINT, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=_NOW)


class EnvEvent(Base):
    __tablename__ = "env_event"
    __table_args__ = (_SCHEMA,)

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    project_id: Mapped[Optional[str]] = mapped_column(
        String(36),
        ForeignKey("public.admin_project.project_id", ondelete="SET NULL"),
        nullable=True,
    )
    profile_id: Mapped[Optional[int]] = mapped_column(
        BIGINT, ForeignKey("cluster_env.profile.id", ondelete="SET NULL"), nullable=True
    )
    from_contract_id: Mapped[Optional[int]] = mapped_column(
        BIGINT, ForeignKey("cluster_env.contract.id", ondelete="SET NULL"), nullable=True
    )
    to_contract_id: Mapped[Optional[int]] = mapped_column(
        BIGINT, ForeignKey("cluster_env.contract.id", ondelete="SET NULL"), nullable=True
    )
    event_type: Mapped[str] = mapped_column(TEXT, nullable=False)
    actor: Mapped[Optional[int]] = mapped_column(BIGINT, nullable=True)
    reason: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("''"))
    diff: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=_NOW)


class EnvConflict(Base):
    __tablename__ = "env_conflict"
    __table_args__ = (_SCHEMA,)

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(
        BIGINT, ForeignKey("cluster_env.contract.id", ondelete="CASCADE"), nullable=False
    )
    field: Mapped[str] = mapped_column(TEXT, nullable=False)
    prompt_value: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    actual_value: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    source: Mapped[str] = mapped_column(TEXT, nullable=False)
    resolution: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("'pending'"))
    resolved_by: Mapped[Optional[int]] = mapped_column(BIGINT, nullable=True)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=_NOW)


class VerificationRun(Base):
    __tablename__ = "verification_run"
    __table_args__ = (_SCHEMA,)

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    project_id: Mapped[Optional[str]] = mapped_column(
        String(36),
        ForeignKey("public.admin_project.project_id", ondelete="SET NULL"),
        nullable=True,
    )
    contract_id: Mapped[int] = mapped_column(
        BIGINT, ForeignKey("cluster_env.contract.id", ondelete="RESTRICT"), nullable=False
    )
    env_version: Mapped[int] = mapped_column(INTEGER, nullable=False)
    gate: Mapped[str] = mapped_column(TEXT, nullable=False)
    status: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("'pending'"))
    command: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("''"))
    exit_code: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    evidence: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    started_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    created_by: Mapped[Optional[int]] = mapped_column(BIGINT, nullable=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=_NOW)


class ProbeSnapshot(Base):
    __tablename__ = "probe_snapshot"
    __table_args__ = (_SCHEMA,)

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(
        BIGINT, ForeignKey("cluster_env.contract.id", ondelete="CASCADE"), nullable=False
    )
    node: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("''"))
    command: Mapped[str] = mapped_column(TEXT, nullable=False)
    kind: Mapped[str] = mapped_column(TEXT, nullable=False)
    output_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    summary: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    storage_path: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=_NOW)


class PublicConfig(Base):
    __tablename__ = "public_config"
    __table_args__ = (_SCHEMA,)

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(TEXT, nullable=False, unique=True)
    current_snapshot_id: Mapped[Optional[int]] = mapped_column(
        BIGINT, ForeignKey("cluster_env.config_snapshot.id", ondelete="SET NULL"), nullable=True
    )
    created_by: Mapped[Optional[int]] = mapped_column(BIGINT, nullable=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=_NOW)
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=_NOW)


class ConfigSnapshot(Base):
    __tablename__ = "config_snapshot"
    __table_args__ = (_SCHEMA,)

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    scope: Mapped[str] = mapped_column(TEXT, nullable=False)
    scope_key: Mapped[str] = mapped_column(TEXT, nullable=False)
    public_config_id: Mapped[Optional[int]] = mapped_column(
        BIGINT, ForeignKey("cluster_env.public_config.id", ondelete="RESTRICT"), nullable=True
    )
    profile_id: Mapped[Optional[int]] = mapped_column(
        BIGINT, ForeignKey("cluster_env.profile.id", ondelete="RESTRICT"), nullable=True
    )
    project_id: Mapped[Optional[str]] = mapped_column(
        String(36),
        ForeignKey("public.admin_project.project_id", ondelete="SET NULL"),
        nullable=True,
    )
    version: Mapped[int] = mapped_column(INTEGER, nullable=False)
    bundle_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    manifest: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    source: Mapped[str] = mapped_column(TEXT, nullable=False)
    parent_snapshot_id: Mapped[Optional[int]] = mapped_column(
        BIGINT, ForeignKey("cluster_env.config_snapshot.id", ondelete="SET NULL"), nullable=True
    )
    status: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("'active'"))
    pinned: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    actor: Mapped[Optional[int]] = mapped_column(BIGINT, nullable=True)
    reason: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("''"))
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=_NOW)
    retained_until: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    content_purged_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)


class ConfigSnapshotFile(Base):
    __tablename__ = "config_snapshot_file"
    __table_args__ = (_SCHEMA,)

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    snapshot_id: Mapped[int] = mapped_column(
        BIGINT, ForeignKey("cluster_env.config_snapshot.id", ondelete="CASCADE"), nullable=False
    )
    filename: Mapped[str] = mapped_column(TEXT, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BIGINT, nullable=False)
    mime_type: Mapped[str] = mapped_column(
        TEXT, nullable=False, server_default=text("'application/octet-stream'")
    )
    content: Mapped[Optional[bytes]] = mapped_column(LargeBinary, nullable=True)
    content_omitted: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    storage_path: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=_NOW)


class ConfigSnapshotArchive(Base):
    __tablename__ = "config_snapshot_archive"
    __table_args__ = (_SCHEMA,)

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    snapshot_id: Mapped[int] = mapped_column(
        BIGINT, ForeignKey("cluster_env.config_snapshot.id", ondelete="RESTRICT"),
        nullable=False, unique=True,
    )
    scope: Mapped[str] = mapped_column(TEXT, nullable=False)
    path: Mapped[str] = mapped_column(TEXT, nullable=False, unique=True)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BIGINT, nullable=False)
    status: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("'archived'"))
    created_by: Mapped[Optional[int]] = mapped_column(BIGINT, nullable=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=_NOW)
    deleted_by: Mapped[Optional[int]] = mapped_column(BIGINT, nullable=True)
    deleted_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    note: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("''"))


class ProjectResource(Base):
    __tablename__ = "project_resource"
    __table_args__ = (_SCHEMA,)

    project_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("public.admin_project.project_id", ondelete="CASCADE"),
        primary_key=True,
    )
    profile_id: Mapped[Optional[int]] = mapped_column(
        BIGINT, ForeignKey("cluster_env.profile.id", ondelete="SET NULL"), nullable=True
    )
    profile_revision: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    public_snapshot_id: Mapped[Optional[int]] = mapped_column(
        BIGINT, ForeignKey("cluster_env.config_snapshot.id", ondelete="SET NULL"), nullable=True
    )
    profile_snapshot_id: Mapped[Optional[int]] = mapped_column(
        BIGINT, ForeignKey("cluster_env.config_snapshot.id", ondelete="SET NULL"), nullable=True
    )
    materialized_snapshot_id: Mapped[Optional[int]] = mapped_column(
        BIGINT, ForeignKey("cluster_env.config_snapshot.id", ondelete="SET NULL"), nullable=True
    )
    effective_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    managed_files: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    target_path: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("''"))
    created_by: Mapped[Optional[int]] = mapped_column(BIGINT, nullable=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=_NOW)
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=_NOW)


class ConfigDrift(Base):
    __tablename__ = "config_drift"
    __table_args__ = (_SCHEMA,)

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    scope: Mapped[str] = mapped_column(TEXT, nullable=False)
    project_id: Mapped[Optional[str]] = mapped_column(
        String(36),
        ForeignKey("public.admin_project.project_id", ondelete="CASCADE"),
        nullable=True,
    )
    public_config_id: Mapped[Optional[int]] = mapped_column(
        BIGINT, ForeignKey("cluster_env.public_config.id", ondelete="CASCADE"), nullable=True
    )
    expected_snapshot_id: Mapped[Optional[int]] = mapped_column(
        BIGINT, ForeignKey("cluster_env.config_snapshot.id", ondelete="SET NULL"), nullable=True
    )
    expected_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    actual_files: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    actual_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("'pending'"))
    detected_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=_NOW)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    resolved_by: Mapped[Optional[int]] = mapped_column(BIGINT, nullable=True)
    note: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("''"))
