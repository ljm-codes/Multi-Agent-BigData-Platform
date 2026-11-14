from datetime import datetime
from typing import Optional

from sqlalchemy import (
    String, Boolean, TEXT, TIMESTAMP,
    CheckConstraint, UniqueConstraint, text, BIGINT, INTEGER, Table, MetaData, Column
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

metadata = MetaData()


class Base(DeclarativeBase):
    pass


class CheckLog(Base):
    __tablename__ = "checker_log"
    __table_args__ = (
        CheckConstraint(
            "app_type in ('SPARK', 'FLINK')",
            name="app_type_check_constraint",
        ),
        CheckConstraint(
            "status in ('PENDING', 'PROCESSING', 'DONE', 'ESCALATED', 'FAILED')",
            name="status_check_constraint",
        ),
        {"schema": "log"},
    )

    id: Mapped[int] = mapped_column(
        BIGINT,
        primary_key=True,
    )
    app_id: Mapped[str] = mapped_column(String(255), nullable=False)
    app_type: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    dedup_key: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, server_default=text("'PENDING'"))
    checker_error: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True, server_default=text("''"))
    is_solved: Mapped[Optional[bool]] = mapped_column(
        Boolean,
        server_default=text("false"),
    )
    check_type: Mapped[str] = mapped_column(String(200), nullable=False)
    error_type: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    check_describe: Mapped[Optional[str]] = mapped_column(
        TEXT,
        server_default=text("''"),
    )
    check_data: Mapped[Optional[dict]] = mapped_column(
        JSONB,
        server_default=text("'{}'::jsonb"),
    )
    repeat_count: Mapped[int] = mapped_column(
        INTEGER,
        nullable=False,
        server_default=text("1"),
    )
    check_time: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("now()"),
    )
    create_time: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("now()"),
    )
    update_time: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("now()"),
    )


class AppLease(Base):
    __tablename__ = "app_leases"
    __table_args__ = (
        {"schema": "public"},
    )
    app_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    session_id: Mapped[str] = mapped_column(String(255), nullable=False)
    acquired_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("now()"),
    )
    heartbeat_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("now()"),
    )


class AgentRun(Base):
    """运维 Agent 的一次运行记录（app_lease 只负责锁，删锁不影响历史）"""
    __tablename__ = "agent_runs"
    __table_args__ = (
        CheckConstraint(
            "status in ('RUNNING', 'PAUSED', 'DONE', 'ERROR', 'ABANDONED')",
            name="agent_run_status_check",
        ),
        {"schema": "log"},
    )
    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    app_id: Mapped[str] = mapped_column(String(255), nullable=False)
    session_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    app_type: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, server_default=text("'RUNNING'"))
    error_count: Mapped[int] = mapped_column(INTEGER, nullable=False, server_default=text("0"))
    started_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("now()"),
    )
    finished_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    out_msg: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)


class AdminInfo(Base):
    __tablename__ = "admin_info"
    __table_args__ = (
        CheckConstraint(
            "permission in (0, 1, 2)",
            name="permission_check_constraint",
        ),
        {"schema": "public"},
    )
    id: Mapped[int] = mapped_column(
        BIGINT,
        primary_key=True,
    )
    username: Mapped[str] = mapped_column(String(255), nullable=False)
    password: Mapped[str] = mapped_column(String(255), nullable=False)
    permission: Mapped[int] = mapped_column(
        INTEGER,
        nullable=False,
        server_default=text("0"),
    )
    disabled: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("false"),
    )
    email: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        unique=True,
    )
    create_time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("now()"),
    )
    update_time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("now()"),
    )


class RecommendApprovalTable(Base):
    __tablename__ = "recommend_approval_log"
    __table_args__ = (
        CheckConstraint(
            "level in (0, 1, 2)",
            name="level_check_constraint",
        ),
        CheckConstraint(
            "recommended_level in (0, 1, 2)",
            name="recommended_level_check_constraint",
        ),
        {"schema": "log"},
    )
    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, )
    recommend_id: Mapped[int] = mapped_column(BIGINT, nullable=False)
    reason: Mapped[Optional[str]] = mapped_column(
        TEXT,
        nullable=True,
        server_default=text("''"),
    )
    level: Mapped[int] = mapped_column(
        INTEGER, nullable=False, server_default=text("0")
    )
    recommended_level: Mapped[int] = mapped_column(
        INTEGER, nullable=False, server_default=text("0")
    )
    is_pass: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    create_time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("now()"),
    )
    update_time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("now()"),
    )


class AdminThread(Base):
    __tablename__ = "admin_thread"
    __table_args__ = (
        {"schema": "public"},
    )
    id: Mapped[int] = mapped_column(BIGINT, primary_key=True,)
    thread_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    thread_name: Mapped[str] = mapped_column(String(20), nullable=False)
    admin_id: Mapped[int] = mapped_column(BIGINT, nullable=False)
    is_archive: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    is_top: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    create_time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("now()"),
    )
    update_time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("now()"),
    )


class AppFileIndex(Base):
    __tablename__ = "app_file_idx"
    __table_args__ = (
        CheckConstraint(
            "app_type in ('SPARK', 'FLINK')",
            name="app_type_check_constraint",
        ),
        CheckConstraint(
            "source_type in ('hdfs', 'local', 'git', 'ssh')",
            name="source_type_check_constraint",
        ),
        {"schema": "code"},
    )
    id: Mapped[int] = mapped_column(BIGINT, primary_key=True,)
    app_type: Mapped[str] = mapped_column(String(20), nullable=False)
    app_name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_type: Mapped[str] = mapped_column(String(10), nullable=False)
    source_dir: Mapped[str] = mapped_column(TEXT, nullable=False)
    commit_cmd: Mapped[str] = mapped_column(TEXT, nullable=False)
    create_time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("now()"),
    )
    update_time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("now()"),
    )


app_lease = Table(
    "app_leases", metadata,
    Column("app_id", String, primary_key=True),
    Column("session_id", String, nullable=False),
    Column("acquired_at", TIMESTAMP(timezone=True), nullable=False),
    Column("heartbeat_at", TIMESTAMP(timezone=True), nullable=False),
)


class ToolTraceInfo(Base):
    """工具调用轨迹的 UI 投影；完整审计仍在 LangGraph messages 中。"""

    __tablename__ = "tool_trace"
    __table_args__ = (
        CheckConstraint(
            "status in ('running', 'success', 'error')",
            name="tool_trace_status_check",
        ),
        UniqueConstraint("thread_id", "trace_id", name="uk_tool_trace_thread_call"),
        {"schema": "log"},
    )

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    trace_id: Mapped[str] = mapped_column(TEXT, nullable=False)
    thread_id: Mapped[str] = mapped_column(String(255), nullable=False)
    agent: Mapped[str] = mapped_column(String(32), nullable=False)
    tool: Mapped[str] = mapped_column(String(128), nullable=False)
    args: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    output: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)
    output_truncated: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default=text("'running'"))
    error: Mapped[Optional[str]] = mapped_column(TEXT, nullable=True)
    started_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )
    finished_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    duration_ms: Mapped[Optional[int]] = mapped_column(INTEGER, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )


class AdminProject(Base):
    """用户历史项目/任务登记；模型只接触 project_id，真实目录由 app_type/date/project_name 解析。"""

    __tablename__ = "admin_project"
    __table_args__ = (
        CheckConstraint("app_type in ('spark', 'flink')", name="admin_project_app_type_check"),
        CheckConstraint(
            "status in ('active', 'completed', 'failed', 'missing', 'archived')",
            name="admin_project_status_check",
        ),
        CheckConstraint(
            "verify_status in ('unknown', 'passed', 'failed', 'error')",
            name="admin_project_verify_status_check",
        ),
        UniqueConstraint(
            "admin_id", "app_type", "project_date", "project_name",
            name="uk_admin_project_coords",
        ),
        {"schema": "public"},
    )

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    project_id: Mapped[str] = mapped_column(String(36), nullable=False, unique=True)
    admin_id: Mapped[int] = mapped_column(BIGINT, nullable=False)
    thread_id: Mapped[str] = mapped_column(String(255), nullable=False)
    title: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("''"))
    summary: Mapped[str] = mapped_column(TEXT, nullable=False, server_default=text("''"))
    app_type: Mapped[str] = mapped_column(String(16), nullable=False)
    project_date: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    project_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    start_message_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    end_message_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default=text("'active'"))
    verify_status: Mapped[str] = mapped_column(String(16), nullable=False, server_default=text("'unknown'"))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")
    )


class AdminProjectConversation(Base):
    """项目在某一个会话里的对话区间；跨会话续聊会新增一条。"""

    __tablename__ = "admin_project_conversation"
    __table_args__ = (
        UniqueConstraint(
            "project_id", "thread_id", "start_message_id",
            name="uk_project_conversation_segment",
        ),
        {"schema": "public"},
    )

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    project_id: Mapped[str] = mapped_column(String(36), nullable=False)
    thread_id: Mapped[str] = mapped_column(String(255), nullable=False)
    start_message_id: Mapped[str] = mapped_column(String(64), nullable=False)
    end_message_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=text("now()")
    )
