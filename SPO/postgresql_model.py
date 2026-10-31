from datetime import datetime
from typing import Optional

from sqlalchemy import (
    String, Boolean, TEXT, TIMESTAMP,
    CheckConstraint, text, BIGINT, INTEGER, Table, MetaData, Column
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


app_lease = Table(
    "app_leases", metadata,
    Column("app_id", String, primary_key=True),
    Column("session_id", String, nullable=False),
    Column("acquired_at", TIMESTAMP(timezone=True), nullable=False),
    Column("heartbeat_at", TIMESTAMP(timezone=True), nullable=False),
)
