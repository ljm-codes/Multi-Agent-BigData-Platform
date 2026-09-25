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
    __tablename__ = "app_lease"
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


app_lease = Table(
    "app_lease", metadata,
    Column("app_id", String, primary_key=True),
    Column("session_id", String, nullable=False),
    Column("acquired_at", TIMESTAMP(timezone=True), nullable=False),
    Column("heartbeat_at", TIMESTAMP(timezone=True), nullable=False),
)
