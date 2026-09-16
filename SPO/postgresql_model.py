import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    UUID, String, CHAR, Boolean, TEXT, JSON, TIMESTAMP,
    CheckConstraint, text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class CheckLog(Base):
    __tablename__ = "checker_log"
    __table_args__ = (
        CheckConstraint(
            "app_type in ('SPARK', 'FLINK')",
            name="app_type_check_constraint",
        ),
        {"schema": "log"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID,
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    app_id: Mapped[str] = mapped_column(String(255), nullable=False)
    app_type: Mapped[str] = mapped_column(CHAR(5), nullable=False)
    is_solved: Mapped[Optional[bool]] = mapped_column(
        Boolean,
        server_default=text("false"),
    )
    check_type: Mapped[str] = mapped_column(String(200), nullable=False)
    error_type: Mapped[str] = mapped_column(String(200), nullable=False)
    describe: Mapped[Optional[str]] = mapped_column(
        TEXT,
        server_default=text("''"),
    )
    check_data: Mapped[Optional[dict]] = mapped_column(
        JSON,
        server_default=text("'{}'::json"),
    )
    create_time: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("now()"),
    )
