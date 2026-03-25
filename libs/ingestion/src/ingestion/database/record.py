from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import JSON, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from ingestion.database.base import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class StoredRecordORM(Base):
    __tablename__ = "ingestion_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    provider: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    record_key: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    payload: Mapped[Any] = mapped_column(JSON, nullable=False)
    metadata_: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSON,
        nullable=False,
        default=dict,
    )
    occurred_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
