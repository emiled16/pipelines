from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import JSON, DateTime, String, UniqueConstraint, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from ingestion.database.base import Base
from ingestion.utils.time import utc_now


class StoredRecordORM(Base):
    __tablename__ = "ingestion_records"
    __table_args__ = (
        UniqueConstraint(
            "provider",
            "record_key",
            name="uq_ingestion_records_provider_record_key",
        ),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
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
