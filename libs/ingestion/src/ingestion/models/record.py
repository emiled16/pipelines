from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from ingestion.utils.time import utc_now


@dataclass(slots=True, frozen=True)
class Record:
    provider: str
    payload: Any
    key: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    occurred_at: datetime | None = None
    fetched_at: datetime = field(default_factory=utc_now)


@dataclass(slots=True, frozen=True)
class WriteResult:
    records_written: int
