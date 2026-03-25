from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True, frozen=True)
class CursorCheckpoint:
    value: str
    updated_at: datetime = field(default_factory=utc_now)
