from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True, frozen=True)
class RssCheckpoint:
    feed_url: str
    last_entry_id: str
    updated_at: datetime = field(default_factory=utc_now)
