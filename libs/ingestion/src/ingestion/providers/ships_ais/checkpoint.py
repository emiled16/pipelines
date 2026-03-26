from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True, frozen=True)
class ShipsAisCheckpoint:
    stream_name: str
    last_record_key: str
    last_occurred_at: datetime | None = None
