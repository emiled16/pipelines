from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True, frozen=True)
class USAspendingAwardsCheckpoint:
    query_hash: str
    cursor: str
    last_modified_at: datetime | None = None
