from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True, frozen=True)
class GdeltArticlesCheckpoint:
    query: str
    cursor: str
    last_seen_at: datetime
