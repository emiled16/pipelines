from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True, frozen=True)
class ReliefWebReportsCheckpoint:
    cursor: str
    created_at: datetime | None = None
