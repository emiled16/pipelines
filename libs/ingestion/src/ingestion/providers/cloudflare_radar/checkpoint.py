from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True, frozen=True)
class CloudflareRadarCheckpoint:
    scope: str
    cursor_started_at: datetime
    anomaly_ids_at_cursor: tuple[str, ...]
