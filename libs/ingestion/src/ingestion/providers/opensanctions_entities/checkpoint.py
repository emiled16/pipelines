from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True, frozen=True)
class OpenSanctionsEntitiesCheckpoint:
    dataset: str
    version: str
    entities_url: str
    checksum: str | None = None
    updated_at: datetime | None = None
    last_change: datetime | None = None
