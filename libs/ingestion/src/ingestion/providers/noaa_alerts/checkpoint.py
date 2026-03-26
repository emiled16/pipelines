from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class NoaaAlertsCheckpoint:
    alerts_url: str
    cursor: str
    etag: str | None = None
    last_modified: str | None = None
