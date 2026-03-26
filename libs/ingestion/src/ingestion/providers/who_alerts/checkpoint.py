from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class WhoAlertsCheckpoint:
    api_url: str
    last_alert_id: str
    etag: str | None = None
    last_modified: str | None = None
