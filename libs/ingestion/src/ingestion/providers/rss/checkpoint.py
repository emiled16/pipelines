from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class RssCheckpoint:
    feed_url: str
    last_entry_id: str
    etag: str | None = None
    last_modified: str | None = None
