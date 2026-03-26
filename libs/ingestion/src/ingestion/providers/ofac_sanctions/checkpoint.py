from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class OfacSanctionsCheckpoint:
    source_url: str
    etag: str | None = None
    last_modified: str | None = None
