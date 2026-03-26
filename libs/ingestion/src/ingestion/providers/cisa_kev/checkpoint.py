from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class CisaKevCheckpoint:
    catalog_version: str | None = None
    etag: str | None = None
    last_modified: str | None = None
