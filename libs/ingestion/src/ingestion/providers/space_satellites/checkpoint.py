from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class SpaceSatellitesCheckpoint:
    group: str
    last_record_id: str

