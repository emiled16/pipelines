from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class FredSeriesCheckpoint:
    series_id: str
    cursor: str
