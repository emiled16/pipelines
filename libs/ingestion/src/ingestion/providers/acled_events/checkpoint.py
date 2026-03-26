from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class AcledEventsCheckpoint:
    endpoint: str
    query_params: dict[str, str]
    cursor: str
