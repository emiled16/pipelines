from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class FirmsFiresCheckpoint:
    source: str
    area: str
    days: int
    last_entry_id: str
