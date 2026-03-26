from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class EiaEnergyCheckpoint:
    route: str
    cursor: str
    last_period: str | None = None
