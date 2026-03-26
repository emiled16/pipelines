from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class TreasuryMetricsCheckpoint:
    endpoint: str
    cursor: str
