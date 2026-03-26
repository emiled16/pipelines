from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class SafecastRadiationCheckpoint:
    cursor: str
