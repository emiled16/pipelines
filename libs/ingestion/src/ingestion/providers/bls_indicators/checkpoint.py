from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True, frozen=True)
class BlsIndicatorsCheckpoint:
    series_ids: tuple[str, ...]
    cursor: str | None = None
    series_cursors: dict[str, str] = field(default_factory=dict)
