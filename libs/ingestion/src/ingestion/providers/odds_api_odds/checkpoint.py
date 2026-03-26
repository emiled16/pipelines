from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True, frozen=True)
class OddsApiOddsCheckpoint:
    sport: str
    regions: tuple[str, ...]
    markets: tuple[str, ...]
    bookmakers: tuple[str, ...] = ()
    last_seen_at: datetime | None = None
    event_ids_at_last_seen_at: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "regions", tuple(self.regions))
        object.__setattr__(self, "markets", tuple(self.markets))
        object.__setattr__(self, "bookmakers", tuple(self.bookmakers))
        object.__setattr__(
            self,
            "event_ids_at_last_seen_at",
            tuple(self.event_ids_at_last_seen_at),
        )

