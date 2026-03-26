from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class ComtradeTradeCheckpoint:
    query_signature: str
    cursor: str
