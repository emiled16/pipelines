from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True, frozen=True)
class PolymarketQuotesCheckpoint:
    token_ids: tuple[str, ...]
    book_versions: dict[str, str] = field(default_factory=dict)
