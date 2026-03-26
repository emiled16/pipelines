from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True, frozen=True)
class YahooFinanceQuotesCheckpoint:
    symbols: list[str]
    last_quote_time_by_symbol: dict[str, int] = field(default_factory=dict)
    cursor: str | None = None
