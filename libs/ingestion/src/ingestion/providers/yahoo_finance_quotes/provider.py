from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from typing import Any

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.yahoo_finance_quotes.checkpoint import YahooFinanceQuotesCheckpoint
from ingestion.providers.yahoo_finance_quotes.loader import (
    YahooFinanceQuotesLoadResult,
    load_quotes,
)
from ingestion.utils.time import utc_now

YahooFinanceQuote = Mapping[str, Any]
QuotesLoader = Callable[
    [YahooFinanceQuotesCheckpoint | None],
    YahooFinanceQuotesLoadResult
    | Iterable[YahooFinanceQuote]
    | Awaitable[YahooFinanceQuotesLoadResult | Iterable[YahooFinanceQuote]],
]


@dataclass(slots=True)
class _FetchState:
    latest_quote_time_by_symbol: dict[str, int] = field(default_factory=dict)


class YahooFinanceQuotesProvider(BatchProvider[YahooFinanceQuotesCheckpoint]):
    def __init__(
        self,
        *,
        symbols: Iterable[str],
        quotes_loader: QuotesLoader | None = None,
        name: str | None = None,
    ) -> None:
        self.symbols = _normalize_symbols(symbols)
        self.name = name or f"yahoo_finance_quotes:{','.join(self.symbols)}"
        self._quotes_loader = quotes_loader or partial(load_quotes, self.symbols)
        self._fetch_state = _FetchState()

    async def fetch(
        self, *, checkpoint: YahooFinanceQuotesCheckpoint | None = None
    ) -> AsyncIterator[Record]:
        load_result = await self._load_quotes(checkpoint)
        self._fetch_state = _FetchState()

        batch_fetched_at = utc_now()
        quotes = iter(load_result.quotes)
        try:
            for quote in quotes:
                symbol = str(quote["symbol"])
                quote_timestamp = _coerce_int(quote.get("quote_timestamp"))

                if self._should_skip_quote(
                    checkpoint=checkpoint,
                    symbol=symbol,
                    quote_timestamp=quote_timestamp,
                ):
                    continue

                if quote_timestamp is not None:
                    self._fetch_state.latest_quote_time_by_symbol[symbol] = quote_timestamp

                yield Record(
                    provider=self.name,
                    key=_build_record_key(symbol, quote_timestamp),
                    payload=dict(quote),
                    occurred_at=_coerce_occurred_at(quote.get("occurred_at")),
                    fetched_at=batch_fetched_at,
                    metadata={"symbol": symbol},
                )
        finally:
            close = getattr(quotes, "close", None)
            if callable(close):
                close()

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: YahooFinanceQuotesCheckpoint | None,
    ) -> YahooFinanceQuotesCheckpoint | None:
        if not self._fetch_state.latest_quote_time_by_symbol:
            if previous_checkpoint is not None and _matches_symbols(
                previous_checkpoint, self.symbols
            ):
                return previous_checkpoint
            return None

        merged_quote_times: dict[str, int] = {}
        if previous_checkpoint is not None and _matches_symbols(previous_checkpoint, self.symbols):
            merged_quote_times.update(previous_checkpoint.last_quote_time_by_symbol)

        merged_quote_times.update(self._fetch_state.latest_quote_time_by_symbol)
        return YahooFinanceQuotesCheckpoint(
            symbols=list(self.symbols),
            last_quote_time_by_symbol=merged_quote_times,
            cursor=str(max(merged_quote_times.values())),
        )

    async def _load_quotes(
        self, checkpoint: YahooFinanceQuotesCheckpoint | None
    ) -> YahooFinanceQuotesLoadResult:
        result = self._quotes_loader(checkpoint)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, YahooFinanceQuotesLoadResult):
            return result
        return YahooFinanceQuotesLoadResult(quotes=result)

    def _should_skip_quote(
        self,
        *,
        checkpoint: YahooFinanceQuotesCheckpoint | None,
        symbol: str,
        quote_timestamp: int | None,
    ) -> bool:
        if quote_timestamp is None or checkpoint is None:
            return False
        if not _matches_symbols(checkpoint, self.symbols):
            return False

        last_quote_time = checkpoint.last_quote_time_by_symbol.get(symbol)
        return last_quote_time is not None and quote_timestamp <= last_quote_time


def _normalize_symbols(symbols: Iterable[str]) -> list[str]:
    normalized_symbols: list[str] = []
    seen_symbols: set[str] = set()

    for symbol in symbols:
        normalized_symbol = symbol.strip().upper()
        if not normalized_symbol or normalized_symbol in seen_symbols:
            continue
        normalized_symbols.append(normalized_symbol)
        seen_symbols.add(normalized_symbol)

    if not normalized_symbols:
        raise ValueError("symbols must contain at least one non-empty symbol")

    return normalized_symbols


def _matches_symbols(
    checkpoint: YahooFinanceQuotesCheckpoint, provider_symbols: list[str]
) -> bool:
    return _normalize_symbols(checkpoint.symbols) == provider_symbols


def _build_record_key(symbol: str, quote_timestamp: int | None) -> str:
    if quote_timestamp is None:
        return symbol
    return f"{symbol}:{quote_timestamp}"


def _coerce_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _coerce_occurred_at(value: object):
    return value if isinstance(value, datetime) else None
