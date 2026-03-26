from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime, timezone

from ingestion.providers.yahoo_finance_quotes.checkpoint import YahooFinanceQuotesCheckpoint
from ingestion.providers.yahoo_finance_quotes.loader import YahooFinanceQuotesLoadResult
from ingestion.providers.yahoo_finance_quotes.provider import YahooFinanceQuotesProvider


def test_yahoo_finance_quotes_provider_normalizes_symbols() -> None:
    provider = YahooFinanceQuotesProvider(symbols=[" aapl ", "MSFT", "aapl"])

    assert provider.symbols == ["AAPL", "MSFT"]
    assert provider.name == "yahoo_finance_quotes:AAPL,MSFT"


def test_yahoo_finance_quotes_provider_yields_all_quotes_without_checkpoint() -> None:
    async def run() -> None:
        provider = YahooFinanceQuotesProvider(
            symbols=["AAPL", "MSFT"],
            quotes_loader=lambda checkpoint: _quotes(
                [
                    {
                        "symbol": "AAPL",
                        "quote_timestamp": 1774460400,
                        "occurred_at": datetime(2026, 3, 24, 0, 20, tzinfo=timezone.utc),
                        "market_price": 219.1,
                    },
                    {
                        "symbol": "MSFT",
                        "quote_timestamp": 1774460405,
                        "occurred_at": datetime(2026, 3, 24, 0, 20, 5, tzinfo=timezone.utc),
                        "market_price": 389.2,
                    },
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == ["AAPL:1774460400", "MSFT:1774460405"]
        assert [record.metadata["symbol"] for record in records] == ["AAPL", "MSFT"]

    asyncio.run(run())


def test_yahoo_finance_quotes_provider_skips_quotes_at_or_before_checkpoint() -> None:
    async def run() -> None:
        provider = YahooFinanceQuotesProvider(
            symbols=["AAPL", "MSFT"],
            quotes_loader=lambda checkpoint: _quotes(
                [
                    {
                        "symbol": "AAPL",
                        "quote_timestamp": 1774460500,
                        "occurred_at": datetime(2026, 3, 24, 0, 21, 40, tzinfo=timezone.utc),
                    },
                    {
                        "symbol": "MSFT",
                        "quote_timestamp": 1774460405,
                        "occurred_at": datetime(2026, 3, 24, 0, 20, 5, tzinfo=timezone.utc),
                    },
                ]
            ),
        )
        checkpoint = YahooFinanceQuotesCheckpoint(
            symbols=["AAPL", "MSFT"],
            last_quote_time_by_symbol={"AAPL": 1774460400, "MSFT": 1774460405},
            cursor="1774460405",
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["AAPL:1774460500"]

    asyncio.run(run())


def test_yahoo_finance_quotes_provider_assigns_one_fetched_at_per_batch() -> None:
    async def run() -> None:
        provider = YahooFinanceQuotesProvider(
            symbols=["AAPL", "MSFT"],
            quotes_loader=lambda checkpoint: _quotes(
                [
                    {"symbol": "AAPL", "quote_timestamp": 1774460400},
                    {"symbol": "MSFT", "quote_timestamp": 1774460405},
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert len({record.fetched_at for record in records}) == 1

    asyncio.run(run())


def test_yahoo_finance_quotes_provider_builds_checkpoint_from_latest_emitted_quotes() -> None:
    async def run() -> None:
        provider = YahooFinanceQuotesProvider(
            symbols=["AAPL", "MSFT"],
            quotes_loader=lambda checkpoint: _load_result(
                [
                    {"symbol": "AAPL", "quote_timestamp": 1774460500},
                    {"symbol": "MSFT", "quote_timestamp": 1774460405},
                ]
            ),
        )
        previous_checkpoint = YahooFinanceQuotesCheckpoint(
            symbols=["AAPL", "MSFT"],
            last_quote_time_by_symbol={"AAPL": 1774460400},
            cursor="1774460400",
        )

        records = [record async for record in provider.fetch(checkpoint=previous_checkpoint)]
        checkpoint = provider.build_checkpoint(previous_checkpoint=previous_checkpoint)

        assert len(records) == 2
        assert checkpoint == YahooFinanceQuotesCheckpoint(
            symbols=["AAPL", "MSFT"],
            last_quote_time_by_symbol={"AAPL": 1774460500, "MSFT": 1774460405},
            cursor="1774460500",
        )

    asyncio.run(run())


def test_yahoo_finance_quotes_provider_preserves_checkpoint_when_nothing_is_new() -> None:
    async def run() -> None:
        provider = YahooFinanceQuotesProvider(
            symbols=["AAPL", "MSFT"],
            quotes_loader=lambda checkpoint: _load_result(
                [
                    {"symbol": "AAPL", "quote_timestamp": 1774460400},
                    {"symbol": "MSFT", "quote_timestamp": 1774460405},
                ]
            ),
        )
        previous_checkpoint = YahooFinanceQuotesCheckpoint(
            symbols=["AAPL", "MSFT"],
            last_quote_time_by_symbol={"AAPL": 1774460400, "MSFT": 1774460405},
            cursor="1774460405",
        )

        records = [record async for record in provider.fetch(checkpoint=previous_checkpoint)]
        checkpoint = provider.build_checkpoint(previous_checkpoint=previous_checkpoint)

        assert records == []
        assert checkpoint == previous_checkpoint

    asyncio.run(run())


def test_yahoo_finance_quotes_provider_closes_quote_iterator() -> None:
    async def run() -> None:
        quotes = _ClosableQuotes(
            [
                {"symbol": "AAPL", "quote_timestamp": 1774460500},
                {"symbol": "MSFT", "quote_timestamp": 1774460405},
            ]
        )
        provider = YahooFinanceQuotesProvider(
            symbols=["AAPL", "MSFT"],
            quotes_loader=lambda checkpoint: YahooFinanceQuotesLoadResult(quotes=quotes),
        )

        records = [record async for record in provider.fetch()]

        assert len(records) == 2
        assert quotes.closed is True

    asyncio.run(run())


def _quotes(items: list[dict[str, object]]) -> Iterable[dict[str, object]]:
    for item in items:
        yield item


def _load_result(items: list[dict[str, object]]) -> YahooFinanceQuotesLoadResult:
    return YahooFinanceQuotesLoadResult(quotes=_quotes(items))


class _ClosableQuotes:
    def __init__(self, items: list[dict[str, object]]) -> None:
        self._items = iter(items)
        self.closed = False

    def __iter__(self) -> _ClosableQuotes:
        return self

    def __next__(self) -> dict[str, object]:
        return next(self._items)

    def close(self) -> None:
        self.closed = True
