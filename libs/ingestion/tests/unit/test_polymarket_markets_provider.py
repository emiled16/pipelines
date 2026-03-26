from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime, timezone

from ingestion.providers.polymarket_markets.checkpoint import PolymarketMarketsCheckpoint
from ingestion.providers.polymarket_markets.provider import PolymarketMarketsProvider


def test_polymarket_markets_provider_yields_all_markets_without_checkpoint() -> None:
    async def run() -> None:
        provider = PolymarketMarketsProvider(
            markets_loader=lambda checkpoint: _markets(
                [
                    {
                        "id": "market-3",
                        "question": "Newest",
                        "updated_at": datetime(2026, 3, 24, tzinfo=timezone.utc),
                    },
                    {
                        "id": "market-2",
                        "question": "Middle",
                        "updated_at": datetime(2026, 3, 23, tzinfo=timezone.utc),
                    },
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == [
            "market-3:2026-03-24T00:00:00+00:00",
            "market-2:2026-03-23T00:00:00+00:00",
        ]
        assert records[0].metadata["api_url"] == "https://gamma-api.polymarket.com"

    asyncio.run(run())


def test_polymarket_markets_provider_stops_once_checkpoint_cursor_is_reached() -> None:
    async def run() -> None:
        provider = PolymarketMarketsProvider(
            markets_loader=lambda checkpoint: _markets(
                [
                    {
                        "id": "market-3",
                        "updated_at": datetime(2026, 3, 24, tzinfo=timezone.utc),
                    },
                    {
                        "id": "market-2",
                        "updated_at": datetime(2026, 3, 23, tzinfo=timezone.utc),
                    },
                    {
                        "id": "market-1",
                        "updated_at": datetime(2026, 3, 22, tzinfo=timezone.utc),
                    },
                ]
            ),
        )
        checkpoint = PolymarketMarketsCheckpoint(
            api_url="https://gamma-api.polymarket.com",
            cursor="market-2:2026-03-23T00:00:00+00:00",
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["market-3:2026-03-24T00:00:00+00:00"]

    asyncio.run(run())


def test_polymarket_markets_provider_assigns_one_fetched_at_per_batch() -> None:
    async def run() -> None:
        provider = PolymarketMarketsProvider(
            markets_loader=lambda checkpoint: _markets(
                [
                    {"id": "market-3"},
                    {"id": "market-2"},
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert len({record.fetched_at for record in records}) == 1

    asyncio.run(run())


def test_polymarket_markets_provider_builds_checkpoint_from_newest_record_cursor() -> None:
    async def run() -> None:
        provider = PolymarketMarketsProvider(
            markets_loader=lambda checkpoint: _markets(
                [
                    {
                        "id": "market-3",
                        "updated_at": datetime(2026, 3, 24, tzinfo=timezone.utc),
                    }
                ]
            ),
        )

        records = [record async for record in provider.fetch()]
        checkpoint = provider.build_checkpoint(
            previous_checkpoint=None,
            cursor=records[0].key,
        )

        assert checkpoint == PolymarketMarketsCheckpoint(
            api_url="https://gamma-api.polymarket.com",
            cursor="market-3:2026-03-24T00:00:00+00:00",
        )

    asyncio.run(run())


def test_polymarket_markets_provider_preserves_previous_checkpoint_without_new_records() -> None:
    async def run() -> None:
        provider = PolymarketMarketsProvider(markets_loader=lambda checkpoint: _markets([]))
        previous_checkpoint = PolymarketMarketsCheckpoint(
            api_url="https://gamma-api.polymarket.com",
            cursor="market-3:2026-03-24T00:00:00+00:00",
        )

        records = [record async for record in provider.fetch(checkpoint=previous_checkpoint)]
        checkpoint = provider.build_checkpoint(
            previous_checkpoint=previous_checkpoint,
            cursor=None,
        )

        assert records == []
        assert checkpoint == previous_checkpoint

    asyncio.run(run())


def test_polymarket_markets_provider_closes_iterator_when_checkpoint_stops_iteration() -> None:
    async def run() -> None:
        markets = _ClosableMarkets(
            [
                {
                    "id": "market-3",
                    "updated_at": datetime(2026, 3, 24, tzinfo=timezone.utc),
                },
                {
                    "id": "market-2",
                    "updated_at": datetime(2026, 3, 23, tzinfo=timezone.utc),
                },
                {
                    "id": "market-1",
                    "updated_at": datetime(2026, 3, 22, tzinfo=timezone.utc),
                },
            ]
        )
        provider = PolymarketMarketsProvider(markets_loader=lambda checkpoint: markets)
        checkpoint = PolymarketMarketsCheckpoint(
            api_url="https://gamma-api.polymarket.com",
            cursor="market-2:2026-03-23T00:00:00+00:00",
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["market-3:2026-03-24T00:00:00+00:00"]
        assert markets.closed is True

    asyncio.run(run())


def _markets(items: list[dict[str, object]]) -> Iterable[dict[str, object]]:
    for item in items:
        yield item


class _ClosableMarkets:
    def __init__(self, items: list[dict[str, object]]) -> None:
        self._items = iter(items)
        self.closed = False

    def __iter__(self) -> _ClosableMarkets:
        return self

    def __next__(self) -> dict[str, object]:
        return next(self._items)

    def close(self) -> None:
        self.closed = True
