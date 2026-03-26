from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime, timezone

from ingestion.providers.polymarket_quotes.checkpoint import PolymarketQuotesCheckpoint
from ingestion.providers.polymarket_quotes.provider import PolymarketQuotesProvider


def test_polymarket_quotes_provider_yields_changed_books_without_checkpoint() -> None:
    async def run() -> None:
        provider = PolymarketQuotesProvider(
            token_ids=["token-1", "token-2"],
            books_loader=lambda token_ids: _books(
                [
                    {
                        "market": "market-1",
                        "asset_id": "token-1",
                        "timestamp": "2026-03-25T10:15:00Z",
                        "hash": "hash-1",
                        "bids": [{"price": "0.48", "size": "100"}],
                        "asks": [{"price": "0.52", "size": "80"}],
                    },
                    {
                        "market": "market-2",
                        "asset_id": "token-2",
                        "timestamp": "2026-03-25T10:16:00Z",
                        "hash": "hash-2",
                        "bids": [{"price": "0.12", "size": "50"}],
                        "asks": [{"price": "0.17", "size": "75"}],
                    },
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == ["token-1:hash-1", "token-2:hash-2"]
        assert records[0].payload["midpoint"] == "0.5"
        assert records[0].payload["spread"] == "0.04"
        assert records[0].metadata["api_url"] == "https://clob.polymarket.com"
        assert records[0].occurred_at == datetime(2026, 3, 25, 10, 15, tzinfo=timezone.utc)

    asyncio.run(run())


def test_polymarket_quotes_provider_skips_unchanged_books_from_checkpoint() -> None:
    async def run() -> None:
        provider = PolymarketQuotesProvider(
            token_ids=["token-1", "token-2"],
            books_loader=lambda token_ids: _books(
                [
                    {"asset_id": "token-1", "hash": "hash-1"},
                    {"asset_id": "token-2", "hash": "hash-2"},
                ]
            ),
        )
        checkpoint = PolymarketQuotesCheckpoint(
            token_ids=("token-1", "token-2"),
            book_versions={"token-1": "hash-1"},
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["token-2:hash-2"]

    asyncio.run(run())


def test_polymarket_quotes_provider_assigns_one_fetched_at_per_batch() -> None:
    async def run() -> None:
        provider = PolymarketQuotesProvider(
            token_ids=["token-1", "token-2"],
            books_loader=lambda token_ids: _books(
                [
                    {"asset_id": "token-1", "hash": "hash-1"},
                    {"asset_id": "token-2", "hash": "hash-2"},
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert len({record.fetched_at for record in records}) == 1

    asyncio.run(run())


def test_polymarket_quotes_provider_builds_checkpoint_from_latest_versions() -> None:
    async def run() -> None:
        provider = PolymarketQuotesProvider(
            token_ids=["token-1", "token-2"],
            books_loader=lambda token_ids: _books(
                [
                    {"asset_id": "token-1", "hash": "hash-1"},
                    {"asset_id": "token-2", "timestamp": "2026-03-25T10:16:00Z"},
                ]
            ),
        )

        records = [record async for record in provider.fetch()]
        checkpoint = provider.build_checkpoint()

        assert len(records) == 2
        assert checkpoint == PolymarketQuotesCheckpoint(
            token_ids=("token-1", "token-2"),
            book_versions={
                "token-1": "hash-1",
                "token-2": "2026-03-25T10:16:00Z",
            },
        )

    asyncio.run(run())


def test_polymarket_quotes_provider_preserves_previous_versions_when_no_changes_are_emitted() -> None:
    async def run() -> None:
        provider = PolymarketQuotesProvider(
            token_ids=["token-1", "token-2"],
            books_loader=lambda token_ids: _books(
                [
                    {"asset_id": "token-1", "hash": "hash-1"},
                    {"asset_id": "token-2", "hash": "hash-2"},
                ]
            ),
        )
        previous_checkpoint = PolymarketQuotesCheckpoint(
            token_ids=("token-1", "token-2"),
            book_versions={
                "token-1": "hash-1",
                "token-2": "hash-2",
            },
        )

        records = [record async for record in provider.fetch(checkpoint=previous_checkpoint)]
        checkpoint = provider.build_checkpoint(previous_checkpoint=previous_checkpoint)

        assert records == []
        assert checkpoint == previous_checkpoint

    asyncio.run(run())


def test_polymarket_quotes_provider_closes_iterator_when_checkpoint_skips_items() -> None:
    async def run() -> None:
        entries = _ClosableBooks(
            [
                {"asset_id": "token-1", "hash": "hash-1"},
                {"asset_id": "token-2", "hash": "hash-2"},
            ]
        )
        provider = PolymarketQuotesProvider(
            token_ids=["token-1", "token-2"],
            books_loader=lambda token_ids: entries,
        )
        checkpoint = PolymarketQuotesCheckpoint(
            token_ids=("token-1", "token-2"),
            book_versions={
                "token-1": "hash-1",
                "token-2": "hash-2",
            },
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert records == []
        assert entries.closed is True

    asyncio.run(run())


def _books(items: list[dict[str, object]]) -> Iterable[dict[str, object]]:
    for item in items:
        yield item


class _ClosableBooks:
    def __init__(self, items: list[dict[str, object]]) -> None:
        self._items = iter(items)
        self.closed = False

    def __iter__(self) -> _ClosableBooks:
        return self

    def __next__(self) -> dict[str, object]:
        return next(self._items)

    def close(self) -> None:
        self.closed = True
