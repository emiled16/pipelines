from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import UTC, datetime

from ingestion.providers.gdelt_articles.checkpoint import GdeltArticlesCheckpoint
from ingestion.providers.gdelt_articles.provider import GdeltArticlesProvider


def test_gdelt_articles_provider_yields_all_entries_without_checkpoint() -> None:
    async def run() -> None:
        provider = GdeltArticlesProvider(
            query="climate change",
            entries_loader=lambda checkpoint: _entries(
                [
                    {
                        "id": "https://example.com/articles/3",
                        "url": "https://example.com/articles/3",
                        "title": "Newest",
                        "seen_at": datetime(2026, 3, 25, 10, 20, tzinfo=UTC),
                    },
                    {
                        "id": "https://example.com/articles/2",
                        "url": "https://example.com/articles/2",
                        "title": "Older",
                        "seen_at": datetime(2026, 3, 25, 10, 15, tzinfo=UTC),
                    },
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == [
            "https://example.com/articles/3",
            "https://example.com/articles/2",
        ]
        assert records[0].metadata["query"] == "climate change"
        assert records[0].occurred_at == datetime(2026, 3, 25, 10, 20, tzinfo=UTC)

    asyncio.run(run())


def test_gdelt_articles_provider_stops_at_checkpoint_boundary() -> None:
    async def run() -> None:
        provider = GdeltArticlesProvider(
            query="climate change",
            entries_loader=lambda checkpoint: _entries(
                [
                    {
                        "id": "https://example.com/articles/4",
                        "title": "Newest",
                        "seen_at": datetime(2026, 3, 25, 10, 30, tzinfo=UTC),
                    },
                    {
                        "id": "https://example.com/articles/3",
                        "title": "Same timestamp but not checkpoint",
                        "seen_at": datetime(2026, 3, 25, 10, 20, tzinfo=UTC),
                    },
                    {
                        "id": "https://example.com/articles/2",
                        "title": "Checkpoint article",
                        "seen_at": datetime(2026, 3, 25, 10, 20, tzinfo=UTC),
                    },
                    {
                        "id": "https://example.com/articles/1",
                        "title": "Older article",
                        "seen_at": datetime(2026, 3, 25, 10, 10, tzinfo=UTC),
                    },
                ]
            ),
        )
        checkpoint = GdeltArticlesCheckpoint(
            query="climate change",
            cursor="https://example.com/articles/2",
            last_seen_at=datetime(2026, 3, 25, 10, 20, tzinfo=UTC),
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == [
            "https://example.com/articles/4",
            "https://example.com/articles/3",
        ]

    asyncio.run(run())


def test_gdelt_articles_provider_assigns_one_fetched_at_per_batch() -> None:
    async def run() -> None:
        provider = GdeltArticlesProvider(
            query="climate change",
            entries_loader=lambda checkpoint: _entries(
                [
                    {"id": "https://example.com/articles/2", "title": "Two"},
                    {"id": "https://example.com/articles/1", "title": "One"},
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert len({record.fetched_at for record in records}) == 1

    asyncio.run(run())


def test_gdelt_articles_provider_builds_checkpoint_from_newest_record() -> None:
    provider = GdeltArticlesProvider(query="climate change")

    checkpoint = provider.build_checkpoint(
        previous_checkpoint=None,
        last_article_id="https://example.com/articles/5",
        last_seen_at=datetime(2026, 3, 25, 10, 35, tzinfo=UTC),
    )

    assert checkpoint == GdeltArticlesCheckpoint(
        query="climate change",
        cursor="https://example.com/articles/5",
        last_seen_at=datetime(2026, 3, 25, 10, 35, tzinfo=UTC),
    )


def test_gdelt_articles_provider_preserves_checkpoint_when_no_new_records() -> None:
    provider = GdeltArticlesProvider(query="climate change")
    previous_checkpoint = GdeltArticlesCheckpoint(
        query="climate change",
        cursor="https://example.com/articles/5",
        last_seen_at=datetime(2026, 3, 25, 10, 35, tzinfo=UTC),
    )

    checkpoint = provider.build_checkpoint(
        previous_checkpoint=previous_checkpoint,
        last_article_id=None,
        last_seen_at=None,
    )

    assert checkpoint == previous_checkpoint


def test_gdelt_articles_provider_closes_entries_iterator_when_checkpoint_stops_iteration() -> None:
    async def run() -> None:
        from ingestion.providers.gdelt_articles.loader import GdeltArticlesLoadResult

        entries = _ClosableEntries(
            [
                {
                    "id": "https://example.com/articles/4",
                    "title": "Newest",
                    "seen_at": datetime(2026, 3, 25, 10, 30, tzinfo=UTC),
                },
                {
                    "id": "https://example.com/articles/3",
                    "title": "Checkpoint article",
                    "seen_at": datetime(2026, 3, 25, 10, 20, tzinfo=UTC),
                },
                {
                    "id": "https://example.com/articles/2",
                    "title": "Older article",
                    "seen_at": datetime(2026, 3, 25, 10, 10, tzinfo=UTC),
                },
            ]
        )
        provider = GdeltArticlesProvider(
            query="climate change",
            entries_loader=lambda checkpoint: GdeltArticlesLoadResult(entries=entries),
        )
        checkpoint = GdeltArticlesCheckpoint(
            query="climate change",
            cursor="https://example.com/articles/3",
            last_seen_at=datetime(2026, 3, 25, 10, 20, tzinfo=UTC),
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["https://example.com/articles/4"]
        assert entries.closed is True

    asyncio.run(run())


def _entries(items: list[dict[str, object]]) -> Iterable[dict[str, object]]:
    for item in items:
        yield item


class _ClosableEntries:
    def __init__(self, items: list[dict[str, object]]) -> None:
        self._items = iter(items)
        self.closed = False

    def __iter__(self) -> _ClosableEntries:
        return self

    def __next__(self) -> dict[str, object]:
        return next(self._items)

    def close(self) -> None:
        self.closed = True
