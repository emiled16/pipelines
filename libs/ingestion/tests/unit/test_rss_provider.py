from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from ingestion.providers import RssCheckpoint, RssProvider


def test_rss_provider_yields_all_entries_without_checkpoint() -> None:
    async def run() -> None:
        provider = RssProvider(
            feed_url="https://example.com/feed.xml",
            entries_loader=lambda: [
                {
                    "id": "3",
                    "title": "Newest",
                    "published_at": datetime(2026, 3, 24, tzinfo=timezone.utc),
                },
                {
                    "id": "2",
                    "title": "Middle",
                    "published_at": datetime(2026, 3, 23, tzinfo=timezone.utc),
                },
                {
                    "id": "1",
                    "title": "Oldest",
                    "published_at": datetime(2026, 3, 22, tzinfo=timezone.utc),
                },
            ],
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == ["3", "2", "1"]
        assert records[0].metadata["feed_url"] == "https://example.com/feed.xml"

    asyncio.run(run())


def test_rss_provider_stops_once_checkpoint_entry_is_reached() -> None:
    async def run() -> None:
        provider = RssProvider(
            feed_url="https://example.com/feed.xml",
            entries_loader=lambda: [
                {"id": "3", "title": "Newest"},
                {"id": "2", "title": "Middle"},
                {"id": "1", "title": "Oldest"},
            ],
        )
        checkpoint = RssCheckpoint(
            feed_url="https://example.com/feed.xml",
            last_entry_id="2",
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["3"]

    asyncio.run(run())
