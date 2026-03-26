from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime, timezone

from ingestion.providers.telegram_channels.checkpoint import TelegramChannelsCheckpoint
from ingestion.providers.telegram_channels.provider import TelegramChannelsProvider


def test_telegram_channels_provider_yields_all_entries_without_checkpoint() -> None:
    async def run() -> None:
        provider = TelegramChannelsProvider(
            channel_name="@example_channel",
            entries_loader=lambda checkpoint: _entries(
                [
                    {
                        "id": "103",
                        "text": "Newest",
                        "published_at": datetime(2026, 3, 24, tzinfo=timezone.utc),
                    },
                    {
                        "id": "102",
                        "text": "Middle",
                        "published_at": datetime(2026, 3, 23, tzinfo=timezone.utc),
                    },
                    {
                        "id": "101",
                        "text": "Oldest",
                        "published_at": datetime(2026, 3, 22, tzinfo=timezone.utc),
                    },
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == ["103", "102", "101"]
        assert records[0].metadata["channel_name"] == "example_channel"
        assert records[0].metadata["channel_url"] == "https://t.me/s/example_channel"

    asyncio.run(run())


def test_telegram_channels_provider_stops_once_checkpoint_entry_is_reached() -> None:
    async def run() -> None:
        provider = TelegramChannelsProvider(
            channel_name="example_channel",
            entries_loader=lambda checkpoint: _entries(
                [
                    {"id": "103", "text": "Newest"},
                    {"id": "102", "text": "Middle"},
                    {"id": "101", "text": "Oldest"},
                ]
            ),
        )
        checkpoint = TelegramChannelsCheckpoint(
            channel_name="example_channel",
            last_entry_id="102",
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["103"]

    asyncio.run(run())


def test_telegram_channels_provider_assigns_one_fetched_at_per_batch() -> None:
    async def run() -> None:
        provider = TelegramChannelsProvider(
            channel_name="example_channel",
            entries_loader=lambda checkpoint: _entries(
                [
                    {"id": "103", "text": "Newest"},
                    {"id": "102", "text": "Middle"},
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert len({record.fetched_at for record in records}) == 1

    asyncio.run(run())


def test_telegram_channels_provider_builds_checkpoint_from_latest_entry() -> None:
    provider = TelegramChannelsProvider(channel_name="example_channel")

    checkpoint = provider.build_checkpoint(previous_checkpoint=None, last_entry_id="103")

    assert checkpoint == TelegramChannelsCheckpoint(
        channel_name="example_channel",
        last_entry_id="103",
    )


def test_telegram_channels_provider_preserves_previous_checkpoint_without_new_records() -> None:
    provider = TelegramChannelsProvider(channel_name="example_channel")
    previous_checkpoint = TelegramChannelsCheckpoint(
        channel_name="example_channel",
        last_entry_id="103",
    )

    checkpoint = provider.build_checkpoint(
        previous_checkpoint=previous_checkpoint,
        last_entry_id=None,
    )

    assert checkpoint == previous_checkpoint


def _entries(items: list[dict[str, object]]) -> Iterable[dict[str, object]]:
    for item in items:
        yield item
