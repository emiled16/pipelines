from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from ingestion.providers.kiwisdr_receivers.checkpoint import KiwiSdrReceiversCheckpoint
from ingestion.providers.kiwisdr_receivers.loader import KiwiSdrLoadResult
from ingestion.providers.kiwisdr_receivers.provider import KiwiSdrReceiversProvider


def test_kiwisdr_provider_yields_all_receivers_without_checkpoint() -> None:
    async def run() -> None:
        provider = KiwiSdrReceiversProvider(
            directory_url="https://kiwisdr.com/.public/",
            entries_loader=lambda checkpoint: [
                {
                    "id": "https://kiwi-1.example:8073/",
                    "name": "Receiver One",
                    "directory_generated_at": datetime(2026, 3, 25, 3, 8, 49, tzinfo=timezone.utc),
                },
                {
                    "id": "https://kiwi-2.example:8073/",
                    "name": "Receiver Two",
                    "directory_generated_at": datetime(2026, 3, 25, 3, 8, 49, tzinfo=timezone.utc),
                },
            ],
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == [
            "https://kiwi-1.example:8073/",
            "https://kiwi-2.example:8073/",
        ]
        assert records[0].metadata["directory_url"] == "https://kiwisdr.com/.public/"
        assert records[0].occurred_at == datetime(2026, 3, 25, 3, 8, 49, tzinfo=timezone.utc)

    asyncio.run(run())


def test_kiwisdr_provider_assigns_one_fetched_at_per_batch() -> None:
    async def run() -> None:
        provider = KiwiSdrReceiversProvider(
            entries_loader=lambda checkpoint: [
                {"id": "https://kiwi-1.example:8073/"},
                {"id": "https://kiwi-2.example:8073/"},
            ],
        )

        records = [record async for record in provider.fetch()]

        assert len({record.fetched_at for record in records}) == 1

    asyncio.run(run())


def test_kiwisdr_provider_builds_checkpoint_with_http_cache_metadata() -> None:
    async def run() -> None:
        provider = KiwiSdrReceiversProvider(
            directory_url="https://kiwisdr.com/.public/",
            entries_loader=lambda checkpoint: KiwiSdrLoadResult(
                entries=[{"id": "https://kiwi-1.example:8073/"}],
                etag='"etag-2"',
                last_modified="Wed, 25 Mar 2026 10:15:00 GMT",
            ),
        )

        _ = [record async for record in provider.fetch()]
        checkpoint = provider.build_checkpoint(previous_checkpoint=None)

        assert checkpoint == KiwiSdrReceiversCheckpoint(
            directory_url="https://kiwisdr.com/.public/",
            etag='"etag-2"',
            last_modified="Wed, 25 Mar 2026 10:15:00 GMT",
        )

    asyncio.run(run())


def test_kiwisdr_provider_preserves_checkpoint_on_not_modified_response() -> None:
    async def run() -> None:
        provider = KiwiSdrReceiversProvider(
            directory_url="https://kiwisdr.com/.public/",
            entries_loader=lambda checkpoint: KiwiSdrLoadResult(
                entries=[],
                etag=checkpoint.etag if checkpoint is not None else None,
                last_modified=checkpoint.last_modified if checkpoint is not None else None,
                not_modified=True,
            ),
        )
        previous_checkpoint = KiwiSdrReceiversCheckpoint(
            directory_url="https://kiwisdr.com/.public/",
            etag='"etag-2"',
            last_modified="Wed, 25 Mar 2026 10:15:00 GMT",
        )

        records = [record async for record in provider.fetch(checkpoint=previous_checkpoint)]
        checkpoint = provider.build_checkpoint(previous_checkpoint=previous_checkpoint)

        assert records == []
        assert checkpoint == previous_checkpoint

    asyncio.run(run())


def test_kiwisdr_provider_closes_entries_iterator_after_fetch() -> None:
    async def run() -> None:
        entries = _ClosableEntries(
            [
                {"id": "https://kiwi-1.example:8073/"},
                {"id": "https://kiwi-2.example:8073/"},
            ]
        )
        provider = KiwiSdrReceiversProvider(
            entries_loader=lambda checkpoint: KiwiSdrLoadResult(entries=entries),
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == [
            "https://kiwi-1.example:8073/",
            "https://kiwi-2.example:8073/",
        ]
        assert entries.closed is True

    asyncio.run(run())


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
