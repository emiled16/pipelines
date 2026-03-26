from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime, timezone

from ingestion.providers.space_satellites.checkpoint import SpaceSatellitesCheckpoint
from ingestion.providers.space_satellites.provider import SpaceSatellitesProvider


def test_space_satellites_provider_orders_entries_by_epoch_then_catalog_id() -> None:
    async def run() -> None:
        provider = SpaceSatellitesProvider(
            group="active",
            entries_loader=lambda checkpoint: _entries(
                [
                    {
                        "id": "25544:2026-03-23T10:15:00+00:00",
                        "norad_cat_id": 25544,
                        "epoch": datetime(2026, 3, 23, 10, 15, tzinfo=timezone.utc),
                    },
                    {
                        "id": "43013:2026-03-24T10:15:00+00:00",
                        "norad_cat_id": 43013,
                        "epoch": datetime(2026, 3, 24, 10, 15, tzinfo=timezone.utc),
                    },
                    {
                        "id": "20580:2026-03-24T10:15:00+00:00",
                        "norad_cat_id": 20580,
                        "epoch": datetime(2026, 3, 24, 10, 15, tzinfo=timezone.utc),
                    },
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == [
            "43013:2026-03-24T10:15:00+00:00",
            "20580:2026-03-24T10:15:00+00:00",
            "25544:2026-03-23T10:15:00+00:00",
        ]
        assert records[0].metadata["group"] == "ACTIVE"

    asyncio.run(run())


def test_space_satellites_provider_stops_once_checkpoint_record_is_reached() -> None:
    async def run() -> None:
        provider = SpaceSatellitesProvider(
            group="active",
            entries_loader=lambda checkpoint: _entries(
                [
                    {
                        "id": "43013:2026-03-24T10:15:00+00:00",
                        "norad_cat_id": 43013,
                        "epoch": datetime(2026, 3, 24, 10, 15, tzinfo=timezone.utc),
                    },
                    {
                        "id": "25544:2026-03-23T10:15:00+00:00",
                        "norad_cat_id": 25544,
                        "epoch": datetime(2026, 3, 23, 10, 15, tzinfo=timezone.utc),
                    },
                    {
                        "id": "20580:2026-03-22T10:15:00+00:00",
                        "norad_cat_id": 20580,
                        "epoch": datetime(2026, 3, 22, 10, 15, tzinfo=timezone.utc),
                    },
                ]
            ),
        )
        checkpoint = SpaceSatellitesCheckpoint(
            group="ACTIVE",
            last_record_id="25544:2026-03-23T10:15:00+00:00",
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["43013:2026-03-24T10:15:00+00:00"]

    asyncio.run(run())


def test_space_satellites_provider_assigns_one_fetched_at_per_batch() -> None:
    async def run() -> None:
        provider = SpaceSatellitesProvider(
            entries_loader=lambda checkpoint: _entries(
                [
                    {
                        "id": "43013:2026-03-24T10:15:00+00:00",
                        "norad_cat_id": 43013,
                        "epoch": datetime(2026, 3, 24, 10, 15, tzinfo=timezone.utc),
                    },
                    {
                        "id": "25544:2026-03-23T10:15:00+00:00",
                        "norad_cat_id": 25544,
                        "epoch": datetime(2026, 3, 23, 10, 15, tzinfo=timezone.utc),
                    },
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert len({record.fetched_at for record in records}) == 1

    asyncio.run(run())


def test_space_satellites_provider_builds_checkpoint_from_latest_record() -> None:
    async def run() -> None:
        provider = SpaceSatellitesProvider(
            group="active",
            entries_loader=lambda checkpoint: _entries(
                [
                    {
                        "id": "43013:2026-03-24T10:15:00+00:00",
                        "norad_cat_id": 43013,
                        "epoch": datetime(2026, 3, 24, 10, 15, tzinfo=timezone.utc),
                    }
                ]
            ),
        )

        records = [record async for record in provider.fetch()]
        checkpoint = provider.build_checkpoint(
            previous_checkpoint=None,
            last_record_id=records[0].key,
        )

        assert checkpoint == SpaceSatellitesCheckpoint(
            group="ACTIVE",
            last_record_id="43013:2026-03-24T10:15:00+00:00",
        )

    asyncio.run(run())


def test_space_satellites_provider_closes_entries_iterator_after_loading() -> None:
    async def run() -> None:
        entries = _ClosableEntries(
            [
                {
                    "id": "43013:2026-03-24T10:15:00+00:00",
                    "norad_cat_id": 43013,
                    "epoch": datetime(2026, 3, 24, 10, 15, tzinfo=timezone.utc),
                },
                {
                    "id": "25544:2026-03-23T10:15:00+00:00",
                    "norad_cat_id": 25544,
                    "epoch": datetime(2026, 3, 23, 10, 15, tzinfo=timezone.utc),
                },
            ]
        )
        provider = SpaceSatellitesProvider(entries_loader=lambda checkpoint: entries)

        records = [record async for record in provider.fetch()]

        assert len(records) == 2
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
