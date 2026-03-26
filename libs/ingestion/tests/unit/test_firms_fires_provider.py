from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import UTC, datetime

from ingestion.providers.firms_fires.checkpoint import FirmsFiresCheckpoint
from ingestion.providers.firms_fires.provider import FirmsFiresProvider


def test_firms_fires_provider_requires_api_key_without_loader() -> None:
    try:
        FirmsFiresProvider(
            source="VIIRS_SNPP_NRT",
            area="-124,32,-113,42",
        )
    except ValueError as exc:
        assert str(exc) == "api_key is required when entries_loader is not provided"
    else:
        raise AssertionError("expected provider construction to fail without api_key")


def test_firms_fires_provider_yields_newest_entries_first() -> None:
    async def run() -> None:
        provider = FirmsFiresProvider(
            source="VIIRS_SNPP_NRT",
            area="-124,32,-113,42",
            entries_loader=lambda checkpoint: _entries(
                [
                    _entry("oldest", "2026-03-22T01:00:00+00:00"),
                    _entry("newest", "2026-03-24T03:00:00+00:00"),
                    _entry("middle", "2026-03-23T02:00:00+00:00"),
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == ["newest", "middle", "oldest"]
        assert records[0].metadata == {
            "source": "VIIRS_SNPP_NRT",
            "area": "-124,32,-113,42",
            "days": 1,
        }

    asyncio.run(run())


def test_firms_fires_provider_stops_once_checkpoint_entry_is_reached() -> None:
    async def run() -> None:
        provider = FirmsFiresProvider(
            source="VIIRS_SNPP_NRT",
            area="-124,32,-113,42",
            entries_loader=lambda checkpoint: _entries(
                [
                    _entry("newest", "2026-03-24T03:00:00+00:00"),
                    _entry("middle", "2026-03-23T02:00:00+00:00"),
                    _entry("oldest", "2026-03-22T01:00:00+00:00"),
                ]
            ),
        )
        checkpoint = FirmsFiresCheckpoint(
            source="VIIRS_SNPP_NRT",
            area="-124,32,-113,42",
            days=1,
            last_entry_id="middle",
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["newest"]

    asyncio.run(run())


def test_firms_fires_provider_builds_checkpoint_from_latest_record() -> None:
    async def run() -> None:
        provider = FirmsFiresProvider(
            source="VIIRS_SNPP_NRT",
            area="-124,32,-113,42",
            entries_loader=lambda checkpoint: _entries(
                [_entry("newest", "2026-03-24T03:00:00+00:00")]
            ),
        )

        records = [record async for record in provider.fetch()]
        checkpoint = provider.build_checkpoint(
            previous_checkpoint=None,
            last_entry_id=records[0].key,
        )

        assert checkpoint == FirmsFiresCheckpoint(
            source="VIIRS_SNPP_NRT",
            area="-124,32,-113,42",
            days=1,
            last_entry_id="newest",
        )

    asyncio.run(run())


def test_firms_fires_provider_preserves_previous_checkpoint_without_new_records() -> None:
    async def run() -> None:
        provider = FirmsFiresProvider(
            source="VIIRS_SNPP_NRT",
            area="-124,32,-113,42",
            entries_loader=lambda checkpoint: _entries([]),
        )
        previous_checkpoint = FirmsFiresCheckpoint(
            source="VIIRS_SNPP_NRT",
            area="-124,32,-113,42",
            days=1,
            last_entry_id="newest",
        )

        records = [record async for record in provider.fetch(checkpoint=previous_checkpoint)]
        checkpoint = provider.build_checkpoint(
            previous_checkpoint=previous_checkpoint,
            last_entry_id=None,
        )

        assert records == []
        assert checkpoint == previous_checkpoint

    asyncio.run(run())


def _entries(items: list[dict[str, object]]) -> Iterable[dict[str, object]]:
    for item in items:
        yield item


def _entry(entry_id: str, occurred_at: str) -> dict[str, object]:
    return {
        "id": entry_id,
        "latitude": "34.12",
        "longitude": "-118.45",
        "occurred_at": datetime.fromisoformat(occurred_at).astimezone(UTC),
    }
