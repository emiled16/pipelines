from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime, timezone

from ingestion.providers.opensky_flights.checkpoint import OpenSkyFlightsCheckpoint
from ingestion.providers.opensky_flights.provider import OpenSkyFlightsProvider


def test_opensky_provider_uses_start_cursor_without_checkpoint() -> None:
    async def run() -> None:
        requested_window: tuple[int, int] | None = None

        def _loader(begin: int, end: int):
            nonlocal requested_window
            requested_window = (begin, end)
            return _flights(
                [
                    {
                        "id": "abc123:1711540800:1711544400",
                        "icao24": "abc123",
                        "callsign": "ACA701",
                        "first_seen_at": datetime(2024, 3, 27, 14, 40, tzinfo=timezone.utc),
                        "last_seen_at": datetime(2024, 3, 27, 15, 40, tzinfo=timezone.utc),
                    }
                ]
            )

        provider = OpenSkyFlightsProvider(
            start_at=100,
            window_seconds=10,
            clock=lambda: datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
            flights_loader=_loader,
        )

        records = [record async for record in provider.fetch()]

        assert requested_window == (100, 109)
        assert [record.key for record in records] == ["abc123:1711540800:1711544400"]
        assert records[0].metadata["begin"] == 100
        assert records[0].metadata["end"] == 109

    asyncio.run(run())


def test_opensky_provider_uses_checkpoint_cursor_for_next_window() -> None:
    async def run() -> None:
        requested_window: tuple[int, int] | None = None

        def _loader(begin: int, end: int):
            nonlocal requested_window
            requested_window = (begin, end)
            return _flights([])

        provider = OpenSkyFlightsProvider(
            window_seconds=10,
            clock=lambda: datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
            flights_loader=_loader,
        )
        checkpoint = OpenSkyFlightsCheckpoint(cursor=250)

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]
        next_checkpoint = provider.build_checkpoint(
            previous_checkpoint=checkpoint,
            last_entry_id=None,
        )

        assert records == []
        assert requested_window == (250, 259)
        assert next_checkpoint == OpenSkyFlightsCheckpoint(cursor=260)

    asyncio.run(run())


def test_opensky_provider_advances_checkpoint_when_interval_is_empty() -> None:
    async def run() -> None:
        provider = OpenSkyFlightsProvider(
            start_at=100,
            window_seconds=10,
            clock=lambda: datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
            flights_loader=lambda begin, end: _flights([]),
        )

        records = [record async for record in provider.fetch()]
        checkpoint = provider.build_checkpoint(previous_checkpoint=None, last_entry_id=None)

        assert records == []
        assert checkpoint == OpenSkyFlightsCheckpoint(cursor=110)

    asyncio.run(run())


def test_opensky_provider_preserves_checkpoint_when_no_window_is_available() -> None:
    async def run() -> None:
        provider = OpenSkyFlightsProvider(
            window_seconds=10,
            clock=lambda: datetime.fromtimestamp(105, tz=timezone.utc),
            flights_loader=lambda begin, end: _flights([]),
        )
        previous_checkpoint = OpenSkyFlightsCheckpoint(cursor=200)

        records = [record async for record in provider.fetch(checkpoint=previous_checkpoint)]
        checkpoint = provider.build_checkpoint(
            previous_checkpoint=previous_checkpoint,
            last_entry_id=None,
        )

        assert records == []
        assert checkpoint == previous_checkpoint

    asyncio.run(run())


def _flights(items: list[dict[str, object]]) -> Iterable[dict[str, object]]:
    for item in items:
        yield item
