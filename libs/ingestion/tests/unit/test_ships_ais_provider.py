from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timezone

from ingestion.models.record import Record
from ingestion.providers.ships_ais.checkpoint import ShipsAisCheckpoint
from ingestion.providers.ships_ais.loader import ShipsAisSubscription
from ingestion.providers.ships_ais.provider import ShipsAisProvider


def test_ships_ais_provider_yields_stream_records() -> None:
    async def run() -> None:
        provider = ShipsAisProvider(
            subscription=ShipsAisSubscription(api_key="secret"),
            message_source=lambda subscription: _messages(
                [
                    {
                        "message_type": "PositionReport",
                        "mmsi": 111111111,
                        "latitude": 42.0,
                        "longitude": -70.0,
                        "occurred_at": datetime(2026, 3, 25, 10, 15, tzinfo=timezone.utc),
                        "metadata": {},
                        "message": {"UserID": 111111111},
                    },
                    {
                        "message_type": "ShipStaticData",
                        "mmsi": 111111111,
                        "ship_name": "TEST SHIP",
                        "occurred_at": datetime(2026, 3, 25, 10, 16, tzinfo=timezone.utc),
                        "metadata": {},
                        "message": {"UserID": 111111111},
                    },
                ]
            ),
        )

        records = [record async for record in provider.stream()]

        assert len(records) == 2
        assert records[0].provider == "ships_ais"
        assert records[0].metadata["message_type"] == "PositionReport"
        assert records[0].metadata["stream_url"] == "wss://stream.aisstream.io/v0/stream"

    asyncio.run(run())


def test_ships_ais_provider_skips_records_covered_by_checkpoint() -> None:
    async def run() -> None:
        provider = ShipsAisProvider(
            subscription=ShipsAisSubscription(api_key="secret"),
            message_source=lambda subscription: _messages(
                [
                    {
                        "message_type": "PositionReport",
                        "mmsi": 111111111,
                        "occurred_at": datetime(2026, 3, 25, 10, 15, tzinfo=timezone.utc),
                        "metadata": {},
                        "message": {"UserID": 111111111},
                    },
                    {
                        "message_type": "PositionReport",
                        "mmsi": 111111111,
                        "occurred_at": datetime(2026, 3, 25, 10, 16, tzinfo=timezone.utc),
                        "metadata": {},
                        "message": {"UserID": 111111111},
                    },
                ]
            ),
        )

        all_records = [record async for record in provider.stream()]
        checkpoint = ShipsAisCheckpoint(
            stream_name=provider.name,
            last_record_key=all_records[0].key or "",
            last_occurred_at=all_records[0].occurred_at,
        )

        records = [record async for record in provider.stream(checkpoint=checkpoint)]

        assert [record.key for record in records] == [all_records[1].key]

    asyncio.run(run())


def test_ships_ais_provider_builds_checkpoint_from_last_record() -> None:
    provider = ShipsAisProvider(subscription=ShipsAisSubscription(api_key="secret"))
    last_record = Record(
        provider=provider.name,
        key="PositionReport:111111111:2026-03-25T10:15:00+00:00:deadbeef",
        payload={},
        occurred_at=datetime(2026, 3, 25, 10, 15, tzinfo=timezone.utc),
    )

    checkpoint = provider.build_checkpoint(previous_checkpoint=None, last_record=last_record)

    assert checkpoint == ShipsAisCheckpoint(
        stream_name="ships_ais",
        last_record_key="PositionReport:111111111:2026-03-25T10:15:00+00:00:deadbeef",
        last_occurred_at=datetime(2026, 3, 25, 10, 15, tzinfo=timezone.utc),
    )


async def _messages(items: list[dict[str, object]]) -> AsyncIterator[dict[str, object]]:
    for item in items:
        yield item
