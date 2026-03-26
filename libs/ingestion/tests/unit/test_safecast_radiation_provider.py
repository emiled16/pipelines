from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime, timezone

from ingestion.providers.safecast_radiation.checkpoint import SafecastRadiationCheckpoint
from ingestion.providers.safecast_radiation.loader import (
    SafecastRadiationLoadResult,
    SafecastRadiationRequest,
)
from ingestion.providers.safecast_radiation.provider import SafecastRadiationProvider


def test_safecast_provider_uses_initial_since_and_yields_records() -> None:
    async def run() -> None:
        requests: list[SafecastRadiationRequest] = []
        provider = SafecastRadiationProvider(
            initial_since="2026-03-25T00:00:00+00:00",
            page_size=500,
            clock=lambda: datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
            measurements_loader=lambda request: _load_result(
                requests,
                request,
                [
                    {
                        "id": 301,
                        "value": 0.11,
                        "captured_at": datetime(2026, 3, 26, 11, 30, tzinfo=timezone.utc),
                    }
                ],
            ),
        )

        records = [record async for record in provider.fetch()]

        assert len(records) == 1
        assert records[0].key == "301"
        assert records[0].occurred_at == datetime(2026, 3, 26, 11, 30, tzinfo=timezone.utc)
        assert records[0].metadata["api_url"] == provider.api_url
        assert requests == [
            SafecastRadiationRequest(
                since="2026-03-25 00:00:00",
                until="2026-03-26 12:00:00",
                page_size=500,
            )
        ]

    asyncio.run(run())


def test_safecast_provider_builds_checkpoint_from_request_window() -> None:
    async def run() -> None:
        provider = SafecastRadiationProvider(
            clock=lambda: datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
            measurements_loader=lambda request: _load_result([], request, [{"id": 401}]),
        )

        records = [record async for record in provider.fetch()]
        checkpoint = provider.build_checkpoint(
            previous_checkpoint=None,
            last_measurement_id=records[0].key,
        )

        assert checkpoint == SafecastRadiationCheckpoint(cursor="2026-03-26T12:00:00+00:00")

    asyncio.run(run())


def test_safecast_provider_advances_checkpoint_on_empty_batch() -> None:
    async def run() -> None:
        provider = SafecastRadiationProvider(
            clock=lambda: datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc),
            measurements_loader=lambda request: _load_result([], request, []),
        )

        records = [record async for record in provider.fetch()]
        checkpoint = provider.build_checkpoint(
            previous_checkpoint=SafecastRadiationCheckpoint(
                cursor="2026-03-26T11:00:00+00:00"
            ),
            last_measurement_id=None,
        )

        assert records == []
        assert checkpoint == SafecastRadiationCheckpoint(cursor="2026-03-26T12:00:00+00:00")

    asyncio.run(run())


def test_safecast_provider_closes_measurements_iterator_when_iteration_stops() -> None:
    async def run() -> None:
        measurements = _ClosableMeasurements(
            [
                {"id": 501},
                {"id": 502},
            ]
        )
        provider = SafecastRadiationProvider(
            measurements_loader=lambda request: SafecastRadiationLoadResult(
                measurements=measurements
            )
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == ["501", "502"]
        assert measurements.closed is True

    asyncio.run(run())


def _load_result(
    requests: list[SafecastRadiationRequest],
    request: SafecastRadiationRequest,
    measurements: Iterable[dict[str, object]],
) -> SafecastRadiationLoadResult:
    requests.append(request)
    return SafecastRadiationLoadResult(measurements=_measurements(measurements))


def _measurements(items: Iterable[dict[str, object]]) -> Iterable[dict[str, object]]:
    for item in items:
        yield item


class _ClosableMeasurements:
    def __init__(self, items: list[dict[str, object]]) -> None:
        self._items = iter(items)
        self.closed = False

    def __iter__(self) -> _ClosableMeasurements:
        return self

    def __next__(self) -> dict[str, object]:
        return next(self._items)

    def close(self) -> None:
        self.closed = True
