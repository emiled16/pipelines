from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime, timezone

from ingestion.providers.fred_series.checkpoint import FredSeriesCheckpoint
from ingestion.providers.fred_series.provider import FredSeriesProvider


def test_fred_series_provider_yields_all_observations_without_checkpoint() -> None:
    async def run() -> None:
        provider = FredSeriesProvider(
            series_id="FEDFUNDS",
            api_key="secret-key",
            observations_loader=lambda checkpoint, offset: _observations_page(
                [
                    {
                        "id": "2026-03-01",
                        "series_id": "FEDFUNDS",
                        "date": "2026-03-01",
                        "value": "4.33",
                        "observed_at": datetime(2026, 3, 1, tzinfo=timezone.utc),
                    },
                    {
                        "id": "2026-02-01",
                        "series_id": "FEDFUNDS",
                        "date": "2026-02-01",
                        "value": "4.31",
                        "observed_at": datetime(2026, 2, 1, tzinfo=timezone.utc),
                    },
                ],
                offset=offset,
                limit=1000,
                count=2,
            ),
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == ["2026-03-01", "2026-02-01"]
        assert records[0].metadata["series_id"] == "FEDFUNDS"

    asyncio.run(run())


def test_fred_series_provider_stops_at_checkpoint_across_pages() -> None:
    async def run() -> None:
        pages = {
            0: _observations_page(
                [
                    {"id": "2026-03-01", "date": "2026-03-01", "series_id": "FEDFUNDS"},
                    {"id": "2026-02-01", "date": "2026-02-01", "series_id": "FEDFUNDS"},
                ],
                offset=0,
                limit=2,
                count=4,
            ),
            2: _observations_page(
                [
                    {"id": "2026-01-01", "date": "2026-01-01", "series_id": "FEDFUNDS"},
                    {"id": "2025-12-01", "date": "2025-12-01", "series_id": "FEDFUNDS"},
                ],
                offset=2,
                limit=2,
                count=4,
            ),
        }
        provider = FredSeriesProvider(
            series_id="FEDFUNDS",
            api_key="secret-key",
            page_size=2,
            observations_loader=lambda checkpoint, offset: pages[offset],
        )
        checkpoint = FredSeriesCheckpoint(series_id="FEDFUNDS", cursor="2026-01-01")

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["2026-03-01", "2026-02-01"]

    asyncio.run(run())


def test_fred_series_provider_assigns_one_fetched_at_per_batch() -> None:
    async def run() -> None:
        pages = {
            0: _observations_page(
                [{"id": "2026-03-01", "date": "2026-03-01", "series_id": "FEDFUNDS"}],
                offset=0,
                limit=1,
                count=2,
            ),
            1: _observations_page(
                [{"id": "2026-02-01", "date": "2026-02-01", "series_id": "FEDFUNDS"}],
                offset=1,
                limit=1,
                count=2,
            ),
        }
        provider = FredSeriesProvider(
            series_id="FEDFUNDS",
            api_key="secret-key",
            page_size=1,
            observations_loader=lambda checkpoint, offset: pages[offset],
        )

        records = [record async for record in provider.fetch()]

        assert len({record.fetched_at for record in records}) == 1

    asyncio.run(run())


def test_fred_series_provider_builds_checkpoint_from_latest_record() -> None:
    provider = FredSeriesProvider(series_id="FEDFUNDS", api_key="secret-key")

    checkpoint = provider.build_checkpoint(previous_checkpoint=None, cursor="2026-03-01")

    assert checkpoint == FredSeriesCheckpoint(series_id="FEDFUNDS", cursor="2026-03-01")


def test_fred_series_provider_preserves_previous_checkpoint_without_new_records() -> None:
    provider = FredSeriesProvider(series_id="FEDFUNDS", api_key="secret-key")
    previous_checkpoint = FredSeriesCheckpoint(series_id="FEDFUNDS", cursor="2026-02-01")

    checkpoint = provider.build_checkpoint(previous_checkpoint=previous_checkpoint, cursor=None)

    assert checkpoint == previous_checkpoint


def test_fred_series_provider_closes_page_iterator_when_checkpoint_stops_iteration() -> None:
    async def run() -> None:
        observations = _ClosableObservations(
            [
                {"id": "2026-03-01", "date": "2026-03-01", "series_id": "FEDFUNDS"},
                {"id": "2026-02-01", "date": "2026-02-01", "series_id": "FEDFUNDS"},
                {"id": "2026-01-01", "date": "2026-01-01", "series_id": "FEDFUNDS"},
            ]
        )
        provider = FredSeriesProvider(
            series_id="FEDFUNDS",
            api_key="secret-key",
            observations_loader=lambda checkpoint, offset: _observations_page(
                observations,
                offset=offset,
                limit=3,
                count=3,
            ),
        )
        checkpoint = FredSeriesCheckpoint(series_id="FEDFUNDS", cursor="2026-02-01")

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["2026-03-01"]
        assert observations.closed is True

    asyncio.run(run())


def _observations_page(
    items: Iterable[dict[str, object]],
    *,
    offset: int,
    limit: int,
    count: int,
):
    from ingestion.providers.fred_series.loader import FredSeriesLoadResult

    return FredSeriesLoadResult(
        observations=items,
        offset=offset,
        limit=limit,
        count=count,
    )


class _ClosableObservations:
    def __init__(self, items: list[dict[str, object]]) -> None:
        self._items = iter(items)
        self.closed = False

    def __iter__(self) -> _ClosableObservations:
        return self

    def __next__(self) -> dict[str, object]:
        return next(self._items)

    def close(self) -> None:
        self.closed = True
