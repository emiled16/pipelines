from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime, timezone

from ingestion.providers.noaa_alerts.checkpoint import NoaaAlertsCheckpoint
from ingestion.providers.noaa_alerts.provider import NoaaAlertsProvider


def test_noaa_alerts_provider_yields_all_entries_without_checkpoint() -> None:
    async def run() -> None:
        provider = NoaaAlertsProvider(
            alerts_url="https://api.weather.gov/alerts/active",
            query={"area": "KS"},
            alerts_loader=lambda checkpoint: _entries(
                [
                    {
                        "id": "https://api.weather.gov/alerts/3",
                        "event": "Tornado Warning",
                        "sent_at": datetime(2026, 3, 24, tzinfo=timezone.utc),
                    },
                    {
                        "id": "https://api.weather.gov/alerts/2",
                        "event": "Severe Thunderstorm Warning",
                        "sent_at": datetime(2026, 3, 23, tzinfo=timezone.utc),
                    },
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == [
            "https://api.weather.gov/alerts/3",
            "https://api.weather.gov/alerts/2",
        ]
        assert records[0].metadata["alerts_url"] == "https://api.weather.gov/alerts/active?area=KS"

    asyncio.run(run())


def test_noaa_alerts_provider_stops_once_checkpoint_cursor_is_reached() -> None:
    async def run() -> None:
        provider = NoaaAlertsProvider(
            alerts_url="https://api.weather.gov/alerts/active",
            query={"area": "KS"},
            alerts_loader=lambda checkpoint: _entries(
                [
                    {"id": "https://api.weather.gov/alerts/3", "event": "Newest"},
                    {"id": "https://api.weather.gov/alerts/2", "event": "Middle"},
                    {"id": "https://api.weather.gov/alerts/1", "event": "Oldest"},
                ]
            ),
        )
        checkpoint = NoaaAlertsCheckpoint(
            alerts_url="https://api.weather.gov/alerts/active?area=KS",
            cursor="https://api.weather.gov/alerts/2",
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["https://api.weather.gov/alerts/3"]

    asyncio.run(run())


def test_noaa_alerts_provider_assigns_one_fetched_at_per_batch() -> None:
    async def run() -> None:
        provider = NoaaAlertsProvider(
            alerts_loader=lambda checkpoint: _entries(
                [
                    {"id": "https://api.weather.gov/alerts/3", "event": "Newest"},
                    {"id": "https://api.weather.gov/alerts/2", "event": "Middle"},
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert len({record.fetched_at for record in records}) == 1

    asyncio.run(run())


def test_noaa_alerts_provider_builds_checkpoint_with_http_cache_metadata() -> None:
    async def run() -> None:
        provider = NoaaAlertsProvider(
            query={"area": "KS"},
            alerts_loader=lambda checkpoint: _load_result(
                entries=[{"id": "https://api.weather.gov/alerts/3", "event": "Newest"}],
                etag='"etag-2"',
                last_modified="Wed, 25 Mar 2026 10:15:00 GMT",
            ),
        )

        records = [record async for record in provider.fetch()]
        checkpoint = provider.build_checkpoint(
            previous_checkpoint=None,
            last_alert_id=records[0].key,
        )

        assert checkpoint == NoaaAlertsCheckpoint(
            alerts_url="https://api.weather.gov/alerts/active?area=KS",
            cursor="https://api.weather.gov/alerts/3",
            etag='"etag-2"',
            last_modified="Wed, 25 Mar 2026 10:15:00 GMT",
        )

    asyncio.run(run())


def test_noaa_alerts_provider_preserves_cursor_on_not_modified_response() -> None:
    async def run() -> None:
        provider = NoaaAlertsProvider(
            query={"area": "KS"},
            alerts_loader=lambda checkpoint: _load_result(
                entries=[],
                etag=checkpoint.etag if checkpoint is not None else None,
                last_modified=checkpoint.last_modified if checkpoint is not None else None,
                not_modified=True,
            ),
        )
        previous_checkpoint = NoaaAlertsCheckpoint(
            alerts_url="https://api.weather.gov/alerts/active?area=KS",
            cursor="https://api.weather.gov/alerts/3",
            etag='"etag-2"',
            last_modified="Wed, 25 Mar 2026 10:15:00 GMT",
        )

        records = [record async for record in provider.fetch(checkpoint=previous_checkpoint)]
        checkpoint = provider.build_checkpoint(
            previous_checkpoint=previous_checkpoint,
            last_alert_id=None,
        )

        assert records == []
        assert checkpoint == previous_checkpoint

    asyncio.run(run())


def test_noaa_alerts_provider_closes_entries_iterator_when_checkpoint_stops_iteration() -> None:
    async def run() -> None:
        from ingestion.providers.noaa_alerts.loader import NoaaAlertsLoadResult

        entries = _ClosableEntries(
            [
                {"id": "https://api.weather.gov/alerts/3", "event": "Newest"},
                {"id": "https://api.weather.gov/alerts/2", "event": "Middle"},
                {"id": "https://api.weather.gov/alerts/1", "event": "Oldest"},
            ]
        )
        provider = NoaaAlertsProvider(
            query={"area": "KS"},
            alerts_loader=lambda checkpoint: NoaaAlertsLoadResult(entries=entries),
        )
        checkpoint = NoaaAlertsCheckpoint(
            alerts_url="https://api.weather.gov/alerts/active?area=KS",
            cursor="https://api.weather.gov/alerts/2",
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["https://api.weather.gov/alerts/3"]
        assert entries.closed is True

    asyncio.run(run())


def _entries(items: list[dict[str, object]]) -> Iterable[dict[str, object]]:
    for item in items:
        yield item


def _load_result(
    *,
    entries: Iterable[dict[str, object]],
    etag: str | None = None,
    last_modified: str | None = None,
    not_modified: bool = False,
):
    from ingestion.providers.noaa_alerts.loader import NoaaAlertsLoadResult

    return NoaaAlertsLoadResult(
        entries=_entries(entries),
        etag=etag,
        last_modified=last_modified,
        not_modified=not_modified,
    )


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
