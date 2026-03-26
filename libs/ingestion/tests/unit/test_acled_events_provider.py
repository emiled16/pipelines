from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from ingestion.providers.acled_events.checkpoint import AcledEventsCheckpoint
from ingestion.providers.acled_events.loader import AcledEventsLoadResult
from ingestion.providers.acled_events.provider import AcledEventsProvider


def test_acled_events_provider_yields_all_events_across_pages() -> None:
    async def run() -> None:
        provider = AcledEventsProvider(
            email="analyst@example.com",
            api_key="secret",
            query_params={"country": "Sudan"},
            page_loader=lambda request: _page(
                request.page,
                {
                    1: AcledEventsLoadResult(
                        events=_entries(
                            [
                                {"event_id_cnty": "3", "event_date": "2026-03-24"},
                                {"event_id_cnty": "2", "event_date": "2026-03-23"},
                            ]
                        ),
                        next_page=2,
                    ),
                    2: AcledEventsLoadResult(
                        events=_entries([{"event_id_cnty": "1", "event_date": "2026-03-22"}]),
                        next_page=None,
                    ),
                },
            ),
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == ["3", "2", "1"]
        assert records[0].metadata["endpoint"] == "https://api.acleddata.com/acled/read/"
        assert records[0].metadata["page"] == 1
        assert records[0].metadata["query_params"] == {"country": "Sudan"}
        assert records[0].occurred_at == datetime(2026, 3, 24, tzinfo=timezone.utc)

    asyncio.run(run())


def test_acled_events_provider_stops_once_checkpoint_cursor_is_reached() -> None:
    async def run() -> None:
        page_two_entries = _ClosableEntries(
            [
                {"event_id_cnty": "3", "event_date": "2026-03-23"},
                {"event_id_cnty": "2", "event_date": "2026-03-22"},
            ]
        )
        requested_pages: list[int] = []

        def page_loader(request):
            requested_pages.append(request.page)
            if request.page == 1:
                return AcledEventsLoadResult(
                    events=_entries(
                        [
                            {"event_id_cnty": "5", "event_date": "2026-03-25"},
                            {"event_id_cnty": "4", "event_date": "2026-03-24"},
                        ]
                    ),
                    next_page=2,
                )
            if request.page == 2:
                return AcledEventsLoadResult(events=page_two_entries, next_page=3)
            raise AssertionError("provider requested an unexpected page")

        provider = AcledEventsProvider(
            email="analyst@example.com",
            api_key="secret",
            query_params={"country": "Sudan"},
            page_loader=page_loader,
        )
        checkpoint = AcledEventsCheckpoint(
            endpoint="https://api.acleddata.com/acled/read/",
            query_params={"country": "Sudan"},
            cursor="3",
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["5", "4"]
        assert requested_pages == [1, 2]
        assert page_two_entries.closed is True

    asyncio.run(run())


def test_acled_events_provider_assigns_one_fetched_at_per_batch() -> None:
    async def run() -> None:
        provider = AcledEventsProvider(
            email="analyst@example.com",
            api_key="secret",
            page_loader=lambda request: AcledEventsLoadResult(
                events=_entries(
                    [
                        {"event_id_cnty": "2", "event_date": "2026-03-23"},
                        {"event_id_cnty": "1", "event_date": "2026-03-22"},
                    ]
                )
            ),
        )

        records = [record async for record in provider.fetch()]

        assert len({record.fetched_at for record in records}) == 1

    asyncio.run(run())


def test_acled_events_provider_builds_checkpoint_from_latest_cursor() -> None:
    provider = AcledEventsProvider(
        email="analyst@example.com",
        api_key="secret",
        query_params={"country": "Sudan"},
    )

    checkpoint = provider.build_checkpoint(previous_checkpoint=None, cursor="9")

    assert checkpoint == AcledEventsCheckpoint(
        endpoint="https://api.acleddata.com/acled/read/",
        query_params={"country": "Sudan"},
        cursor="9",
    )


def test_acled_events_provider_preserves_previous_cursor_when_no_new_records_arrive() -> None:
    provider = AcledEventsProvider(
        email="analyst@example.com",
        api_key="secret",
        query_params={"country": "Sudan"},
    )
    previous_checkpoint = AcledEventsCheckpoint(
        endpoint="https://api.acleddata.com/acled/read/",
        query_params={"country": "Sudan"},
        cursor="9",
    )

    checkpoint = provider.build_checkpoint(previous_checkpoint=previous_checkpoint, cursor=None)

    assert checkpoint == previous_checkpoint


def test_acled_events_provider_rejects_reserved_query_parameter_names() -> None:
    try:
        AcledEventsProvider(
            email="analyst@example.com",
            api_key="secret",
            query_params={"page": 2},
        )
    except ValueError as exc:
        assert "reserved names" in str(exc)
    else:
        raise AssertionError("expected reserved query parameter validation to fail")


def _entries(items: list[dict[str, object]]):
    for item in items:
        yield item


def _page(page: int, pages: dict[int, AcledEventsLoadResult]) -> AcledEventsLoadResult:
    return pages[page]


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
