from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from ingestion.providers.reliefweb_reports.checkpoint import ReliefWebReportsCheckpoint
from ingestion.providers.reliefweb_reports.loader import ReliefWebReportsPage
from ingestion.providers.reliefweb_reports.provider import ReliefWebReportsProvider


def test_reliefweb_reports_provider_paginates_until_checkpoint_cursor_is_reached() -> None:
    async def run() -> None:
        captured_requests = []

        def page_loader(request):
            captured_requests.append(request)
            if request.offset == 0:
                return ReliefWebReportsPage(
                    reports=(
                        {
                            "id": "103",
                            "title": "Newest",
                            "url": "https://reliefweb.int/report/example/newest",
                            "date_created": datetime(2026, 3, 25, 10, 15, tzinfo=timezone.utc),
                            "date_changed": None,
                            "fields": {"title": "Newest"},
                            "href": None,
                            "score": 1,
                        },
                        {
                            "id": "102",
                            "title": "Middle",
                            "url": "https://reliefweb.int/report/example/middle",
                            "date_created": datetime(2026, 3, 25, 9, 0, tzinfo=timezone.utc),
                            "date_changed": None,
                            "fields": {"title": "Middle"},
                            "href": None,
                            "score": 1,
                        },
                    ),
                    total_count=4,
                    count=2,
                    next_offset=2,
                )
            return ReliefWebReportsPage(
                reports=(
                    {
                        "id": "101",
                        "title": "Checkpoint",
                        "url": "https://reliefweb.int/report/example/checkpoint",
                        "date_created": datetime(2026, 3, 24, 18, 30, tzinfo=timezone.utc),
                        "date_changed": None,
                        "fields": {"title": "Checkpoint"},
                        "href": None,
                        "score": 1,
                    },
                    {
                        "id": "100",
                        "title": "Older",
                        "url": "https://reliefweb.int/report/example/older",
                        "date_created": datetime(2026, 3, 24, 17, 0, tzinfo=timezone.utc),
                        "date_changed": None,
                        "fields": {"title": "Older"},
                        "href": None,
                        "score": 1,
                    },
                ),
                total_count=4,
                count=2,
                next_offset=None,
            )

        provider = ReliefWebReportsProvider(
            appname="example-codex",
            page_size=2,
            filter={"field": "source.name", "value": "OCHA"},
            page_loader=page_loader,
        )
        checkpoint = ReliefWebReportsCheckpoint(
            cursor="101",
            created_at=datetime(2026, 3, 24, 18, 30, tzinfo=timezone.utc),
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["103", "102"]
        assert len({record.fetched_at for record in records}) == 1
        assert records[0].metadata == {"endpoint": "reports", "appname": "example-codex"}
        assert captured_requests[0].offset == 0
        assert captured_requests[1].offset == 2
        assert captured_requests[0].filter == {
            "operator": "AND",
            "conditions": [
                {"field": "source.name", "value": "OCHA"},
                {
                    "field": "date.created",
                    "value": {"from": "2026-03-24T18:30:00+00:00"},
                },
            ],
        }

    asyncio.run(run())


def test_reliefweb_reports_provider_builds_checkpoint_from_latest_record() -> None:
    async def run() -> None:
        provider = ReliefWebReportsProvider(
            appname="example-codex",
            page_loader=lambda request: ReliefWebReportsPage(
                reports=(
                    {
                        "id": "103",
                        "title": "Newest",
                        "url": "https://reliefweb.int/report/example/newest",
                        "date_created": datetime(2026, 3, 25, 10, 15, tzinfo=timezone.utc),
                        "date_changed": None,
                        "fields": {"title": "Newest"},
                        "href": None,
                        "score": 1,
                    },
                ),
                total_count=1,
                count=1,
                next_offset=None,
            ),
        )

        records = [record async for record in provider.fetch()]
        checkpoint = provider.build_checkpoint(
            previous_checkpoint=None,
            last_report_id=records[0].key,
        )

        assert checkpoint == ReliefWebReportsCheckpoint(
            cursor="103",
            created_at=datetime(2026, 3, 25, 10, 15, tzinfo=timezone.utc),
        )

    asyncio.run(run())


def test_reliefweb_reports_provider_preserves_previous_checkpoint_when_no_new_records_arrive() -> None:
    async def run() -> None:
        provider = ReliefWebReportsProvider(
            appname="example-codex",
            page_loader=lambda request: ReliefWebReportsPage(
                reports=(),
                total_count=0,
                count=0,
                next_offset=None,
            ),
        )
        previous_checkpoint = ReliefWebReportsCheckpoint(
            cursor="103",
            created_at=datetime(2026, 3, 25, 10, 15, tzinfo=timezone.utc),
        )

        records = [record async for record in provider.fetch(checkpoint=previous_checkpoint)]
        checkpoint = provider.build_checkpoint(
            previous_checkpoint=previous_checkpoint,
            last_report_id=None,
        )

        assert records == []
        assert checkpoint == previous_checkpoint

    asyncio.run(run())
