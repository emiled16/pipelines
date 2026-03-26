from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime, timezone

from ingestion.providers.usaspending_awards.checkpoint import USAspendingAwardsCheckpoint
from ingestion.providers.usaspending_awards.loader import USAspendingAwardsLoadResult
from ingestion.providers.usaspending_awards.provider import USAspendingAwardsProvider


def test_usaspending_awards_provider_paginates_and_yields_records() -> None:
    async def run() -> None:
        provider = USAspendingAwardsProvider(
            name="usaspending_awards",
            search_body={"filters": {"award_type_codes": ["02"]}},
            page_loader=lambda page, checkpoint: _load_result(
                [
                    {"generated_internal_id": "award-3", "Last Modified Date": "2026-03-25"},
                    {"generated_internal_id": "award-2", "Last Modified Date": "2026-03-24"},
                ]
                if page == 1
                else [{"generated_internal_id": "award-1", "Last Modified Date": "2026-03-23"}],
                has_next_page=page == 1,
            ),
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == ["award-3", "award-2", "award-1"]
        assert records[0].metadata["page"] == 1
        assert records[-1].metadata["page"] == 2
        assert len({record.fetched_at for record in records}) == 1
        assert records[0].occurred_at == datetime(2026, 3, 25, tzinfo=timezone.utc)

    asyncio.run(run())


def test_usaspending_awards_provider_stops_at_checkpoint_and_closes_iterator() -> None:
    async def run() -> None:
        awards = _ClosableAwards(
            [
                {"generated_internal_id": "award-3"},
                {"generated_internal_id": "award-2"},
                {"generated_internal_id": "award-1"},
            ]
        )
        pages_seen: list[int] = []
        provider = USAspendingAwardsProvider(
            search_body={"filters": {"award_type_codes": ["02"]}},
            page_loader=lambda page, checkpoint: _page_loader(page, checkpoint, awards, pages_seen),
        )
        checkpoint = USAspendingAwardsCheckpoint(
            query_hash=provider.query_hash,
            cursor="award-2",
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["award-3"]
        assert awards.closed is True
        assert pages_seen == [1]

    asyncio.run(run())


def test_usaspending_awards_provider_builds_checkpoint_from_latest_record() -> None:
    async def run() -> None:
        provider = USAspendingAwardsProvider(
            search_body={"filters": {"award_type_codes": ["02"]}},
            page_loader=lambda page, checkpoint: _load_result(
                [
                    {
                        "generated_internal_id": "award-3",
                        "Last Modified Date": "2026-03-25T10:15:00Z",
                    }
                ]
            ),
        )

        records = [record async for record in provider.fetch()]
        checkpoint = provider.build_checkpoint(
            previous_checkpoint=None,
            last_entry_id=records[0].key,
        )

        assert checkpoint == USAspendingAwardsCheckpoint(
            query_hash=provider.query_hash,
            cursor="award-3",
            last_modified_at=datetime(2026, 3, 25, 10, 15, tzinfo=timezone.utc),
        )

    asyncio.run(run())


def test_usaspending_awards_provider_preserves_cursor_when_no_records_arrive() -> None:
    async def run() -> None:
        provider = USAspendingAwardsProvider(
            search_body={"filters": {"award_type_codes": ["02"]}},
            page_loader=lambda page, checkpoint: _load_result([], has_next_page=False),
        )
        previous_checkpoint = USAspendingAwardsCheckpoint(
            query_hash=provider.query_hash,
            cursor="award-3",
            last_modified_at=datetime(2026, 3, 25, 10, 15, tzinfo=timezone.utc),
        )

        records = [record async for record in provider.fetch(checkpoint=previous_checkpoint)]
        checkpoint = provider.build_checkpoint(
            previous_checkpoint=previous_checkpoint,
            last_entry_id=None,
        )

        assert records == []
        assert checkpoint == previous_checkpoint

    asyncio.run(run())


def test_usaspending_awards_provider_ignores_checkpoint_from_another_query() -> None:
    async def run() -> None:
        provider = USAspendingAwardsProvider(
            search_body={"filters": {"award_type_codes": ["02"]}},
            page_loader=lambda page, checkpoint: _load_result(
                [{"generated_internal_id": "award-3"}],
                has_next_page=False,
            ),
        )
        checkpoint = USAspendingAwardsCheckpoint(
            query_hash="another-query",
            cursor="award-3",
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["award-3"]

    asyncio.run(run())


def _load_result(
    awards: Iterable[dict[str, object]],
    *,
    has_next_page: bool = False,
) -> USAspendingAwardsLoadResult:
    return USAspendingAwardsLoadResult(awards=_iter_awards(awards), has_next_page=has_next_page)


def _iter_awards(awards: Iterable[dict[str, object]]) -> Iterable[dict[str, object]]:
    for award in awards:
        yield award


def _page_loader(
    page: int,
    checkpoint: USAspendingAwardsCheckpoint | None,
    awards: _ClosableAwards,
    pages_seen: list[int],
) -> USAspendingAwardsLoadResult:
    del checkpoint
    pages_seen.append(page)
    if page != 1:
        raise AssertionError("provider should stop before requesting a second page")
    return USAspendingAwardsLoadResult(awards=awards, has_next_page=True)


class _ClosableAwards:
    def __init__(self, awards: list[dict[str, object]]) -> None:
        self._awards = iter(awards)
        self.closed = False

    def __iter__(self) -> _ClosableAwards:
        return self

    def __next__(self) -> dict[str, object]:
        return next(self._awards)

    def close(self) -> None:
        self.closed = True
