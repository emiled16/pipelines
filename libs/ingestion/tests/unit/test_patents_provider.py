from __future__ import annotations

import asyncio
from collections.abc import Iterable

from ingestion.providers.patents.checkpoint import PatentsCheckpoint
from ingestion.providers.patents.provider import PatentsProvider


def test_patents_provider_yields_all_patents_without_checkpoint() -> None:
    async def run() -> None:
        provider = PatentsProvider(
            patents_loader=lambda checkpoint: _patents(
                [
                    {
                        "patent_id": "01234568",
                        "patent_date": "2026-03-26",
                        "patent_title": "Newest",
                    },
                    {
                        "patent_id": "01234567",
                        "patent_date": "2026-03-25",
                        "patent_title": "Older",
                    },
                ]
            )
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == ["01234568", "01234567"]
        assert records[0].metadata["source"] == "PatentsView"
        assert records[0].occurred_at is not None

    asyncio.run(run())


def test_patents_provider_stops_once_checkpoint_patent_is_reached() -> None:
    async def run() -> None:
        provider = PatentsProvider(
            patents_loader=lambda checkpoint: _patents(
                [
                    {"patent_id": "01234568", "patent_date": "2026-03-26"},
                    {"patent_id": "01234567", "patent_date": "2026-03-25"},
                    {"patent_id": "01234566", "patent_date": "2026-03-24"},
                ]
            )
        )
        checkpoint = PatentsCheckpoint(
            query_hash=provider.query_hash,
            last_patent_id="01234567",
            last_patent_date="2026-03-25",
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["01234568"]

    asyncio.run(run())


def test_patents_provider_ignores_incompatible_checkpoint() -> None:
    async def run() -> None:
        provider = PatentsProvider(
            patents_loader=lambda checkpoint: _patents(
                [
                    {"patent_id": "01234568", "patent_date": "2026-03-26"},
                    {"patent_id": "01234567", "patent_date": "2026-03-25"},
                ]
            )
        )
        checkpoint = PatentsCheckpoint(
            query_hash="different-query",
            last_patent_id="01234567",
            last_patent_date="2026-03-25",
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["01234568", "01234567"]

    asyncio.run(run())


def test_patents_provider_assigns_one_fetched_at_per_batch() -> None:
    async def run() -> None:
        provider = PatentsProvider(
            patents_loader=lambda checkpoint: _patents(
                [
                    {"patent_id": "01234568", "patent_date": "2026-03-26"},
                    {"patent_id": "01234567", "patent_date": "2026-03-25"},
                ]
            )
        )

        records = [record async for record in provider.fetch()]

        assert len({record.fetched_at for record in records}) == 1

    asyncio.run(run())


def test_patents_provider_builds_checkpoint_from_newest_record() -> None:
    async def run() -> None:
        provider = PatentsProvider(
            patents_loader=lambda checkpoint: _patents(
                [
                    {"patent_id": "01234568", "patent_date": "2026-03-26"},
                    {"patent_id": "01234567", "patent_date": "2026-03-25"},
                ]
            )
        )

        records = [record async for record in provider.fetch()]
        checkpoint = provider.build_checkpoint(
            previous_checkpoint=None,
            last_patent_id=records[0].key,
        )

        assert checkpoint == PatentsCheckpoint(
            query_hash=provider.query_hash,
            last_patent_id="01234568",
            last_patent_date="2026-03-26",
        )

    asyncio.run(run())


def test_patents_provider_preserves_cursor_when_no_new_records_are_fetched() -> None:
    async def run() -> None:
        provider = PatentsProvider(patents_loader=lambda checkpoint: _patents([]))
        previous_checkpoint = PatentsCheckpoint(
            query_hash=provider.query_hash,
            last_patent_id="01234568",
            last_patent_date="2026-03-26",
        )

        records = [record async for record in provider.fetch(checkpoint=previous_checkpoint)]
        checkpoint = provider.build_checkpoint(
            previous_checkpoint=previous_checkpoint,
            last_patent_id=None,
        )

        assert records == []
        assert checkpoint == previous_checkpoint

    asyncio.run(run())


def test_patents_provider_closes_iterator_when_checkpoint_stops_iteration() -> None:
    async def run() -> None:
        from ingestion.providers.patents.loader import PatentsLoadResult

        patents = _ClosablePatents(
            [
                {"patent_id": "01234568", "patent_date": "2026-03-26"},
                {"patent_id": "01234567", "patent_date": "2026-03-25"},
                {"patent_id": "01234566", "patent_date": "2026-03-24"},
            ]
        )
        provider = PatentsProvider(
            patents_loader=lambda checkpoint: PatentsLoadResult(patents=patents)
        )
        checkpoint = PatentsCheckpoint(
            query_hash=provider.query_hash,
            last_patent_id="01234567",
            last_patent_date="2026-03-25",
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["01234568"]
        assert patents.closed is True

    asyncio.run(run())


def test_patents_provider_requires_api_key_without_custom_loader() -> None:
    try:
        PatentsProvider()
    except ValueError as exc:
        assert str(exc) == "api_key is required unless patents_loader is provided"
    else:
        raise AssertionError("expected provider construction to fail without api_key")


def _patents(items: list[dict[str, object]]) -> Iterable[dict[str, object]]:
    for item in items:
        yield item


class _ClosablePatents:
    def __init__(self, items: list[dict[str, object]]) -> None:
        self._items = iter(items)
        self.closed = False

    def __iter__(self) -> _ClosablePatents:
        return self

    def __next__(self) -> dict[str, object]:
        return next(self._items)

    def close(self) -> None:
        self.closed = True
