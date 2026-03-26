from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime

from ingestion.providers.opensanctions_entities.checkpoint import (
    OpenSanctionsEntitiesCheckpoint,
)
from ingestion.providers.opensanctions_entities.provider import OpenSanctionsEntitiesProvider


def test_opensanctions_provider_yields_all_entities_without_checkpoint() -> None:
    async def run() -> None:
        provider = OpenSanctionsEntitiesProvider(
            dataset="default",
            entries_loader=lambda checkpoint: _entries(
                [
                    {
                        "id": "os-1",
                        "caption": "Alice",
                        "last_change": "2026-03-26T12:36:01",
                    },
                    {
                        "id": "os-2",
                        "caption": "Bob",
                        "first_seen": "2026-03-20T08:00:00",
                    },
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == ["os-1", "os-2"]
        assert records[0].metadata["dataset"] == "default"
        assert records[0].occurred_at == datetime(2026, 3, 26, 12, 36, 1)

    asyncio.run(run())


def test_opensanctions_provider_assigns_one_fetched_at_per_batch() -> None:
    async def run() -> None:
        provider = OpenSanctionsEntitiesProvider(
            dataset="default",
            entries_loader=lambda checkpoint: _entries(
                [
                    {"id": "os-1", "caption": "Alice"},
                    {"id": "os-2", "caption": "Bob"},
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert len({record.fetched_at for record in records}) == 1

    asyncio.run(run())


def test_opensanctions_provider_builds_checkpoint_from_latest_dataset_version() -> None:
    async def run() -> None:
        provider = OpenSanctionsEntitiesProvider(
            dataset="default",
            entries_loader=lambda checkpoint: _load_result(
                entries=[{"id": "os-1", "caption": "Alice"}],
                version="20260326125427-hxr",
                entities_url="https://data.opensanctions.org/datasets/latest/default/entities.ftm.json",
                checksum="abc123",
                updated_at=datetime(2026, 3, 26, 12, 54, 27),
                last_change=datetime(2026, 3, 26, 12, 36, 1),
            ),
        )

        records = [record async for record in provider.fetch()]
        checkpoint = provider.build_checkpoint(
            previous_checkpoint=None,
            last_entity_id=records[0].key,
        )

        assert checkpoint == OpenSanctionsEntitiesCheckpoint(
            dataset="default",
            version="20260326125427-hxr",
            entities_url="https://data.opensanctions.org/datasets/latest/default/entities.ftm.json",
            checksum="abc123",
            updated_at=datetime(2026, 3, 26, 12, 54, 27),
            last_change=datetime(2026, 3, 26, 12, 36, 1),
        )

    asyncio.run(run())


def test_opensanctions_provider_preserves_checkpoint_on_not_modified_response() -> None:
    async def run() -> None:
        previous_checkpoint = OpenSanctionsEntitiesCheckpoint(
            dataset="default",
            version="20260326125427-hxr",
            entities_url="https://data.opensanctions.org/datasets/latest/default/entities.ftm.json",
            checksum="abc123",
            updated_at=datetime(2026, 3, 26, 12, 54, 27),
            last_change=datetime(2026, 3, 26, 12, 36, 1),
        )
        provider = OpenSanctionsEntitiesProvider(
            dataset="default",
            entries_loader=lambda checkpoint: _load_result(
                entries=[],
                version=previous_checkpoint.version,
                entities_url=previous_checkpoint.entities_url,
                checksum=previous_checkpoint.checksum,
                updated_at=previous_checkpoint.updated_at,
                last_change=previous_checkpoint.last_change,
                not_modified=True,
            ),
        )

        records = [record async for record in provider.fetch(checkpoint=previous_checkpoint)]
        checkpoint = provider.build_checkpoint(
            previous_checkpoint=previous_checkpoint,
            last_entity_id=None,
        )

        assert records == []
        assert checkpoint == previous_checkpoint

    asyncio.run(run())


def test_opensanctions_provider_closes_entries_iterator() -> None:
    async def run() -> None:
        from ingestion.providers.opensanctions_entities.loader import OpenSanctionsLoadResult

        entries = _ClosableEntries(
            [
                {"id": "os-1", "caption": "Alice"},
                {"id": "os-2", "caption": "Bob"},
            ]
        )
        provider = OpenSanctionsEntitiesProvider(
            dataset="default",
            entries_loader=lambda checkpoint: OpenSanctionsLoadResult(
                entries=entries,
                dataset="default",
                version="20260326125427-hxr",
                entities_url="https://data.opensanctions.org/datasets/latest/default/entities.ftm.json",
            ),
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == ["os-1", "os-2"]
        assert entries.closed is True

    asyncio.run(run())


def _entries(items: list[dict[str, object]]) -> Iterable[dict[str, object]]:
    for item in items:
        yield item


def _load_result(
    *,
    entries: Iterable[dict[str, object]],
    version: str,
    entities_url: str,
    checksum: str | None = None,
    updated_at: datetime | None = None,
    last_change: datetime | None = None,
    not_modified: bool = False,
):
    from ingestion.providers.opensanctions_entities.loader import OpenSanctionsLoadResult

    return OpenSanctionsLoadResult(
        entries=_entries(entries),
        dataset="default",
        version=version,
        entities_url=entities_url,
        checksum=checksum,
        updated_at=updated_at,
        last_change=last_change,
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
