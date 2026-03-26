from __future__ import annotations

import asyncio
from collections.abc import Iterable

from ingestion.providers.ofac_sanctions.checkpoint import OfacSanctionsCheckpoint
from ingestion.providers.ofac_sanctions.loader import DEFAULT_SOURCE_URL, OfacSanctionsLoadResult
from ingestion.providers.ofac_sanctions.provider import OfacSanctionsProvider


def test_ofac_sanctions_provider_yields_records_with_source_metadata() -> None:
    async def run() -> None:
        entries = _ClosableEntries(
            [
                {"id": "100", "name": "Jane Doe", "programs": ["CYBER2"]},
                {"id": "101", "name": "Example Shipping Ltd", "sdn_type": "Entity"},
            ],
            publish_date="03/25/2026",
            record_count=2,
        )
        provider = OfacSanctionsProvider(
            entries_loader=lambda checkpoint: OfacSanctionsLoadResult(entries=entries),
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == ["100", "101"]
        assert len({record.fetched_at for record in records}) == 1
        assert records[0].metadata == {
            "source_url": DEFAULT_SOURCE_URL,
            "publish_date": "03/25/2026",
            "record_count": 2,
        }
        assert records[0].payload["name"] == "Jane Doe"
        assert entries.closed is True

    asyncio.run(run())


def test_ofac_sanctions_provider_builds_checkpoint_from_response_metadata() -> None:
    async def run() -> None:
        provider = OfacSanctionsProvider(
            entries_loader=lambda checkpoint: OfacSanctionsLoadResult(
                entries=_entries([{"id": "100", "name": "Jane Doe"}]),
                etag='"etag-2"',
                last_modified="Wed, 25 Mar 2026 10:15:00 GMT",
            ),
        )

        _ = [record async for record in provider.fetch()]
        checkpoint = provider.build_checkpoint(previous_checkpoint=None)

        assert checkpoint == OfacSanctionsCheckpoint(
            source_url=DEFAULT_SOURCE_URL,
            etag='"etag-2"',
            last_modified="Wed, 25 Mar 2026 10:15:00 GMT",
        )

    asyncio.run(run())


def test_ofac_sanctions_provider_preserves_checkpoint_on_not_modified_response() -> None:
    async def run() -> None:
        provider = OfacSanctionsProvider(
            entries_loader=lambda checkpoint: OfacSanctionsLoadResult(
                entries=[],
                etag=checkpoint.etag if checkpoint is not None else None,
                last_modified=checkpoint.last_modified if checkpoint is not None else None,
                not_modified=True,
            ),
        )
        previous_checkpoint = OfacSanctionsCheckpoint(
            source_url=DEFAULT_SOURCE_URL,
            etag='"etag-2"',
            last_modified="Wed, 25 Mar 2026 10:15:00 GMT",
        )

        records = [record async for record in provider.fetch(checkpoint=previous_checkpoint)]
        checkpoint = provider.build_checkpoint(previous_checkpoint=previous_checkpoint)

        assert records == []
        assert checkpoint == previous_checkpoint

    asyncio.run(run())


def _entries(items: list[dict[str, object]]) -> Iterable[dict[str, object]]:
    for item in items:
        yield item


class _ClosableEntries:
    def __init__(
        self,
        items: list[dict[str, object]],
        *,
        publish_date: str | None,
        record_count: int | None,
    ) -> None:
        self._items = iter(items)
        self.publish_date = publish_date
        self.record_count = record_count
        self.closed = False

    def __iter__(self) -> _ClosableEntries:
        return self

    def __next__(self) -> dict[str, object]:
        return next(self._items)

    def close(self) -> None:
        self.closed = True
