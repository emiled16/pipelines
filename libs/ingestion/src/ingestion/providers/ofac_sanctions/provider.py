from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass
from functools import partial

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.ofac_sanctions.checkpoint import OfacSanctionsCheckpoint
from ingestion.providers.ofac_sanctions.loader import (
    DEFAULT_SOURCE_URL,
    OfacSanctionsLoadResult,
    load_sanctions_entries,
)
from ingestion.utils.time import utc_now

OfacEntry = dict[str, object]
EntriesLoader = Callable[
    [OfacSanctionsCheckpoint | None],
    OfacSanctionsLoadResult
    | Iterable[OfacEntry]
    | Awaitable[OfacSanctionsLoadResult | Iterable[OfacEntry]],
]


@dataclass(slots=True, frozen=True)
class OfacResponseMetadata:
    etag: str | None = None
    last_modified: str | None = None
    not_modified: bool = False


class OfacSanctionsProvider(BatchProvider[OfacSanctionsCheckpoint]):
    def __init__(
        self,
        *,
        source_url: str = DEFAULT_SOURCE_URL,
        entries_loader: EntriesLoader | None = None,
        name: str = "ofac_sanctions",
    ) -> None:
        self.source_url = source_url
        self.name = name
        self._entries_loader = entries_loader or partial(load_sanctions_entries, source_url)
        self._response_metadata = OfacResponseMetadata()

    async def fetch(
        self,
        *,
        checkpoint: OfacSanctionsCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        load_result = await self._load_entries(checkpoint)
        self._response_metadata = OfacResponseMetadata(
            etag=load_result.etag,
            last_modified=load_result.last_modified,
            not_modified=load_result.not_modified,
        )
        if load_result.not_modified:
            return

        batch_fetched_at = utc_now()
        entries = iter(load_result.entries)
        try:
            for entry in entries:
                yield Record(
                    provider=self.name,
                    key=str(entry["id"]),
                    payload=dict(entry),
                    fetched_at=batch_fetched_at,
                    metadata=_record_metadata(self.source_url, entries),
                )
        finally:
            close = getattr(entries, "close", None)
            if callable(close):
                close()

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: OfacSanctionsCheckpoint | None,
    ) -> OfacSanctionsCheckpoint:
        return OfacSanctionsCheckpoint(
            source_url=self.source_url,
            etag=self._response_metadata.etag
            if self._response_metadata.etag is not None
            else (
                previous_checkpoint.etag
                if (
                    previous_checkpoint is not None
                    and previous_checkpoint.source_url == self.source_url
                )
                else None
            ),
            last_modified=self._response_metadata.last_modified
            if self._response_metadata.last_modified is not None
            else (
                previous_checkpoint.last_modified
                if (
                    previous_checkpoint is not None
                    and previous_checkpoint.source_url == self.source_url
                )
                else None
            ),
        )

    async def _load_entries(
        self,
        checkpoint: OfacSanctionsCheckpoint | None,
    ) -> OfacSanctionsLoadResult:
        result = self._entries_loader(checkpoint)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, OfacSanctionsLoadResult):
            return result
        return OfacSanctionsLoadResult(entries=result)


def _record_metadata(source_url: str, entries: object) -> dict[str, object]:
    metadata: dict[str, object] = {"source_url": source_url}

    publish_date = getattr(entries, "publish_date", None)
    if publish_date is not None:
        metadata["publish_date"] = publish_date

    record_count = getattr(entries, "record_count", None)
    if record_count is not None:
        metadata["record_count"] = record_count

    return metadata
