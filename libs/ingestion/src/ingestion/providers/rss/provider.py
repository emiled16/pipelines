from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Any

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.rss.checkpoint import RssCheckpoint
from ingestion.providers.rss.loader import RssLoadResult, load_feed_entries
from ingestion.utils.time import utc_now

RssEntry = Mapping[str, Any]
EntriesLoader = Callable[
    [RssCheckpoint | None],
    RssLoadResult | Iterable[RssEntry] | Awaitable[RssLoadResult | Iterable[RssEntry]],
]


@dataclass(slots=True, frozen=True)
class RssResponseMetadata:
    etag: str | None = None
    last_modified: str | None = None
    not_modified: bool = False


class RssProvider(BatchProvider[RssCheckpoint]):
    def __init__(
        self,
        *,
        feed_url: str,
        entries_loader: EntriesLoader | None = None,
        name: str | None = None,
    ) -> None:
        self.feed_url = feed_url
        self.name = name or f"rss:{feed_url}"
        self._entries_loader = entries_loader or partial(load_feed_entries, feed_url)
        self._response_metadata = RssResponseMetadata()

    async def fetch(self, *, checkpoint: RssCheckpoint | None = None) -> AsyncIterator[Record]:
        load_result = await self._load_entries(checkpoint)
        self._response_metadata = RssResponseMetadata(
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
                entry_id = str(entry["id"])

                if (
                    checkpoint is not None
                    and checkpoint.feed_url == self.feed_url
                    and checkpoint.last_entry_id == entry_id
                ):
                    break

                yield Record(
                    provider=self.name,
                    key=entry_id,
                    payload=dict(entry),
                    occurred_at=_coerce_datetime(entry.get("published_at")),
                    fetched_at=batch_fetched_at,
                    metadata={"feed_url": self.feed_url},
                )
        finally:
            close = getattr(entries, "close", None)
            if callable(close):
                close()

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: RssCheckpoint | None,
        last_entry_id: str | None,
    ) -> RssCheckpoint | None:
        checkpoint_entry_id = last_entry_id
        if checkpoint_entry_id is None and previous_checkpoint is not None:
            checkpoint_entry_id = previous_checkpoint.last_entry_id

        if checkpoint_entry_id is None:
            return None

        return RssCheckpoint(
            feed_url=self.feed_url,
            last_entry_id=checkpoint_entry_id,
            etag=self._response_metadata.etag
            if self._response_metadata.etag is not None
            else (previous_checkpoint.etag if previous_checkpoint is not None else None),
            last_modified=self._response_metadata.last_modified
            if self._response_metadata.last_modified is not None
            else (
                previous_checkpoint.last_modified if previous_checkpoint is not None else None
            ),
        )

    async def _load_entries(self, checkpoint: RssCheckpoint | None) -> RssLoadResult:
        result = self._entries_loader(checkpoint)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, RssLoadResult):
            return result
        return RssLoadResult(entries=result)


def _coerce_datetime(value: Any) -> datetime | None:
    return value if isinstance(value, datetime) else None
