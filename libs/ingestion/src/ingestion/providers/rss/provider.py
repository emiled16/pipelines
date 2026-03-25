from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from datetime import datetime
from typing import Any

from ingestion.abstractions import BatchProvider
from ingestion.models import Record
from ingestion.providers.rss.checkpoint import RssCheckpoint

RssEntry = Mapping[str, Any]
EntriesLoader = Callable[[], Iterable[RssEntry] | Awaitable[Iterable[RssEntry]]]


class RssProvider(BatchProvider[RssCheckpoint]):
    def __init__(
        self,
        *,
        feed_url: str,
        entries_loader: EntriesLoader,
        name: str | None = None,
    ) -> None:
        self.feed_url = feed_url
        self.name = name or f"rss:{feed_url}"
        self._entries_loader = entries_loader

    async def fetch(self, *, checkpoint: RssCheckpoint | None = None) -> AsyncIterator[Record]:
        entries = await self._load_entries()

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
                metadata={"feed_url": self.feed_url},
            )

    async def _load_entries(self) -> Iterable[RssEntry]:
        result = self._entries_loader()
        if inspect.isawaitable(result):
            return await result
        return result


def _coerce_datetime(value: Any) -> datetime | None:
    return value if isinstance(value, datetime) else None
