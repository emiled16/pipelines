from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from functools import partial

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.kiwisdr_receivers.checkpoint import KiwiSdrReceiversCheckpoint
from ingestion.providers.kiwisdr_receivers.loader import KiwiSdrLoadResult, load_receivers
from ingestion.utils.time import utc_now

ReceiverEntry = dict[str, object]
EntriesLoader = Callable[
    [KiwiSdrReceiversCheckpoint | None],
    KiwiSdrLoadResult | Iterable[ReceiverEntry] | Awaitable[KiwiSdrLoadResult | Iterable[ReceiverEntry]],
]

DEFAULT_DIRECTORY_URL = "https://rx.kiwisdr.com/"


@dataclass(slots=True, frozen=True)
class KiwiSdrResponseMetadata:
    etag: str | None = None
    last_modified: str | None = None
    not_modified: bool = False
    directory_generated_at: datetime | None = None
    directory_summary: str | None = None


class KiwiSdrReceiversProvider(BatchProvider[KiwiSdrReceiversCheckpoint]):
    def __init__(
        self,
        *,
        directory_url: str = DEFAULT_DIRECTORY_URL,
        entries_loader: EntriesLoader | None = None,
        name: str = "kiwisdr_receivers",
    ) -> None:
        self.directory_url = directory_url
        self.name = name
        self._entries_loader = entries_loader or partial(load_receivers, directory_url)
        self._response_metadata = KiwiSdrResponseMetadata()

    async def fetch(
        self,
        *,
        checkpoint: KiwiSdrReceiversCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        load_result = await self._load_entries(checkpoint)
        self._response_metadata = KiwiSdrResponseMetadata(
            etag=load_result.etag,
            last_modified=load_result.last_modified,
            not_modified=load_result.not_modified,
            directory_generated_at=load_result.directory_generated_at,
            directory_summary=load_result.directory_summary,
        )
        if load_result.not_modified:
            return

        batch_fetched_at = utc_now()
        entries = iter(load_result.entries)
        try:
            for entry in entries:
                entry_id = str(entry["id"])
                metadata = {"directory_url": self.directory_url}
                if load_result.directory_summary is not None:
                    metadata["directory_summary"] = load_result.directory_summary

                yield Record(
                    provider=self.name,
                    key=entry_id,
                    payload=dict(entry),
                    occurred_at=_coerce_datetime(entry.get("directory_generated_at")),
                    fetched_at=batch_fetched_at,
                    metadata=metadata,
                )
        finally:
            close = getattr(entries, "close", None)
            if callable(close):
                close()

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: KiwiSdrReceiversCheckpoint | None,
    ) -> KiwiSdrReceiversCheckpoint | None:
        etag = self._response_metadata.etag
        last_modified = self._response_metadata.last_modified

        if etag is None and previous_checkpoint is not None:
            etag = previous_checkpoint.etag
        if last_modified is None and previous_checkpoint is not None:
            last_modified = previous_checkpoint.last_modified

        if etag is None and last_modified is None and previous_checkpoint is None:
            return None

        return KiwiSdrReceiversCheckpoint(
            directory_url=self.directory_url,
            etag=etag,
            last_modified=last_modified,
        )

    async def _load_entries(
        self,
        checkpoint: KiwiSdrReceiversCheckpoint | None,
    ) -> KiwiSdrLoadResult:
        result = self._entries_loader(checkpoint)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, KiwiSdrLoadResult):
            return result
        return KiwiSdrLoadResult(entries=result)


def _coerce_datetime(value: object) -> datetime | None:
    return value if isinstance(value, datetime) else None
