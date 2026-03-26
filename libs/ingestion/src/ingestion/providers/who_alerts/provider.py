from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Any

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.who_alerts.checkpoint import WhoAlertsCheckpoint
from ingestion.providers.who_alerts.loader import (
    DEFAULT_WHO_ALERTS_API_URL,
    DEFAULT_WHO_ALERTS_PAGE_URL,
    WhoAlertsLoadResult,
    load_alert_entries,
)
from ingestion.utils.time import utc_now

WhoAlertEntry = Mapping[str, Any]
EntriesLoader = Callable[
    [WhoAlertsCheckpoint | None],
    WhoAlertsLoadResult
    | Iterable[WhoAlertEntry]
    | Awaitable[WhoAlertsLoadResult | Iterable[WhoAlertEntry]],
]


@dataclass(slots=True, frozen=True)
class WhoAlertsResponseMetadata:
    etag: str | None = None
    last_modified: str | None = None
    not_modified: bool = False


class WhoAlertsProvider(BatchProvider[WhoAlertsCheckpoint]):
    def __init__(
        self,
        *,
        api_url: str = DEFAULT_WHO_ALERTS_API_URL,
        page_url: str = DEFAULT_WHO_ALERTS_PAGE_URL,
        entries_loader: EntriesLoader | None = None,
        name: str = "who_alerts",
    ) -> None:
        self.api_url = api_url
        self.page_url = page_url
        self.name = name
        self._entries_loader = entries_loader or partial(load_alert_entries, api_url)
        self._response_metadata = WhoAlertsResponseMetadata()

    async def fetch(
        self,
        *,
        checkpoint: WhoAlertsCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        load_result = await self._load_entries(checkpoint)
        self._response_metadata = WhoAlertsResponseMetadata(
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
                alert_id = str(entry["id"])

                if (
                    checkpoint is not None
                    and checkpoint.api_url == self.api_url
                    and checkpoint.last_alert_id == alert_id
                ):
                    break

                yield Record(
                    provider=self.name,
                    key=alert_id,
                    payload=dict(entry),
                    occurred_at=_coerce_datetime(entry.get("published_at")),
                    fetched_at=batch_fetched_at,
                    metadata={
                        "api_url": self.api_url,
                        "page_url": self.page_url,
                    },
                )
        finally:
            close = getattr(entries, "close", None)
            if callable(close):
                close()

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: WhoAlertsCheckpoint | None,
        last_alert_id: str | None,
    ) -> WhoAlertsCheckpoint | None:
        checkpoint_alert_id = last_alert_id
        if checkpoint_alert_id is None and previous_checkpoint is not None:
            checkpoint_alert_id = previous_checkpoint.last_alert_id

        if checkpoint_alert_id is None:
            return None

        return WhoAlertsCheckpoint(
            api_url=self.api_url,
            last_alert_id=checkpoint_alert_id,
            etag=self._response_metadata.etag
            if self._response_metadata.etag is not None
            else (previous_checkpoint.etag if previous_checkpoint is not None else None),
            last_modified=self._response_metadata.last_modified
            if self._response_metadata.last_modified is not None
            else (
                previous_checkpoint.last_modified if previous_checkpoint is not None else None
            ),
        )

    async def _load_entries(self, checkpoint: WhoAlertsCheckpoint | None) -> WhoAlertsLoadResult:
        result = self._entries_loader(checkpoint)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, WhoAlertsLoadResult):
            return result
        return WhoAlertsLoadResult(entries=result)


def _coerce_datetime(value: Any) -> datetime | None:
    return value if isinstance(value, datetime) else None
