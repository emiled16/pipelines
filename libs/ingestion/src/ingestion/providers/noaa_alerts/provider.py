from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Any

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.noaa_alerts.checkpoint import NoaaAlertsCheckpoint
from ingestion.providers.noaa_alerts.loader import (
    DEFAULT_ALERTS_URL,
    DEFAULT_USER_AGENT,
    NoaaAlertsLoadResult,
    QueryValue,
    build_alerts_url,
    load_alert_entries,
)
from ingestion.utils.time import utc_now

NoaaAlert = Mapping[str, Any]
AlertsLoader = Callable[
    [NoaaAlertsCheckpoint | None],
    NoaaAlertsLoadResult
    | Iterable[NoaaAlert]
    | Awaitable[NoaaAlertsLoadResult | Iterable[NoaaAlert]],
]


@dataclass(slots=True, frozen=True)
class NoaaAlertsResponseMetadata:
    etag: str | None = None
    last_modified: str | None = None
    not_modified: bool = False


class NoaaAlertsProvider(BatchProvider[NoaaAlertsCheckpoint]):
    def __init__(
        self,
        *,
        alerts_url: str = DEFAULT_ALERTS_URL,
        query: Mapping[str, QueryValue] | None = None,
        user_agent: str = DEFAULT_USER_AGENT,
        alerts_loader: AlertsLoader | None = None,
        name: str | None = None,
    ) -> None:
        self.alerts_url = build_alerts_url(alerts_url, query)
        self.name = name or f"noaa_alerts:{self.alerts_url}"
        self._alerts_loader = alerts_loader or partial(
            load_alert_entries,
            self.alerts_url,
            user_agent=user_agent,
        )
        self._response_metadata = NoaaAlertsResponseMetadata()

    async def fetch(
        self,
        *,
        checkpoint: NoaaAlertsCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        load_result = await self._load_alerts(checkpoint)
        self._response_metadata = NoaaAlertsResponseMetadata(
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
                    and checkpoint.alerts_url == self.alerts_url
                    and checkpoint.cursor == alert_id
                ):
                    break

                yield Record(
                    provider=self.name,
                    key=alert_id,
                    payload=dict(entry),
                    occurred_at=_coerce_datetime(
                        entry.get("sent_at") or entry.get("effective_at") or entry.get("onset_at")
                    ),
                    fetched_at=batch_fetched_at,
                    metadata={"alerts_url": self.alerts_url},
                )
        finally:
            close = getattr(entries, "close", None)
            if callable(close):
                close()

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: NoaaAlertsCheckpoint | None,
        last_alert_id: str | None,
    ) -> NoaaAlertsCheckpoint | None:
        checkpoint_cursor = last_alert_id
        if checkpoint_cursor is None and previous_checkpoint is not None:
            checkpoint_cursor = previous_checkpoint.cursor

        if checkpoint_cursor is None:
            return None

        return NoaaAlertsCheckpoint(
            alerts_url=self.alerts_url,
            cursor=checkpoint_cursor,
            etag=self._response_metadata.etag
            if self._response_metadata.etag is not None
            else (previous_checkpoint.etag if previous_checkpoint is not None else None),
            last_modified=self._response_metadata.last_modified
            if self._response_metadata.last_modified is not None
            else (
                previous_checkpoint.last_modified if previous_checkpoint is not None else None
            ),
        )

    async def _load_alerts(self, checkpoint: NoaaAlertsCheckpoint | None) -> NoaaAlertsLoadResult:
        result = self._alerts_loader(checkpoint)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, NoaaAlertsLoadResult):
            return result
        return NoaaAlertsLoadResult(entries=result)


def _coerce_datetime(value: Any) -> datetime | None:
    return value if isinstance(value, datetime) else None
