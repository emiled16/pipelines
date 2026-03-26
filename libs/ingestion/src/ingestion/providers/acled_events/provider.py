from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from datetime import date, datetime, timezone
from functools import partial
from typing import Any
from urllib.parse import urlencode

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.acled_events.checkpoint import AcledEventsCheckpoint
from ingestion.providers.acled_events.loader import (
    AcledEvent,
    AcledEventsLoadResult,
    AcledEventsPageRequest,
    ScalarQueryValue,
    load_event_page,
)
from ingestion.utils.time import utc_now

DEFAULT_ACLED_EVENTS_ENDPOINT = "https://api.acleddata.com/acled/read/"
RESERVED_QUERY_PARAM_NAMES = frozenset({"email", "format", "key", "limit", "page"})

AcledPageLoader = Callable[
    [AcledEventsPageRequest],
    AcledEventsLoadResult
    | Iterable[AcledEvent]
    | Awaitable[AcledEventsLoadResult | Iterable[AcledEvent]],
]


class AcledEventsProvider(BatchProvider[AcledEventsCheckpoint]):
    def __init__(
        self,
        *,
        email: str,
        api_key: str,
        query_params: Mapping[str, ScalarQueryValue] | None = None,
        limit: int = 500,
        endpoint: str = DEFAULT_ACLED_EVENTS_ENDPOINT,
        page_loader: AcledPageLoader | None = None,
        name: str | None = None,
    ) -> None:
        if limit < 1:
            raise ValueError("limit must be positive")

        normalized_query_params = _normalize_query_params(query_params)
        reserved_names = sorted(set(normalized_query_params) & RESERVED_QUERY_PARAM_NAMES)
        if reserved_names:
            reserved_names_joined = ", ".join(reserved_names)
            message = f"query_params cannot override reserved names: {reserved_names_joined}"
            raise ValueError(message)

        self.email = email
        self.api_key = api_key
        self.endpoint = endpoint
        self.limit = limit
        self.query_params = normalized_query_params
        self.name = name or _build_provider_name(self.query_params)
        self._page_loader = page_loader or partial(
            load_event_page,
            endpoint=self.endpoint,
            email=self.email,
            api_key=self.api_key,
            params=self.query_params,
        )

    async def fetch(
        self,
        *,
        checkpoint: AcledEventsCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        batch_fetched_at = utc_now()
        page = 1

        while True:
            load_result = await self._load_page(page, checkpoint)
            events = iter(load_result.events)
            stop_fetch = False

            try:
                for event in events:
                    event_key = _event_key(event)
                    if event_key is None:
                        continue

                    if self._checkpoint_applies(checkpoint) and checkpoint.cursor == event_key:
                        stop_fetch = True
                        break

                    yield Record(
                        provider=self.name,
                        key=event_key,
                        payload=dict(event),
                        occurred_at=_coerce_occurred_at(event),
                        fetched_at=batch_fetched_at,
                        metadata={
                            "endpoint": self.endpoint,
                            "page": page,
                            "query_params": dict(self.query_params),
                        },
                    )
            finally:
                close = getattr(events, "close", None)
                if callable(close):
                    close()

            if stop_fetch or load_result.next_page is None:
                return

            page = load_result.next_page

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: AcledEventsCheckpoint | None,
        cursor: str | None,
    ) -> AcledEventsCheckpoint | None:
        checkpoint_cursor = cursor
        if checkpoint_cursor is None and previous_checkpoint is not None:
            checkpoint_cursor = previous_checkpoint.cursor

        if checkpoint_cursor is None:
            return None

        return AcledEventsCheckpoint(
            endpoint=self.endpoint,
            query_params=dict(self.query_params),
            cursor=checkpoint_cursor,
        )

    async def _load_page(
        self,
        page: int,
        checkpoint: AcledEventsCheckpoint | None,
    ) -> AcledEventsLoadResult:
        result = self._page_loader(
            AcledEventsPageRequest(
                page=page,
                limit=self.limit,
                checkpoint=checkpoint,
            )
        )
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, AcledEventsLoadResult):
            return result
        return AcledEventsLoadResult(events=result)

    def _checkpoint_applies(self, checkpoint: AcledEventsCheckpoint | None) -> bool:
        return (
            checkpoint is not None
            and checkpoint.endpoint == self.endpoint
            and checkpoint.query_params == self.query_params
        )


def _build_provider_name(query_params: Mapping[str, str]) -> str:
    if not query_params:
        return "acled_events"
    return f"acled_events:{urlencode(sorted(query_params.items()))}"


def _normalize_query_params(
    query_params: Mapping[str, ScalarQueryValue] | None,
) -> dict[str, str]:
    if query_params is None:
        return {}
    return {str(key): _stringify_query_value(value) for key, value in query_params.items()}


def _stringify_query_value(value: ScalarQueryValue) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _event_key(event: Mapping[str, Any]) -> str | None:
    for field_name in ("event_id_cnty", "event_id_no_cnty", "event_id", "id"):
        value = event.get(field_name)
        if value is None or value == "":
            continue
        return str(value)
    return None


def _coerce_occurred_at(event: Mapping[str, Any]) -> datetime | None:
    for field_name in ("timestamp", "event_date", "disorder_date"):
        value = event.get(field_name)
        parsed = _parse_datetime_value(value)
        if parsed is not None:
            return parsed
    return None


def _parse_datetime_value(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)

    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)

    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)

    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None

    if text.isdigit():
        return datetime.fromtimestamp(int(text), tz=timezone.utc)

    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        parsed = None

    if parsed is not None:
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)

    for time_format in ("%Y-%m-%d", "%d %B %Y", "%Y-%m-%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(text, time_format)
        except ValueError:
            continue
        return parsed.replace(tzinfo=timezone.utc)

    return None
