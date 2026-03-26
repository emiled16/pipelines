from __future__ import annotations

import inspect
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from functools import partial
from typing import Any
from urllib.parse import urlencode

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.treasury_metrics.checkpoint import TreasuryMetricsCheckpoint
from ingestion.providers.treasury_metrics.loader import (
    TREASURY_FISCAL_DATA_API_BASE_URL,
    TreasuryMetricsLoadResult,
    load_treasury_metric_rows,
)
from ingestion.utils.time import utc_now

TreasuryMetricsRow = Mapping[str, Any]
RowsLoader = Callable[
    [TreasuryMetricsCheckpoint | None],
    TreasuryMetricsLoadResult
    | Iterable[TreasuryMetricsRow]
    | Awaitable[TreasuryMetricsLoadResult | Iterable[TreasuryMetricsRow]],
]


@dataclass(slots=True, frozen=True)
class TreasuryMetricsRecordMetadata:
    endpoint: str
    cursor_field: str
    base_url: str = TREASURY_FISCAL_DATA_API_BASE_URL


class TreasuryMetricsProvider(BatchProvider[TreasuryMetricsCheckpoint]):
    def __init__(
        self,
        *,
        endpoint: str,
        cursor_field: str = "record_date",
        occurred_at_field: str | None = None,
        key_fields: Sequence[str] = (),
        fields: Sequence[str] = (),
        filters: Sequence[str] = (),
        sort_fields: Sequence[str] = (),
        page_size: int = 1_000,
        base_url: str = TREASURY_FISCAL_DATA_API_BASE_URL,
        rows_loader: RowsLoader | None = None,
        name: str | None = None,
    ) -> None:
        if page_size <= 0:
            raise ValueError("page_size must be greater than zero")

        normalized_endpoint = endpoint.lstrip("/")
        if not normalized_endpoint:
            raise ValueError("endpoint must not be empty")
        normalized_cursor_field = cursor_field.strip()
        if not normalized_cursor_field:
            raise ValueError("cursor_field must not be empty")
        effective_occurred_at_field = occurred_at_field or normalized_cursor_field
        effective_key_fields = tuple(key_fields)
        effective_fields = _required_fields(
            requested_fields=fields,
            cursor_field=normalized_cursor_field,
            occurred_at_field=effective_occurred_at_field,
            key_fields=effective_key_fields,
        )

        self.endpoint = normalized_endpoint
        self.cursor_field = normalized_cursor_field
        self.occurred_at_field = effective_occurred_at_field
        self.key_fields = effective_key_fields
        self.fields = effective_fields
        self.filters = tuple(filters)
        self.sort_fields = tuple(sort_fields) or (f"-{normalized_cursor_field}",)
        self.page_size = page_size
        self.base_url = base_url
        self.name = name or _default_provider_name(
            endpoint=normalized_endpoint,
            cursor_field=self.cursor_field,
            occurred_at_field=self.occurred_at_field,
            key_fields=self.key_fields,
            fields=self.fields,
            filters=self.filters,
        )
        self._rows_loader = rows_loader or partial(
            load_treasury_metric_rows,
            endpoint=self.endpoint,
            cursor_field=self.cursor_field,
            base_url=self.base_url,
            fields=self.fields,
            filters=self.filters,
            sort_fields=self.sort_fields,
            page_size=self.page_size,
        )
        self._latest_cursor: str | None = None

    async def fetch(
        self,
        *,
        checkpoint: TreasuryMetricsCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        self._latest_cursor = None
        rows = await self._load_rows(checkpoint)
        batch_fetched_at = utc_now()
        iterator = iter(rows)

        try:
            for row in iterator:
                row_cursor = _coerce_string(row.get(self.cursor_field))
                if self._latest_cursor is None and row_cursor is not None:
                    self._latest_cursor = row_cursor

                yield Record(
                    provider=self.name,
                    key=_row_key(row, self.key_fields),
                    payload=dict(row),
                    occurred_at=_coerce_datetime(row.get(self.occurred_at_field)),
                    fetched_at=batch_fetched_at,
                    metadata=asdict(
                        TreasuryMetricsRecordMetadata(
                            endpoint=self.endpoint,
                            cursor_field=self.cursor_field,
                            base_url=self.base_url,
                        )
                    ),
                )
        finally:
            close = getattr(iterator, "close", None)
            if callable(close):
                close()

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: TreasuryMetricsCheckpoint | None,
        latest_cursor: str | None = None,
    ) -> TreasuryMetricsCheckpoint | None:
        checkpoint_cursor = latest_cursor or self._latest_cursor
        if checkpoint_cursor is None and previous_checkpoint is not None:
            checkpoint_cursor = previous_checkpoint.cursor

        if checkpoint_cursor is None:
            return None

        return TreasuryMetricsCheckpoint(
            endpoint=self.endpoint,
            cursor=checkpoint_cursor,
        )

    async def _load_rows(
        self,
        checkpoint: TreasuryMetricsCheckpoint | None,
    ) -> TreasuryMetricsLoadResult | Iterable[TreasuryMetricsRow]:
        result = self._rows_loader(checkpoint)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, TreasuryMetricsLoadResult):
            return result.rows
        return result


def _default_provider_name(
    *,
    endpoint: str,
    cursor_field: str,
    occurred_at_field: str,
    key_fields: Sequence[str],
    fields: Sequence[str],
    filters: Sequence[str],
) -> str:
    params: list[tuple[str, str]] = [("cursor", cursor_field)]
    if occurred_at_field != cursor_field:
        params.append(("occurred_at", occurred_at_field))
    params.extend(("key", value) for value in key_fields)
    params.extend(("field", value) for value in fields)
    params.extend(("filter", value) for value in filters)
    suffix = urlencode(params, doseq=True)
    if not suffix:
        return f"treasury_metrics:{endpoint}"
    return f"treasury_metrics:{endpoint}?{suffix}"


def _row_key(row: Mapping[str, Any], key_fields: Sequence[str]) -> str:
    if key_fields:
        payload = [row.get(field) for field in key_fields]
    else:
        payload = {key: row[key] for key in sorted(row)}
    return json.dumps(payload, separators=(",", ":"), sort_keys=not key_fields, default=str)


def _required_fields(
    *,
    requested_fields: Sequence[str],
    cursor_field: str,
    occurred_at_field: str,
    key_fields: Sequence[str],
) -> tuple[str, ...]:
    if not requested_fields:
        return ()

    fields: list[str] = []
    seen: set[str] = set()
    for field in (*requested_fields, cursor_field, occurred_at_field, *key_fields):
        if field in seen:
            continue
        seen.add(field)
        fields.append(field)
    return tuple(fields)


def _coerce_string(value: Any) -> str | None:
    if value is None:
        return None
    return value if isinstance(value, str) else str(value)


def _coerce_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None

    candidate = value.strip()
    if not candidate:
        return None

    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed
