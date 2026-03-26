from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from datetime import datetime, timezone
from functools import partial
from typing import Any

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.safecast_radiation.checkpoint import SafecastRadiationCheckpoint
from ingestion.providers.safecast_radiation.loader import (
    DEFAULT_PAGE_SIZE,
    DEFAULT_SAFECAST_API_URL,
    SafecastRadiationLoadResult,
    SafecastRadiationRequest,
    load_measurements,
)
from ingestion.utils.time import utc_now

SafecastMeasurement = Mapping[str, Any]
MeasurementsLoader = Callable[
    [SafecastRadiationRequest],
    SafecastRadiationLoadResult
    | Iterable[SafecastMeasurement]
    | Awaitable[SafecastRadiationLoadResult | Iterable[SafecastMeasurement]],
]


class SafecastRadiationProvider(BatchProvider[SafecastRadiationCheckpoint]):
    def __init__(
        self,
        *,
        api_url: str = DEFAULT_SAFECAST_API_URL,
        initial_since: datetime | str | None = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        measurements_loader: MeasurementsLoader | None = None,
        name: str | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if page_size < 1:
            raise ValueError("page_size must be at least 1")

        self.api_url = api_url
        self.initial_since = _normalize_cursor(initial_since)
        self.page_size = page_size
        self.name = name or "safecast_radiation"
        self._measurements_loader = measurements_loader or partial(load_measurements, api_url)
        self._clock = clock or utc_now
        self._last_window_end: str | None = None

    async def fetch(
        self,
        *,
        checkpoint: SafecastRadiationCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        window_end = self._clock()
        self._last_window_end = _normalize_cursor(window_end)
        load_result = await self._load_measurements(
            SafecastRadiationRequest(
                since=_format_api_timestamp(
                    _parse_cursor(_cursor_for(checkpoint, self.initial_since))
                ),
                until=_format_api_timestamp(window_end),
                page_size=self.page_size,
            )
        )

        batch_fetched_at = utc_now()
        measurements = iter(load_result.measurements)
        try:
            for measurement in measurements:
                measurement_id = measurement.get("id")
                if measurement_id is None:
                    continue

                yield Record(
                    provider=self.name,
                    key=str(measurement_id),
                    payload=dict(measurement),
                    occurred_at=_coerce_datetime(measurement.get("captured_at")),
                    fetched_at=batch_fetched_at,
                    metadata={"api_url": self.api_url},
                )
        finally:
            close = getattr(measurements, "close", None)
            if callable(close):
                close()

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: SafecastRadiationCheckpoint | None,
        last_measurement_id: str | None,
    ) -> SafecastRadiationCheckpoint | None:
        if self._last_window_end is None:
            return previous_checkpoint
        return SafecastRadiationCheckpoint(cursor=self._last_window_end)

    async def _load_measurements(
        self,
        request: SafecastRadiationRequest,
    ) -> SafecastRadiationLoadResult:
        result = self._measurements_loader(request)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, SafecastRadiationLoadResult):
            return result
        return SafecastRadiationLoadResult(measurements=result)


def _cursor_for(
    checkpoint: SafecastRadiationCheckpoint | None,
    initial_since: str | None,
) -> str | None:
    if checkpoint is not None:
        return checkpoint.cursor
    return initial_since


def _normalize_cursor(value: datetime | str | None) -> str | None:
    parsed = _parse_cursor(value)
    return None if parsed is None else parsed.isoformat()


def _parse_cursor(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        if normalized.endswith("Z"):
            normalized = f"{normalized[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            parsed = datetime.strptime(normalized, "%Y-%m-%d %H:%M:%S")
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    raise TypeError("cursor must be a datetime, string, or None")


def _format_api_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _coerce_datetime(value: Any) -> datetime | None:
    return value if isinstance(value, datetime) else None
