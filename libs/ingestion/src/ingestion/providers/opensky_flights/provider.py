from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Any

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.opensky_flights.checkpoint import OpenSkyFlightsCheckpoint
from ingestion.providers.opensky_flights.loader import (
    DEFAULT_API_BASE_URL,
    OpenSkyFlightsLoadResult,
    OpenSkyTokenManager,
    load_flights,
)
from ingestion.utils.time import utc_now

OpenSkyFlight = Mapping[str, Any]
FlightsLoader = Callable[
    [int, int],
    OpenSkyFlightsLoadResult
    | Iterable[OpenSkyFlight]
    | Awaitable[OpenSkyFlightsLoadResult | Iterable[OpenSkyFlight]],
]


@dataclass(slots=True, frozen=True)
class _FetchWindow:
    begin: int
    end: int


class OpenSkyFlightsProvider(BatchProvider[OpenSkyFlightsCheckpoint]):
    def __init__(
        self,
        *,
        window_seconds: int = 7200,
        lag_seconds: int = 0,
        start_at: datetime | int | None = None,
        flights_loader: FlightsLoader | None = None,
        access_token: str | None = None,
        token_manager: OpenSkyTokenManager | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        api_base_url: str = DEFAULT_API_BASE_URL,
        name: str | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        if window_seconds <= 0 or window_seconds > 7200:
            raise ValueError("window_seconds must be between 1 and 7200")
        if lag_seconds < 0:
            raise ValueError("lag_seconds must be non-negative")
        if access_token is not None and (
            token_manager is not None or client_id is not None or client_secret is not None
        ):
            raise ValueError(
                "access_token cannot be combined with token_manager or client credentials"
            )
        if token_manager is not None and (client_id is not None or client_secret is not None):
            raise ValueError("token_manager cannot be combined with client credentials")
        if (client_id is None) != (client_secret is None):
            raise ValueError("client_id and client_secret must be provided together")

        if token_manager is None and client_id is not None and client_secret is not None:
            token_manager = OpenSkyTokenManager(client_id=client_id, client_secret=client_secret)

        self.name = name or "opensky_flights"
        self.window_seconds = window_seconds
        self.lag_seconds = lag_seconds
        self.api_base_url = api_base_url
        self._clock = clock
        self._start_cursor = _coerce_cursor(start_at)
        self._flights_loader = flights_loader or partial(
            load_flights,
            api_base_url=api_base_url,
            access_token=access_token,
            token_manager=token_manager,
        )
        self._last_fetch_window: _FetchWindow | None = None

    async def fetch(
        self,
        *,
        checkpoint: OpenSkyFlightsCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        self._last_fetch_window = None
        fetch_window = self._resolve_window(checkpoint)
        if fetch_window is None:
            return

        self._last_fetch_window = fetch_window
        load_result = await self._load_flights(fetch_window.begin, fetch_window.end)
        batch_fetched_at = self._clock()

        for flight in load_result.flights:
            occurred_at = _coerce_datetime(flight.get("last_seen_at")) or _coerce_datetime(
                flight.get("first_seen_at")
            )
            yield Record(
                provider=self.name,
                key=str(flight["id"]),
                payload=dict(flight),
                occurred_at=occurred_at,
                fetched_at=batch_fetched_at,
                metadata={
                    "begin": fetch_window.begin,
                    "end": fetch_window.end,
                    "api_base_url": self.api_base_url,
                },
            )

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: OpenSkyFlightsCheckpoint | None,
        last_entry_id: str | None,
    ) -> OpenSkyFlightsCheckpoint | None:
        del last_entry_id

        if self._last_fetch_window is None:
            return previous_checkpoint

        return OpenSkyFlightsCheckpoint(cursor=self._last_fetch_window.end + 1)

    async def _load_flights(self, begin: int, end: int) -> OpenSkyFlightsLoadResult:
        result = self._flights_loader(begin, end)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, OpenSkyFlightsLoadResult):
            return result
        return OpenSkyFlightsLoadResult(flights=result)

    def _resolve_window(
        self,
        checkpoint: OpenSkyFlightsCheckpoint | None,
    ) -> _FetchWindow | None:
        available_end = int(self._clock().timestamp()) - self.lag_seconds
        begin = checkpoint.cursor if checkpoint is not None else self._initial_begin(available_end)
        end = min(begin + self.window_seconds - 1, available_end)
        if end < begin:
            return None
        return _FetchWindow(begin=begin, end=end)

    def _initial_begin(self, available_end: int) -> int:
        if self._start_cursor is not None:
            return self._start_cursor
        return max(0, available_end - self.window_seconds + 1)


def _coerce_cursor(value: datetime | int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return int(value.timestamp())
    return int(value)


def _coerce_datetime(value: Any) -> datetime | None:
    return value if isinstance(value, datetime) else None
