from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Any

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.fred_series.checkpoint import FredSeriesCheckpoint
from ingestion.providers.fred_series.loader import FredSeriesLoadResult, load_series_observations
from ingestion.utils.time import utc_now

FredObservation = Mapping[str, Any]
ObservationsLoader = Callable[
    [FredSeriesCheckpoint | None, int],
    FredSeriesLoadResult
    | Iterable[FredObservation]
    | Awaitable[FredSeriesLoadResult | Iterable[FredObservation]],
]


@dataclass(slots=True, frozen=True)
class FredSeriesMetadata:
    series_id: str


class FredSeriesProvider(BatchProvider[FredSeriesCheckpoint]):
    def __init__(
        self,
        *,
        series_id: str,
        api_key: str,
        observations_loader: ObservationsLoader | None = None,
        name: str | None = None,
        page_size: int = 1000,
    ) -> None:
        if page_size <= 0:
            raise ValueError("page_size must be greater than zero")

        self.series_id = series_id
        self.name = name or f"fred_series:{series_id}"
        self._page_size = page_size
        self._observations_loader = observations_loader or partial(
            load_series_observations,
            series_id,
            api_key,
            limit=page_size,
        )
        self._metadata = FredSeriesMetadata(series_id=series_id)

    async def fetch(
        self,
        *,
        checkpoint: FredSeriesCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        batch_fetched_at = utc_now()
        offset = 0

        while True:
            load_result = await self._load_observations(checkpoint, offset)
            should_stop = False
            observations = iter(load_result.observations)

            try:
                for observation in observations:
                    observation_date = str(observation["date"])

                    if (
                        checkpoint is not None
                        and checkpoint.series_id == self.series_id
                        and checkpoint.cursor == observation_date
                    ):
                        should_stop = True
                        break

                    yield Record(
                        provider=self.name,
                        key=observation_date,
                        payload=dict(observation),
                        occurred_at=_coerce_datetime(observation.get("observed_at")),
                        fetched_at=batch_fetched_at,
                        metadata={"series_id": self._metadata.series_id},
                    )
            finally:
                close = getattr(observations, "close", None)
                if callable(close):
                    close()

            if should_stop or load_result.offset + load_result.limit >= load_result.count:
                return

            offset = load_result.offset + load_result.limit

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: FredSeriesCheckpoint | None,
        cursor: str | None,
    ) -> FredSeriesCheckpoint | None:
        checkpoint_cursor = cursor
        if checkpoint_cursor is None and previous_checkpoint is not None:
            checkpoint_cursor = previous_checkpoint.cursor

        if checkpoint_cursor is None:
            return None

        return FredSeriesCheckpoint(series_id=self.series_id, cursor=checkpoint_cursor)

    async def _load_observations(
        self,
        checkpoint: FredSeriesCheckpoint | None,
        offset: int,
    ) -> FredSeriesLoadResult:
        result = self._observations_loader(checkpoint, offset)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, FredSeriesLoadResult):
            return result

        observations = list(result)
        return FredSeriesLoadResult(
            observations=observations,
            count=offset + len(observations),
            offset=offset,
            limit=self._page_size,
        )


def _coerce_datetime(value: Any) -> datetime | None:
    return value if isinstance(value, datetime) else None
