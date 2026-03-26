from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from typing import Any

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.bls_indicators.checkpoint import BlsIndicatorsCheckpoint
from ingestion.providers.bls_indicators.loader import BlsLoadResult, load_series_observations
from ingestion.utils.time import utc_now

BlsObservation = Mapping[str, Any]
ObservationsLoader = Callable[
    [BlsIndicatorsCheckpoint | None],
    BlsLoadResult | Iterable[BlsObservation] | Awaitable[BlsLoadResult | Iterable[BlsObservation]],
]


@dataclass(slots=True, frozen=True)
class BlsRequestConfig:
    series_ids: tuple[str, ...]
    start_year: int | None = None
    end_year: int | None = None
    catalog: bool = False
    calculations: bool = False
    annualaverage: bool = False
    aspects: bool = False


class BlsIndicatorsProvider(BatchProvider[BlsIndicatorsCheckpoint]):
    def __init__(
        self,
        *,
        series_ids: Sequence[str],
        registration_key: str | None = None,
        start_year: int | None = None,
        end_year: int | None = None,
        catalog: bool = False,
        calculations: bool = False,
        annualaverage: bool = False,
        aspects: bool = False,
        observations_loader: ObservationsLoader | None = None,
        name: str | None = None,
    ) -> None:
        normalized_series_ids = tuple(str(series_id).strip().upper() for series_id in series_ids)
        self.request = BlsRequestConfig(
            series_ids=normalized_series_ids,
            start_year=start_year,
            end_year=end_year,
            catalog=catalog,
            calculations=calculations,
            annualaverage=annualaverage,
            aspects=aspects,
        )
        self.name = name or _default_provider_name(self.request)
        self._observations_loader = observations_loader or partial(
            load_series_observations,
            series_ids=self.request.series_ids,
            registration_key=registration_key,
            start_year=self.request.start_year,
            end_year=self.request.end_year,
            catalog=self.request.catalog,
            calculations=self.request.calculations,
            annualaverage=self.request.annualaverage,
            aspects=self.request.aspects,
        )
        self._latest_series_cursors: dict[str, str] = {}
        self._messages: tuple[str, ...] = ()

    @property
    def messages(self) -> tuple[str, ...]:
        return self._messages

    async def fetch(
        self,
        *,
        checkpoint: BlsIndicatorsCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        load_result = await self._load_observations(checkpoint)
        self._latest_series_cursors = {}
        self._messages = load_result.messages

        active_checkpoint = _matching_checkpoint(
            checkpoint,
            expected_series_ids=self.request.series_ids,
        )
        series_cursors = (
            dict(active_checkpoint.series_cursors)
            if active_checkpoint is not None
            else {}
        )
        completed_series: set[str] = set()
        batch_fetched_at = utc_now()
        observations = iter(load_result.observations)

        try:
            for observation in observations:
                series_id = str(observation["series_id"])
                if series_id in completed_series:
                    continue

                observation_key = _observation_key(
                    series_id=series_id,
                    year=str(observation["year"]),
                    period=str(observation["period"]),
                )
                if series_cursors.get(series_id) == observation_key:
                    completed_series.add(series_id)
                    continue

                self._latest_series_cursors.setdefault(series_id, observation_key)
                payload = dict(observation)

                yield Record(
                    provider=self.name,
                    key=observation_key,
                    payload=payload,
                    occurred_at=_coerce_occurrence_datetime(
                        year=str(observation["year"]),
                        period=str(observation["period"]),
                    ),
                    fetched_at=batch_fetched_at,
                    metadata={
                        "series_id": series_id,
                        "period": observation["period"],
                    },
                )
        finally:
            close = getattr(observations, "close", None)
            if callable(close):
                close()

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: BlsIndicatorsCheckpoint | None,
        last_entry_id: str | None,
    ) -> BlsIndicatorsCheckpoint | None:
        series_cursors = (
            dict(previous_checkpoint.series_cursors)
            if previous_checkpoint is not None
            and previous_checkpoint.series_ids == self.request.series_ids
            else {}
        )
        series_cursors.update(self._latest_series_cursors)

        if not series_cursors:
            return None

        checkpoint_cursor = last_entry_id
        if checkpoint_cursor is None and previous_checkpoint is not None:
            checkpoint_cursor = previous_checkpoint.cursor

        return BlsIndicatorsCheckpoint(
            series_ids=self.request.series_ids,
            cursor=checkpoint_cursor,
            series_cursors=series_cursors,
        )

    async def _load_observations(
        self,
        checkpoint: BlsIndicatorsCheckpoint | None,
    ) -> BlsLoadResult:
        result = self._observations_loader(checkpoint)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, BlsLoadResult):
            return result
        return BlsLoadResult(observations=result)


def _matching_checkpoint(
    checkpoint: BlsIndicatorsCheckpoint | None,
    *,
    expected_series_ids: tuple[str, ...],
) -> BlsIndicatorsCheckpoint | None:
    if checkpoint is None:
        return None
    if checkpoint.series_ids != expected_series_ids:
        return None
    return checkpoint


def _default_provider_name(config: BlsRequestConfig) -> str:
    parts = [",".join(config.series_ids)]
    if config.start_year is not None and config.end_year is not None:
        parts.append(f"{config.start_year}-{config.end_year}")

    enabled_flags = [
        flag_name
        for flag_name, enabled in (
            ("catalog", config.catalog),
            ("calculations", config.calculations),
            ("annualaverage", config.annualaverage),
            ("aspects", config.aspects),
        )
        if enabled
    ]
    if enabled_flags:
        parts.append("+".join(enabled_flags))

    return f"bls_indicators:{'|'.join(parts)}"


def _observation_key(*, series_id: str, year: str, period: str) -> str:
    return f"{series_id}:{year}:{period}"


def _coerce_occurrence_datetime(*, year: str, period: str) -> datetime | None:
    if not year.isdigit():
        return None

    numeric_year = int(year)

    if period.startswith("M") and period[1:].isdigit():
        month = int(period[1:])
        if 1 <= month <= 12:
            return datetime(numeric_year, month, 1, tzinfo=timezone.utc)

    if period.startswith("Q") and period[1:].isdigit():
        quarter = int(period[1:])
        if 1 <= quarter <= 4:
            month = ((quarter - 1) * 3) + 1
            return datetime(numeric_year, month, 1, tzinfo=timezone.utc)

    if period.startswith("S") and period[1:].isdigit():
        semester = int(period[1:])
        if 1 <= semester <= 2:
            month = 1 if semester == 1 else 7
            return datetime(numeric_year, month, 1, tzinfo=timezone.utc)

    if period == "A01":
        return datetime(numeric_year, 1, 1, tzinfo=timezone.utc)

    return None
