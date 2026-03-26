from __future__ import annotations

import inspect
import json
import re
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from typing import Any

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.eia_energy.checkpoint import EiaEnergyCheckpoint
from ingestion.providers.eia_energy.loader import (
    DEFAULT_PAGE_SIZE,
    EiaEnergyLoadResult,
    EiaSort,
    load_data_rows,
)
from ingestion.utils.time import utc_now

EiaRow = Mapping[str, Any]
RowsLoader = Callable[
    [EiaEnergyCheckpoint | None, int, int],
    EiaEnergyLoadResult | Iterable[EiaRow] | Awaitable[EiaEnergyLoadResult | Iterable[EiaRow]],
]

_QUARTER_PATTERN = re.compile(r"^(?P<year>\d{4})-Q(?P<quarter>[1-4])$")


@dataclass(slots=True, frozen=True)
class EiaEnergyResponseMetadata:
    total: int | None = None
    frequency: str | None = None
    date_format: str | None = None


class EiaEnergyProvider(BatchProvider[EiaEnergyCheckpoint]):
    def __init__(
        self,
        *,
        api_key: str,
        route: str,
        data: Sequence[str] | None = None,
        facets: Mapping[str, Sequence[str]] | None = None,
        frequency: str | None = None,
        start: str | None = None,
        end: str | None = None,
        sort: Sequence[EiaSort] | None = None,
        key_fields: Sequence[str] | None = None,
        rows_loader: RowsLoader | None = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        name: str | None = None,
    ) -> None:
        self.api_key = api_key
        self.route = route.strip("/")
        self.data = tuple(data or ())
        self.facets = {key: tuple(values) for key, values in (facets or {}).items()}
        self.frequency = frequency
        self.start = start
        self.end = end
        self.sort = tuple(sort or (EiaSort(column="period", direction="desc"),))
        self.key_fields = tuple(key_fields) if key_fields is not None else None
        self.page_size = page_size
        self.name = name or _default_name(
            route=self.route,
            data=self.data,
            facets=self.facets,
            frequency=self.frequency,
            start=self.start,
            end=self.end,
        )
        self._rows_loader = rows_loader or partial(
            load_data_rows,
            api_key=api_key,
            route=self.route,
            data=self.data,
            facets=self.facets,
            frequency=self.frequency,
            start=self.start,
            end=self.end,
            sort=self.sort,
        )
        self._response_metadata = EiaEnergyResponseMetadata()

    async def fetch(
        self,
        *,
        checkpoint: EiaEnergyCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        offset = 0
        batch_fetched_at = utc_now()

        while True:
            load_result = await self._load_rows(checkpoint, offset=offset, length=self.page_size)
            self._response_metadata = EiaEnergyResponseMetadata(
                total=load_result.total,
                frequency=load_result.frequency,
                date_format=load_result.date_format,
            )

            rows = iter(load_result.rows)
            rows_seen = 0
            try:
                for row in rows:
                    rows_seen += 1
                    row_key = _build_record_key(
                        row,
                        key_fields=self.key_fields,
                        data_fields=self.data,
                    )

                    if (
                        checkpoint is not None
                        and checkpoint.route == self.route
                        and checkpoint.cursor == row_key
                    ):
                        return

                    yield Record(
                        provider=self.name,
                        key=row_key,
                        payload=dict(row),
                        occurred_at=_coerce_period(
                            row.get("period"),
                            date_format=load_result.date_format,
                        ),
                        fetched_at=batch_fetched_at,
                        metadata={
                            "route": self.route,
                            "frequency": load_result.frequency or self.frequency,
                            "data": list(self.data),
                            "facets": {key: list(values) for key, values in self.facets.items()},
                        },
                    )
            finally:
                close = getattr(rows, "close", None)
                if callable(close):
                    close()

            if rows_seen < self.page_size:
                return
            offset += rows_seen

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: EiaEnergyCheckpoint | None,
        cursor: str | None,
        last_period: str | None = None,
    ) -> EiaEnergyCheckpoint | None:
        checkpoint_cursor = cursor
        if checkpoint_cursor is None and previous_checkpoint is not None:
            checkpoint_cursor = previous_checkpoint.cursor

        if checkpoint_cursor is None:
            return None

        return EiaEnergyCheckpoint(
            route=self.route,
            cursor=checkpoint_cursor,
            last_period=last_period
            if last_period is not None
            else (previous_checkpoint.last_period if previous_checkpoint is not None else None),
        )

    async def _load_rows(
        self,
        checkpoint: EiaEnergyCheckpoint | None,
        *,
        offset: int,
        length: int,
    ) -> EiaEnergyLoadResult:
        result = self._rows_loader(checkpoint, offset, length)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, EiaEnergyLoadResult):
            return result
        return EiaEnergyLoadResult(rows=result)


def _default_name(
    *,
    route: str,
    data: Sequence[str],
    facets: Mapping[str, Sequence[str]],
    frequency: str | None,
    start: str | None,
    end: str | None,
) -> str:
    parts: list[str] = []
    if data:
        parts.append(f"data={','.join(data)}")
    if frequency:
        parts.append(f"frequency={frequency}")
    if start:
        parts.append(f"start={start}")
    if end:
        parts.append(f"end={end}")
    for facet_name in sorted(facets):
        parts.append(f"{facet_name}={','.join(facets[facet_name])}")
    suffix = f"?{'&'.join(parts)}" if parts else ""
    return f"eia_energy:{route}{suffix}"


def _build_record_key(
    row: Mapping[str, Any],
    *,
    key_fields: Sequence[str] | None,
    data_fields: Sequence[str],
) -> str:
    fields = tuple(key_fields or _infer_key_fields(row, data_fields=data_fields))
    if not fields:
        raise ValueError("Unable to infer a stable key for EIA row; provide key_fields explicitly")
    return "|".join(
        f"{field}={json.dumps(row.get(field), sort_keys=True, default=str, ensure_ascii=True)}"
        for field in fields
    )


def _infer_key_fields(row: Mapping[str, Any], *, data_fields: Sequence[str]) -> list[str]:
    key_candidates: list[str] = []
    if "period" in row:
        key_candidates.append("period")

    id_fields = sorted(
        field_name
        for field_name in row
        if field_name != "period" and field_name.lower().endswith("id")
    )
    if id_fields:
        key_candidates.extend(id_fields)
        return key_candidates

    excluded_fields = set(data_fields)
    excluded_fields.update(f"{field_name}-units" for field_name in data_fields)
    excluded_fields.update(field_name for field_name in row if field_name.endswith("-units"))

    structural_fields = sorted(
        field_name for field_name in row if field_name not in excluded_fields
    )
    return structural_fields


def _coerce_period(value: Any, *, date_format: str | None) -> datetime | None:
    if not isinstance(value, str) or date_format is None:
        return None

    if date_format == "YYYY":
        return datetime(int(value), 1, 1, tzinfo=timezone.utc)
    if date_format == "YYYY-MM":
        return datetime.strptime(value, "%Y-%m").replace(tzinfo=timezone.utc)
    if date_format == "YYYY-MM-DD":
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    quarter_match = _QUARTER_PATTERN.match(value)
    if date_format == "YYYY-\"Q\"Q" and quarter_match is not None:
        quarter = int(quarter_match.group("quarter"))
        month = ((quarter - 1) * 3) + 1
        return datetime(int(quarter_match.group("year")), month, 1, tzinfo=timezone.utc)

    return None
