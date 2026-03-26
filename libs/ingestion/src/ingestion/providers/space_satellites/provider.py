from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.space_satellites.checkpoint import SpaceSatellitesCheckpoint
from ingestion.providers.space_satellites.loader import (
    SpaceSatellitesLoadResult,
    build_satellite_entries_url,
    load_satellite_entries,
)
from ingestion.utils.time import utc_now

SpaceSatelliteEntry = Mapping[str, Any]
SatelliteEntriesLoader = Callable[
    [SpaceSatellitesCheckpoint | None],
    SpaceSatellitesLoadResult
    | Iterable[SpaceSatelliteEntry]
    | Awaitable[SpaceSatellitesLoadResult | Iterable[SpaceSatelliteEntry]],
]


@dataclass(slots=True, frozen=True)
class SpaceSatellitesResponseMetadata:
    source_url: str | None = None


class SpaceSatellitesProvider(BatchProvider[SpaceSatellitesCheckpoint]):
    def __init__(
        self,
        *,
        group: str = "ACTIVE",
        entries_loader: SatelliteEntriesLoader | None = None,
        name: str | None = None,
    ) -> None:
        self.group = group.upper()
        self.name = name or f"space_satellites:{self.group.lower()}"
        self._entries_loader = entries_loader or _default_entries_loader(self.group)
        self._response_metadata = SpaceSatellitesResponseMetadata()

    async def fetch(
        self,
        *,
        checkpoint: SpaceSatellitesCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        load_result = await self._load_entries(checkpoint)
        self._response_metadata = SpaceSatellitesResponseMetadata(source_url=load_result.source_url)
        batch_fetched_at = utc_now()
        checkpoint_record_id = (
            checkpoint.last_record_id
            if checkpoint is not None and checkpoint.group == self.group
            else None
        )

        for entry in self._ordered_entries(load_result.entries):
            entry_id = str(entry["id"])
            if checkpoint_record_id == entry_id:
                break

            yield Record(
                provider=self.name,
                key=entry_id,
                payload=dict(entry),
                occurred_at=_coerce_datetime(entry.get("epoch")),
                fetched_at=batch_fetched_at,
                metadata={
                    "group": self.group,
                    "source_url": self._response_metadata.source_url,
                },
            )

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: SpaceSatellitesCheckpoint | None,
        last_record_id: str | None,
    ) -> SpaceSatellitesCheckpoint | None:
        checkpoint_record_id = last_record_id
        if checkpoint_record_id is None and previous_checkpoint is not None:
            checkpoint_record_id = previous_checkpoint.last_record_id

        if checkpoint_record_id is None:
            return None

        return SpaceSatellitesCheckpoint(
            group=self.group,
            last_record_id=checkpoint_record_id,
        )

    async def _load_entries(
        self,
        checkpoint: SpaceSatellitesCheckpoint | None,
    ) -> SpaceSatellitesLoadResult:
        result = self._entries_loader(checkpoint)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, SpaceSatellitesLoadResult):
            return result
        return SpaceSatellitesLoadResult(
            entries=result,
            source_url=build_satellite_entries_url(self.group),
        )

    def _ordered_entries(self, entries: Iterable[SpaceSatelliteEntry]) -> list[SpaceSatelliteEntry]:
        try:
            ordered_entries = list(entries)
        finally:
            close = getattr(entries, "close", None)
            if callable(close):
                close()

        ordered_entries.sort(key=_entry_sort_key, reverse=True)
        return ordered_entries


def _entry_sort_key(entry: SpaceSatelliteEntry) -> tuple[float, int, str]:
    epoch = _coerce_datetime(entry.get("epoch"))
    norad_cat_id = _coerce_int(entry.get("norad_cat_id"))
    return (
        epoch.timestamp() if epoch is not None else float("-inf"),
        norad_cat_id if norad_cat_id is not None else -1,
        str(entry["id"]),
    )


def _coerce_datetime(value: Any) -> datetime | None:
    return value if isinstance(value, datetime) else None


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _default_entries_loader(group: str) -> SatelliteEntriesLoader:
    return lambda checkpoint: load_satellite_entries(group)
