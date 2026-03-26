from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from functools import partial

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.firms_fires.checkpoint import FirmsFiresCheckpoint
from ingestion.providers.firms_fires.loader import FirmsFiresLoadResult, load_fire_entries
from ingestion.utils.time import utc_now

FireEntry = dict[str, object]
EntriesLoader = Callable[
    [FirmsFiresCheckpoint | None],
    FirmsFiresLoadResult | Iterable[FireEntry] | Awaitable[FirmsFiresLoadResult | Iterable[FireEntry]],
]


@dataclass(slots=True, frozen=True)
class FirmsFiresRequest:
    source: str
    area: str
    days: int


class FirmsFiresProvider(BatchProvider[FirmsFiresCheckpoint]):
    def __init__(
        self,
        *,
        source: str,
        area: str,
        days: int = 1,
        api_key: str | None = None,
        base_url: str | None = None,
        entries_loader: EntriesLoader | None = None,
        name: str | None = None,
    ) -> None:
        if days < 1:
            raise ValueError("days must be at least 1")
        if entries_loader is None and api_key is None:
            raise ValueError("api_key is required when entries_loader is not provided")

        self.request = FirmsFiresRequest(source=source, area=area, days=days)
        self.name = name or f"firms_fires:{source}:{area}:{days}"

        if entries_loader is None:
            assert api_key is not None
            if base_url is None:
                self._entries_loader = partial(load_fire_entries, api_key, source, area, days)
            else:
                self._entries_loader = partial(
                    load_fire_entries,
                    api_key,
                    source,
                    area,
                    days,
                    base_url=base_url,
                )
        else:
            self._entries_loader = entries_loader

    async def fetch(
        self, *, checkpoint: FirmsFiresCheckpoint | None = None
    ) -> AsyncIterator[Record]:
        load_result = await self._load_entries(checkpoint)
        batch_fetched_at = utc_now()

        for entry in self._sorted_entries(load_result.entries):
            entry_id = str(entry["id"])

            if (
                checkpoint is not None
                and checkpoint.source == self.request.source
                and checkpoint.area == self.request.area
                and checkpoint.days == self.request.days
                and checkpoint.last_entry_id == entry_id
            ):
                break

            yield Record(
                provider=self.name,
                key=entry_id,
                payload=dict(entry),
                occurred_at=_coerce_datetime(entry.get("occurred_at")),
                fetched_at=batch_fetched_at,
                metadata={
                    "source": self.request.source,
                    "area": self.request.area,
                    "days": self.request.days,
                },
            )

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: FirmsFiresCheckpoint | None,
        last_entry_id: str | None,
    ) -> FirmsFiresCheckpoint | None:
        checkpoint_entry_id = last_entry_id
        if checkpoint_entry_id is None and previous_checkpoint is not None:
            checkpoint_entry_id = previous_checkpoint.last_entry_id

        if checkpoint_entry_id is None:
            return None

        return FirmsFiresCheckpoint(
            source=self.request.source,
            area=self.request.area,
            days=self.request.days,
            last_entry_id=checkpoint_entry_id,
        )

    async def _load_entries(
        self, checkpoint: FirmsFiresCheckpoint | None
    ) -> FirmsFiresLoadResult:
        result = self._entries_loader(checkpoint)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, FirmsFiresLoadResult):
            return result
        return FirmsFiresLoadResult(entries=result)

    def _sorted_entries(self, entries: Iterable[FireEntry]) -> list[FireEntry]:
        return sorted(entries, key=_entry_sort_key, reverse=True)


def _entry_sort_key(entry: FireEntry) -> tuple[int, datetime | str, str]:
    occurred_at = _coerce_datetime(entry.get("occurred_at"))
    return (
        1 if occurred_at is not None else 0,
        occurred_at or "",
        str(entry.get("id") or ""),
    )


def _coerce_datetime(value: object) -> datetime | None:
    return value if isinstance(value, datetime) else None
