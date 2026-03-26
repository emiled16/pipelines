from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Any

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.opensanctions_entities.checkpoint import (
    OpenSanctionsEntitiesCheckpoint,
)
from ingestion.providers.opensanctions_entities.loader import (
    DEFAULT_DATASETS_URL,
    OpenSanctionsLoadResult,
    load_entities,
)
from ingestion.utils.time import utc_now

OpenSanctionsEntity = Mapping[str, Any]
EntriesLoader = Callable[
    [OpenSanctionsEntitiesCheckpoint | None],
    OpenSanctionsLoadResult
    | Iterable[OpenSanctionsEntity]
    | Awaitable[OpenSanctionsLoadResult | Iterable[OpenSanctionsEntity]],
]


@dataclass(slots=True, frozen=True)
class OpenSanctionsDatasetMetadata:
    version: str | None = None
    entities_url: str | None = None
    checksum: str | None = None
    updated_at: datetime | None = None
    last_change: datetime | None = None
    not_modified: bool = False


class OpenSanctionsEntitiesProvider(BatchProvider[OpenSanctionsEntitiesCheckpoint]):
    def __init__(
        self,
        *,
        dataset: str = "default",
        datasets_url: str = DEFAULT_DATASETS_URL,
        entries_loader: EntriesLoader | None = None,
        name: str | None = None,
    ) -> None:
        self.dataset = dataset
        self.datasets_url = datasets_url
        self.name = name or f"opensanctions_entities:{dataset}"
        self._entries_loader = entries_loader or partial(
            load_entities,
            dataset,
            datasets_url=datasets_url,
        )
        self._dataset_metadata = OpenSanctionsDatasetMetadata()

    async def fetch(
        self,
        *,
        checkpoint: OpenSanctionsEntitiesCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        load_result = await self._load_entries(checkpoint)
        self._dataset_metadata = OpenSanctionsDatasetMetadata(
            version=load_result.version,
            entities_url=load_result.entities_url,
            checksum=load_result.checksum,
            updated_at=load_result.updated_at,
            last_change=load_result.last_change,
            not_modified=load_result.not_modified,
        )
        if load_result.not_modified:
            return

        batch_fetched_at = utc_now()
        entries = iter(load_result.entries)
        try:
            for entry in entries:
                entity_id = entry.get("id")
                if entity_id is None:
                    continue

                yield Record(
                    provider=self.name,
                    key=str(entity_id),
                    payload=dict(entry),
                    occurred_at=_coerce_datetime(
                        entry.get("last_change") or entry.get("last_seen") or entry.get("first_seen")
                    ),
                    fetched_at=batch_fetched_at,
                    metadata={
                        "dataset": self.dataset,
                        "datasets_url": self.datasets_url,
                        "version": load_result.version,
                        "entities_url": load_result.entities_url,
                        "checksum": load_result.checksum,
                    },
                )
        finally:
            close = getattr(entries, "close", None)
            if callable(close):
                close()

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: OpenSanctionsEntitiesCheckpoint | None,
        last_entity_id: str | None = None,
    ) -> OpenSanctionsEntitiesCheckpoint | None:
        if self._dataset_metadata.version is None or self._dataset_metadata.entities_url is None:
            return previous_checkpoint

        if self._dataset_metadata.not_modified and previous_checkpoint is not None:
            return previous_checkpoint

        return OpenSanctionsEntitiesCheckpoint(
            dataset=self.dataset,
            version=self._dataset_metadata.version,
            entities_url=self._dataset_metadata.entities_url,
            checksum=self._dataset_metadata.checksum,
            updated_at=self._dataset_metadata.updated_at,
            last_change=self._dataset_metadata.last_change,
        )

    async def _load_entries(
        self,
        checkpoint: OpenSanctionsEntitiesCheckpoint | None,
    ) -> OpenSanctionsLoadResult:
        result = self._entries_loader(checkpoint)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, OpenSanctionsLoadResult):
            return result
        return OpenSanctionsLoadResult(
            entries=result,
            dataset=self.dataset,
            version=checkpoint.version if checkpoint is not None else None,
            entities_url=checkpoint.entities_url if checkpoint is not None else None,
        )


def _coerce_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None
