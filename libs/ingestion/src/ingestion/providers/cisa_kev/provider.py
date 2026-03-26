from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.cisa_kev.checkpoint import CisaKevCheckpoint
from ingestion.providers.cisa_kev.loader import (
    DEFAULT_CISA_KEV_URL,
    CisaKevCatalog,
    CisaKevLoadResult,
    load_catalog,
)
from ingestion.utils.time import utc_now

CatalogLoader = Callable[
    [CisaKevCheckpoint | None],
    CisaKevLoadResult | CisaKevCatalog | Awaitable[CisaKevLoadResult | CisaKevCatalog],
]


@dataclass(slots=True, frozen=True)
class CisaKevResponseMetadata:
    catalog_version: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    not_modified: bool = False


class CisaKevProvider(BatchProvider[CisaKevCheckpoint]):
    def __init__(
        self,
        *,
        catalog_url: str = DEFAULT_CISA_KEV_URL,
        catalog_loader: CatalogLoader | None = None,
        name: str | None = None,
    ) -> None:
        self.catalog_url = catalog_url
        self.name = name or "cisa_kev"
        self._catalog_loader = catalog_loader or (
            lambda checkpoint: load_catalog(catalog_url, checkpoint)
        )
        self._response_metadata = CisaKevResponseMetadata()

    async def fetch(self, *, checkpoint: CisaKevCheckpoint | None = None) -> AsyncIterator[Record]:
        load_result = await self._load_catalog(checkpoint)
        self._response_metadata = CisaKevResponseMetadata(
            catalog_version=load_result.catalog.catalog_version,
            etag=load_result.etag,
            last_modified=load_result.last_modified,
            not_modified=load_result.not_modified,
        )
        if load_result.not_modified:
            return

        if (
            checkpoint is not None
            and checkpoint.catalog_version is not None
            and checkpoint.catalog_version == load_result.catalog.catalog_version
        ):
            return

        batch_fetched_at = utc_now()
        metadata = {
            "catalog_url": self.catalog_url,
            "catalog_version": load_result.catalog.catalog_version,
            "date_released": load_result.catalog.date_released,
        }
        for vulnerability in load_result.catalog.vulnerabilities:
            yield Record(
                provider=self.name,
                key=str(vulnerability["cveID"]),
                payload=dict(vulnerability),
                occurred_at=_coerce_catalog_date(vulnerability.get("dateAdded")),
                fetched_at=batch_fetched_at,
                metadata=metadata,
            )

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: CisaKevCheckpoint | None,
    ) -> CisaKevCheckpoint | None:
        catalog_version = self._response_metadata.catalog_version
        if catalog_version is None and previous_checkpoint is not None:
            catalog_version = previous_checkpoint.catalog_version

        etag = self._response_metadata.etag
        if etag is None and previous_checkpoint is not None:
            etag = previous_checkpoint.etag

        last_modified = self._response_metadata.last_modified
        if last_modified is None and previous_checkpoint is not None:
            last_modified = previous_checkpoint.last_modified

        if catalog_version is None and etag is None and last_modified is None:
            return None

        return CisaKevCheckpoint(
            catalog_version=catalog_version,
            etag=etag,
            last_modified=last_modified,
        )

    async def _load_catalog(self, checkpoint: CisaKevCheckpoint | None) -> CisaKevLoadResult:
        result = self._catalog_loader(checkpoint)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, CisaKevLoadResult):
            return result
        return CisaKevLoadResult(catalog=result)


def _coerce_catalog_date(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value).replace(tzinfo=UTC)
    except ValueError:
        return None
