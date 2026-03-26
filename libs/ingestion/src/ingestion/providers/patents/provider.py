from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from typing import Any

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.patents.checkpoint import PatentsCheckpoint
from ingestion.providers.patents.loader import (
    DEFAULT_PATENT_FIELDS,
    PatentsLoadResult,
    build_patents_query_hash,
    load_patents,
)
from ingestion.utils.time import utc_now

PatentEntry = Mapping[str, Any]
PatentsLoader = Callable[
    [PatentsCheckpoint | None],
    PatentsLoadResult
    | Iterable[PatentEntry]
    | Awaitable[PatentsLoadResult | Iterable[PatentEntry]],
]


@dataclass(slots=True, frozen=True)
class _LatestPatentCursor:
    patent_id: str
    patent_date: str


class PatentsProvider(BatchProvider[PatentsCheckpoint]):
    def __init__(
        self,
        *,
        api_key: str | None = None,
        query: Mapping[str, Any] | None = None,
        fields: Sequence[str] | None = None,
        page_size: int = 1000,
        patents_loader: PatentsLoader | None = None,
        name: str | None = None,
    ) -> None:
        if patents_loader is None and not api_key:
            raise ValueError("api_key is required unless patents_loader is provided")

        self.query = dict(query or {})
        self.fields = tuple(fields or DEFAULT_PATENT_FIELDS)
        self.page_size = page_size
        self.query_hash = build_patents_query_hash(self.query)
        self.name = name or "patents"
        self._patents_loader = patents_loader or partial(
            load_patents,
            api_key=api_key or "",
            query=self.query,
            fields=self.fields,
            page_size=self.page_size,
        )
        self._latest_cursor: _LatestPatentCursor | None = None

    async def fetch(self, *, checkpoint: PatentsCheckpoint | None = None) -> AsyncIterator[Record]:
        effective_checkpoint = checkpoint if self._matches_query(checkpoint) else None
        load_result = await self._load_patents(effective_checkpoint)
        batch_fetched_at = utc_now()
        self._latest_cursor = None
        patents = iter(load_result.patents)

        try:
            for patent in patents:
                patent_id = _require_string(patent, "patent_id")
                patent_date = _require_string(patent, "patent_date")

                if (
                    effective_checkpoint is not None
                    and effective_checkpoint.last_patent_id == patent_id
                    and effective_checkpoint.last_patent_date == patent_date
                ):
                    break

                if self._latest_cursor is None:
                    self._latest_cursor = _LatestPatentCursor(
                        patent_id=patent_id,
                        patent_date=patent_date,
                    )

                yield Record(
                    provider=self.name,
                    key=patent_id,
                    payload=dict(patent),
                    occurred_at=_coerce_patent_date(patent_date),
                    fetched_at=batch_fetched_at,
                    metadata={
                        "query_hash": self.query_hash,
                        "source": "PatentsView",
                    },
                )
        finally:
            close = getattr(patents, "close", None)
            if callable(close):
                close()

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: PatentsCheckpoint | None,
        last_patent_id: str | None,
    ) -> PatentsCheckpoint | None:
        patent_id = last_patent_id
        patent_date = self._latest_cursor.patent_date if self._latest_cursor is not None else None

        if (
            patent_id is None
            and previous_checkpoint is not None
            and self._matches_query(previous_checkpoint)
        ):
            patent_id = previous_checkpoint.last_patent_id
        if (
            patent_date is None
            and previous_checkpoint is not None
            and self._matches_query(previous_checkpoint)
        ):
            patent_date = previous_checkpoint.last_patent_date

        if patent_id is None or patent_date is None:
            return None

        return PatentsCheckpoint(
            query_hash=self.query_hash,
            last_patent_id=patent_id,
            last_patent_date=patent_date,
        )

    async def _load_patents(self, checkpoint: PatentsCheckpoint | None) -> PatentsLoadResult:
        result = self._patents_loader(checkpoint)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, PatentsLoadResult):
            return result
        return PatentsLoadResult(patents=result)

    def _matches_query(self, checkpoint: PatentsCheckpoint | None) -> bool:
        return checkpoint is not None and checkpoint.query_hash == self.query_hash


def _require_string(patent: Mapping[str, Any], field_name: str) -> str:
    value = patent.get(field_name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"PatentsView response is missing required {field_name}")
    return value


def _coerce_patent_date(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=UTC)
