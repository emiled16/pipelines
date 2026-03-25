from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable

from ingestion.models import Record, WriteResult


class RecordSink(ABC):
    @abstractmethod
    async def write(self, record: Record) -> None:
        """Persist a single record."""

    async def write_many(self, records: Iterable[Record]) -> WriteResult:
        count = 0
        for record in records:
            await self.write(record)
            count += 1
        return WriteResult(records_written=count)

    async def flush(self) -> None:
        """Flush buffered writes if the implementation batches internally."""
