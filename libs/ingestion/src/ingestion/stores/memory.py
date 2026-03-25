from __future__ import annotations

from typing import Generic, TypeVar

from ingestion.abstractions import CheckpointStore, RecordSink
from ingestion.models import Record

CheckpointT = TypeVar("CheckpointT")


class InMemoryCheckpointStore(CheckpointStore[CheckpointT], Generic[CheckpointT]):
    def __init__(self) -> None:
        self._values: dict[str, CheckpointT] = {}

    async def load(self, scope: str) -> CheckpointT | None:
        return self._values.get(scope)

    async def save(self, scope: str, checkpoint: CheckpointT) -> None:
        self._values[scope] = checkpoint


class InMemoryRecordSink(RecordSink):
    def __init__(self) -> None:
        self.records: list[Record] = []

    async def write(self, record: Record) -> None:
        self.records.append(record)
