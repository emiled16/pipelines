from __future__ import annotations

from typing import Generic

from ingestion.abstractions.checkpoint_store import CheckpointStore, CheckpointT
from ingestion.abstractions.sink import RecordSink
from ingestion.models.record import Record


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
