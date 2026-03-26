from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Generic

from ingestion.abstractions.checkpoint_store import CheckpointT
from ingestion.models.record import Record


class Provider(ABC, Generic[CheckpointT]):
    name: str


class BatchProvider(Provider[CheckpointT], ABC):
    @abstractmethod
    async def fetch(self, *, checkpoint: CheckpointT | None = None) -> AsyncIterator[Record]:
        """Yield a bounded set of normalized records."""


class StreamingProvider(Provider[CheckpointT], ABC):
    @abstractmethod
    async def stream(self, *, checkpoint: CheckpointT | None = None) -> AsyncIterator[Record]:
        """Yield normalized records continuously."""
