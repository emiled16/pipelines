from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Generic, TypeVar

from ingestion.models import Record

CheckpointT = TypeVar("CheckpointT")


class BaseProvider(ABC, Generic[CheckpointT]):
    name: str


class BatchProvider(BaseProvider[CheckpointT], ABC):
    @abstractmethod
    async def fetch(self, *, checkpoint: CheckpointT | None = None) -> AsyncIterator[Record]:
        """Yield a bounded set of normalized records."""


class StreamingProvider(BaseProvider[CheckpointT], ABC):
    @abstractmethod
    async def stream(self, *, checkpoint: CheckpointT | None = None) -> AsyncIterator[Record]:
        """Yield normalized records continuously."""
