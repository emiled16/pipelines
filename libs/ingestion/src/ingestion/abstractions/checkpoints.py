from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

CheckpointT = TypeVar("CheckpointT")


class CheckpointStore(ABC, Generic[CheckpointT]):
    @abstractmethod
    async def load(self, scope: str) -> CheckpointT | None:
        """Return the latest committed checkpoint for the given scope."""

    @abstractmethod
    async def save(self, scope: str, checkpoint: CheckpointT) -> None:
        """Persist the latest checkpoint for the given scope."""
