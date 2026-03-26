from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, Mapping, Protocol, TypeVar

CheckpointT = TypeVar("CheckpointT")


class CheckpointCodec(Protocol[CheckpointT]):
    def dump(self, checkpoint: CheckpointT) -> dict[str, Any]:
        """Serialize a checkpoint into a JSON-compatible payload."""

    def load(self, payload: Mapping[str, Any]) -> CheckpointT:
        """Deserialize a checkpoint from a JSON-compatible payload."""

    def cursor(self, checkpoint: CheckpointT) -> str | None:
        """Return the checkpoint cursor to store in a dedicated column."""


class CheckpointStore(ABC, Generic[CheckpointT]):
    @abstractmethod
    async def load(self, scope: str) -> CheckpointT | None:
        """Return the latest committed checkpoint for the given scope."""

    @abstractmethod
    async def save(self, scope: str, checkpoint: CheckpointT) -> None:
        """Persist the latest checkpoint for the given scope."""
