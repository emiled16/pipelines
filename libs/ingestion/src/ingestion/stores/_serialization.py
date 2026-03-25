from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any, Generic, Mapping, Protocol, TypeVar

CheckpointT = TypeVar("CheckpointT")

DATETIME_MARKER = "__ingestion_datetime__"


class CheckpointCodec(Protocol[CheckpointT]):
    def dump(self, checkpoint: CheckpointT) -> dict[str, Any]:
        """Serialize a checkpoint into a JSON-compatible payload."""

    def load(self, payload: Mapping[str, Any]) -> CheckpointT:
        """Deserialize a checkpoint from a JSON-compatible payload."""


class DataclassCheckpointCodec(Generic[CheckpointT]):
    def __init__(self, checkpoint_type: type[CheckpointT]) -> None:
        if not is_dataclass(checkpoint_type):
            raise TypeError("checkpoint_type must be a dataclass type")
        self._checkpoint_type = checkpoint_type

    def dump(self, checkpoint: CheckpointT) -> dict[str, Any]:
        if not is_dataclass(checkpoint):
            raise TypeError("checkpoint must be a dataclass instance")
        return to_json_compatible(asdict(checkpoint))

    def load(self, payload: Mapping[str, Any]) -> CheckpointT:
        return self._checkpoint_type(**from_json_compatible(dict(payload)))


def to_json_compatible(value: Any) -> Any:
    if isinstance(value, datetime):
        return {DATETIME_MARKER: value.isoformat()}
    if isinstance(value, dict):
        return {str(key): to_json_compatible(item) for key, item in value.items()}
    if isinstance(value, list):
        return [to_json_compatible(item) for item in value]
    if isinstance(value, tuple):
        return [to_json_compatible(item) for item in value]
    return value


def from_json_compatible(value: Any) -> Any:
    if isinstance(value, dict):
        if set(value) == {DATETIME_MARKER}:
            return datetime.fromisoformat(value[DATETIME_MARKER])
        return {key: from_json_compatible(item) for key, item in value.items()}
    if isinstance(value, list):
        return [from_json_compatible(item) for item in value]
    return value
