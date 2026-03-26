from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

DATETIME_MARKER = "__ingestion_datetime__"


def to_json_compatible(value: Any) -> Any:
    if isinstance(value, datetime):
        return {DATETIME_MARKER: value.isoformat()}
    if isinstance(value, Mapping):
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
