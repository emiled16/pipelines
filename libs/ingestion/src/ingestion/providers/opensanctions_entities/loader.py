from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Any, BinaryIO
from urllib.request import Request, urlopen

from ingestion.providers.opensanctions_entities.checkpoint import (
    OpenSanctionsEntitiesCheckpoint,
)

DEFAULT_DATASETS_URL = "https://data.opensanctions.org/datasets/latest"


@dataclass(slots=True, frozen=True)
class OpenSanctionsLoadResult:
    entries: Iterable[dict[str, object]]
    dataset: str
    version: str | None = None
    entities_url: str | None = None
    checksum: str | None = None
    updated_at: datetime | None = None
    last_change: datetime | None = None
    not_modified: bool = False


def load_entities(
    dataset: str,
    checkpoint: OpenSanctionsEntitiesCheckpoint | None = None,
    *,
    datasets_url: str = DEFAULT_DATASETS_URL,
) -> OpenSanctionsLoadResult:
    dataset_index = load_dataset_index(dataset, datasets_url=datasets_url)
    version = str(dataset_index["version"])
    resource = _resource_by_name(dataset_index, "entities.ftm.json")
    entities_url = _string_value(resource, "url")
    checksum = _optional_string_value(resource, "checksum")
    updated_at = _parse_datetime(_optional_string_value(dataset_index, "updated_at"))
    last_change = _parse_datetime(_optional_string_value(dataset_index, "last_change"))

    if checkpoint is not None and checkpoint.dataset == dataset and checkpoint.version == version:
        return OpenSanctionsLoadResult(
            entries=(),
            dataset=dataset,
            version=version,
            entities_url=entities_url,
            checksum=checksum,
            updated_at=updated_at,
            last_change=last_change,
            not_modified=True,
        )

    response = urlopen(Request(entities_url))
    return OpenSanctionsLoadResult(
        entries=_iter_response_entries(response),
        dataset=dataset,
        version=version,
        entities_url=entities_url,
        checksum=checksum,
        updated_at=updated_at,
        last_change=last_change,
    )


def load_dataset_index(
    dataset: str,
    *,
    datasets_url: str = DEFAULT_DATASETS_URL,
) -> Mapping[str, Any]:
    index_url = f"{datasets_url.rstrip('/')}/{dataset}/index.json"
    response = urlopen(Request(index_url))
    try:
        payload = json.load(response)
    finally:
        response.close()

    if not isinstance(payload, Mapping):
        raise ValueError("OpenSanctions dataset index must be a JSON object")
    return payload


def parse_entity_entries(json_source: bytes | BinaryIO) -> Iterator[dict[str, object]]:
    if isinstance(json_source, bytes):
        stream: BinaryIO = BytesIO(json_source)
    else:
        stream = json_source
    yield from _iter_entities(stream)


def _iter_response_entries(stream: BinaryIO) -> Iterator[dict[str, object]]:
    try:
        yield from parse_entity_entries(stream)
    finally:
        stream.close()


def _iter_entities(stream: BinaryIO) -> Iterator[dict[str, object]]:
    for raw_line in stream:
        line = raw_line.strip()
        if not line:
            continue

        entry = json.loads(line)
        if not isinstance(entry, dict):
            continue

        yield {str(key): value for key, value in entry.items()}


def _resource_by_name(dataset_index: Mapping[str, Any], resource_name: str) -> Mapping[str, Any]:
    resources = dataset_index.get("resources")
    if not isinstance(resources, list):
        raise ValueError("OpenSanctions dataset index is missing resources")

    for resource in resources:
        if not isinstance(resource, Mapping):
            continue
        if resource.get("name") == resource_name:
            return resource

    raise ValueError(f"OpenSanctions dataset index is missing {resource_name!r}")


def _string_value(mapping: Mapping[str, Any], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"OpenSanctions payload is missing {key!r}")
    return value


def _optional_string_value(mapping: Mapping[str, Any], key: str) -> str | None:
    value = mapping.get(key)
    return value if isinstance(value, str) and value else None


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)
