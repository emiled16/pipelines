from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, BinaryIO
from urllib.parse import urlencode
from urllib.request import Request, urlopen

CELESTRAK_GP_URL = "https://celestrak.org/NORAD/elements/gp.php"


@dataclass(slots=True, frozen=True)
class SpaceSatellitesLoadResult:
    entries: Iterable[dict[str, object]]
    source_url: str


def load_satellite_entries(group: str) -> SpaceSatellitesLoadResult:
    source_url = build_satellite_entries_url(group)
    response = urlopen(Request(source_url))
    return SpaceSatellitesLoadResult(
        entries=_iter_response_entries(response),
        source_url=source_url,
    )


def parse_satellite_entries(json_source: bytes | BinaryIO) -> Iterator[dict[str, object]]:
    if isinstance(json_source, bytes):
        stream: BinaryIO = BytesIO(json_source)
    else:
        stream = json_source

    payload = json.load(stream)
    for item in _iter_items(payload):
        yield _normalize_entry(item)


def build_satellite_entries_url(group: str) -> str:
    query = urlencode({"GROUP": group.upper(), "FORMAT": "JSON"})
    return f"{CELESTRAK_GP_URL}?{query}"


def _iter_response_entries(stream: BinaryIO) -> Iterator[dict[str, object]]:
    try:
        yield from parse_satellite_entries(stream)
    finally:
        stream.close()


def _iter_items(payload: Any) -> Iterator[dict[str, Any]]:
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item
        return
    if isinstance(payload, dict):
        yield payload
        return
    raise ValueError("CelesTrak response must be a JSON object or array")


def _normalize_entry(item: dict[str, Any]) -> dict[str, object]:
    normalized = {str(key).lower(): value for key, value in item.items()}
    epoch = _parse_epoch(item.get("EPOCH"))
    normalized["epoch"] = epoch

    record_id = _build_record_id(
        norad_cat_id=normalized.get("norad_cat_id"),
        epoch=epoch,
        epoch_text=item.get("EPOCH"),
        object_name=normalized.get("object_name"),
    )
    normalized["id"] = record_id
    return normalized


def _build_record_id(
    *,
    norad_cat_id: object,
    epoch: datetime | None,
    epoch_text: object,
    object_name: object,
) -> str:
    satellite_id = _string_or_none(norad_cat_id)
    if satellite_id is None:
        satellite_id = _string_or_none(object_name) or "unknown"

    if epoch is not None:
        return f"{satellite_id}:{epoch.isoformat()}"

    raw_epoch = _string_or_none(epoch_text)
    if raw_epoch is not None:
        return f"{satellite_id}:{raw_epoch}"
    return satellite_id


def _string_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_epoch(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None

    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    try:
        epoch = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if epoch.tzinfo is None:
        return epoch.replace(tzinfo=timezone.utc)
    return epoch
