from __future__ import annotations

import csv
import hashlib
import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from io import StringIO, TextIOWrapper
from typing import BinaryIO
from urllib.parse import quote
from urllib.request import Request, urlopen

from ingestion.providers.firms_fires.checkpoint import FirmsFiresCheckpoint

DEFAULT_FIRMS_BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
_FIRMS_TIMESTAMP_FORMAT = "%Y-%m-%d %H%M"


@dataclass(slots=True, frozen=True)
class FirmsFiresLoadResult:
    entries: Iterable[dict[str, object]]


def build_area_csv_url(
    *,
    api_key: str,
    source: str,
    area: str,
    days: int,
    base_url: str = DEFAULT_FIRMS_BASE_URL,
) -> str:
    normalized_base_url = base_url.rstrip("/")
    return (
        f"{normalized_base_url}/{quote(api_key, safe='')}/{quote(source, safe='')}/"
        f"{quote(area, safe=',')}/{days}"
    )


def load_fire_entries(
    api_key: str,
    source: str,
    area: str,
    days: int,
    checkpoint: FirmsFiresCheckpoint | None = None,
    *,
    base_url: str = DEFAULT_FIRMS_BASE_URL,
) -> FirmsFiresLoadResult:
    del checkpoint
    request = Request(
        build_area_csv_url(
            api_key=api_key,
            source=source,
            area=area,
            days=days,
            base_url=base_url,
        ),
        headers={"Accept": "text/csv"},
    )
    response = urlopen(request)
    return FirmsFiresLoadResult(entries=_iter_response_entries(response))


def parse_fire_entries(csv_source: bytes | BinaryIO) -> Iterator[dict[str, object]]:
    if isinstance(csv_source, bytes):
        text_stream: StringIO | TextIOWrapper = StringIO(csv_source.decode("utf-8-sig"))
        yield from _iter_rows(text_stream)
        return

    text_stream = TextIOWrapper(csv_source, encoding="utf-8-sig", newline="")
    try:
        yield from _iter_rows(text_stream)
    finally:
        text_stream.detach()


def _iter_response_entries(stream: BinaryIO) -> Iterator[dict[str, object]]:
    try:
        yield from parse_fire_entries(stream)
    finally:
        stream.close()


def _iter_rows(stream: StringIO | TextIOWrapper) -> Iterator[dict[str, object]]:
    for raw_row in csv.DictReader(stream):
        row = _normalize_row(raw_row)
        if row is not None:
            yield row


def _normalize_row(raw_row: dict[str, str | None]) -> dict[str, object] | None:
    row = {
        str(key).strip(): _clean_value(value)
        for key, value in raw_row.items()
        if key is not None and key.strip()
    }
    if not any(value is not None for value in row.values()):
        return None

    occurred_at = _parse_occurred_at(
        acq_date=_as_string(row.get("acq_date")),
        acq_time=_as_string(row.get("acq_time")),
    )
    if occurred_at is not None:
        row["occurred_at"] = occurred_at

    row["id"] = _build_entry_id(row)
    return row


def _clean_value(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _parse_occurred_at(*, acq_date: str | None, acq_time: str | None) -> datetime | None:
    if acq_date is None or acq_time is None:
        return None

    normalized_time = acq_time.zfill(4)
    try:
        return datetime.strptime(f"{acq_date} {normalized_time}", _FIRMS_TIMESTAMP_FORMAT).replace(
            tzinfo=UTC
        )
    except ValueError:
        return None


def _build_entry_id(row: dict[str, object]) -> str:
    canonical_payload = {
        key: _serialize_for_hash(value)
        for key, value in sorted(row.items())
        if key != "id"
    }
    digest = hashlib.sha256(
        json.dumps(canonical_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )
    return digest.hexdigest()


def _serialize_for_hash(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _as_string(value: object) -> str | None:
    return value if isinstance(value, str) else None
