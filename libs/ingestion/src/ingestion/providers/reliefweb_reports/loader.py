from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, BinaryIO
from urllib.parse import urlencode
from urllib.request import Request, urlopen

RELIEFWEB_REPORTS_ENDPOINT = "https://api.reliefweb.int/v2/reports"
DEFAULT_PAGE_SIZE = 100
DEFAULT_REPORT_FIELDS = (
    "title",
    "url",
    "date.created",
    "date.changed",
    "source",
    "country",
    "primary_country",
    "format",
    "language",
)


@dataclass(slots=True, frozen=True)
class ReliefWebReportsRequest:
    appname: str
    offset: int = 0
    limit: int = DEFAULT_PAGE_SIZE
    preset: str | None = "latest"
    fields: tuple[str, ...] = DEFAULT_REPORT_FIELDS
    query: Mapping[str, Any] | None = None
    filter: Mapping[str, Any] | None = None


@dataclass(slots=True, frozen=True)
class ReliefWebReportsPage:
    reports: tuple[dict[str, object], ...]
    total_count: int | None = None
    count: int | None = None
    next_offset: int | None = None


def load_reports_page(request: ReliefWebReportsRequest) -> ReliefWebReportsPage:
    response = urlopen(
        Request(
            _request_url(request.appname),
            data=_request_body(request),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
    )
    try:
        return parse_reports_page(
            response,
            request_offset=request.offset,
            request_limit=request.limit,
        )
    finally:
        response.close()


def parse_reports_page(
    json_source: bytes | BinaryIO,
    *,
    request_offset: int = 0,
    request_limit: int = DEFAULT_PAGE_SIZE,
) -> ReliefWebReportsPage:
    payload = json.loads(_read_bytes(json_source))
    if not isinstance(payload, Mapping):
        raise ValueError("ReliefWeb response payload must be a JSON object")

    raw_data = payload.get("data", [])
    if not isinstance(raw_data, Sequence) or isinstance(raw_data, (str, bytes, bytearray)):
        raise ValueError("ReliefWeb response data must be a list")

    reports = tuple(_normalize_report(item) for item in raw_data)
    total_count = _coerce_int(payload.get("totalCount"))
    count = _coerce_int(payload.get("count"))
    effective_count = count if count is not None else len(reports)

    return ReliefWebReportsPage(
        reports=reports,
        total_count=total_count,
        count=effective_count,
        next_offset=_next_offset(
            request_offset=request_offset,
            request_limit=request_limit,
            count=effective_count,
            total_count=total_count,
        ),
    )


def _request_url(appname: str) -> str:
    return f"{RELIEFWEB_REPORTS_ENDPOINT}?{urlencode({'appname': appname})}"


def _request_body(request: ReliefWebReportsRequest) -> bytes:
    body: dict[str, object] = {
        "offset": request.offset,
        "limit": request.limit,
        "sort": ["date.created:desc", "id:desc"],
    }

    if request.preset is not None:
        body["preset"] = request.preset
    if request.fields:
        body["fields"] = {"include": list(request.fields)}
    if request.query is not None:
        body["query"] = dict(request.query)
    if request.filter is not None:
        body["filter"] = dict(request.filter)

    return json.dumps(body).encode("utf-8")


def _normalize_report(item: object) -> dict[str, object]:
    if not isinstance(item, Mapping):
        raise ValueError("ReliefWeb report entries must be objects")

    raw_fields = item.get("fields")
    if not isinstance(raw_fields, Mapping):
        raise ValueError("ReliefWeb report entry is missing fields")

    raw_report_id = item.get("id")
    if raw_report_id is None:
        raise ValueError("ReliefWeb report entry is missing id")

    date_fields = raw_fields.get("date")
    date_mapping = date_fields if isinstance(date_fields, Mapping) else {}

    return {
        "id": str(raw_report_id),
        "title": _coerce_str(raw_fields.get("title")),
        "url": _coerce_str(raw_fields.get("url")) or _coerce_str(item.get("href")),
        "date_created": _parse_datetime(date_mapping.get("created")),
        "date_changed": _parse_datetime(date_mapping.get("changed")),
        "fields": dict(raw_fields),
        "href": _coerce_str(item.get("href")),
        "score": item.get("score"),
    }


def _read_bytes(json_source: bytes | BinaryIO) -> bytes:
    if isinstance(json_source, bytes):
        return json_source
    return json_source.read()


def _parse_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("ReliefWeb datetime values must be strings")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _coerce_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("ReliefWeb pagination counts must be integers")
    return value


def _coerce_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _next_offset(
    *,
    request_offset: int,
    request_limit: int,
    count: int,
    total_count: int | None,
) -> int | None:
    if count <= 0:
        return None

    next_offset = request_offset + count
    if total_count is not None:
        return next_offset if next_offset < total_count else None

    return next_offset if count >= request_limit else None
