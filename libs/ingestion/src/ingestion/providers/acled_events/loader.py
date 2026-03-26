from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ingestion.providers.acled_events.checkpoint import AcledEventsCheckpoint

ScalarQueryValue = str | int | float | bool | date | datetime
AcledEvent = Mapping[str, Any]


@dataclass(slots=True, frozen=True)
class AcledEventsPageRequest:
    page: int
    limit: int
    checkpoint: AcledEventsCheckpoint | None = None


@dataclass(slots=True, frozen=True)
class AcledEventsLoadResult:
    events: Iterable[dict[str, object]]
    next_page: int | None = None


def load_event_page(
    request: AcledEventsPageRequest,
    *,
    endpoint: str,
    email: str,
    api_key: str,
    params: Mapping[str, ScalarQueryValue] | None = None,
) -> AcledEventsLoadResult:
    request_url = _build_request_url(
        endpoint=endpoint,
        email=email,
        api_key=api_key,
        page=request.page,
        limit=request.limit,
        params=params,
    )
    response = urlopen(Request(request_url, headers={"Accept": "application/json"}))

    try:
        payload = json.load(response)
    finally:
        response.close()

    events = _extract_events(payload)
    return AcledEventsLoadResult(
        events=events,
        next_page=_extract_next_page(
            payload,
            current_page=request.page,
            limit=request.limit,
            event_count=len(events),
        ),
    )


def _build_request_url(
    *,
    endpoint: str,
    email: str,
    api_key: str,
    page: int,
    limit: int,
    params: Mapping[str, ScalarQueryValue] | None,
) -> str:
    query = {
        "email": email,
        "key": api_key,
        "format": "json",
        "page": str(page),
        "limit": str(limit),
    }
    for key, value in (params or {}).items():
        query[str(key)] = _coerce_query_value(value)
    return f"{endpoint}?{urlencode(query)}"


def _coerce_query_value(value: ScalarQueryValue) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _extract_events(payload: object) -> list[dict[str, object]]:
    raw_events = _find_first_list(
        payload,
        paths=(("data",), ("data", "data"), ("results",), ("events",)),
    )
    if raw_events is None:
        raise ValueError("ACLED response did not include an events collection")

    events: list[dict[str, object]] = []
    for event in raw_events:
        if not isinstance(event, Mapping):
            raise TypeError("ACLED event payloads must be mapping objects")
        events.append({str(key): value for key, value in event.items()})
    return events


def _extract_next_page(
    payload: object,
    *,
    current_page: int,
    limit: int,
    event_count: int,
) -> int | None:
    next_page = _find_first_int(
        payload,
        paths=(("pagination", "next_page"), ("data", "next_page"), ("next_page",)),
    )
    if next_page is not None:
        return next_page if next_page > current_page else None

    total_pages = _find_first_int(
        payload,
        paths=(
            ("pagination", "total_pages"),
            ("pagination", "pages"),
            ("data", "total_pages"),
            ("data", "pages"),
            ("total_pages",),
            ("pages",),
        ),
    )
    if total_pages is not None:
        return current_page + 1 if current_page < total_pages else None

    has_next = _find_first_bool(
        payload,
        paths=(("pagination", "has_next"), ("data", "has_next"), ("has_next",)),
    )
    if has_next is not None:
        return current_page + 1 if has_next else None

    if event_count == 0 or event_count < limit:
        return None
    return current_page + 1


def _find_first_list(
    payload: object,
    *,
    paths: tuple[tuple[str, ...], ...],
) -> list[object] | None:
    for path in paths:
        value = _get_path(payload, path)
        if isinstance(value, list):
            return value
    return None


def _find_first_int(
    payload: object,
    *,
    paths: tuple[tuple[str, ...], ...],
) -> int | None:
    for path in paths:
        value = _get_path(payload, path)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _find_first_bool(
    payload: object,
    *,
    paths: tuple[tuple[str, ...], ...],
) -> bool | None:
    for path in paths:
        value = _get_path(payload, path)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "false"}:
                return normalized == "true"
    return None


def _get_path(payload: object, path: tuple[str, ...]) -> object | None:
    current = payload
    for key in path:
        if not isinstance(current, Mapping) or key not in current:
            return None
        current = current[key]
    return current
