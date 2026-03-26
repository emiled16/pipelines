from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DEFAULT_SAFECAST_API_URL = "https://api.safecast.org/measurements.json"
DEFAULT_PAGE_SIZE = 1000


@dataclass(slots=True, frozen=True)
class SafecastRadiationRequest:
    since: str | None = None
    until: str | None = None
    page_size: int = DEFAULT_PAGE_SIZE


@dataclass(slots=True, frozen=True)
class SafecastRadiationLoadResult:
    measurements: Iterable[dict[str, object]]


def load_measurements(
    api_url: str = DEFAULT_SAFECAST_API_URL,
    request: SafecastRadiationRequest | None = None,
) -> SafecastRadiationLoadResult:
    measurement_request = request or SafecastRadiationRequest()
    return SafecastRadiationLoadResult(
        measurements=_iter_measurements(api_url, measurement_request)
    )


def _iter_measurements(
    api_url: str,
    request: SafecastRadiationRequest,
) -> Iterator[dict[str, object]]:
    page = 1

    while True:
        response = urlopen(
            Request(
                _build_url(api_url, request=request, page=page),
                headers={"Accept": "application/json"},
            )
        )
        try:
            measurements = _normalize_payload(json.load(response))
        finally:
            response.close()

        if not measurements:
            break

        yield from measurements

        if len(measurements) < request.page_size:
            break

        page += 1


def _build_url(api_url: str, *, request: SafecastRadiationRequest, page: int) -> str:
    query: dict[str, object] = {
        "page": page,
        "per_page": request.page_size,
    }
    if request.since is not None:
        query["since"] = request.since
    if request.until is not None:
        query["until"] = request.until
    return f"{api_url}?{urlencode(query)}"


def _normalize_payload(payload: Any) -> list[dict[str, object]]:
    measurements: Any = payload
    if isinstance(payload, Mapping):
        measurements = payload.get("measurements")

    if not isinstance(measurements, list):
        raise ValueError("Safecast measurements response must be a list of objects")

    normalized: list[dict[str, object]] = []
    for measurement in measurements:
        if not isinstance(measurement, Mapping):
            continue
        normalized.append(_normalize_measurement(measurement))
    return normalized


def _normalize_measurement(measurement: Mapping[str, Any]) -> dict[str, object]:
    normalized = dict(measurement)
    for field_name in ("captured_at", "created_at", "updated_at"):
        parsed = _parse_datetime(measurement.get(field_name))
        if parsed is not None:
            normalized[field_name] = parsed
    return normalized


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None

    normalized = value.strip()
    if not normalized:
        return None

    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
