from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ingestion.providers.eia_energy.checkpoint import EiaEnergyCheckpoint

DEFAULT_BASE_URL = "https://api.eia.gov/v2"
DEFAULT_PAGE_SIZE = 5000


@dataclass(slots=True, frozen=True)
class EiaSort:
    column: str
    direction: str = "desc"


@dataclass(slots=True, frozen=True)
class EiaEnergyLoadResult:
    rows: Iterable[dict[str, object]]
    total: int | None = None
    frequency: str | None = None
    date_format: str | None = None


def load_data_rows(
    *,
    api_key: str,
    route: str,
    checkpoint: EiaEnergyCheckpoint | None = None,
    data: Sequence[str] | None = None,
    facets: Mapping[str, Sequence[str]] | None = None,
    frequency: str | None = None,
    start: str | None = None,
    end: str | None = None,
    sort: Sequence[EiaSort] | None = None,
    offset: int = 0,
    length: int = DEFAULT_PAGE_SIZE,
    timeout: float | None = None,
    base_url: str = DEFAULT_BASE_URL,
) -> EiaEnergyLoadResult:
    request = Request(_build_request_url(
        api_key=api_key,
        route=route,
        data=data,
        facets=facets,
        frequency=frequency,
        start=start,
        end=end,
        sort=sort,
        offset=offset,
        length=length,
        base_url=base_url,
    ))

    with urlopen(request, timeout=timeout) as response:
        payload = json.load(response)

    response_payload = payload.get("response")
    if not isinstance(response_payload, dict):
        raise ValueError("EIA API response did not include a 'response' object")

    rows = response_payload.get("data")
    if not isinstance(rows, list):
        raise ValueError("EIA API response did not include a 'response.data' array")

    return EiaEnergyLoadResult(
        rows=[_normalize_row(row) for row in rows],
        total=_coerce_int(response_payload.get("total")),
        frequency=_coerce_str(response_payload.get("frequency")),
        date_format=_coerce_str(response_payload.get("dateFormat")),
    )


def _build_request_url(
    *,
    api_key: str,
    route: str,
    data: Sequence[str] | None,
    facets: Mapping[str, Sequence[str]] | None,
    frequency: str | None,
    start: str | None,
    end: str | None,
    sort: Sequence[EiaSort] | None,
    offset: int,
    length: int,
    base_url: str,
) -> str:
    clean_route = route.strip("/")
    params: list[tuple[str, str]] = [
        ("api_key", api_key),
        ("offset", str(offset)),
        ("length", str(length)),
    ]

    if data:
        for field in data:
            params.append(("data[]", field))
    if frequency:
        params.append(("frequency", frequency))
    if start:
        params.append(("start", start))
    if end:
        params.append(("end", end))
    if facets:
        for facet_name in sorted(facets):
            for facet_value in facets[facet_name]:
                params.append((f"facets[{facet_name}][]", facet_value))
    if sort:
        for index, item in enumerate(sort):
            params.append((f"sort[{index}][column]", item.column))
            params.append((f"sort[{index}][direction]", item.direction))

    encoded_params = urlencode(params)
    base_path = base_url.rstrip("/")
    data_url = f"{base_path}/{clean_route}/data"
    return f"{data_url}?{encoded_params}"


def _normalize_row(row: object) -> dict[str, object]:
    if not isinstance(row, dict):
        raise ValueError("EIA API row payload must be a JSON object")
    return {str(key): value for key, value in row.items()}


def _coerce_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value)
    raise ValueError(f"Expected int-compatible value, got {type(value).__name__}")


def _coerce_str(value: object) -> str | None:
    return value if isinstance(value, str) else None
