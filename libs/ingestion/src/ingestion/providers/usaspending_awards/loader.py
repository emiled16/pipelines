from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.request import Request, urlopen

DEFAULT_API_BASE_URL = "https://api.usaspending.gov"
DEFAULT_SEARCH_PATH = "/api/v2/search/spending_by_award/"


@dataclass(slots=True, frozen=True)
class USAspendingAwardsLoadResult:
    awards: Iterable[dict[str, object]]
    has_next_page: bool


def load_awards_page(
    *,
    page: int,
    limit: int,
    search_body: Mapping[str, Any] | None = None,
    api_base_url: str = DEFAULT_API_BASE_URL,
    search_path: str = DEFAULT_SEARCH_PATH,
    timeout: float = 30.0,
) -> USAspendingAwardsLoadResult:
    if page < 1:
        raise ValueError("page must be >= 1")
    if limit < 1:
        raise ValueError("limit must be >= 1")

    request = Request(
        url=_build_url(api_base_url, search_path),
        data=json.dumps(_build_request_body(search_body, page=page, limit=limit)).encode("utf-8"),
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "ingestion/usaspending_awards",
        },
        method="POST",
    )

    with urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))

    if not isinstance(payload, Mapping):
        raise ValueError("USAspending awards response must be a JSON object")

    awards = _extract_awards(payload)
    return USAspendingAwardsLoadResult(
        awards=awards,
        has_next_page=_extract_has_next_page(
            payload,
            awards_count=len(awards),
            page=page,
            limit=limit,
        ),
    )


def _build_request_body(
    search_body: Mapping[str, Any] | None,
    *,
    page: int,
    limit: int,
) -> dict[str, Any]:
    request_body = dict(search_body or {})
    request_body.setdefault("filters", {})
    request_body.setdefault("sort", "Last Modified Date")
    request_body.setdefault("order", "desc")
    request_body["page"] = page
    request_body["limit"] = limit
    return request_body


def _build_url(api_base_url: str, search_path: str) -> str:
    return f"{api_base_url.rstrip('/')}/{search_path.lstrip('/')}"


def _extract_awards(payload: Mapping[str, object]) -> list[dict[str, object]]:
    results = payload.get("results")
    if not isinstance(results, list):
        awards = payload.get("awards")
        if not isinstance(awards, list):
            raise ValueError("USAspending awards response must include a list of results")
        results = awards

    normalized_results: list[dict[str, object]] = []
    for result in results:
        if not isinstance(result, Mapping):
            raise ValueError("USAspending awards results must contain JSON objects")
        normalized_results.append(dict(result))
    return normalized_results


def _extract_has_next_page(
    payload: Mapping[str, object],
    *,
    awards_count: int,
    page: int,
    limit: int,
) -> bool:
    page_metadata = payload.get("page_metadata")
    if isinstance(page_metadata, Mapping):
        has_next_page = _mapping_bool(
            page_metadata,
            "hasNext",
            "has_next",
            "has_next_page",
        )
        if has_next_page is not None:
            return has_next_page

        total_count = _mapping_int(page_metadata, "count", "total", "total_records")
        if total_count is not None:
            return page * limit < total_count

    has_next_page = _mapping_bool(payload, "hasNext", "has_next", "has_next_page")
    if has_next_page is not None:
        return has_next_page

    return awards_count == limit


def _mapping_bool(mapping: Mapping[str, object], *keys: str) -> bool | None:
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, bool):
            return value
    return None


def _mapping_int(mapping: Mapping[str, object], *keys: str) -> int | None:
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, int):
            return value
    return None
