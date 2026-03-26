from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from datetime import datetime
from io import BytesIO
from typing import Any, BinaryIO
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ingestion.providers.polymarket_markets.checkpoint import PolymarketMarketsCheckpoint

DEFAULT_API_URL = "https://gamma-api.polymarket.com"
DEFAULT_PAGE_SIZE = 100
DEFAULT_TIMEOUT = 30.0

_DATETIME_FIELD_ALIASES = {
    "acceptingOrdersAt": "accepting_orders_at",
    "accepting_orders_at": "accepting_orders_at",
    "createdAt": "created_at",
    "created_at": "created_at",
    "endDate": "end_date",
    "end_date": "end_date",
    "startDate": "start_date",
    "start_date": "start_date",
    "updatedAt": "updated_at",
    "updated_at": "updated_at",
}
_FIELD_ALIASES = {
    "conditionId": "condition_id",
    "condition_id": "condition_id",
}


def load_markets(
    checkpoint: PolymarketMarketsCheckpoint | None = None,
    *,
    api_url: str = DEFAULT_API_URL,
    page_size: int = DEFAULT_PAGE_SIZE,
    query_params: Mapping[str, object] | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> Iterator[dict[str, object]]:
    if page_size <= 0:
        raise ValueError("page_size must be greater than zero")

    offset = 0
    headers = _request_headers()

    while True:
        request = Request(
            _build_markets_url(
                api_url,
                limit=page_size,
                offset=offset,
                query_params=query_params,
            ),
            headers=headers,
        )
        response = urlopen(request, timeout=timeout)
        try:
            markets = parse_markets(response)
        finally:
            response.close()

        if not markets:
            return

        yield from markets

        if len(markets) < page_size:
            return

        offset += len(markets)


def parse_markets(json_source: bytes | str | BinaryIO) -> list[dict[str, object]]:
    if isinstance(json_source, bytes):
        stream: BinaryIO = BytesIO(json_source)
    elif isinstance(json_source, str):
        stream = BytesIO(json_source.encode("utf-8"))
    else:
        stream = json_source

    payload = json.load(stream)
    return _extract_markets(payload)


def _build_markets_url(
    api_url: str,
    *,
    limit: int,
    offset: int,
    query_params: Mapping[str, object] | None,
) -> str:
    params: dict[str, object] = {
        "limit": limit,
        "offset": offset,
    }
    if query_params is not None:
        params.update(query_params)
    return f"{api_url.rstrip('/')}/markets?{urlencode(params, doseq=True)}"


def _extract_markets(payload: Any) -> list[dict[str, object]]:
    if isinstance(payload, list):
        raw_markets = payload
    elif isinstance(payload, dict):
        if isinstance(payload.get("data"), list):
            raw_markets = payload["data"]
        elif isinstance(payload.get("markets"), list):
            raw_markets = payload["markets"]
        else:
            raise ValueError("unsupported Polymarket markets payload")
    else:
        raise ValueError("unsupported Polymarket markets payload")

    return [_normalize_market(item) for item in raw_markets if isinstance(item, Mapping)]


def _normalize_market(market: Mapping[str, Any]) -> dict[str, object]:
    normalized = dict(market)

    market_id = market.get("id")
    if market_id is None:
        condition_id = market.get("conditionId") or market.get("condition_id")
        slug = market.get("slug")
        market_id = condition_id or slug
    if market_id is not None:
        normalized["id"] = str(market_id)

    for source_field, target_field in _FIELD_ALIASES.items():
        value = market.get(source_field)
        if value is not None:
            normalized[target_field] = str(value)

    for source_field, target_field in _DATETIME_FIELD_ALIASES.items():
        parsed_value = _parse_datetime(market.get(source_field))
        if parsed_value is not None:
            normalized[target_field] = parsed_value

    return normalized


def _parse_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None

    candidate = value.strip()
    if not candidate:
        return None
    if candidate.endswith("Z"):
        candidate = f"{candidate[:-1]}+00:00"

    try:
        return datetime.fromisoformat(candidate)
    except ValueError:
        return None


def _request_headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
    }
