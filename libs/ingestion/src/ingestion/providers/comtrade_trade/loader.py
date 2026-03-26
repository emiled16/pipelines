from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any, BinaryIO
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ingestion.providers.comtrade_trade.checkpoint import ComtradeTradeCheckpoint

PUBLIC_API_BASE_URL = "https://comtradeapi.un.org/public/v1/preview"
AUTHENTICATED_API_BASE_URL = "https://comtradeapi.un.org/data/v1/get"


@dataclass(slots=True, frozen=True)
class ComtradeTradeQuery:
    period: str
    reporter_code: str | int | None = None
    cmd_code: str | int | None = "TOTAL"
    flow_code: str | int | None = None
    partner_code: str | int | None = None
    partner2_code: str | int | None = None
    customs_code: str | int | None = None
    mot_code: str | int | None = None
    type_code: str = "C"
    freq_code: str = "A"
    cl_code: str = "HS"
    max_records: int | None = None
    aggregate_by: str | None = None
    breakdown_mode: str | None = "classic"
    include_desc: bool | None = True


@dataclass(slots=True, frozen=True)
class ComtradeTradeLoadResult:
    rows: Iterable[dict[str, object]]


class ComtradeTradeAPIError(RuntimeError):
    pass


def load_trade_rows(
    query: ComtradeTradeQuery,
    checkpoint: ComtradeTradeCheckpoint | None = None,
    *,
    subscription_key: str | None = None,
) -> ComtradeTradeLoadResult:
    del checkpoint

    request = Request(build_trade_request_url(query, subscription_key=subscription_key))

    try:
        with urlopen(request) as response:
            return ComtradeTradeLoadResult(rows=list(parse_trade_rows(response)))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace").strip()
        message = f"Comtrade API request failed with status {exc.code}"
        if detail:
            message = f"{message}: {detail}"
        raise ComtradeTradeAPIError(message) from exc


def build_trade_request_url(
    query: ComtradeTradeQuery,
    *,
    subscription_key: str | None = None,
) -> str:
    base_url = _build_base_url(query, subscription_key=subscription_key)
    params = _query_params(query, subscription_key=subscription_key)
    encoded_params = urlencode(params)
    if not encoded_params:
        return base_url
    return f"{base_url}?{encoded_params}"


def parse_trade_rows(json_source: bytes | str | BinaryIO) -> Iterator[dict[str, object]]:
    payload = _load_json_payload(json_source)
    rows = payload.get("data", [])
    if not isinstance(rows, list):
        raise ValueError("expected Comtrade API response field 'data' to be a list")

    for row in rows:
        if not isinstance(row, Mapping):
            raise ValueError("expected Comtrade API row entries to be JSON objects")
        yield {str(key): _normalize_json_value(value) for key, value in row.items()}


def _build_base_url(query: ComtradeTradeQuery, *, subscription_key: str | None) -> str:
    base_url = AUTHENTICATED_API_BASE_URL if subscription_key else PUBLIC_API_BASE_URL
    return f"{base_url}/{query.type_code}/{query.freq_code}/{query.cl_code}"


def _query_params(
    query: ComtradeTradeQuery,
    *,
    subscription_key: str | None,
) -> dict[str, str]:
    params = {
        "reportercode": query.reporter_code,
        "flowCode": query.flow_code,
        "period": query.period,
        "cmdCode": query.cmd_code,
        "partnerCode": query.partner_code,
        "partner2Code": query.partner2_code,
        "motCode": query.mot_code,
        "customsCode": query.customs_code,
        "maxRecords": query.max_records,
        "format": "JSON",
        "aggregateBy": query.aggregate_by,
        "breakdownMode": query.breakdown_mode,
        "includeDesc": query.include_desc,
        "subscription-key": subscription_key,
    }
    return {
        key: str(value)
        for key, value in params.items()
        if value is not None
    }


def _load_json_payload(json_source: bytes | str | BinaryIO) -> dict[str, object]:
    if isinstance(json_source, bytes):
        payload = json.loads(json_source)
    elif isinstance(json_source, str):
        payload = json.loads(json_source)
    else:
        payload = json.load(json_source)

    if not isinstance(payload, dict):
        raise ValueError("expected Comtrade API response to be a JSON object")
    return payload


def _normalize_json_value(value: Any) -> object:
    if isinstance(value, Mapping):
        return {str(key): _normalize_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_json_value(item) for item in value]
    return value
