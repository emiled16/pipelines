from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, BinaryIO
from urllib.parse import quote
from urllib.request import Request, urlopen

from ingestion.providers.yahoo_finance_quotes.checkpoint import YahooFinanceQuotesCheckpoint

QUOTE_URL_TEMPLATE = "https://finance.yahoo.com/quote/{symbol}"
CHART_URL_TEMPLATE = (
    "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    "?interval=1d&range=1d&includePrePost=true"
)
REQUEST_HEADERS = {
    "Accept": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    ),
}


@dataclass(slots=True, frozen=True)
class YahooFinanceQuotesLoadResult:
    quotes: Iterable[dict[str, object]]


def load_quotes(
    symbols: Sequence[str], checkpoint: YahooFinanceQuotesCheckpoint | None = None
) -> YahooFinanceQuotesLoadResult:
    del checkpoint
    return YahooFinanceQuotesLoadResult(quotes=[load_quote(symbol) for symbol in symbols])


def load_quote(symbol: str) -> dict[str, object]:
    request = Request(
        CHART_URL_TEMPLATE.format(symbol=quote(symbol, safe="")),
        headers=REQUEST_HEADERS,
    )
    with urlopen(request) as response:
        return parse_chart_response(response)


def parse_chart_response(json_source: bytes | BinaryIO) -> dict[str, object]:
    if isinstance(json_source, bytes):
        stream: BinaryIO = BytesIO(json_source)
    else:
        stream = json_source

    payload = json.load(stream)
    result = payload.get("chart", {}).get("result") or []
    if not result:
        error = payload.get("chart", {}).get("error")
        raise ValueError(f"Yahoo Finance chart response did not contain quote data: {error!r}")

    meta = result[0].get("meta") or {}
    symbol = _require_string(meta.get("symbol"), field_name="symbol")
    quote_timestamp = _pick_quote_timestamp(meta)
    occurred_at = (
        datetime.fromtimestamp(quote_timestamp, tz=timezone.utc)
        if quote_timestamp is not None
        else None
    )
    current_market = _pick_latest_market(meta, quote_timestamp)

    return {
        "symbol": symbol,
        "short_name": _as_string(meta.get("shortName")),
        "long_name": _as_string(meta.get("longName")),
        "currency": _as_string(meta.get("currency")),
        "exchange": _as_string(meta.get("fullExchangeName") or meta.get("exchangeName")),
        "exchange_timezone_name": _as_string(meta.get("exchangeTimezoneName")),
        "market_state": _as_string(meta.get("marketState")),
        "quote_type": _as_string(meta.get("instrumentType")),
        "regular_market_price": _as_float(meta.get("regularMarketPrice")),
        "regular_market_change": _as_float(meta.get("regularMarketChange")),
        "regular_market_change_percent": _as_float(meta.get("regularMarketChangePercent")),
        "regular_market_time": _as_int(meta.get("regularMarketTime")),
        "pre_market_price": _as_float(meta.get("preMarketPrice")),
        "pre_market_change": _as_float(meta.get("preMarketChange")),
        "pre_market_change_percent": _as_float(meta.get("preMarketChangePercent")),
        "pre_market_time": _as_int(meta.get("preMarketTime")),
        "post_market_price": _as_float(meta.get("postMarketPrice")),
        "post_market_change": _as_float(meta.get("postMarketChange")),
        "post_market_change_percent": _as_float(meta.get("postMarketChangePercent")),
        "post_market_time": _as_int(meta.get("postMarketTime")),
        "market_price": current_market["price"],
        "market_change": current_market["change"],
        "market_change_percent": current_market["change_percent"],
        "quote_timestamp": quote_timestamp,
        "occurred_at": occurred_at,
        "quote_url": QUOTE_URL_TEMPLATE.format(symbol=quote(symbol, safe="")),
    }


def _pick_quote_timestamp(meta: dict[str, Any]) -> int | None:
    timestamps = [
        _as_int(meta.get("postMarketTime")),
        _as_int(meta.get("preMarketTime")),
        _as_int(meta.get("regularMarketTime")),
    ]
    valid_timestamps = [timestamp for timestamp in timestamps if timestamp is not None]
    return max(valid_timestamps) if valid_timestamps else None


def _pick_latest_market(
    meta: dict[str, Any], quote_timestamp: int | None
) -> dict[str, float | None]:
    market_snapshots = [
        (
            _as_int(meta.get("postMarketTime")),
            {
                "price": _as_float(meta.get("postMarketPrice")),
                "change": _as_float(meta.get("postMarketChange")),
                "change_percent": _as_float(meta.get("postMarketChangePercent")),
            },
        ),
        (
            _as_int(meta.get("preMarketTime")),
            {
                "price": _as_float(meta.get("preMarketPrice")),
                "change": _as_float(meta.get("preMarketChange")),
                "change_percent": _as_float(meta.get("preMarketChangePercent")),
            },
        ),
        (
            _as_int(meta.get("regularMarketTime")),
            {
                "price": _as_float(meta.get("regularMarketPrice")),
                "change": _as_float(meta.get("regularMarketChange")),
                "change_percent": _as_float(meta.get("regularMarketChangePercent")),
            },
        ),
    ]
    for timestamp, snapshot in market_snapshots:
        if timestamp is not None and timestamp == quote_timestamp:
            return snapshot
    return {
        "price": _as_float(meta.get("regularMarketPrice")),
        "change": _as_float(meta.get("regularMarketChange")),
        "change_percent": _as_float(meta.get("regularMarketChangePercent")),
    }


def _require_string(value: object, *, field_name: str) -> str:
    if isinstance(value, str) and value:
        return value
    raise ValueError(f"Yahoo Finance chart response missing {field_name}")


def _as_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


def _as_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    return None
