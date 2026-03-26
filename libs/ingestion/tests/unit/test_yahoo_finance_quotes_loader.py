from __future__ import annotations

import json
from datetime import datetime, timezone

from ingestion.providers.yahoo_finance_quotes.loader import parse_chart_response


def test_parse_chart_response_prefers_latest_market_snapshot() -> None:
    payload = {
        "chart": {
            "result": [
                {
                    "meta": {
                        "symbol": "AAPL",
                        "shortName": "Apple Inc.",
                        "longName": "Apple Inc.",
                        "currency": "USD",
                        "fullExchangeName": "NasdaqGS",
                        "exchangeTimezoneName": "America/New_York",
                        "marketState": "POST",
                        "instrumentType": "EQUITY",
                        "regularMarketPrice": 219.5,
                        "regularMarketChange": 1.2,
                        "regularMarketChangePercent": 0.55,
                        "regularMarketTime": 1774456800,
                        "postMarketPrice": 219.1,
                        "postMarketChange": -0.4,
                        "postMarketChangePercent": -0.18,
                        "postMarketTime": 1774460400,
                    }
                }
            ],
            "error": None,
        }
    }

    quote = parse_chart_response(json.dumps(payload).encode())

    assert quote["symbol"] == "AAPL"
    assert quote["market_price"] == 219.1
    assert quote["quote_timestamp"] == 1774460400
    assert quote["occurred_at"] == datetime(2026, 3, 25, 17, 40, tzinfo=timezone.utc)
    assert quote["quote_url"] == "https://finance.yahoo.com/quote/AAPL"


def test_parse_chart_response_raises_when_result_is_missing() -> None:
    payload = {"chart": {"result": [], "error": {"code": "Not Found"}}}

    try:
        parse_chart_response(json.dumps(payload).encode())
    except ValueError as exc:
        assert "quote data" in str(exc)
    else:
        raise AssertionError("parse_chart_response should raise ValueError for empty results")
