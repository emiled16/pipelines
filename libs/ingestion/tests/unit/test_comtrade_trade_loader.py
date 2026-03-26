from __future__ import annotations

from io import BytesIO
from urllib.error import HTTPError

import pytest

from ingestion.providers.comtrade_trade import loader
from ingestion.providers.comtrade_trade.loader import (
    AUTHENTICATED_API_BASE_URL,
    PUBLIC_API_BASE_URL,
    ComtradeTradeAPIError,
    ComtradeTradeQuery,
    build_trade_request_url,
    load_trade_rows,
    parse_trade_rows,
)


def test_build_trade_request_url_uses_public_preview_without_subscription_key() -> None:
    query = ComtradeTradeQuery(
        period="2024",
        reporter_code="840",
        flow_code="X",
        partner_code="0",
    )

    url = build_trade_request_url(query)

    assert url.startswith(f"{PUBLIC_API_BASE_URL}/C/A/HS?")
    assert "reportercode=840" in url
    assert "flowCode=X" in url
    assert "partnerCode=0" in url
    assert "subscription-key" not in url


def test_build_trade_request_url_uses_authenticated_endpoint_with_subscription_key() -> None:
    query = ComtradeTradeQuery(
        period="202405",
        reporter_code="840",
        cmd_code="TOTAL",
        freq_code="M",
    )

    url = build_trade_request_url(query, subscription_key="secret-key")

    assert url.startswith(f"{AUTHENTICATED_API_BASE_URL}/C/M/HS?")
    assert "subscription-key=secret-key" in url
    assert "period=202405" in url


def test_parse_trade_rows_extracts_json_rows() -> None:
    payload = b"""
    {
      "data": [
        {
          "period": 2024,
          "reporterCode": 840,
          "flowCode": "X",
          "primaryValue": 123.45
        }
      ]
    }
    """

    rows = list(parse_trade_rows(payload))

    assert rows == [
        {
            "period": 2024,
            "reporterCode": 840,
            "flowCode": "X",
            "primaryValue": 123.45,
        }
    ]


def test_load_trade_rows_raises_api_error_with_response_details(monkeypatch) -> None:
    query = ComtradeTradeQuery(period="2024")
    error = HTTPError(
        url="https://example.com",
        code=429,
        msg="Too Many Requests",
        hdrs=None,
        fp=BytesIO(b'{"message":"rate limited"}'),
    )

    monkeypatch.setattr(loader, "urlopen", lambda request: (_ for _ in ()).throw(error))

    with pytest.raises(ComtradeTradeAPIError, match="429"):
        load_trade_rows(query)


def test_load_trade_rows_parses_response_payload(monkeypatch) -> None:
    query = ComtradeTradeQuery(period="2024")
    response = _FakeResponse(b'{"data":[{"period":2024,"reporterCode":840}]}')

    monkeypatch.setattr(loader, "urlopen", lambda request: response)

    load_result = load_trade_rows(query)

    assert list(load_result.rows) == [{"period": 2024, "reporterCode": 840}]


class _FakeResponse:
    def __init__(self, body: bytes) -> None:
        self._body = BytesIO(body)

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._body.close()

    def read(self, size: int = -1) -> bytes:
        return self._body.read(size)
