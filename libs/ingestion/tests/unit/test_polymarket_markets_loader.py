from __future__ import annotations

from io import BytesIO
from urllib.parse import parse_qs, urlparse

from ingestion.providers.polymarket_markets import loader
from ingestion.providers.polymarket_markets.loader import load_markets, parse_markets


def test_parse_markets_normalizes_common_market_fields() -> None:
    json_bytes = b"""
    [
      {
        "id": "market-1",
        "slug": "election-2028",
        "question": "Who will win?",
        "conditionId": "0xabc",
        "endDate": "2026-03-25T10:15:00Z",
        "updatedAt": "2026-03-24T10:15:00Z"
      }
    ]
    """

    markets = parse_markets(json_bytes)

    assert len(markets) == 1
    assert markets[0]["id"] == "market-1"
    assert markets[0]["condition_id"] == "0xabc"
    assert markets[0]["updated_at"].isoformat() == "2026-03-24T10:15:00+00:00"
    assert markets[0]["end_date"].isoformat() == "2026-03-25T10:15:00+00:00"


def test_parse_markets_accepts_dictionary_wrapped_payloads() -> None:
    json_bytes = b"""
    {
      "data": [
        {
          "id": "market-2",
          "conditionId": "0xdef",
          "createdAt": "2026-03-20T09:30:00+00:00"
        }
      ]
    }
    """

    markets = parse_markets(json_bytes)

    assert len(markets) == 1
    assert markets[0]["id"] == "market-2"
    assert markets[0]["condition_id"] == "0xdef"
    assert markets[0]["created_at"].isoformat() == "2026-03-20T09:30:00+00:00"


def test_load_markets_paginates_until_a_partial_page_is_returned(monkeypatch) -> None:
    requested_urls: list[str] = []
    payloads = [
        b'[{"id": "market-3"}, {"id": "market-2"}]',
        b'[{"id": "market-1"}]',
    ]

    def fake_urlopen(request, timeout):
        requested_urls.append(request.full_url)
        return _FakeResponse(payloads.pop(0))

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    markets = list(load_markets(page_size=2))

    assert [market["id"] for market in markets] == ["market-3", "market-2", "market-1"]
    assert len(requested_urls) == 2
    assert parse_qs(urlparse(requested_urls[0]).query) == {"limit": ["2"], "offset": ["0"]}
    assert parse_qs(urlparse(requested_urls[1]).query) == {"limit": ["2"], "offset": ["2"]}


def test_load_markets_sends_json_accept_header(monkeypatch) -> None:
    seen_headers: list[dict[str, str]] = []

    def fake_urlopen(request, timeout):
        seen_headers.append(dict(request.header_items()))
        return _FakeResponse(b"[]")

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    list(load_markets())

    assert seen_headers[0]["Accept"] == "application/json"


class _FakeResponse:
    def __init__(self, body: bytes) -> None:
        self._stream = BytesIO(body)

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)

    def close(self) -> None:
        return None
