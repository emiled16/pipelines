from __future__ import annotations

import json
from io import BytesIO

import pytest

from ingestion.providers.polymarket_quotes import loader
from ingestion.providers.polymarket_quotes.loader import load_order_books, parse_order_books


def test_parse_order_books_extracts_books_from_bytes() -> None:
    payload = json.dumps(
        [
            {
                "market": "market-1",
                "asset_id": "token-1",
                "timestamp": "2026-03-25T10:15:00Z",
                "hash": "hash-1",
                "bids": [{"price": "0.48", "size": "100"}],
                "asks": [{"price": "0.52", "size": "80"}],
            }
        ]
    ).encode("utf-8")

    books = parse_order_books(payload)

    assert len(books) == 1
    assert books[0]["asset_id"] == "token-1"
    assert books[0]["hash"] == "hash-1"


def test_parse_order_books_accepts_file_like_streams() -> None:
    stream = BytesIO(b'[{"asset_id": "token-1"}, {"asset_id": "token-2"}]')

    books = parse_order_books(stream)

    assert [book["asset_id"] for book in books] == ["token-1", "token-2"]


def test_parse_order_books_rejects_non_list_payloads() -> None:
    with pytest.raises(TypeError):
        parse_order_books(b'{"asset_id": "token-1"}')


def test_load_order_books_posts_batch_request_and_closes_response(monkeypatch) -> None:
    response = _FakeResponse(
        b'[{"asset_id": "token-1", "hash": "hash-1"}, {"asset_id": "token-2", "hash": "hash-2"}]'
    )
    captured: dict[str, object] = {}

    def fake_urlopen(request):
        captured["url"] = request.full_url
        captured["method"] = request.get_method()
        captured["headers"] = dict(request.header_items())
        captured["body"] = request.data
        return response

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    load_result = load_order_books(["token-1", "token-2"])

    assert captured["url"] == "https://clob.polymarket.com/books"
    assert captured["method"] == "POST"
    assert captured["headers"]["Content-type"] == "application/json"
    assert json.loads(captured["body"].decode("utf-8")) == [
        {"token_id": "token-1"},
        {"token_id": "token-2"},
    ]
    assert [book["hash"] for book in load_result.books] == ["hash-1", "hash-2"]
    assert response.closed is True


def test_load_order_books_rejects_empty_token_ids() -> None:
    with pytest.raises(ValueError):
        load_order_books([])


class _FakeResponse:
    def __init__(self, body: bytes) -> None:
        self._stream = BytesIO(body)
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise AssertionError("response was closed before the loader finished reading it")
        return self._stream.read(size)

    def close(self) -> None:
        self.closed = True
