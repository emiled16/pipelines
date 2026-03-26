from __future__ import annotations

from datetime import UTC
from io import BytesIO
from urllib.parse import parse_qs, urlsplit

from ingestion.providers.gdelt_articles import loader
from ingestion.providers.gdelt_articles.loader import load_article_entries, parse_article_entries


def test_parse_article_entries_extracts_common_gdelt_fields() -> None:
    payload = b"""
    {
      "articles": [
        {
          "url": "https://example.com/articles/1",
          "title": "First article",
          "seendate": "20260325T101500Z",
          "domain": "example.com"
        }
      ]
    }
    """

    entries = list(parse_article_entries(payload))

    assert len(entries) == 1
    assert entries[0]["id"] == "https://example.com/articles/1"
    assert entries[0]["title"] == "First article"
    assert entries[0]["domain"] == "example.com"
    assert entries[0]["seen_at"].tzinfo is UTC


def test_parse_article_entries_falls_back_to_title_when_url_is_missing() -> None:
    payload = b"""
    {
      "articles": [
        {
          "title": "Untitled URL-less article",
          "seendate": "20260325101500"
        }
      ]
    }
    """

    entries = list(parse_article_entries(payload))

    assert len(entries) == 1
    assert entries[0]["id"] == "Untitled URL-less article"
    assert entries[0]["seen_at"].tzinfo is UTC


def test_load_article_entries_builds_expected_request(monkeypatch) -> None:
    payload = b"""
    {
      "articles": [
        {
          "url": "https://example.com/articles/1",
          "title": "First article",
          "seendate": "20260325T101500Z"
        }
      ]
    }
    """
    captured: dict[str, object] = {}

    def fake_urlopen(request, timeout: float = 0.0):  # type: ignore[no-untyped-def]
        captured["url"] = request.full_url
        captured["headers"] = dict(request.header_items())
        captured["timeout"] = timeout
        return _FakeResponse(payload)

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    load_result = load_article_entries(
        "climate change",
        endpoint="https://api.example.test/doc",
        max_records=25,
        timeout=12.5,
    )

    params = parse_qs(urlsplit(str(captured["url"])).query)

    assert [entry["id"] for entry in load_result.entries] == ["https://example.com/articles/1"]
    assert params["query"] == ["climate change"]
    assert params["format"] == ["json"]
    assert params["mode"] == ["ArtList"]
    assert params["sort"] == ["DateDesc"]
    assert params["maxrecords"] == ["25"]
    assert captured["timeout"] == 12.5
    assert captured["headers"]["Accept"] == "application/json"


class _FakeResponse:
    def __init__(self, body: bytes) -> None:
        self._stream = BytesIO(body)

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        self.close()

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)

    def close(self) -> None:
        self._stream.close()
