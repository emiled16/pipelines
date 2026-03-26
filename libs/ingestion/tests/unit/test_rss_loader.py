from __future__ import annotations

from datetime import timezone
from io import BytesIO

from ingestion.providers.rss import loader
from ingestion.providers.rss.checkpoint import RssCheckpoint
from ingestion.providers.rss.loader import load_feed_entries, parse_feed_entries


def test_parse_feed_entries_extracts_common_rss_fields() -> None:
    xml_bytes = b"""
    <rss version="2.0">
      <channel>
        <item>
          <guid>entry-1</guid>
          <link>https://example.com/posts/1</link>
          <title>First entry</title>
          <pubDate>Tue, 24 Mar 2026 10:15:00 +0000</pubDate>
        </item>
      </channel>
    </rss>
    """

    entries = list(parse_feed_entries(xml_bytes))

    assert len(entries) == 1
    assert entries[0]["id"] == "entry-1"
    assert entries[0]["link"] == "https://example.com/posts/1"
    assert entries[0]["title"] == "First entry"
    assert entries[0]["published_at"].tzinfo == timezone.utc


def test_parse_feed_entries_falls_back_to_link_or_title() -> None:
    xml_bytes = b"""
    <rss version="2.0">
      <channel>
        <item>
          <link>https://example.com/posts/2</link>
          <title>Second entry</title>
        </item>
        <item>
          <title>Third entry</title>
        </item>
      </channel>
    </rss>
    """

    entries = list(parse_feed_entries(xml_bytes))

    assert [entry["id"] for entry in entries] == [
        "https://example.com/posts/2",
        "Third entry",
    ]


def test_parse_feed_entries_recovers_from_bare_ampersands() -> None:
    xml_bytes = b"""
    <rss version="2.0">
      <channel>
        <item>
          <guid>entry-4</guid>
          <title>Earth & Space</title>
        </item>
      </channel>
    </rss>
    """

    entries = list(parse_feed_entries(xml_bytes))

    assert len(entries) == 1
    assert entries[0]["id"] == "entry-4"
    assert entries[0]["title"] == "Earth & Space"


def test_parse_feed_entries_streams_items_in_document_order() -> None:
    xml_bytes = b"""
    <rss version="2.0">
      <channel>
        <item><guid>entry-1</guid><title>One</title></item>
        <item><guid>entry-2</guid><title>Two</title></item>
      </channel>
    </rss>
    """

    entries = parse_feed_entries(xml_bytes)

    assert next(entries)["id"] == "entry-1"
    assert next(entries)["id"] == "entry-2"


def test_load_feed_entries_keeps_response_open_until_entries_are_consumed(monkeypatch) -> None:
    xml_bytes = b"""
    <rss version="2.0">
      <channel>
        <item><guid>entry-1</guid><title>One</title></item>
      </channel>
    </rss>
    """
    response = _FakeResponse(xml_bytes)

    monkeypatch.setattr(loader, "urlopen", lambda request: response)

    load_result = load_feed_entries("https://example.com/feed.xml")

    assert response.closed is False
    assert [entry["id"] for entry in load_result.entries] == ["entry-1"]
    assert response.closed is True


def test_rss_checkpoint_supports_http_cache_fields() -> None:
    checkpoint = RssCheckpoint(
        feed_url="https://example.com/feed.xml",
        last_entry_id="entry-2",
        etag='"etag-1"',
        last_modified="Wed, 25 Mar 2026 10:15:00 GMT",
    )

    assert checkpoint.etag == '"etag-1"'
    assert checkpoint.last_modified == "Wed, 25 Mar 2026 10:15:00 GMT"


class _FakeResponse:
    def __init__(self, body: bytes) -> None:
        self._stream = BytesIO(body)
        self.headers = {}
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise AssertionError("response was closed before the loader finished reading it")
        return self._stream.read(size)

    def close(self) -> None:
        self.closed = True
