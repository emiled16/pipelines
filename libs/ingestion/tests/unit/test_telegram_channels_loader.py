from __future__ import annotations

from datetime import timezone
from io import BytesIO

from ingestion.providers.telegram_channels import loader
from ingestion.providers.telegram_channels.loader import (
    build_channel_url,
    load_channel_messages,
    normalize_channel_name,
    parse_channel_messages,
)


def test_parse_channel_messages_extracts_common_fields() -> None:
    html_bytes = b"""
    <div class="tgme_widget_message_wrap">
      <div class="tgme_widget_message" data-post="example_channel/101">
        <div class="tgme_widget_message_author">Example Channel</div>
        <a class="tgme_widget_message_date" href="/example_channel/101">
          <time datetime="2026-03-24T10:15:00+00:00">Mar 24</time>
        </a>
        <div class="tgme_widget_message_text js-message_text">
          First <b>update</b><br />Line two
        </div>
        <span class="tgme_widget_message_views">1.2K</span>
      </div>
    </div>
    """

    entries = list(parse_channel_messages(html_bytes))

    assert len(entries) == 1
    assert entries[0]["id"] == "101"
    assert entries[0]["channel_name"] == "example_channel"
    assert entries[0]["url"] == "https://t.me/example_channel/101"
    assert entries[0]["author"] == "Example Channel"
    assert entries[0]["text"] == "First update\nLine two"
    assert entries[0]["views"] == "1.2K"
    assert entries[0]["published_at"].tzinfo == timezone.utc


def test_parse_channel_messages_sorts_newest_message_first() -> None:
    html_bytes = b"""
    <div class="tgme_widget_message" data-post="example_channel/9"></div>
    <div class="tgme_widget_message" data-post="example_channel/11"></div>
    <div class="tgme_widget_message" data-post="example_channel/10"></div>
    """

    entries = list(parse_channel_messages(html_bytes))

    assert [entry["id"] for entry in entries] == ["11", "10", "9"]


def test_load_channel_messages_keeps_response_open_until_entries_are_consumed(monkeypatch) -> None:
    html_bytes = b"""
    <div class="tgme_widget_message" data-post="example_channel/101">
      <div class="tgme_widget_message_text">One</div>
    </div>
    """
    response = _FakeResponse(html_bytes)

    monkeypatch.setattr(loader, "urlopen", lambda request: response)

    load_result = load_channel_messages("@example_channel")

    assert response.closed is False
    assert [entry["id"] for entry in load_result.entries] == ["101"]
    assert response.closed is True


def test_normalize_channel_name_accepts_handles_and_urls() -> None:
    assert normalize_channel_name("@example_channel") == "example_channel"
    assert normalize_channel_name("https://t.me/s/example_channel/") == "example_channel"
    assert build_channel_url("@example_channel") == "https://t.me/s/example_channel"


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

