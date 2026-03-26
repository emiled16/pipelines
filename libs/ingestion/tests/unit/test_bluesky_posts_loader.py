from __future__ import annotations

import json
from datetime import timezone

import pytest

from ingestion.providers.bluesky_posts import loader
from ingestion.providers.bluesky_posts.loader import load_author_feed_page, parse_author_feed_page


def test_parse_author_feed_page_extracts_normalized_post_fields() -> None:
    page = parse_author_feed_page(
        {
            "cursor": "cursor-2",
            "feed": [
                {
                    "post": {
                        "uri": "at://did:plc:alice/app.bsky.feed.post/3lf4",
                        "cid": "bafy-post",
                        "author": {
                            "did": "did:plc:alice",
                            "handle": "alice.bsky.social",
                            "displayName": "Alice",
                        },
                        "record": {
                            "text": "First post",
                            "createdAt": "2026-03-25T10:15:00Z",
                            "langs": ["en"],
                        },
                        "indexedAt": "2026-03-25T10:15:05Z",
                        "replyCount": 2,
                        "repostCount": 3,
                        "likeCount": 5,
                        "quoteCount": 1,
                        "labels": [{"val": "news"}],
                        "embed": {"$type": "app.bsky.embed.images#view"},
                    },
                    "reply": {
                        "root": {"uri": "at://did:plc:alice/app.bsky.feed.post/root"},
                        "parent": {"uri": "at://did:plc:alice/app.bsky.feed.post/parent"},
                    },
                }
            ],
        },
        actor="alice.bsky.social",
    )

    entries = list(page.entries)

    assert page.cursor == "cursor-2"
    assert [entry["id"] for entry in entries] == ["at://did:plc:alice/app.bsky.feed.post/3lf4"]
    assert entries[0]["text"] == "First post"
    assert entries[0]["created_at"].tzinfo == timezone.utc
    assert entries[0]["labels"] == ["news"]
    assert entries[0]["reply_root_uri"] == "at://did:plc:alice/app.bsky.feed.post/root"
    assert entries[0]["embed_type"] == "app.bsky.embed.images#view"


def test_parse_author_feed_page_skips_reposts_from_other_authors() -> None:
    page = parse_author_feed_page(
        {
            "feed": [
                {
                    "post": {
                        "uri": "at://did:plc:bob/app.bsky.feed.post/3lf4",
                        "author": {
                            "did": "did:plc:bob",
                            "handle": "bob.bsky.social",
                        },
                        "record": {
                            "text": "A reposted post",
                            "createdAt": "2026-03-25T10:15:00Z",
                        },
                    },
                    "reason": {
                        "$type": "app.bsky.feed.defs#reasonRepost",
                        "by": {
                            "did": "did:plc:alice",
                            "handle": "alice.bsky.social",
                        },
                    },
                }
            ]
        },
        actor="alice.bsky.social",
    )

    assert list(page.entries) == []


def test_load_author_feed_page_calls_public_appview(monkeypatch: pytest.MonkeyPatch) -> None:
    response = _FakeResponse(
        {
            "cursor": "cursor-2",
            "feed": [
                {
                    "post": {
                        "uri": "at://did:plc:alice/app.bsky.feed.post/3lf4",
                        "author": {
                            "did": "did:plc:alice",
                            "handle": "alice.bsky.social",
                        },
                        "record": {"text": "Hello"},
                    }
                }
            ],
        }
    )
    captured_url: list[str] = []

    def fake_urlopen(request):
        captured_url.append(request.full_url)
        return response

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    page = load_author_feed_page(actor="alice.bsky.social", cursor="cursor-1", limit=25)

    assert captured_url == [
        "https://public.api.bsky.app/xrpc/app.bsky.feed.getAuthorFeed"
        "?actor=alice.bsky.social&limit=25&cursor=cursor-1"
    ]
    assert [entry["id"] for entry in page.entries] == ["at://did:plc:alice/app.bsky.feed.post/3lf4"]
    assert response.closed is True


def test_load_author_feed_page_validates_limit() -> None:
    with pytest.raises(ValueError, match="limit must be between 1 and 100"):
        load_author_feed_page(actor="alice.bsky.social", limit=0)


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._body = json.dumps(payload).encode("utf-8")
        self.closed = False

    def read(self) -> bytes:
        return self._body

    def close(self) -> None:
        self.closed = True
