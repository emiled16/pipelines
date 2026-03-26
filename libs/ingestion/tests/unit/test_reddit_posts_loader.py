from __future__ import annotations

from datetime import timezone
from io import BytesIO

from ingestion.providers.reddit_posts import loader
from ingestion.providers.reddit_posts.loader import load_subreddit_posts, parse_listing_posts


def test_parse_listing_posts_extracts_common_post_fields() -> None:
    json_bytes = b"""
    {
      "data": {
        "after": "t3_def456",
        "children": [
          {
            "kind": "t3",
            "data": {
              "name": "t3_abc123",
              "id": "abc123",
              "subreddit": "python",
              "author": "guido",
              "title": "First post",
              "selftext": "Hello world",
              "url": "https://example.com/posts/1",
              "permalink": "/r/python/comments/abc123/first_post/",
              "created_utc": 1774347300,
              "score": 42,
              "num_comments": 7,
              "over_18": false,
              "spoiler": false,
              "is_self": true
            }
          }
        ]
      }
    }
    """

    result = parse_listing_posts(json_bytes)
    posts = list(result.posts)

    assert result.after == "t3_def456"
    assert len(posts) == 1
    assert posts[0]["id"] == "t3_abc123"
    assert posts[0]["reddit_id"] == "abc123"
    assert posts[0]["permalink"] == "https://www.reddit.com/r/python/comments/abc123/first_post/"
    assert posts[0]["created_at"].tzinfo == timezone.utc


def test_parse_listing_posts_skips_non_post_children() -> None:
    json_bytes = b"""
    {
      "data": {
        "children": [
          {"kind": "more", "data": {"id": "ignore-me"}},
          {"kind": "t3", "data": {"id": "abc123"}}
        ]
      }
    }
    """

    result = parse_listing_posts(json_bytes)

    assert [post["id"] for post in result.posts] == ["t3_abc123"]


def test_load_subreddit_posts_sets_expected_headers_and_query_params(monkeypatch) -> None:
    json_bytes = b'{"data": {"after": null, "children": []}}'
    observed: dict[str, object] = {}
    response = _FakeResponse(json_bytes)

    def fake_urlopen(request, timeout: float = 0.0):
        observed["url"] = request.full_url
        observed["headers"] = dict(request.header_items())
        observed["timeout"] = timeout
        return response

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    load_subreddit_posts(
        subreddit="python",
        listing="new",
        limit=50,
        after="t3_cursor",
        access_token="token-123",
        user_agent="ingestion-tests/1.0",
        timeout=12.5,
    )

    assert observed["url"] == (
        "https://oauth.reddit.com/r/python/new.json?limit=50&raw_json=1&after=t3_cursor"
    )
    assert observed["headers"] == {
        "Accept": "application/json",
        "Authorization": "Bearer token-123",
        "User-agent": "ingestion-tests/1.0",
    }
    assert observed["timeout"] == 12.5
    assert response.closed is True


class _FakeResponse:
    def __init__(self, body: bytes) -> None:
        self._stream = BytesIO(body)
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)

    def close(self) -> None:
        self.closed = True

