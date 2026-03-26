from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, BinaryIO
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_LIMIT = 100
DEFAULT_OAUTH_BASE_URL = "https://oauth.reddit.com"
DEFAULT_PUBLIC_BASE_URL = "https://www.reddit.com"
REDDIT_URL = "https://www.reddit.com"


@dataclass(slots=True, frozen=True)
class RedditPostsLoadResult:
    posts: Iterable[dict[str, object]]
    after: str | None = None


def load_subreddit_posts(
    *,
    subreddit: str,
    listing: str = "new",
    limit: int = DEFAULT_LIMIT,
    after: str | None = None,
    access_token: str | None = None,
    user_agent: str,
    timeout: float = 30.0,
) -> RedditPostsLoadResult:
    request = Request(
        _listing_url(
            subreddit=subreddit,
            listing=listing,
            limit=limit,
            after=after,
            access_token=access_token,
        ),
        headers=_request_headers(user_agent=user_agent, access_token=access_token),
    )
    response = urlopen(request, timeout=timeout)
    try:
        return parse_listing_posts(response)
    finally:
        response.close()


def parse_listing_posts(json_source: bytes | BinaryIO) -> RedditPostsLoadResult:
    if isinstance(json_source, bytes):
        stream: BinaryIO = BytesIO(json_source)
    else:
        stream = json_source

    payload = json.load(stream)
    data = _mapping(payload.get("data"))
    children = data.get("children")
    if not isinstance(children, list):
        return RedditPostsLoadResult(posts=(), after=_optional_string(data.get("after")))

    posts: list[dict[str, object]] = []
    for child in children:
        post = _normalize_child(child)
        if post is not None:
            posts.append(post)

    return RedditPostsLoadResult(posts=posts, after=_optional_string(data.get("after")))


def _listing_url(
    *,
    subreddit: str,
    listing: str,
    limit: int,
    after: str | None,
    access_token: str | None,
) -> str:
    params = {"limit": str(limit), "raw_json": "1"}
    if after is not None:
        params["after"] = after

    base_url = DEFAULT_OAUTH_BASE_URL if access_token is not None else DEFAULT_PUBLIC_BASE_URL
    return f"{base_url}/r/{subreddit}/{listing}.json?{urlencode(params)}"


def _request_headers(*, user_agent: str, access_token: str | None) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "User-Agent": user_agent,
    }
    if access_token is not None:
        headers["Authorization"] = f"Bearer {access_token}"
    return headers


def _normalize_child(child: object) -> dict[str, object] | None:
    child_mapping = _mapping(child)
    if child_mapping.get("kind") != "t3":
        return None

    post_data = _mapping(child_mapping.get("data"))
    post_id = _post_fullname(post_data)
    if post_id is None:
        return None

    permalink = _optional_string(post_data.get("permalink"))
    absolute_permalink = f"{REDDIT_URL}{permalink}" if permalink is not None else None
    outbound_url = _optional_string(
        post_data.get("url_overridden_by_dest") or post_data.get("url")
    )

    return {
        "id": post_id,
        "reddit_id": _optional_string(post_data.get("id")),
        "subreddit": _optional_string(post_data.get("subreddit")),
        "author": _optional_string(post_data.get("author")),
        "title": _optional_string(post_data.get("title")),
        "selftext": _non_empty_string(post_data.get("selftext")),
        "url": outbound_url,
        "permalink": absolute_permalink,
        "created_at": _unix_timestamp(post_data.get("created_utc")),
        "score": _optional_int(post_data.get("score")),
        "num_comments": _optional_int(post_data.get("num_comments")),
        "over_18": _optional_bool(post_data.get("over_18")),
        "spoiler": _optional_bool(post_data.get("spoiler")),
        "is_self": _optional_bool(post_data.get("is_self")),
    }


def _post_fullname(post_data: dict[str, Any]) -> str | None:
    fullname = _optional_string(post_data.get("name"))
    if fullname is not None:
        return fullname

    short_id = _optional_string(post_data.get("id"))
    if short_id is None:
        return None
    return f"t3_{short_id}"


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value != "" else None


def _non_empty_string(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _optional_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _optional_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _unix_timestamp(value: object) -> datetime | None:
    if not isinstance(value, int | float) or isinstance(value, bool):
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc)

