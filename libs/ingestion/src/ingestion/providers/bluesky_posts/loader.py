from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BLUESKY_PUBLIC_API_BASE_URL = "https://public.api.bsky.app"


@dataclass(slots=True, frozen=True)
class BlueskyFeedPage:
    entries: Iterable[dict[str, object]]
    cursor: str | None = None


def load_author_feed_page(
    *,
    actor: str,
    cursor: str | None = None,
    limit: int = 100,
    api_base_url: str = BLUESKY_PUBLIC_API_BASE_URL,
) -> BlueskyFeedPage:
    _validate_limit(limit)
    request = Request(
        _author_feed_url(
            actor=actor,
            cursor=cursor,
            limit=limit,
            api_base_url=api_base_url,
        ),
        headers={"Accept": "application/json"},
    )
    response = urlopen(request)
    try:
        payload = json.loads(response.read())
    finally:
        response.close()
    if not isinstance(payload, Mapping):
        raise ValueError("Bluesky author feed response must be a JSON object")
    return parse_author_feed_page(payload, actor=actor)


def parse_author_feed_page(
    payload: Mapping[str, object],
    *,
    actor: str,
) -> BlueskyFeedPage:
    entries = list(_iter_feed_entries(payload.get("feed"), actor=actor))
    return BlueskyFeedPage(
        entries=entries,
        cursor=_as_str(payload.get("cursor")),
    )


def _author_feed_url(
    *,
    actor: str,
    cursor: str | None,
    limit: int,
    api_base_url: str,
) -> str:
    params = {"actor": actor, "limit": str(limit)}
    if cursor is not None:
        params["cursor"] = cursor
    return f"{api_base_url.rstrip('/')}/xrpc/app.bsky.feed.getAuthorFeed?{urlencode(params)}"


def _iter_feed_entries(
    value: object,
    *,
    actor: str,
) -> Iterable[dict[str, object]]:
    if not isinstance(value, Sequence):
        return ()

    actor_identifier = _normalize_actor_identifier(actor)
    entries: list[dict[str, object]] = []

    for item in value:
        if not isinstance(item, Mapping):
            continue

        post = _as_mapping(item.get("post"))
        if post is None:
            continue

        author = _as_mapping(post.get("author"))
        if author is None or not _is_authored_post(actor_identifier, author):
            continue

        uri = _as_str(post.get("uri"))
        if uri is None:
            continue

        record = _as_mapping(post.get("record")) or {}
        embed = _as_mapping(post.get("embed"))
        reply = _as_mapping(item.get("reply"))
        entries.append(
            {
                "id": uri,
                "uri": uri,
                "cid": _as_str(post.get("cid")),
                "author_did": _as_str(author.get("did")),
                "author_handle": _as_str(author.get("handle")),
                "author_display_name": _as_str(author.get("displayName")),
                "text": _as_str(record.get("text")),
                "created_at": _parse_datetime(_as_str(record.get("createdAt"))),
                "indexed_at": _parse_datetime(_as_str(post.get("indexedAt"))),
                "reply_count": _as_int(post.get("replyCount")),
                "repost_count": _as_int(post.get("repostCount")),
                "like_count": _as_int(post.get("likeCount")),
                "quote_count": _as_int(post.get("quoteCount")),
                "langs": _as_string_list(record.get("langs")),
                "labels": _extract_labels(post.get("labels")),
                "embed_type": _as_str(embed.get("$type")) if embed is not None else None,
                "reply_root_uri": _extract_reply_ref(reply, "root"),
                "reply_parent_uri": _extract_reply_ref(reply, "parent"),
                "author": dict(author),
                "record": dict(record),
                "embed": dict(embed) if embed is not None else None,
            }
        )

    return entries


def _is_authored_post(actor_identifier: str, author: Mapping[str, object]) -> bool:
    for key in ("did", "handle"):
        value = _as_str(author.get(key))
        if value is None:
            continue
        if _normalize_actor_identifier(value) == actor_identifier:
            return True
    return False


def _extract_reply_ref(reply: Mapping[str, object] | None, key: str) -> str | None:
    if reply is None:
        return None
    ref = _as_mapping(reply.get(key))
    if ref is None:
        return None
    return _as_str(ref.get("uri"))


def _extract_labels(value: object) -> list[str]:
    if not isinstance(value, Sequence):
        return []
    labels: list[str] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        label = _as_str(item.get("val"))
        if label is not None:
            labels.append(label)
    return labels


def _as_mapping(value: object) -> Mapping[str, object] | None:
    return value if isinstance(value, Mapping) else None


def _as_int(value: object) -> int | None:
    return value if isinstance(value, int) else None


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _as_string_list(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, str):
        return []
    strings: list[str] = []
    for item in value:
        if isinstance(item, str):
            strings.append(item)
    return strings


def _normalize_actor_identifier(value: str) -> str:
    return value.lstrip("@").casefold()


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _validate_limit(limit: int) -> None:
    if limit < 1 or limit > 100:
        raise ValueError("limit must be between 1 and 100")
