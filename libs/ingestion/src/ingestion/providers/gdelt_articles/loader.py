from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, BinaryIO, Mapping
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ingestion.providers.gdelt_articles.checkpoint import GdeltArticlesCheckpoint

DEFAULT_GDELT_ARTICLES_ENDPOINT = "https://api.gdeltproject.org/api/v2/doc/doc"


@dataclass(slots=True, frozen=True)
class GdeltArticlesLoadResult:
    entries: Iterable[dict[str, object]]


def load_article_entries(
    query: str,
    checkpoint: GdeltArticlesCheckpoint | None = None,
    *,
    endpoint: str = DEFAULT_GDELT_ARTICLES_ENDPOINT,
    max_records: int = 50,
    mode: str = "ArtList",
    sort: str = "DateDesc",
    timeout: float = 30.0,
) -> GdeltArticlesLoadResult:
    request = Request(
        _build_request_url(
            endpoint=endpoint,
            query=query,
            max_records=max_records,
            mode=mode,
            sort=sort,
        ),
        headers={
            "Accept": "application/json",
            "User-Agent": "ingestion/0.1",
        },
    )

    with urlopen(request, timeout=timeout) as response:
        return GdeltArticlesLoadResult(entries=tuple(parse_article_entries(response)))


def parse_article_entries(json_source: bytes | BinaryIO | str) -> Iterator[dict[str, object]]:
    payload = _load_json_payload(json_source)
    articles = payload.get("articles")
    if not isinstance(articles, list):
        return

    for article in articles:
        if not isinstance(article, Mapping):
            continue

        normalized_article = dict(article)
        url = _coerce_optional_str(normalized_article.get("url"))
        title = _coerce_optional_str(normalized_article.get("title"))
        seen_at = _parse_seen_at(_coerce_optional_str(normalized_article.get("seendate")))
        entry_id = url or title
        if entry_id is None:
            continue

        normalized_article["id"] = entry_id
        normalized_article["url"] = url
        normalized_article["title"] = title
        normalized_article["seen_at"] = seen_at
        yield normalized_article


def _build_request_url(
    *,
    endpoint: str,
    query: str,
    max_records: int,
    mode: str,
    sort: str,
) -> str:
    query_string = urlencode(
        {
            "query": query,
            "mode": mode,
            "format": "json",
            "maxrecords": max_records,
            "sort": sort,
        }
    )
    return f"{endpoint}?{query_string}"


def _load_json_payload(json_source: bytes | BinaryIO | str) -> dict[str, Any]:
    if isinstance(json_source, bytes):
        data = json_source.decode("utf-8")
        payload = json.loads(data)
    elif isinstance(json_source, str):
        payload = json.loads(json_source)
    else:
        payload = json.load(json_source)

    if isinstance(payload, dict):
        return payload
    return {}


def _coerce_optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_seen_at(value: str | None) -> datetime | None:
    if value is None:
        return None

    for fmt in ("%Y%m%dT%H%M%SZ", "%Y%m%d%H%M%S"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
