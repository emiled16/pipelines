from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from functools import partial
from typing import Any

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.gdelt_articles.checkpoint import GdeltArticlesCheckpoint
from ingestion.providers.gdelt_articles.loader import (
    DEFAULT_GDELT_ARTICLES_ENDPOINT,
    GdeltArticlesLoadResult,
    load_article_entries,
)
from ingestion.utils.time import utc_now

GdeltArticle = Mapping[str, Any]
ArticlesLoader = Callable[
    [GdeltArticlesCheckpoint | None],
    GdeltArticlesLoadResult
    | Iterable[GdeltArticle]
    | Awaitable[GdeltArticlesLoadResult | Iterable[GdeltArticle]],
]


@dataclass(slots=True, frozen=True)
class GdeltArticlesRequestMetadata:
    endpoint: str
    query: str
    max_records: int
    mode: str
    sort: str


class GdeltArticlesProvider(BatchProvider[GdeltArticlesCheckpoint]):
    def __init__(
        self,
        *,
        query: str,
        max_records: int = 50,
        endpoint: str = DEFAULT_GDELT_ARTICLES_ENDPOINT,
        mode: str = "ArtList",
        sort: str = "DateDesc",
        entries_loader: ArticlesLoader | None = None,
        name: str | None = None,
    ) -> None:
        self.query = query
        self.max_records = max_records
        self.endpoint = endpoint
        self.mode = mode
        self.sort = sort
        self.name = name or f"gdelt_articles:{query}"
        self._entries_loader = entries_loader or partial(
            load_article_entries,
            query,
            endpoint=endpoint,
            max_records=max_records,
            mode=mode,
            sort=sort,
        )
        self._request_metadata = GdeltArticlesRequestMetadata(
            endpoint=endpoint,
            query=query,
            max_records=max_records,
            mode=mode,
            sort=sort,
        )

    async def fetch(
        self,
        *,
        checkpoint: GdeltArticlesCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        batch_fetched_at = utc_now()
        entries = iter((await self._load_entries(checkpoint)).entries)

        try:
            for entry in entries:
                article_id = str(entry["id"])
                seen_at = _coerce_datetime(entry.get("seen_at"))

                if _has_reached_checkpoint(
                    checkpoint=checkpoint,
                    query=self.query,
                    article_id=article_id,
                    seen_at=seen_at,
                ):
                    break

                yield Record(
                    provider=self.name,
                    key=article_id,
                    payload=dict(entry),
                    occurred_at=seen_at,
                    fetched_at=batch_fetched_at,
                    metadata={
                        "endpoint": self._request_metadata.endpoint,
                        "query": self._request_metadata.query,
                        "max_records": self._request_metadata.max_records,
                        "mode": self._request_metadata.mode,
                        "sort": self._request_metadata.sort,
                    },
                )
        finally:
            close = getattr(entries, "close", None)
            if callable(close):
                close()

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: GdeltArticlesCheckpoint | None,
        last_article_id: str | None,
        last_seen_at: Any,
    ) -> GdeltArticlesCheckpoint | None:
        if last_article_id is None or last_seen_at is None:
            if previous_checkpoint is not None and previous_checkpoint.query == self.query:
                return previous_checkpoint
            return None

        checkpoint_seen_at = _coerce_datetime(last_seen_at)
        if checkpoint_seen_at is None:
            raise TypeError("last_seen_at must be a datetime when last_article_id is provided")

        return GdeltArticlesCheckpoint(
            query=self.query,
            cursor=last_article_id,
            last_seen_at=checkpoint_seen_at,
        )

    async def _load_entries(
        self,
        checkpoint: GdeltArticlesCheckpoint | None,
    ) -> GdeltArticlesLoadResult:
        result = self._entries_loader(checkpoint)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, GdeltArticlesLoadResult):
            return result
        return GdeltArticlesLoadResult(entries=result)


def _has_reached_checkpoint(
    *,
    checkpoint: GdeltArticlesCheckpoint | None,
    query: str,
    article_id: str,
    seen_at: Any,
) -> bool:
    if checkpoint is None or checkpoint.query != query:
        return False

    checkpoint_seen_at = checkpoint.last_seen_at
    article_seen_at = _coerce_datetime(seen_at)

    if article_seen_at is not None and article_seen_at < checkpoint_seen_at:
        return True

    return article_seen_at == checkpoint_seen_at and article_id == checkpoint.cursor


def _coerce_datetime(value: Any):
    from datetime import datetime

    return value if isinstance(value, datetime) else None
