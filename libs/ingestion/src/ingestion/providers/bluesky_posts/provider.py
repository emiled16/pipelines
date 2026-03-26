from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.bluesky_posts.checkpoint import BlueskyPostsCheckpoint
from ingestion.providers.bluesky_posts.loader import (
    BLUESKY_PUBLIC_API_BASE_URL,
    BlueskyFeedPage,
    load_author_feed_page,
)
from ingestion.utils.time import utc_now

PageLoader = Callable[
    [str | None],
    BlueskyFeedPage | Awaitable[BlueskyFeedPage],
]


@dataclass(slots=True, frozen=True)
class BlueskyProviderMetadata:
    api_base_url: str
    page_size: int


class BlueskyPostsProvider(BatchProvider[BlueskyPostsCheckpoint]):
    def __init__(
        self,
        *,
        actor: str,
        page_size: int = 100,
        page_loader: PageLoader | None = None,
        api_base_url: str = BLUESKY_PUBLIC_API_BASE_URL,
        name: str | None = None,
    ) -> None:
        if page_size < 1 or page_size > 100:
            raise ValueError("page_size must be between 1 and 100")

        self.actor = actor
        self.name = name or f"bluesky_posts:{actor}"
        self._metadata = BlueskyProviderMetadata(api_base_url=api_base_url, page_size=page_size)
        self._page_loader = page_loader or (
            lambda cursor: load_author_feed_page(
                actor=actor,
                cursor=cursor,
                limit=page_size,
                api_base_url=api_base_url,
            )
        )

    async def fetch(
        self,
        *,
        checkpoint: BlueskyPostsCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        batch_fetched_at = utc_now()
        cursor: str | None = None

        while True:
            page = await self._load_page(cursor)
            matched_checkpoint = False

            for entry in page.entries:
                entry_id = str(entry["id"])
                if (
                    checkpoint is not None
                    and checkpoint.actor == self.actor
                    and checkpoint.last_entry_id == entry_id
                ):
                    matched_checkpoint = True
                    break

                occurred_at = entry.get("created_at") or entry.get("indexed_at")
                yield Record(
                    provider=self.name,
                    key=entry_id,
                    payload=dict(entry),
                    occurred_at=_coerce_datetime(occurred_at),
                    fetched_at=batch_fetched_at,
                    metadata={
                        "actor": self.actor,
                        "api_base_url": self._metadata.api_base_url,
                    },
                )

            if matched_checkpoint or page.cursor is None:
                return

            cursor = page.cursor

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: BlueskyPostsCheckpoint | None,
        last_entry_id: str | None,
    ) -> BlueskyPostsCheckpoint | None:
        checkpoint_entry_id = last_entry_id
        if checkpoint_entry_id is None and previous_checkpoint is not None:
            checkpoint_entry_id = previous_checkpoint.last_entry_id

        if checkpoint_entry_id is None:
            return None

        return BlueskyPostsCheckpoint(
            actor=self.actor,
            last_entry_id=checkpoint_entry_id,
        )

    async def _load_page(self, cursor: str | None) -> BlueskyFeedPage:
        result = self._page_loader(cursor)
        if inspect.isawaitable(result):
            result = await result
        return result


def _coerce_datetime(value: object) -> datetime | None:
    return value if isinstance(value, datetime) else None
