from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from datetime import datetime
from functools import partial
from typing import Any

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.reddit_posts.checkpoint import RedditPostsCheckpoint
from ingestion.providers.reddit_posts.loader import RedditPostsLoadResult, load_subreddit_posts
from ingestion.utils.time import utc_now

RedditPost = Mapping[str, Any]
PostsLoadValue = RedditPostsLoadResult | Iterable[RedditPost]
PostsLoader = Callable[
    [str | None],
    PostsLoadValue | Awaitable[PostsLoadValue],
]


class RedditPostsProvider(BatchProvider[RedditPostsCheckpoint]):
    def __init__(
        self,
        *,
        subreddit: str,
        listing: str = "new",
        page_size: int = 100,
        access_token: str | None = None,
        user_agent: str | None = None,
        posts_loader: PostsLoader | None = None,
        name: str | None = None,
        max_pages: int | None = None,
    ) -> None:
        if posts_loader is None and user_agent is None:
            raise TypeError("user_agent is required when posts_loader is not provided")
        if page_size < 1 or page_size > 100:
            raise ValueError("page_size must be between 1 and 100")
        if max_pages is not None and max_pages < 1:
            raise ValueError("max_pages must be positive when provided")

        self.subreddit = subreddit
        self.listing = listing
        self.page_size = page_size
        self.name = name or f"reddit_posts:r/{subreddit}:{listing}"
        self.max_pages = max_pages
        self._posts_loader = posts_loader or partial(
            load_subreddit_posts,
            subreddit=subreddit,
            listing=listing,
            limit=page_size,
            access_token=access_token,
            user_agent=user_agent or "",
        )

    async def fetch(
        self,
        *,
        checkpoint: RedditPostsCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        batch_fetched_at = utc_now()
        pages_loaded = 0
        after: str | None = None

        while True:
            load_result = await self._load_posts(after)
            pages_loaded += 1

            posts = iter(load_result.posts)
            try:
                for post in posts:
                    post_id = str(post["id"])

                    if self._is_checkpoint_match(checkpoint, post_id):
                        return

                    yield Record(
                        provider=self.name,
                        key=post_id,
                        payload=dict(post),
                        occurred_at=_coerce_datetime(post.get("created_at")),
                        fetched_at=batch_fetched_at,
                        metadata={
                            "subreddit": self.subreddit,
                            "listing": self.listing,
                        },
                    )
            finally:
                close = getattr(posts, "close", None)
                if callable(close):
                    close()

            if load_result.after is None:
                return
            if self.max_pages is not None and pages_loaded >= self.max_pages:
                return

            after = load_result.after

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: RedditPostsCheckpoint | None,
        last_post_id: str | None,
    ) -> RedditPostsCheckpoint | None:
        checkpoint_cursor = last_post_id
        if checkpoint_cursor is None and previous_checkpoint is not None:
            checkpoint_cursor = previous_checkpoint.cursor

        if checkpoint_cursor is None:
            return None

        return RedditPostsCheckpoint(
            subreddit=self.subreddit,
            listing=self.listing,
            cursor=checkpoint_cursor,
        )

    async def _load_posts(self, after: str | None) -> RedditPostsLoadResult:
        result = self._posts_loader(after)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, RedditPostsLoadResult):
            return result
        return RedditPostsLoadResult(posts=result)

    def _is_checkpoint_match(
        self,
        checkpoint: RedditPostsCheckpoint | None,
        post_id: str,
    ) -> bool:
        return (
            checkpoint is not None
            and checkpoint.subreddit == self.subreddit
            and checkpoint.listing == self.listing
            and checkpoint.cursor == post_id
        )


def _coerce_datetime(value: Any) -> datetime | None:
    return value if isinstance(value, datetime) else None
