from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime, timezone

from ingestion.providers.reddit_posts.checkpoint import RedditPostsCheckpoint
from ingestion.providers.reddit_posts.provider import RedditPostsProvider


def test_reddit_posts_provider_yields_posts_across_pages_until_checkpoint() -> None:
    async def run() -> None:
        provider = RedditPostsProvider(
            subreddit="python",
            user_agent="ingestion-tests/1.0",
            posts_loader=lambda after: _page(after),
        )
        checkpoint = RedditPostsCheckpoint(
            subreddit="python",
            listing="new",
            cursor="t3_2",
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["t3_4", "t3_3"]
        assert all(record.metadata["subreddit"] == "python" for record in records)

    asyncio.run(run())


def test_reddit_posts_provider_assigns_one_fetched_at_per_batch() -> None:
    async def run() -> None:
        provider = RedditPostsProvider(
            subreddit="python",
            user_agent="ingestion-tests/1.0",
            posts_loader=lambda after: _load_result(
                posts=[
                    {"id": "t3_2", "title": "Second"},
                    {"id": "t3_1", "title": "First"},
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert len({record.fetched_at for record in records}) == 1

    asyncio.run(run())


def test_reddit_posts_provider_builds_checkpoint_from_newest_post() -> None:
    provider = RedditPostsProvider(
        subreddit="python",
        user_agent="ingestion-tests/1.0",
        posts_loader=lambda after: _load_result(posts=[]),
    )

    checkpoint = provider.build_checkpoint(
        previous_checkpoint=None,
        last_post_id="t3_9",
    )

    assert checkpoint == RedditPostsCheckpoint(
        subreddit="python",
        listing="new",
        cursor="t3_9",
    )


def test_reddit_posts_provider_preserves_cursor_when_no_new_posts() -> None:
    provider = RedditPostsProvider(
        subreddit="python",
        user_agent="ingestion-tests/1.0",
        posts_loader=lambda after: _load_result(posts=[]),
    )
    previous_checkpoint = RedditPostsCheckpoint(
        subreddit="python",
        listing="new",
        cursor="t3_4",
    )

    checkpoint = provider.build_checkpoint(
        previous_checkpoint=previous_checkpoint,
        last_post_id=None,
    )

    assert checkpoint == previous_checkpoint


def test_reddit_posts_provider_closes_posts_iterator_when_checkpoint_stops_iteration() -> None:
    async def run() -> None:
        posts = _ClosablePosts(
            [
                {"id": "t3_4", "title": "Newest"},
                {"id": "t3_3", "title": "Middle"},
                {"id": "t3_2", "title": "Checkpoint"},
            ]
        )
        provider = RedditPostsProvider(
            subreddit="python",
            user_agent="ingestion-tests/1.0",
            posts_loader=lambda after: _load_result(posts=posts),
        )
        checkpoint = RedditPostsCheckpoint(
            subreddit="python",
            listing="new",
            cursor="t3_2",
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["t3_4", "t3_3"]
        assert posts.closed is True

    asyncio.run(run())


def _page(after: str | None):
    if after is None:
        return _load_result(
            posts=[
                {
                    "id": "t3_4",
                    "title": "Newest",
                    "created_at": datetime(2026, 3, 25, tzinfo=timezone.utc),
                },
                {
                    "id": "t3_3",
                    "title": "Newer",
                    "created_at": datetime(2026, 3, 24, tzinfo=timezone.utc),
                },
            ],
            after="t3_3",
        )

    return _load_result(
        posts=[
            {
                "id": "t3_2",
                "title": "Checkpoint",
                "created_at": datetime(2026, 3, 23, tzinfo=timezone.utc),
            },
            {
                "id": "t3_1",
                "title": "Older",
                "created_at": datetime(2026, 3, 22, tzinfo=timezone.utc),
            },
        ],
        after=None,
    )


def _load_result(
    *,
    posts: Iterable[dict[str, object]],
    after: str | None = None,
):
    from ingestion.providers.reddit_posts.loader import RedditPostsLoadResult

    return RedditPostsLoadResult(posts=_posts(posts), after=after)


def _posts(items: Iterable[dict[str, object]]) -> Iterable[dict[str, object]]:
    for item in items:
        yield item


class _ClosablePosts:
    def __init__(self, items: list[dict[str, object]]) -> None:
        self._items = iter(items)
        self.closed = False

    def __iter__(self) -> _ClosablePosts:
        return self

    def __next__(self) -> dict[str, object]:
        return next(self._items)

    def close(self) -> None:
        self.closed = True
