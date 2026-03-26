from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from ingestion.providers.bluesky_posts.checkpoint import BlueskyPostsCheckpoint
from ingestion.providers.bluesky_posts.loader import BlueskyFeedPage
from ingestion.providers.bluesky_posts.provider import BlueskyPostsProvider


def test_bluesky_posts_provider_yields_all_posts_without_checkpoint() -> None:
    async def run() -> None:
        provider = BlueskyPostsProvider(
            actor="alice.bsky.social",
            page_loader=_page_loader(
                {
                    None: BlueskyFeedPage(
                        entries=[
                            {
                                "id": "at://did:plc:alice/app.bsky.feed.post/3",
                                "text": "Newest",
                                "created_at": datetime(2026, 3, 25, tzinfo=timezone.utc),
                            },
                            {
                                "id": "at://did:plc:alice/app.bsky.feed.post/2",
                                "text": "Middle",
                                "created_at": datetime(2026, 3, 24, tzinfo=timezone.utc),
                            },
                        ],
                        cursor=None,
                    )
                }
            ),
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == [
            "at://did:plc:alice/app.bsky.feed.post/3",
            "at://did:plc:alice/app.bsky.feed.post/2",
        ]
        assert records[0].metadata["actor"] == "alice.bsky.social"

    asyncio.run(run())


def test_bluesky_posts_provider_paginates_until_checkpoint_entry_is_reached() -> None:
    async def run() -> None:
        provider = BlueskyPostsProvider(
            actor="alice.bsky.social",
            page_loader=_page_loader(
                {
                    None: BlueskyFeedPage(
                        entries=[
                            {"id": "at://did:plc:alice/app.bsky.feed.post/4", "text": "Newest"},
                            {"id": "at://did:plc:alice/app.bsky.feed.post/3", "text": "New"},
                        ],
                        cursor="cursor-2",
                    ),
                    "cursor-2": BlueskyFeedPage(
                        entries=[
                            {"id": "at://did:plc:alice/app.bsky.feed.post/2", "text": "Seen"},
                            {"id": "at://did:plc:alice/app.bsky.feed.post/1", "text": "Old"},
                        ],
                        cursor="cursor-3",
                    ),
                }
            ),
        )
        checkpoint = BlueskyPostsCheckpoint(
            actor="alice.bsky.social",
            last_entry_id="at://did:plc:alice/app.bsky.feed.post/2",
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == [
            "at://did:plc:alice/app.bsky.feed.post/4",
            "at://did:plc:alice/app.bsky.feed.post/3",
        ]

    asyncio.run(run())


def test_bluesky_posts_provider_assigns_one_fetched_at_per_batch() -> None:
    async def run() -> None:
        provider = BlueskyPostsProvider(
            actor="alice.bsky.social",
            page_loader=_page_loader(
                {
                    None: BlueskyFeedPage(
                        entries=[
                            {"id": "at://did:plc:alice/app.bsky.feed.post/2", "text": "One"},
                            {"id": "at://did:plc:alice/app.bsky.feed.post/1", "text": "Two"},
                        ]
                    )
                }
            ),
        )

        records = [record async for record in provider.fetch()]

        assert len({record.fetched_at for record in records}) == 1

    asyncio.run(run())


def test_bluesky_posts_provider_builds_checkpoint_from_newest_record() -> None:
    provider = BlueskyPostsProvider(actor="alice.bsky.social")

    checkpoint = provider.build_checkpoint(
        previous_checkpoint=None,
        last_entry_id="at://did:plc:alice/app.bsky.feed.post/3",
    )

    assert checkpoint == BlueskyPostsCheckpoint(
        actor="alice.bsky.social",
        last_entry_id="at://did:plc:alice/app.bsky.feed.post/3",
    )


def test_bluesky_posts_provider_preserves_previous_checkpoint_when_no_new_records() -> None:
    provider = BlueskyPostsProvider(actor="alice.bsky.social")
    previous_checkpoint = BlueskyPostsCheckpoint(
        actor="alice.bsky.social",
        last_entry_id="at://did:plc:alice/app.bsky.feed.post/3",
    )

    checkpoint = provider.build_checkpoint(
        previous_checkpoint=previous_checkpoint,
        last_entry_id=None,
    )

    assert checkpoint == previous_checkpoint


def test_bluesky_posts_provider_validates_page_size() -> None:
    with pytest.raises(ValueError, match="page_size must be between 1 and 100"):
        BlueskyPostsProvider(actor="alice.bsky.social", page_size=0)


def _page_loader(pages: dict[str | None, BlueskyFeedPage]):
    async def load(cursor: str | None) -> BlueskyFeedPage:
        try:
            return pages[cursor]
        except KeyError as exc:
            raise AssertionError(f"unexpected cursor: {cursor}") from exc

    return load
