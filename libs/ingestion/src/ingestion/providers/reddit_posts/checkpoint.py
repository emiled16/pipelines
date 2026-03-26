from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class RedditPostsCheckpoint:
    subreddit: str
    cursor: str
    listing: str = "new"

