from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from datetime import datetime, timezone
from functools import partial
from typing import Any

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.usaspending_awards.checkpoint import USAspendingAwardsCheckpoint
from ingestion.providers.usaspending_awards.loader import (
    DEFAULT_API_BASE_URL,
    DEFAULT_SEARCH_PATH,
    USAspendingAwardsLoadResult,
    load_awards_page,
)
from ingestion.utils.serialization import to_json_compatible
from ingestion.utils.time import utc_now

Award = Mapping[str, Any]
AwardPageLoader = Callable[
    [int, USAspendingAwardsCheckpoint | None],
    USAspendingAwardsLoadResult | Awaitable[USAspendingAwardsLoadResult],
]

_AWARD_KEY_FIELDS = (
    "generated_internal_id",
    "internal_id",
    "generated_unique_award_id",
    "award_id",
    "Award ID",
    "id",
)
_AWARD_TIMESTAMP_FIELDS = (
    "Last Modified Date",
    "last_modified_date",
    "last_modified_at",
    "updated_at",
    "date_updated",
    "Update Date",
    "Start Date",
    "start_date",
)


class USAspendingAwardsProvider(BatchProvider[USAspendingAwardsCheckpoint]):
    def __init__(
        self,
        *,
        search_body: Mapping[str, Any] | None = None,
        page_size: int = 100,
        page_loader: AwardPageLoader | None = None,
        name: str | None = None,
        api_base_url: str = DEFAULT_API_BASE_URL,
        search_path: str = DEFAULT_SEARCH_PATH,
    ) -> None:
        if page_size < 1:
            raise ValueError("page_size must be >= 1")

        self.search_body = dict(search_body or {})
        self.page_size = page_size
        self.api_base_url = api_base_url
        self.search_path = search_path
        self.query_hash = _query_hash(
            search_body=self.search_body,
            api_base_url=self.api_base_url,
            search_path=self.search_path,
            page_size=self.page_size,
        )
        self.name = name or f"usaspending_awards:{self.query_hash}"
        self._page_loader = page_loader or partial(
            load_awards_page,
            search_body=self.search_body,
            api_base_url=self.api_base_url,
            search_path=self.search_path,
            limit=self.page_size,
        )
        self._latest_award_modified_at: datetime | None = None

    async def fetch(
        self,
        *,
        checkpoint: USAspendingAwardsCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        self._latest_award_modified_at = None
        batch_fetched_at = utc_now()
        page = 1

        while True:
            load_result = await self._load_awards_page(page, checkpoint)
            awards = iter(load_result.awards)
            should_stop = False

            try:
                for award in awards:
                    award_key = _award_key(award)
                    if (
                        checkpoint is not None
                        and checkpoint.query_hash == self.query_hash
                        and checkpoint.cursor == award_key
                    ):
                        should_stop = True
                        break

                    award_occurred_at = _award_occurred_at(award)
                    if self._latest_award_modified_at is None:
                        self._latest_award_modified_at = award_occurred_at

                    yield Record(
                        provider=self.name,
                        key=award_key,
                        payload=dict(award),
                        occurred_at=award_occurred_at,
                        fetched_at=batch_fetched_at,
                        metadata={
                            "api_base_url": self.api_base_url,
                            "page": page,
                            "search_path": self.search_path,
                        },
                    )
            finally:
                close = getattr(awards, "close", None)
                if callable(close):
                    close()

            if should_stop or not load_result.has_next_page:
                break
            page += 1

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: USAspendingAwardsCheckpoint | None,
        last_entry_id: str | None,
    ) -> USAspendingAwardsCheckpoint | None:
        cursor = last_entry_id
        if (
            cursor is None
            and previous_checkpoint is not None
            and previous_checkpoint.query_hash == self.query_hash
        ):
            cursor = previous_checkpoint.cursor

        if cursor is None:
            return None

        last_modified_at = self._latest_award_modified_at
        if (
            last_modified_at is None
            and previous_checkpoint is not None
            and previous_checkpoint.query_hash == self.query_hash
        ):
            last_modified_at = previous_checkpoint.last_modified_at

        return USAspendingAwardsCheckpoint(
            query_hash=self.query_hash,
            cursor=cursor,
            last_modified_at=last_modified_at,
        )

    async def _load_awards_page(
        self,
        page: int,
        checkpoint: USAspendingAwardsCheckpoint | None,
    ) -> USAspendingAwardsLoadResult:
        result = self._page_loader(page, checkpoint)
        if inspect.isawaitable(result):
            result = await result
        return result


def _query_hash(
    *,
    search_body: Mapping[str, Any],
    api_base_url: str,
    search_path: str,
    page_size: int,
) -> str:
    payload = {
        "api_base_url": api_base_url,
        "page_size": page_size,
        "search_body": to_json_compatible(dict(search_body)),
        "search_path": search_path,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return digest[:12]


def _award_key(award: Award) -> str:
    for field in _AWARD_KEY_FIELDS:
        value = award.get(field)
        if value not in (None, ""):
            return str(value)
    raise KeyError("USAspending award payload is missing a stable identifier")


def _award_occurred_at(award: Award) -> datetime | None:
    for field in _AWARD_TIMESTAMP_FIELDS:
        value = award.get(field)
        occurred_at = _coerce_datetime(value)
        if occurred_at is not None:
            return occurred_at
    return None


def _coerce_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)

    if not isinstance(value, str):
        return None

    normalized = value.strip()
    if not normalized:
        return None

    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        for date_format in ("%Y-%m-%d", "%m/%d/%Y"):
            try:
                parsed = datetime.strptime(normalized, date_format)
                break
            except ValueError:
                continue
        else:
            return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed
