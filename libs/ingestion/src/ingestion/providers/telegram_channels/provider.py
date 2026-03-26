from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from datetime import datetime
from functools import partial
from typing import Any

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.telegram_channels.checkpoint import TelegramChannelsCheckpoint
from ingestion.providers.telegram_channels.loader import (
    TelegramChannelLoadResult,
    build_channel_url,
    load_channel_messages,
    normalize_channel_name,
)
from ingestion.utils.time import utc_now

TelegramEntry = Mapping[str, Any]
EntriesLoader = Callable[
    [TelegramChannelsCheckpoint | None],
    TelegramChannelLoadResult
    | Iterable[TelegramEntry]
    | Awaitable[TelegramChannelLoadResult | Iterable[TelegramEntry]],
]

class TelegramChannelsProvider(BatchProvider[TelegramChannelsCheckpoint]):
    def __init__(
        self,
        *,
        channel_name: str,
        entries_loader: EntriesLoader | None = None,
        name: str | None = None,
    ) -> None:
        normalized_channel_name = normalize_channel_name(channel_name)
        if normalized_channel_name is None:
            raise ValueError("channel_name must not be empty")

        self.channel_name = normalized_channel_name
        self.name = name or f"telegram_channels:{self.channel_name}"
        self._entries_loader = entries_loader or partial(load_channel_messages, self.channel_name)

    async def fetch(
        self,
        *,
        checkpoint: TelegramChannelsCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        batch_fetched_at = utc_now()
        entries = iter(await self._load_entries(checkpoint))
        try:
            for entry in entries:
                entry_id = str(entry["id"])

                if (
                    checkpoint is not None
                    and checkpoint.channel_name == self.channel_name
                    and checkpoint.last_entry_id == entry_id
                ):
                    break

                yield Record(
                    provider=self.name,
                    key=entry_id,
                    payload=dict(entry),
                    occurred_at=_coerce_datetime(entry.get("published_at")),
                    fetched_at=batch_fetched_at,
                    metadata={
                        "channel_name": self.channel_name,
                        "channel_url": build_channel_url(self.channel_name),
                    },
                )
        finally:
            close = getattr(entries, "close", None)
            if callable(close):
                close()

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: TelegramChannelsCheckpoint | None,
        last_entry_id: str | None,
    ) -> TelegramChannelsCheckpoint | None:
        checkpoint_entry_id = last_entry_id
        if checkpoint_entry_id is None and previous_checkpoint is not None:
            checkpoint_entry_id = previous_checkpoint.last_entry_id

        if checkpoint_entry_id is None:
            return None

        return TelegramChannelsCheckpoint(
            channel_name=self.channel_name,
            last_entry_id=checkpoint_entry_id,
        )

    async def _load_entries(
        self,
        checkpoint: TelegramChannelsCheckpoint | None,
    ) -> Iterable[TelegramEntry]:
        result = self._entries_loader(checkpoint)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, TelegramChannelLoadResult):
            return result.entries
        return result


def _coerce_datetime(value: Any) -> datetime | None:
    return value if isinstance(value, datetime) else None
