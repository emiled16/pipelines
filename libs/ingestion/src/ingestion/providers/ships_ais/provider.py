from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Mapping
from datetime import datetime
from typing import Any

from ingestion.abstractions.provider import StreamingProvider
from ingestion.models.record import Record
from ingestion.providers.ships_ais.checkpoint import ShipsAisCheckpoint
from ingestion.providers.ships_ais.loader import ShipsAisSubscription, open_ais_stream
from ingestion.utils.serialization import to_json_compatible

ShipsAisMessage = Mapping[str, Any]
MessageSource = Callable[
    [ShipsAisSubscription],
    AsyncIterable[ShipsAisMessage] | Awaitable[AsyncIterable[ShipsAisMessage]],
]


class ShipsAisProvider(StreamingProvider[ShipsAisCheckpoint]):
    def __init__(
        self,
        *,
        subscription: ShipsAisSubscription,
        message_source: MessageSource | None = None,
        name: str | None = None,
    ) -> None:
        self.subscription = subscription
        self.name = name or "ships_ais"
        self._message_source = message_source or open_ais_stream

    async def stream(
        self,
        *,
        checkpoint: ShipsAisCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        message_stream = await self._open_stream()
        async for message in message_stream:
            record = _record_from_message(
                provider_name=self.name,
                subscription=self.subscription,
                message=message,
            )
            if record is None or _is_checkpointed(record, checkpoint, provider_name=self.name):
                continue
            yield record

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: ShipsAisCheckpoint | None,
        last_record: Record | None,
    ) -> ShipsAisCheckpoint | None:
        if last_record is None:
            return previous_checkpoint
        if last_record.key is None:
            return previous_checkpoint

        return ShipsAisCheckpoint(
            stream_name=self.name,
            last_record_key=last_record.key,
            last_occurred_at=last_record.occurred_at
            if last_record.occurred_at is not None
            else (
                previous_checkpoint.last_occurred_at if previous_checkpoint is not None else None
            ),
        )

    async def _open_stream(self) -> AsyncIterable[ShipsAisMessage]:
        stream = self._message_source(self.subscription)
        if inspect.isawaitable(stream):
            stream = await stream
        return stream


def _record_from_message(
    *,
    provider_name: str,
    subscription: ShipsAisSubscription,
    message: Mapping[str, Any],
) -> Record | None:
    message_type = message.get("message_type")
    if not isinstance(message_type, str):
        return None

    payload = dict(message)
    occurred_at = _coerce_datetime(message.get("occurred_at"))
    record_key = _build_record_key(message_type=message_type, message=payload)
    metadata = {
        "stream_url": subscription.url,
        "message_type": message_type,
    }

    return Record(
        provider=provider_name,
        key=record_key,
        payload=payload,
        occurred_at=occurred_at,
        metadata=metadata,
    )


def _is_checkpointed(
    record: Record,
    checkpoint: ShipsAisCheckpoint | None,
    *,
    provider_name: str,
) -> bool:
    if checkpoint is None or checkpoint.stream_name != provider_name:
        return False

    if checkpoint.last_occurred_at is not None and record.occurred_at is not None:
        if record.occurred_at < checkpoint.last_occurred_at:
            return True
        if record.occurred_at > checkpoint.last_occurred_at:
            return False

    return record.key == checkpoint.last_record_key


def _build_record_key(*, message_type: str, message: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(
        json.dumps(
            to_json_compatible(message),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:16]
    mmsi = message.get("mmsi")
    occurred_at = _coerce_datetime(message.get("occurred_at"))
    key_parts = [message_type]
    if mmsi is not None:
        key_parts.append(str(mmsi))
    if occurred_at is not None:
        key_parts.append(occurred_at.isoformat())
    key_parts.append(digest)
    return ":".join(key_parts)


def _coerce_datetime(value: Any) -> datetime | None:
    return value if isinstance(value, datetime) else None
