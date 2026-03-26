from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from dataclasses import asdict
from datetime import datetime, timezone
from functools import partial
from typing import Any

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.comtrade_trade.checkpoint import ComtradeTradeCheckpoint
from ingestion.providers.comtrade_trade.loader import (
    ComtradeTradeLoadResult,
    ComtradeTradeQuery,
    load_trade_rows,
)
from ingestion.utils.serialization import to_json_compatible
from ingestion.utils.time import utc_now

ComtradeTradeRow = Mapping[str, Any]
TradeRowsLoader = Callable[
    [ComtradeTradeCheckpoint | None],
    ComtradeTradeLoadResult
    | Iterable[ComtradeTradeRow]
    | Awaitable[ComtradeTradeLoadResult | Iterable[ComtradeTradeRow]],
]


class ComtradeTradeProvider(BatchProvider[ComtradeTradeCheckpoint]):
    def __init__(
        self,
        *,
        query: ComtradeTradeQuery,
        subscription_key: str | None = None,
        rows_loader: TradeRowsLoader | None = None,
        name: str | None = None,
    ) -> None:
        self.query = query
        self.subscription_key = subscription_key
        self.query_signature = build_trade_query_signature(query)
        self.name = name or f"comtrade_trade:{self.query_signature}"
        self._rows_loader = rows_loader or partial(
            load_trade_rows,
            query,
            subscription_key=subscription_key,
        )

    async def fetch(
        self,
        *,
        checkpoint: ComtradeTradeCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        load_result = await self._load_rows(checkpoint)
        batch_fetched_at = utc_now()
        rows = iter(load_result.rows)

        try:
            for row in rows:
                record_key = build_trade_row_key(row)

                if (
                    checkpoint is not None
                    and checkpoint.query_signature == self.query_signature
                    and checkpoint.cursor == record_key
                ):
                    break

                yield Record(
                    provider=self.name,
                    key=record_key,
                    payload=dict(row),
                    occurred_at=_coerce_occurred_at(row),
                    fetched_at=batch_fetched_at,
                    metadata={
                        "period": self.query.period,
                        "reporter_code": self.query.reporter_code,
                        "partner_code": self.query.partner_code,
                        "flow_code": self.query.flow_code,
                    },
                )
        finally:
            close = getattr(rows, "close", None)
            if callable(close):
                close()

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: ComtradeTradeCheckpoint | None,
        last_record_key: str | None,
    ) -> ComtradeTradeCheckpoint | None:
        checkpoint_cursor = last_record_key
        if checkpoint_cursor is None and previous_checkpoint is not None:
            if previous_checkpoint.query_signature == self.query_signature:
                checkpoint_cursor = previous_checkpoint.cursor

        if checkpoint_cursor is None:
            return None

        return ComtradeTradeCheckpoint(
            query_signature=self.query_signature,
            cursor=checkpoint_cursor,
        )

    async def _load_rows(
        self,
        checkpoint: ComtradeTradeCheckpoint | None,
    ) -> ComtradeTradeLoadResult:
        result = self._rows_loader(checkpoint)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, ComtradeTradeLoadResult):
            return result
        return ComtradeTradeLoadResult(rows=result)


def build_trade_query_signature(query: ComtradeTradeQuery) -> str:
    payload = {
        key: value
        for key, value in asdict(query).items()
        if value is not None
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def build_trade_row_key(row: Mapping[str, Any]) -> str:
    payload = to_json_compatible(dict(row))
    canonical_payload = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()


def _coerce_occurred_at(row: Mapping[str, Any]) -> datetime | None:
    for field_name in ("refDate", "period"):
        occurred_at = _parse_trade_datetime(row.get(field_name))
        if occurred_at is not None:
            return occurred_at
    return None


def _parse_trade_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value

    if value is None:
        return None

    if isinstance(value, int):
        value = str(value)

    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None

    if len(text) == 4 and text.isdigit():
        return datetime(int(text), 1, 1, tzinfo=timezone.utc)

    if len(text) == 6 and text.isdigit():
        return datetime(int(text[:4]), int(text[4:]), 1, tzinfo=timezone.utc)

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed
