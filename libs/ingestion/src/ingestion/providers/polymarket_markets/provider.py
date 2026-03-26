from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Any

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.polymarket_markets.checkpoint import PolymarketMarketsCheckpoint
from ingestion.providers.polymarket_markets.loader import DEFAULT_API_URL, load_markets
from ingestion.utils.time import utc_now

PolymarketMarket = Mapping[str, Any]
MarketsLoader = Callable[
    [PolymarketMarketsCheckpoint | None],
    Iterable[PolymarketMarket] | Awaitable[Iterable[PolymarketMarket]],
]


@dataclass(slots=True, frozen=True)
class PolymarketMarketsMetadata:
    api_url: str
    query_params: Mapping[str, object]


class PolymarketMarketsProvider(BatchProvider[PolymarketMarketsCheckpoint]):
    def __init__(
        self,
        *,
        api_url: str = DEFAULT_API_URL,
        page_size: int = 100,
        query_params: Mapping[str, object] | None = None,
        markets_loader: MarketsLoader | None = None,
        name: str | None = None,
    ) -> None:
        self.api_url = api_url.rstrip("/")
        self.page_size = page_size
        self.query_params = dict(query_params or {})
        self.name = name or _default_provider_name(self.query_params)
        self._metadata = PolymarketMarketsMetadata(
            api_url=self.api_url,
            query_params=self.query_params,
        )
        self._markets_loader = markets_loader or partial(
            load_markets,
            api_url=self.api_url,
            page_size=self.page_size,
            query_params=self.query_params,
        )

    async def fetch(
        self,
        *,
        checkpoint: PolymarketMarketsCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        batch_fetched_at = utc_now()
        markets = iter(await self._load_markets(checkpoint))

        try:
            for market in markets:
                market_id = str(market["id"])
                market_cursor = _market_cursor(market_id, market)

                if (
                    checkpoint is not None
                    and checkpoint.api_url == self.api_url
                    and checkpoint.cursor == market_cursor
                ):
                    break

                yield Record(
                    provider=self.name,
                    key=market_cursor,
                    payload=dict(market),
                    occurred_at=_market_occurred_at(market),
                    fetched_at=batch_fetched_at,
                    metadata={
                        "api_url": self._metadata.api_url,
                        "query_params": dict(self._metadata.query_params),
                    },
                )
        finally:
            close = getattr(markets, "close", None)
            if callable(close):
                close()

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: PolymarketMarketsCheckpoint | None,
        cursor: str | None,
    ) -> PolymarketMarketsCheckpoint | None:
        checkpoint_cursor = cursor
        if checkpoint_cursor is None and previous_checkpoint is not None:
            checkpoint_cursor = previous_checkpoint.cursor

        if checkpoint_cursor is None:
            return None

        return PolymarketMarketsCheckpoint(
            api_url=self.api_url,
            cursor=checkpoint_cursor,
        )

    async def _load_markets(
        self,
        checkpoint: PolymarketMarketsCheckpoint | None,
    ) -> Iterable[PolymarketMarket]:
        result = self._markets_loader(checkpoint)
        if inspect.isawaitable(result):
            result = await result
        return result


def _default_provider_name(query_params: Mapping[str, object]) -> str:
    if not query_params:
        return "polymarket_markets"

    query_string = "&".join(
        f"{key}={value}"
        for key, value in sorted(query_params.items())
    )
    return f"polymarket_markets?{query_string}"


def _market_cursor(market_id: str, market: Mapping[str, Any]) -> str:
    updated_at = _market_updated_at(market)
    if updated_at is None:
        return market_id
    return f"{market_id}:{updated_at.isoformat()}"


def _market_occurred_at(market: Mapping[str, Any]) -> datetime | None:
    for field_name in ("updated_at", "end_date", "start_date", "created_at"):
        value = market.get(field_name)
        if isinstance(value, datetime):
            return value
    return None


def _market_updated_at(market: Mapping[str, Any]) -> datetime | None:
    for field_name in ("updated_at", "created_at"):
        value = market.get(field_name)
        if isinstance(value, datetime):
            return value
    return None
