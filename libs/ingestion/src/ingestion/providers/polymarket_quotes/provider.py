from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from functools import partial
from hashlib import sha1
from typing import Any

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.polymarket_quotes.checkpoint import PolymarketQuotesCheckpoint
from ingestion.providers.polymarket_quotes.loader import (
    DEFAULT_CLOB_API_URL,
    MAX_BATCH_TOKENS,
    PolymarketQuotesLoadResult,
    load_order_books,
)
from ingestion.utils.time import utc_now

PolymarketBook = Mapping[str, Any]
BooksLoader = Callable[
    [tuple[str, ...]],
    PolymarketQuotesLoadResult
    | Iterable[PolymarketBook]
    | Awaitable[PolymarketQuotesLoadResult | Iterable[PolymarketBook]],
]


@dataclass(slots=True, frozen=True)
class PolymarketQuoteLevel:
    price: str
    size: str


class PolymarketQuotesProvider(BatchProvider[PolymarketQuotesCheckpoint]):
    def __init__(
        self,
        *,
        token_ids: Sequence[str],
        api_url: str = DEFAULT_CLOB_API_URL,
        books_loader: BooksLoader | None = None,
        name: str | None = None,
    ) -> None:
        self.token_ids = _normalize_token_ids(token_ids)
        self.api_url = api_url.rstrip("/")
        self.name = name or _default_name(self.token_ids)
        self._books_loader = books_loader or partial(load_order_books, api_url=self.api_url)
        self._latest_book_versions: dict[str, str] = {}

    async def fetch(
        self,
        *,
        checkpoint: PolymarketQuotesCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        load_result = await self._load_books()
        self._latest_book_versions = {}
        previous_versions = _checkpoint_versions(checkpoint, token_ids=self.token_ids)
        batch_fetched_at = utc_now()
        books = iter(load_result.books)

        try:
            for book in books:
                asset_id = _asset_id(book)
                if asset_id is None:
                    continue

                occurred_at = _parse_timestamp(book.get("timestamp"))
                book_version = _book_version(book, occurred_at=occurred_at)

                if book_version is not None:
                    self._latest_book_versions[asset_id] = book_version
                    if previous_versions.get(asset_id) == book_version:
                        continue

                yield Record(
                    provider=self.name,
                    key=_record_key(asset_id=asset_id, book_version=book_version),
                    payload=_normalize_book(book),
                    occurred_at=occurred_at,
                    fetched_at=batch_fetched_at,
                    metadata=_record_metadata(book, api_url=self.api_url),
                )
        finally:
            close = getattr(books, "close", None)
            if callable(close):
                close()

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: PolymarketQuotesCheckpoint | None = None,
    ) -> PolymarketQuotesCheckpoint | None:
        previous_versions = _checkpoint_versions(previous_checkpoint, token_ids=self.token_ids)
        next_versions = dict(previous_versions)
        next_versions.update(self._latest_book_versions)

        if not next_versions:
            if previous_checkpoint is not None and previous_checkpoint.token_ids == self.token_ids:
                return previous_checkpoint
            return None

        return PolymarketQuotesCheckpoint(
            token_ids=self.token_ids,
            book_versions=next_versions,
        )

    async def _load_books(self) -> PolymarketQuotesLoadResult:
        result = self._books_loader(self.token_ids)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, PolymarketQuotesLoadResult):
            return result
        return PolymarketQuotesLoadResult(books=result)


def _default_name(token_ids: tuple[str, ...]) -> str:
    if len(token_ids) == 1:
        return f"polymarket_quotes:{token_ids[0]}"

    token_scope = ",".join(token_ids)
    digest = sha1(token_scope.encode("utf-8")).hexdigest()[:12]
    return f"polymarket_quotes:{len(token_ids)}:{digest}"


def _normalize_token_ids(token_ids: Sequence[str]) -> tuple[str, ...]:
    normalized_token_ids: list[str] = []
    seen: set[str] = set()

    for token_id in token_ids:
        normalized_token_id = str(token_id).strip()
        if not normalized_token_id or normalized_token_id in seen:
            continue
        seen.add(normalized_token_id)
        normalized_token_ids.append(normalized_token_id)

    if not normalized_token_ids:
        raise ValueError("token_ids must include at least one token id")
    if len(normalized_token_ids) > MAX_BATCH_TOKENS:
        raise ValueError(f"token_ids cannot contain more than {MAX_BATCH_TOKENS} items")

    return tuple(normalized_token_ids)


def _checkpoint_versions(
    checkpoint: PolymarketQuotesCheckpoint | None,
    *,
    token_ids: tuple[str, ...],
) -> dict[str, str]:
    if checkpoint is None or checkpoint.token_ids != token_ids:
        return {}
    return dict(checkpoint.book_versions)


def _asset_id(book: Mapping[str, Any]) -> str | None:
    value = book.get("asset_id")
    if value is None:
        return None
    asset_id = str(value).strip()
    return asset_id or None


def _book_version(
    book: Mapping[str, Any],
    *,
    occurred_at: datetime | None,
) -> str | None:
    for key in ("hash", "timestamp"):
        value = book.get(key)
        if value is None:
            continue
        normalized_value = str(value).strip()
        if normalized_value:
            return normalized_value

    if occurred_at is not None:
        return occurred_at.isoformat()

    return None


def _record_key(*, asset_id: str, book_version: str | None) -> str:
    if book_version is None:
        return asset_id
    return f"{asset_id}:{book_version}"


def _normalize_book(book: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(book)
    best_bid = _first_level(book.get("bids"))
    best_ask = _first_level(book.get("asks"))

    normalized["asset_id"] = _asset_id(book)
    normalized["market"] = _optional_string(book.get("market"))
    normalized["hash"] = _optional_string(book.get("hash"))
    normalized["timestamp"] = _optional_string(book.get("timestamp"))
    normalized["best_bid"] = _level_payload(best_bid)
    normalized["best_ask"] = _level_payload(best_ask)
    normalized["midpoint"] = _midpoint(best_bid, best_ask)
    normalized["spread"] = _spread(best_bid, best_ask)

    return normalized


def _record_metadata(book: Mapping[str, Any], *, api_url: str) -> dict[str, Any]:
    return {
        "api_url": api_url,
        "asset_id": _asset_id(book),
        "market": _optional_string(book.get("market")),
    }


def _first_level(value: Any) -> PolymarketQuoteLevel | None:
    if not isinstance(value, Iterable) or isinstance(value, (str, bytes, bytearray, Mapping)):
        return None

    iterator = iter(value)
    try:
        level = next(iterator)
    except StopIteration:
        return None

    if not isinstance(level, Mapping):
        return None

    price = _optional_string(level.get("price"))
    size = _optional_string(level.get("size"))
    if price is None or size is None:
        return None

    return PolymarketQuoteLevel(price=price, size=size)


def _level_payload(level: PolymarketQuoteLevel | None) -> dict[str, str] | None:
    if level is None:
        return None
    return {"price": level.price, "size": level.size}


def _midpoint(
    best_bid: PolymarketQuoteLevel | None,
    best_ask: PolymarketQuoteLevel | None,
) -> str | None:
    if best_bid is None or best_ask is None:
        return None

    bid = _decimal(best_bid.price)
    ask = _decimal(best_ask.price)
    if bid is None or ask is None:
        return None

    return _format_decimal((bid + ask) / Decimal("2"))


def _spread(
    best_bid: PolymarketQuoteLevel | None,
    best_ask: PolymarketQuoteLevel | None,
) -> str | None:
    if best_bid is None or best_ask is None:
        return None

    bid = _decimal(best_bid.price)
    ask = _decimal(best_ask.price)
    if bid is None or ask is None:
        return None

    return _format_decimal(ask - bid)


def _decimal(value: str) -> Decimal | None:
    try:
        return Decimal(value)
    except (InvalidOperation, TypeError):
        return None


def _format_decimal(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)

    if isinstance(value, (int, float)):
        return _from_epoch(float(value))

    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        try:
            parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
        except ValueError:
            try:
                return _from_epoch(float(normalized))
            except ValueError:
                return None

    return None


def _from_epoch(value: float) -> datetime:
    if value > 1_000_000_000_000:
        value /= 1000
    return datetime.fromtimestamp(value, tz=UTC)
