from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import BinaryIO
from urllib.request import Request, urlopen

DEFAULT_CLOB_API_URL = "https://clob.polymarket.com"
MAX_BATCH_TOKENS = 500


@dataclass(slots=True, frozen=True)
class PolymarketQuotesLoadResult:
    books: Iterable[dict[str, object]]


def load_order_books(
    token_ids: Sequence[str],
    *,
    api_url: str = DEFAULT_CLOB_API_URL,
) -> PolymarketQuotesLoadResult:
    normalized_token_ids = _normalize_token_ids(token_ids)
    request = Request(
        f"{api_url.rstrip('/')}/books",
        data=json.dumps([{"token_id": token_id} for token_id in normalized_token_ids]).encode("utf-8"),
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    response = urlopen(request)
    try:
        return PolymarketQuotesLoadResult(books=parse_order_books(response))
    finally:
        response.close()


def parse_order_books(json_source: bytes | BinaryIO) -> list[dict[str, object]]:
    if isinstance(json_source, bytes):
        payload = json.loads(json_source)
    else:
        payload = json.load(json_source)

    if not isinstance(payload, list):
        raise TypeError("order book response must be a list")

    books: list[dict[str, object]] = []
    for item in payload:
        if not isinstance(item, Mapping):
            raise TypeError("order book entries must be objects")
        books.append(dict(item))
    return books


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
