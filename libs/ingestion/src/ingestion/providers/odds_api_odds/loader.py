from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Any, BinaryIO
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DEFAULT_ODDS_API_BASE_URL = "https://api.the-odds-api.com/v4"


@dataclass(slots=True, frozen=True)
class OddsApiOddsLoadResult:
    events: Iterable[dict[str, object]]
    requests_remaining: int | None = None
    requests_used: int | None = None
    requests_last: int | None = None


def load_odds_events(
    *,
    api_key: str,
    sport: str,
    regions: Sequence[str],
    markets: Sequence[str],
    bookmakers: Sequence[str] = (),
    event_ids: Sequence[str] = (),
    commence_time_from: datetime | str | None = None,
    commence_time_to: datetime | str | None = None,
    date_format: str = "iso",
    odds_format: str = "decimal",
    include_links: bool = False,
    include_sids: bool = False,
    include_bet_limits: bool = False,
    include_rotation_numbers: bool = False,
    base_url: str = DEFAULT_ODDS_API_BASE_URL,
) -> OddsApiOddsLoadResult:
    if not regions and not bookmakers:
        raise ValueError("regions or bookmakers must be provided")

    request = Request(
        _build_url(
            api_key=api_key,
            sport=sport,
            regions=regions,
            markets=markets,
            bookmakers=bookmakers,
            event_ids=event_ids,
            commence_time_from=commence_time_from,
            commence_time_to=commence_time_to,
            date_format=date_format,
            odds_format=odds_format,
            include_links=include_links,
            include_sids=include_sids,
            include_bet_limits=include_bet_limits,
            include_rotation_numbers=include_rotation_numbers,
            base_url=base_url,
        ),
        headers={"Accept": "application/json"},
    )

    response = urlopen(request)
    try:
        body = response.read()
        return OddsApiOddsLoadResult(
            events=tuple(parse_odds_events(body)),
            requests_remaining=_parse_optional_int(response.headers.get("x-requests-remaining")),
            requests_used=_parse_optional_int(response.headers.get("x-requests-used")),
            requests_last=_parse_optional_int(response.headers.get("x-requests-last")),
        )
    finally:
        response.close()


def parse_odds_events(json_source: bytes | BinaryIO) -> Iterator[dict[str, object]]:
    if isinstance(json_source, bytes):
        stream: BinaryIO = BytesIO(json_source)
    else:
        stream = json_source

    payload = json.load(stream)
    if not isinstance(payload, list):
        raise ValueError("expected a JSON array of odds events")

    for item in payload:
        if not isinstance(item, Mapping):
            continue
        normalized = _normalize_event(item)
        if normalized is not None:
            yield normalized


def _build_url(
    *,
    api_key: str,
    sport: str,
    regions: Sequence[str],
    markets: Sequence[str],
    bookmakers: Sequence[str],
    event_ids: Sequence[str],
    commence_time_from: datetime | str | None,
    commence_time_to: datetime | str | None,
    date_format: str,
    odds_format: str,
    include_links: bool,
    include_sids: bool,
    include_bet_limits: bool,
    include_rotation_numbers: bool,
    base_url: str,
) -> str:
    params: dict[str, str] = {
        "apiKey": api_key,
        "markets": ",".join(markets),
        "dateFormat": date_format,
        "oddsFormat": odds_format,
    }
    if regions:
        params["regions"] = ",".join(regions)
    if bookmakers:
        params["bookmakers"] = ",".join(bookmakers)
    if event_ids:
        params["eventIds"] = ",".join(event_ids)
    if commence_time_from is not None:
        params["commenceTimeFrom"] = _coerce_query_datetime(commence_time_from)
    if commence_time_to is not None:
        params["commenceTimeTo"] = _coerce_query_datetime(commence_time_to)
    if include_links:
        params["includeLinks"] = "true"
    if include_sids:
        params["includeSids"] = "true"
    if include_bet_limits:
        params["includeBetLimits"] = "true"
    if include_rotation_numbers:
        params["includeRotationNumbers"] = "true"

    return f"{base_url.rstrip('/')}/sports/{sport}/odds/?{urlencode(params)}"


def _normalize_event(event: Mapping[str, Any]) -> dict[str, object] | None:
    event_id = event.get("id")
    if event_id is None:
        return None

    normalized: dict[str, object] = dict(event)
    normalized["id"] = str(event_id)
    normalized["commence_time"] = _parse_datetime(event.get("commence_time"))

    bookmakers = event.get("bookmakers")
    normalized["bookmakers"] = (
        [_normalize_bookmaker(item) for item in bookmakers if isinstance(item, Mapping)]
        if isinstance(bookmakers, list)
        else []
    )
    return normalized


def _normalize_bookmaker(bookmaker: Mapping[str, Any]) -> dict[str, object]:
    normalized: dict[str, object] = dict(bookmaker)
    normalized["last_update"] = _parse_datetime(bookmaker.get("last_update"))

    markets = bookmaker.get("markets")
    normalized["markets"] = (
        [_normalize_market(item) for item in markets if isinstance(item, Mapping)]
        if isinstance(markets, list)
        else []
    )
    return normalized


def _normalize_market(market: Mapping[str, Any]) -> dict[str, object]:
    normalized: dict[str, object] = dict(market)
    outcomes = market.get("outcomes")
    normalized["outcomes"] = (
        [dict(item) for item in outcomes if isinstance(item, Mapping)]
        if isinstance(outcomes, list)
        else []
    )
    return normalized


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value:
        return None

    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _coerce_query_datetime(value: datetime | str) -> str:
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    return value


def _parse_optional_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None

