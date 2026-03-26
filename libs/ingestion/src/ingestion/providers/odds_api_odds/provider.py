from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from typing import Any

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.odds_api_odds.checkpoint import OddsApiOddsCheckpoint
from ingestion.providers.odds_api_odds.loader import OddsApiOddsLoadResult, load_odds_events
from ingestion.utils.time import utc_now

OddsEvent = Mapping[str, Any]
LoadResultOrEvents = OddsApiOddsLoadResult | Iterable[OddsEvent]
EventsLoader = Callable[
    [OddsApiOddsCheckpoint | None],
    LoadResultOrEvents | Awaitable[LoadResultOrEvents],
]


@dataclass(slots=True, frozen=True)
class OddsApiOddsResponseMetadata:
    requests_remaining: int | None = None
    requests_used: int | None = None
    requests_last: int | None = None


@dataclass(slots=True, frozen=True)
class _NormalizedEvent:
    event_id: str
    payload: dict[str, object]
    observed_at: datetime | None


@dataclass(slots=True, frozen=True)
class _BatchCursor:
    last_seen_at: datetime
    event_ids_at_last_seen_at: tuple[str, ...]


class OddsApiOddsProvider(BatchProvider[OddsApiOddsCheckpoint]):
    def __init__(
        self,
        *,
        sport: str,
        regions: Sequence[str],
        markets: Sequence[str],
        api_key: str | None = None,
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
        events_loader: EventsLoader | None = None,
        name: str | None = None,
    ) -> None:
        if not regions and not bookmakers:
            raise ValueError("regions or bookmakers must be provided")
        if events_loader is None and not api_key:
            raise ValueError("api_key is required when events_loader is not provided")

        self.sport = sport
        self.regions = tuple(regions)
        self.markets = tuple(markets)
        self.bookmakers = tuple(bookmakers)
        self.event_ids = tuple(event_ids)
        self.commence_time_from = commence_time_from
        self.commence_time_to = commence_time_to
        self.date_format = date_format
        self.odds_format = odds_format
        self.include_links = include_links
        self.include_sids = include_sids
        self.include_bet_limits = include_bet_limits
        self.include_rotation_numbers = include_rotation_numbers
        self.name = name or _default_provider_name(self)
        self._events_loader = events_loader or partial(
            load_odds_events,
            api_key=api_key or "",
            sport=self.sport,
            regions=self.regions,
            markets=self.markets,
            bookmakers=self.bookmakers,
            event_ids=self.event_ids,
            commence_time_from=self.commence_time_from,
            commence_time_to=self.commence_time_to,
            date_format=self.date_format,
            odds_format=self.odds_format,
            include_links=self.include_links,
            include_sids=self.include_sids,
            include_bet_limits=self.include_bet_limits,
            include_rotation_numbers=self.include_rotation_numbers,
        )
        self._response_metadata = OddsApiOddsResponseMetadata()
        self._batch_cursor: _BatchCursor | None = None

    async def fetch(
        self,
        *,
        checkpoint: OddsApiOddsCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        load_result = await self._load_events(checkpoint)
        self._response_metadata = OddsApiOddsResponseMetadata(
            requests_remaining=load_result.requests_remaining,
            requests_used=load_result.requests_used,
            requests_last=load_result.requests_last,
        )

        events = _collect_events(load_result.events)
        normalized_events = [
            normalized
            for event in events
            if (normalized := _normalize_event_payload(event)) is not None
        ]
        normalized_events.sort(key=_event_sort_key, reverse=True)
        self._batch_cursor = _build_batch_cursor(normalized_events)

        active_checkpoint = checkpoint if self._matches_checkpoint(checkpoint) else None
        batch_fetched_at = utc_now()
        for event in normalized_events:
            if not _should_emit(event, active_checkpoint):
                continue

            yield Record(
                provider=self.name,
                key=event.event_id,
                payload=event.payload,
                occurred_at=event.observed_at,
                fetched_at=batch_fetched_at,
                metadata=_record_metadata(self),
            )

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: OddsApiOddsCheckpoint | None,
        last_entry_id: str | None = None,
    ) -> OddsApiOddsCheckpoint | None:
        del last_entry_id

        current_cursor = self._batch_cursor
        if current_cursor is None:
            return previous_checkpoint if self._matches_checkpoint(previous_checkpoint) else None

        if previous_checkpoint is not None and self._matches_checkpoint(previous_checkpoint):
            if previous_checkpoint.last_seen_at is None:
                return _checkpoint_from_cursor(self, current_cursor)

            if current_cursor.last_seen_at > previous_checkpoint.last_seen_at:
                return _checkpoint_from_cursor(self, current_cursor)

            if current_cursor.last_seen_at == previous_checkpoint.last_seen_at:
                return OddsApiOddsCheckpoint(
                    sport=self.sport,
                    regions=self.regions,
                    markets=self.markets,
                    bookmakers=self.bookmakers,
                    last_seen_at=previous_checkpoint.last_seen_at,
                    event_ids_at_last_seen_at=tuple(
                        sorted(
                            set(previous_checkpoint.event_ids_at_last_seen_at)
                            | set(current_cursor.event_ids_at_last_seen_at)
                        )
                    ),
                )

            return previous_checkpoint

        return _checkpoint_from_cursor(self, current_cursor)

    async def _load_events(
        self,
        checkpoint: OddsApiOddsCheckpoint | None,
    ) -> OddsApiOddsLoadResult:
        result = self._events_loader(checkpoint)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, OddsApiOddsLoadResult):
            return result
        return OddsApiOddsLoadResult(events=result)

    def _matches_checkpoint(self, checkpoint: OddsApiOddsCheckpoint | None) -> bool:
        return (
            checkpoint is not None
            and checkpoint.sport == self.sport
            and checkpoint.regions == self.regions
            and checkpoint.markets == self.markets
            and checkpoint.bookmakers == self.bookmakers
        )


def _record_metadata(provider: OddsApiOddsProvider) -> dict[str, object]:
    metadata: dict[str, object] = {
        "sport": provider.sport,
        "regions": provider.regions,
        "markets": provider.markets,
    }
    if provider.bookmakers:
        metadata["bookmakers"] = provider.bookmakers
    return metadata


def _collect_events(events: Iterable[OddsEvent]) -> list[OddsEvent]:
    iterator = iter(events)
    collected: list[OddsEvent] = []
    try:
        for event in iterator:
            collected.append(event)
    finally:
        close = getattr(iterator, "close", None)
        if callable(close):
            close()
    return collected


def _normalize_event_payload(event: OddsEvent) -> _NormalizedEvent | None:
    event_id = event.get("id")
    if event_id is None:
        return None

    payload = dict(event)
    payload["id"] = str(event_id)
    observed_at = _event_observed_at(payload)
    return _NormalizedEvent(
        event_id=str(event_id),
        payload=payload,
        observed_at=observed_at,
    )


def _event_observed_at(event: Mapping[str, object]) -> datetime | None:
    latest: datetime | None = None
    bookmakers = event.get("bookmakers")
    if isinstance(bookmakers, list):
        for bookmaker in bookmakers:
            if not isinstance(bookmaker, Mapping):
                continue
            candidate = _coerce_datetime(bookmaker.get("last_update"))
            if candidate is None:
                continue
            if latest is None or candidate > latest:
                latest = candidate
    return latest or _coerce_datetime(event.get("commence_time"))


def _should_emit(
    event: _NormalizedEvent,
    checkpoint: OddsApiOddsCheckpoint | None,
) -> bool:
    if checkpoint is None or checkpoint.last_seen_at is None or event.observed_at is None:
        return True
    if event.observed_at > checkpoint.last_seen_at:
        return True
    if event.observed_at < checkpoint.last_seen_at:
        return False
    return event.event_id not in checkpoint.event_ids_at_last_seen_at


def _build_batch_cursor(events: Iterable[_NormalizedEvent]) -> _BatchCursor | None:
    latest_seen_at: datetime | None = None
    event_ids: list[str] = []

    for event in events:
        if event.observed_at is None:
            continue
        if latest_seen_at is None or event.observed_at > latest_seen_at:
            latest_seen_at = event.observed_at
            event_ids = [event.event_id]
            continue
        if event.observed_at == latest_seen_at:
            event_ids.append(event.event_id)

    if latest_seen_at is None:
        return None

    return _BatchCursor(
        last_seen_at=latest_seen_at,
        event_ids_at_last_seen_at=tuple(sorted(set(event_ids))),
    )


def _checkpoint_from_cursor(
    provider: OddsApiOddsProvider,
    cursor: _BatchCursor,
) -> OddsApiOddsCheckpoint:
    return OddsApiOddsCheckpoint(
        sport=provider.sport,
        regions=provider.regions,
        markets=provider.markets,
        bookmakers=provider.bookmakers,
        last_seen_at=cursor.last_seen_at,
        event_ids_at_last_seen_at=cursor.event_ids_at_last_seen_at,
    )


def _event_sort_key(event: _NormalizedEvent) -> tuple[datetime, str]:
    return (event.observed_at or datetime.min.replace(tzinfo=timezone.utc), event.event_id)


def _coerce_datetime(value: Any) -> datetime | None:
    return value if isinstance(value, datetime) else None


def _default_provider_name(provider: OddsApiOddsProvider) -> str:
    params: list[tuple[str, str]] = [("markets", ",".join(provider.markets))]
    if provider.regions:
        params.append(("regions", ",".join(provider.regions)))
    if provider.bookmakers:
        params.append(("bookmakers", ",".join(provider.bookmakers)))
    if provider.event_ids:
        params.append(("eventIds", ",".join(provider.event_ids)))
    if provider.commence_time_from is not None:
        params.append(("commenceTimeFrom", _stringify_datetime(provider.commence_time_from)))
    if provider.commence_time_to is not None:
        params.append(("commenceTimeTo", _stringify_datetime(provider.commence_time_to)))
    if provider.include_links:
        params.append(("includeLinks", "true"))
    if provider.include_sids:
        params.append(("includeSids", "true"))
    if provider.include_bet_limits:
        params.append(("includeBetLimits", "true"))
    if provider.include_rotation_numbers:
        params.append(("includeRotationNumbers", "true"))
    if provider.date_format != "iso":
        params.append(("dateFormat", provider.date_format))
    if provider.odds_format != "decimal":
        params.append(("oddsFormat", provider.odds_format))

    query = "&".join(f"{key}={value}" for key, value in params)
    return f"odds_api_odds:{provider.sport}?{query}"


def _stringify_datetime(value: datetime | str) -> str:
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    return value
