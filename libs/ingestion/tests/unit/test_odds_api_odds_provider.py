from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime, timezone

from ingestion.providers.odds_api_odds.checkpoint import OddsApiOddsCheckpoint
from ingestion.providers.odds_api_odds.provider import OddsApiOddsProvider


def test_odds_api_provider_yields_all_events_without_checkpoint() -> None:
    async def run() -> None:
        provider = OddsApiOddsProvider(
            sport="basketball_nba",
            regions=("us",),
            markets=("h2h",),
            events_loader=lambda checkpoint: _events(
                [
                    _event(
                        event_id="game-2",
                        bookmaker_updates=["2026-03-26T19:55:00Z"],
                        commence_time="2026-03-26T20:00:00Z",
                    ),
                    _event(
                        event_id="game-1",
                        bookmaker_updates=["2026-03-26T19:50:00Z"],
                        commence_time="2026-03-26T21:00:00Z",
                    ),
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == ["game-2", "game-1"]
        assert records[0].metadata["sport"] == "basketball_nba"
        assert records[0].metadata["regions"] == ("us",)
        assert len({record.fetched_at for record in records}) == 1

    asyncio.run(run())


def test_odds_api_provider_filters_events_using_timestamp_and_id_checkpoint() -> None:
    async def run() -> None:
        provider = OddsApiOddsProvider(
            sport="basketball_nba",
            regions=("us",),
            markets=("h2h",),
            events_loader=lambda checkpoint: _events(
                [
                    _event(
                        event_id="game-4",
                        bookmaker_updates=["2026-03-26T20:00:00Z"],
                        commence_time="2026-03-26T21:00:00Z",
                    ),
                    _event(
                        event_id="game-3",
                        bookmaker_updates=["2026-03-26T19:55:00Z"],
                        commence_time="2026-03-26T20:30:00Z",
                    ),
                    _event(
                        event_id="game-1",
                        bookmaker_updates=["2026-03-26T19:55:00Z"],
                        commence_time="2026-03-26T20:00:00Z",
                    ),
                    _event(
                        event_id="game-2",
                        bookmaker_updates=["2026-03-26T19:50:00Z"],
                        commence_time="2026-03-26T22:00:00Z",
                    ),
                ]
            ),
        )
        checkpoint = OddsApiOddsCheckpoint(
            sport="basketball_nba",
            regions=("us",),
            markets=("h2h",),
            last_seen_at=_dt("2026-03-26T19:55:00Z"),
            event_ids_at_last_seen_at=("game-1",),
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["game-4", "game-3"]

    asyncio.run(run())


def test_odds_api_provider_builds_checkpoint_from_newest_updates() -> None:
    async def run() -> None:
        provider = OddsApiOddsProvider(
            sport="basketball_nba",
            regions=("us",),
            markets=("h2h",),
            events_loader=lambda checkpoint: _events(
                [
                    _event(
                        event_id="game-2",
                        bookmaker_updates=["2026-03-26T20:00:00Z"],
                        commence_time="2026-03-26T21:00:00Z",
                    ),
                    _event(
                        event_id="game-1",
                        bookmaker_updates=["2026-03-26T20:00:00Z", "2026-03-26T19:58:00Z"],
                        commence_time="2026-03-26T20:00:00Z",
                    ),
                ]
            ),
        )

        records = [record async for record in provider.fetch()]
        checkpoint = provider.build_checkpoint(previous_checkpoint=None)

        assert [record.key for record in records] == ["game-2", "game-1"]
        assert checkpoint == OddsApiOddsCheckpoint(
            sport="basketball_nba",
            regions=("us",),
            markets=("h2h",),
            last_seen_at=_dt("2026-03-26T20:00:00Z"),
            event_ids_at_last_seen_at=("game-1", "game-2"),
        )

    asyncio.run(run())


def test_odds_api_provider_merges_ids_when_checkpoint_timestamp_matches() -> None:
    async def run() -> None:
        provider = OddsApiOddsProvider(
            sport="basketball_nba",
            regions=("us",),
            markets=("h2h",),
            events_loader=lambda checkpoint: _events(
                [
                    _event(
                        event_id="game-2",
                        bookmaker_updates=["2026-03-26T20:00:00Z"],
                        commence_time="2026-03-26T21:00:00Z",
                    ),
                    _event(
                        event_id="game-1",
                        bookmaker_updates=["2026-03-26T19:59:00Z"],
                        commence_time="2026-03-26T20:00:00Z",
                    ),
                ]
            ),
        )
        previous_checkpoint = OddsApiOddsCheckpoint(
            sport="basketball_nba",
            regions=("us",),
            markets=("h2h",),
            last_seen_at=_dt("2026-03-26T20:00:00Z"),
            event_ids_at_last_seen_at=("game-0",),
        )

        _ = [record async for record in provider.fetch(checkpoint=previous_checkpoint)]
        checkpoint = provider.build_checkpoint(previous_checkpoint=previous_checkpoint)

        assert checkpoint == OddsApiOddsCheckpoint(
            sport="basketball_nba",
            regions=("us",),
            markets=("h2h",),
            last_seen_at=_dt("2026-03-26T20:00:00Z"),
            event_ids_at_last_seen_at=("game-0", "game-2"),
        )

    asyncio.run(run())


def _event(
    *,
    event_id: str,
    bookmaker_updates: list[str],
    commence_time: str,
) -> dict[str, object]:
    return {
        "id": event_id,
        "sport_key": "basketball_nba",
        "commence_time": _dt(commence_time),
        "home_team": f"{event_id}-home",
        "away_team": f"{event_id}-away",
        "bookmakers": [
            {
                "key": f"book-{index}",
                "title": f"Book {index}",
                "last_update": _dt(value),
                "markets": [
                    {
                        "key": "h2h",
                        "outcomes": [
                            {"name": f"{event_id}-home", "price": -110},
                            {"name": f"{event_id}-away", "price": 100},
                        ],
                    }
                ],
            }
            for index, value in enumerate(bookmaker_updates)
        ],
    }


def _events(items: list[dict[str, object]]) -> Iterable[dict[str, object]]:
    for item in items:
        yield item


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
