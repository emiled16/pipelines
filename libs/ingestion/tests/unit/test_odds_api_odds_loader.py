from __future__ import annotations

from datetime import timezone
from io import BytesIO

from ingestion.providers.odds_api_odds import loader
from ingestion.providers.odds_api_odds.loader import load_odds_events, parse_odds_events


def test_parse_odds_events_extracts_core_fields() -> None:
    json_bytes = b"""
    [
      {
        "id": "game-1",
        "sport_key": "basketball_nba",
        "commence_time": "2026-03-26T20:00:00Z",
        "home_team": "Boston Celtics",
        "away_team": "Toronto Raptors",
        "bookmakers": [
          {
            "key": "draftkings",
            "last_update": "2026-03-26T19:45:00Z",
            "markets": [
              {
                "key": "h2h",
                "outcomes": [
                  {"name": "Boston Celtics", "price": -150},
                  {"name": "Toronto Raptors", "price": 130}
                ]
              }
            ]
          }
        ]
      }
    ]
    """

    events = list(parse_odds_events(json_bytes))

    assert len(events) == 1
    assert events[0]["id"] == "game-1"
    assert events[0]["commence_time"].tzinfo == timezone.utc
    bookmaker = events[0]["bookmakers"][0]
    assert bookmaker["last_update"].tzinfo == timezone.utc
    assert bookmaker["markets"][0]["outcomes"][0]["name"] == "Boston Celtics"


def test_load_odds_events_builds_request_and_parses_usage_headers(monkeypatch) -> None:
    response = _FakeResponse(
        b'[{"id":"game-1","commence_time":"2026-03-26T20:00:00Z","bookmakers":[]}]',
        headers={
            "x-requests-remaining": "97",
            "x-requests-used": "3",
            "x-requests-last": "1",
        },
    )
    captured_request = None

    def fake_urlopen(request):
        nonlocal captured_request
        captured_request = request
        return response

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    load_result = load_odds_events(
        api_key="secret",
        sport="basketball_nba",
        regions=("us",),
        markets=("h2h", "spreads"),
        bookmakers=("draftkings",),
        include_links=True,
    )

    assert captured_request is not None
    assert "/sports/basketball_nba/odds/" in captured_request.full_url
    assert "apiKey=secret" in captured_request.full_url
    assert "regions=us" in captured_request.full_url
    assert "markets=h2h%2Cspreads" in captured_request.full_url
    assert "bookmakers=draftkings" in captured_request.full_url
    assert "includeLinks=true" in captured_request.full_url
    assert load_result.requests_remaining == 97
    assert load_result.requests_used == 3
    assert load_result.requests_last == 1
    assert [event["id"] for event in load_result.events] == ["game-1"]
    assert response.closed is True


class _FakeResponse:
    def __init__(self, body: bytes, *, headers: dict[str, str]) -> None:
        self._stream = BytesIO(body)
        self.headers = headers
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise AssertionError("response was closed before the loader finished reading it")
        return self._stream.read(size)

    def close(self) -> None:
        self.closed = True
