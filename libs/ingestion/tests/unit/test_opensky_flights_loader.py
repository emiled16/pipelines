from __future__ import annotations

from io import BytesIO
from urllib.error import HTTPError

from ingestion.providers.opensky_flights import loader
from ingestion.providers.opensky_flights.loader import (
    OpenSkyTokenManager,
    load_flights,
    parse_flights,
)


def test_parse_flights_normalizes_fields() -> None:
    json_bytes = b"""
    [
      {
        "icao24": "ABC123",
        "firstSeen": 1711540800,
        "estDepartureAirport": "CYYZ",
        "lastSeen": 1711544400,
        "estArrivalAirport": "KJFK",
        "callsign": "ACA701  ",
        "estDepartureAirportHorizDistance": 1200,
        "estDepartureAirportVertDistance": 50,
        "estArrivalAirportHorizDistance": 900,
        "estArrivalAirportVertDistance": 25,
        "departureAirportCandidatesCount": 1,
        "arrivalAirportCandidatesCount": 2
      }
    ]
    """

    flights = list(parse_flights(json_bytes))

    assert len(flights) == 1
    assert flights[0]["id"] == "abc123:1711540800:1711544400"
    assert flights[0]["icao24"] == "abc123"
    assert flights[0]["callsign"] == "ACA701"
    assert flights[0]["est_departure_airport"] == "CYYZ"
    assert flights[0]["est_arrival_airport"] == "KJFK"
    assert flights[0]["first_seen_at"].isoformat() == "2024-03-27T12:00:00+00:00"
    assert flights[0]["last_seen_at"].isoformat() == "2024-03-27T13:00:00+00:00"


def test_load_flights_treats_404_as_empty_result(monkeypatch) -> None:
    def _raise_not_found(request):
        raise HTTPError(
            url=request.full_url,
            code=404,
            msg="Not Found",
            hdrs=None,
            fp=None,
        )

    monkeypatch.setattr(loader, "urlopen", _raise_not_found)

    result = load_flights(100, 200)

    assert list(result.flights) == []


def test_load_flights_adds_bearer_token_header(monkeypatch) -> None:
    response = _FakeResponse(b"[]")
    captured_headers: dict[str, str] = {}

    def _urlopen(request):
        captured_headers.update(dict(request.header_items()))
        return response

    monkeypatch.setattr(loader, "urlopen", _urlopen)

    result = load_flights(100, 200, access_token="test-token")

    assert list(result.flights) == []
    assert captured_headers["Authorization"] == "Bearer test-token"


def test_token_manager_loads_credentials_file(tmp_path) -> None:
    credentials_path = tmp_path / "credentials.json"
    credentials_path.write_text('{"clientId":"client-1","clientSecret":"secret-1"}')

    manager = OpenSkyTokenManager.from_json_file(credentials_path)

    assert manager.client_id == "client-1"
    assert manager.client_secret == "secret-1"


class _FakeResponse:
    def __init__(self, body: bytes) -> None:
        self._stream = BytesIO(body)
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise AssertionError("response was closed before the loader finished reading it")
        return self._stream.read(size)

    def close(self) -> None:
        self.closed = True
