from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, BinaryIO
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DEFAULT_API_BASE_URL = "https://opensky-network.org/api"
DEFAULT_TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
)
TOKEN_REFRESH_MARGIN_SECONDS = 30


@dataclass(slots=True, frozen=True)
class OpenSkyFlightsLoadResult:
    flights: Iterable[dict[str, object]]


@dataclass(slots=True)
class OpenSkyTokenManager:
    client_id: str
    client_secret: str
    token_url: str = DEFAULT_TOKEN_URL
    _access_token: str | None = field(default=None, init=False, repr=False)
    _expires_at: datetime | None = field(default=None, init=False, repr=False)

    @classmethod
    def from_json_file(cls, path: str | Path) -> OpenSkyTokenManager:
        with Path(path).open() as handle:
            credentials = json.load(handle)
        return cls(
            client_id=str(credentials["clientId"]),
            client_secret=str(credentials["clientSecret"]),
        )

    def authorization_header(self) -> str:
        if (
            self._access_token is not None
            and self._expires_at is not None
            and datetime.now(timezone.utc) < self._expires_at
        ):
            return f"Bearer {self._access_token}"
        return f"Bearer {self._refresh_access_token()}"

    def _refresh_access_token(self) -> str:
        request = Request(
            self.token_url,
            data=urlencode(
                {
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                }
            ).encode("utf-8"),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )

        with urlopen(request) as response:
            payload = json.load(response)

        access_token = str(payload["access_token"])
        expires_in = max(int(payload.get("expires_in", 1800)), TOKEN_REFRESH_MARGIN_SECONDS)

        self._access_token = access_token
        self._expires_at = datetime.now(timezone.utc) + timedelta(
            seconds=expires_in - TOKEN_REFRESH_MARGIN_SECONDS
        )
        return access_token


def load_flights(
    begin: int,
    end: int,
    *,
    api_base_url: str = DEFAULT_API_BASE_URL,
    access_token: str | None = None,
    token_manager: OpenSkyTokenManager | None = None,
) -> OpenSkyFlightsLoadResult:
    if begin >= end:
        raise ValueError("end must be greater than begin")
    if end - begin > 7200:
        raise ValueError("OpenSky flight intervals must be 7200 seconds or shorter")
    if access_token is not None and token_manager is not None:
        raise ValueError("access_token and token_manager are mutually exclusive")

    query = urlencode({"begin": begin, "end": end})
    request = Request(
        f"{api_base_url.rstrip('/')}/flights/all?{query}",
        headers=_request_headers(access_token=access_token, token_manager=token_manager),
    )

    try:
        response = urlopen(request)
        return OpenSkyFlightsLoadResult(flights=_iter_response_flights(response))
    except HTTPError as exc:
        if exc.code == 404:
            return OpenSkyFlightsLoadResult(flights=())
        raise


def parse_flights(json_source: bytes | BinaryIO) -> Iterator[dict[str, object]]:
    if isinstance(json_source, bytes):
        stream: BinaryIO = BytesIO(json_source)
    else:
        stream = json_source

    payload = json.load(stream)
    if not isinstance(payload, list):
        raise ValueError("OpenSky flights response must be a JSON array")

    for item in payload:
        if not isinstance(item, Mapping):
            continue

        normalized = _normalize_flight(item)
        if normalized is not None:
            yield normalized


def _iter_response_flights(stream: BinaryIO) -> Iterator[dict[str, object]]:
    try:
        yield from parse_flights(stream)
    finally:
        stream.close()


def _request_headers(
    *,
    access_token: str | None,
    token_manager: OpenSkyTokenManager | None,
) -> dict[str, str]:
    headers = {"Accept": "application/json"}
    if access_token is not None:
        headers["Authorization"] = f"Bearer {access_token}"
    elif token_manager is not None:
        headers["Authorization"] = token_manager.authorization_header()
    return headers


def _normalize_flight(item: Mapping[str, object]) -> dict[str, object] | None:
    icao24 = _normalize_string(item.get("icao24"), lowercase=True)
    first_seen_at = _parse_timestamp(item.get("firstSeen"))
    last_seen_at = _parse_timestamp(item.get("lastSeen"))
    if icao24 is None or first_seen_at is None or last_seen_at is None:
        return None

    return {
        "id": _flight_id(icao24=icao24, first_seen_at=first_seen_at, last_seen_at=last_seen_at),
        "icao24": icao24,
        "callsign": _normalize_string(item.get("callsign")),
        "first_seen_at": first_seen_at,
        "est_departure_airport": _normalize_string(item.get("estDepartureAirport")),
        "last_seen_at": last_seen_at,
        "est_arrival_airport": _normalize_string(item.get("estArrivalAirport")),
        "est_departure_airport_horiz_distance": _coerce_int(
            item.get("estDepartureAirportHorizDistance")
        ),
        "est_departure_airport_vert_distance": _coerce_int(
            item.get("estDepartureAirportVertDistance")
        ),
        "est_arrival_airport_horiz_distance": _coerce_int(
            item.get("estArrivalAirportHorizDistance")
        ),
        "est_arrival_airport_vert_distance": _coerce_int(
            item.get("estArrivalAirportVertDistance")
        ),
        "departure_airport_candidates_count": _coerce_int(
            item.get("departureAirportCandidatesCount")
        ),
        "arrival_airport_candidates_count": _coerce_int(
            item.get("arrivalAirportCandidatesCount")
        ),
    }


def _flight_id(
    *,
    icao24: str,
    first_seen_at: datetime,
    last_seen_at: datetime,
) -> str:
    return ":".join(
        [
            icao24,
            str(int(first_seen_at.timestamp())),
            str(int(last_seen_at.timestamp())),
        ]
    )


def _normalize_string(value: object, *, lowercase: bool = False) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if lowercase:
        return normalized.lower()
    return normalized


def _parse_timestamp(value: object) -> datetime | None:
    if isinstance(value, bool):
        return None
    if not isinstance(value, int | float):
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc)


def _coerce_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None

