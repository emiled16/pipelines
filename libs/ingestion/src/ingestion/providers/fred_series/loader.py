from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ingestion.providers.fred_series.checkpoint import FredSeriesCheckpoint

DEFAULT_FRED_API_URL = "https://api.stlouisfed.org/fred/series/observations"


@dataclass(slots=True, frozen=True)
class FredSeriesLoadResult:
    observations: Iterable[dict[str, object]]
    count: int
    offset: int
    limit: int


def load_series_observations(
    series_id: str,
    api_key: str,
    checkpoint: FredSeriesCheckpoint | None = None,
    offset: int = 0,
    *,
    limit: int = 1000,
    base_url: str = DEFAULT_FRED_API_URL,
) -> FredSeriesLoadResult:
    request = Request(
        _build_request_url(series_id, api_key, checkpoint, offset=offset, limit=limit)
    )

    with urlopen(request) as response:
        payload = json.load(response)

    observations = parse_series_observations(payload, series_id=series_id)
    return FredSeriesLoadResult(
        observations=observations,
        count=_coerce_int(payload.get("count"), default=len(observations)),
        offset=_coerce_int(payload.get("offset"), default=offset),
        limit=_coerce_int(payload.get("limit"), default=limit),
    )


def parse_series_observations(
    payload: Mapping[str, object],
    *,
    series_id: str,
) -> list[dict[str, object]]:
    raw_observations = payload.get("observations")
    if not isinstance(raw_observations, list):
        return []

    observations: list[dict[str, object]] = []
    for item in raw_observations:
        if not isinstance(item, Mapping):
            continue

        observation_date = _coerce_str(item.get("date"))
        if observation_date is None:
            continue

        observations.append(
            {
                "id": observation_date,
                "series_id": series_id,
                "date": observation_date,
                "value": _normalize_value(item.get("value")),
                "realtime_start": _coerce_str(item.get("realtime_start")),
                "realtime_end": _coerce_str(item.get("realtime_end")),
                "observed_at": _parse_observation_date(observation_date),
            }
        )

    return observations


def _build_request_url(
    series_id: str,
    api_key: str,
    checkpoint: FredSeriesCheckpoint | None,
    *,
    offset: int,
    limit: int,
    base_url: str = DEFAULT_FRED_API_URL,
) -> str:
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "offset": offset,
        "limit": limit,
    }
    if checkpoint is not None and checkpoint.series_id == series_id:
        params["observation_start"] = checkpoint.cursor
    return f"{base_url}?{urlencode(params)}"


def _coerce_int(value: object, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _normalize_value(value: object) -> str | None:
    if value is None:
        return None

    value_text = str(value)
    if value_text == ".":
        return None
    return value_text


def _parse_observation_date(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
