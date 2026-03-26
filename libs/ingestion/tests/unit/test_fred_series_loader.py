from __future__ import annotations

import json
from datetime import timezone
from urllib.parse import parse_qs, urlparse

from ingestion.providers.fred_series import loader
from ingestion.providers.fred_series.checkpoint import FredSeriesCheckpoint
from ingestion.providers.fred_series.loader import (
    load_series_observations,
    parse_series_observations,
)


def test_parse_series_observations_extracts_fred_fields() -> None:
    payload = {
        "observations": [
            {
                "realtime_start": "2026-03-25",
                "realtime_end": "2026-03-25",
                "date": "2026-03-01",
                "value": "4.33",
            },
            {
                "realtime_start": "2026-03-25",
                "realtime_end": "2026-03-25",
                "date": "2026-02-01",
                "value": ".",
            },
        ]
    }

    observations = parse_series_observations(payload, series_id="FEDFUNDS")

    assert [observation["id"] for observation in observations] == ["2026-03-01", "2026-02-01"]
    assert observations[0]["series_id"] == "FEDFUNDS"
    assert observations[0]["value"] == "4.33"
    assert observations[0]["observed_at"].tzinfo == timezone.utc
    assert observations[1]["value"] is None


def test_load_series_observations_builds_request_from_checkpoint(monkeypatch) -> None:
    response = _FakeJsonResponse(
        {
            "count": 1,
            "offset": 25,
            "limit": 50,
            "observations": [
                {
                    "realtime_start": "2026-03-25",
                    "realtime_end": "2026-03-25",
                    "date": "2026-03-01",
                    "value": "4.33",
                }
            ],
        }
    )

    monkeypatch.setattr(loader, "urlopen", lambda request: response.capture(request))

    checkpoint = FredSeriesCheckpoint(series_id="FEDFUNDS", cursor="2026-02-01")

    load_result = load_series_observations(
        "FEDFUNDS",
        "secret-key",
        checkpoint=checkpoint,
        offset=25,
        limit=50,
    )

    assert response.request is not None
    query = parse_qs(urlparse(response.request.full_url).query)
    assert query["series_id"] == ["FEDFUNDS"]
    assert query["api_key"] == ["secret-key"]
    assert query["file_type"] == ["json"]
    assert query["sort_order"] == ["desc"]
    assert query["observation_start"] == ["2026-02-01"]
    assert query["offset"] == ["25"]
    assert query["limit"] == ["50"]
    assert [observation["id"] for observation in load_result.observations] == ["2026-03-01"]


class _FakeJsonResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload
        self.request = None

    def capture(self, request):
        self.request = request
        return self

    def read(self, size: int = -1) -> bytes:
        return json.dumps(self._payload).encode()

    def __enter__(self) -> _FakeJsonResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None
