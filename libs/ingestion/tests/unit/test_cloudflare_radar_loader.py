from __future__ import annotations

import json
from datetime import datetime, timezone
from io import BytesIO
from urllib.parse import parse_qs, urlparse

import pytest

from ingestion.providers.cloudflare_radar import loader
from ingestion.providers.cloudflare_radar.loader import (
    CloudflareRadarApiError,
    TrafficAnomaliesRequest,
    load_traffic_anomalies,
)


def test_load_traffic_anomalies_builds_authorized_request(monkeypatch) -> None:
    captured_request = None

    def fake_urlopen(request):
        nonlocal captured_request
        captured_request = request
        return _FakeResponse(
            {
                "success": True,
                "result": {
                    "trafficAnomalies": [
                        {
                            "uuid": "anomaly-1",
                            "startDate": "2026-03-25T10:15:00Z",
                            "status": "VERIFIED",
                            "type": "LOCATION",
                        }
                    ]
                },
            }
        )

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    result = load_traffic_anomalies(
        api_token="secret-token",
        request=TrafficAnomaliesRequest(
            date_range="7d",
            location="US",
            status="VERIFIED",
            types=("LOCATION", "AS"),
            limit=50,
        ),
    )

    assert [anomaly["uuid"] for anomaly in result.anomalies] == ["anomaly-1"]
    assert captured_request is not None
    assert captured_request.headers["Authorization"] == "Bearer secret-token"
    parsed = urlparse(captured_request.full_url)
    params = parse_qs(parsed.query)
    assert parsed.path == "/client/v4/radar/traffic_anomalies"
    assert params["format"] == ["JSON"]
    assert params["dateRange"] == ["7d"]
    assert params["location"] == ["US"]
    assert params["status"] == ["VERIFIED"]
    assert params["type"] == ["LOCATION", "AS"]
    assert params["limit"] == ["50"]


def test_load_traffic_anomalies_normalizes_datetime_fields(monkeypatch) -> None:
    def fake_urlopen(request):
        return _FakeResponse(
            {
                "success": True,
                "result": {
                    "trafficAnomalies": [
                        {
                            "uuid": "anomaly-1",
                            "startDate": "2026-03-25T10:15:00Z",
                            "endDate": "2026-03-25T10:45:00Z",
                            "status": "VERIFIED",
                            "type": "LOCATION",
                            "visibleInDataSources": ["radar", "outage-center"],
                        }
                    ]
                },
            }
        )

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    result = load_traffic_anomalies(
        api_token="secret-token",
        request=TrafficAnomaliesRequest(limit=10),
    )

    assert result.anomalies[0]["start_date"] == datetime(2026, 3, 25, 10, 15, tzinfo=timezone.utc)
    assert result.anomalies[0]["end_date"] == datetime(2026, 3, 25, 10, 45, tzinfo=timezone.utc)
    assert result.anomalies[0]["visible_in_data_sources"] == ["radar", "outage-center"]


def test_load_traffic_anomalies_raises_on_api_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        loader,
        "urlopen",
        lambda request: _FakeResponse(
            {
                "success": False,
                "errors": [{"code": 1000, "message": "bad request"}],
            }
        ),
    )

    with pytest.raises(CloudflareRadarApiError, match="1000: bad request"):
        load_traffic_anomalies(
            api_token="secret-token",
            request=TrafficAnomaliesRequest(limit=10),
        )


def test_traffic_anomalies_request_rejects_mixed_range_and_dates() -> None:
    with pytest.raises(ValueError, match="date_range cannot be combined"):
        TrafficAnomaliesRequest(
            date_range="7d",
            date_start=datetime(2026, 3, 25, tzinfo=timezone.utc),
        )


class _FakeResponse(BytesIO):
    def __init__(self, payload: dict[str, object]) -> None:
        super().__init__(json.dumps(payload).encode("utf-8"))

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
