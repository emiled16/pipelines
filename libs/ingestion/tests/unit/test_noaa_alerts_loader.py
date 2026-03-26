from __future__ import annotations

from datetime import timezone
from io import BytesIO
from urllib.error import HTTPError

from ingestion.providers.noaa_alerts import loader
from ingestion.providers.noaa_alerts.checkpoint import NoaaAlertsCheckpoint
from ingestion.providers.noaa_alerts.loader import (
    build_alerts_url,
    load_alert_entries,
    parse_alert_entries,
)


def test_build_alerts_url_encodes_multi_value_query_parameters() -> None:
    alerts_url = build_alerts_url(
        "https://api.weather.gov/alerts/active",
        query={"area": ["KS", "MO"], "status": "actual"},
    )

    assert alerts_url == "https://api.weather.gov/alerts/active?area=KS&area=MO&status=actual"


def test_parse_alert_entries_extracts_common_noaa_alert_fields() -> None:
    payload = {
        "features": [
            {
                "id": "https://api.weather.gov/alerts/1",
                "geometry": {"type": "Polygon", "coordinates": []},
                "properties": {
                    "areaDesc": "Douglas; Shawnee",
                    "geocode": {"UGC": ["KSC045", "KSC177"]},
                    "affectedZones": ["https://api.weather.gov/zones/forecast/KSC045"],
                    "references": [
                        {
                            "@id": "https://api.weather.gov/alerts/0",
                            "identifier": "prev-1",
                            "sender": "w-nws.webmaster@noaa.gov",
                            "sent": "2026-03-24T10:00:00Z",
                        }
                    ],
                    "sent": "2026-03-24T10:15:00+00:00",
                    "effective": "2026-03-24T10:20:00+00:00",
                    "onset": "2026-03-24T10:25:00+00:00",
                    "expires": "2026-03-24T11:15:00+00:00",
                    "ends": "2026-03-24T11:00:00+00:00",
                    "status": "Actual",
                    "messageType": "Alert",
                    "category": "Met",
                    "severity": "Severe",
                    "certainty": "Observed",
                    "urgency": "Immediate",
                    "event": "Tornado Warning",
                    "sender": "w-nws.webmaster@noaa.gov",
                    "senderName": "NWS Topeka KS",
                    "headline": "Tornado Warning issued March 24 at 10:15AM CDT",
                    "description": "A tornado has been spotted.",
                    "instruction": "Take cover now.",
                    "response": "Shelter",
                    "parameters": {"VTEC": ["/O.NEW.KTOP.TO.W.0001.260324T1015Z-260324T1115Z/"]},
                },
            }
        ]
    }

    entries = list(parse_alert_entries(payload))

    assert len(entries) == 1
    assert entries[0]["id"] == "https://api.weather.gov/alerts/1"
    assert entries[0]["area_desc"] == "Douglas; Shawnee"
    assert entries[0]["event"] == "Tornado Warning"
    assert entries[0]["sent_at"].tzinfo == timezone.utc
    assert entries[0]["references"][0]["sent_at"].tzinfo == timezone.utc


def test_parse_alert_entries_falls_back_to_properties_id() -> None:
    payload = {
        "features": [
            {
                "properties": {
                    "@id": "https://api.weather.gov/alerts/2",
                    "event": "Flood Warning",
                }
            }
        ]
    }

    entries = list(parse_alert_entries(payload))

    assert [entry["id"] for entry in entries] == ["https://api.weather.gov/alerts/2"]


def test_load_alert_entries_sends_required_headers(monkeypatch) -> None:
    response = _FakeResponse(
        b'{"features":[{"id":"https://api.weather.gov/alerts/1","properties":{"event":"Test"}}]}',
        headers={"ETag": '"etag-1"', "Last-Modified": "Wed, 25 Mar 2026 10:15:00 GMT"},
    )
    checkpoint = NoaaAlertsCheckpoint(
        alerts_url="https://api.weather.gov/alerts/active?area=KS",
        cursor="https://api.weather.gov/alerts/0",
        etag='"etag-0"',
        last_modified="Tue, 24 Mar 2026 10:15:00 GMT",
    )
    captured_request = None

    def fake_urlopen(request):
        nonlocal captured_request
        captured_request = request
        return response

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    load_result = load_alert_entries(
        checkpoint.alerts_url,
        checkpoint,
        user_agent="codex-test/1.0",
    )

    assert captured_request is not None
    assert captured_request.get_header("Accept") == "application/geo+json"
    assert captured_request.get_header("User-agent") == "codex-test/1.0"
    assert captured_request.get_header("If-none-match") == '"etag-0"'
    assert captured_request.get_header("If-modified-since") == "Tue, 24 Mar 2026 10:15:00 GMT"
    assert [entry["id"] for entry in load_result.entries] == ["https://api.weather.gov/alerts/1"]
    assert response.closed is True


def test_load_alert_entries_handles_not_modified_responses(monkeypatch) -> None:
    checkpoint = NoaaAlertsCheckpoint(
        alerts_url="https://api.weather.gov/alerts/active?area=KS",
        cursor="https://api.weather.gov/alerts/1",
        etag='"etag-1"',
        last_modified="Wed, 25 Mar 2026 10:15:00 GMT",
    )

    def fake_urlopen(request):
        raise HTTPError(request.full_url, 304, "Not Modified", hdrs={}, fp=None)

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    load_result = load_alert_entries(checkpoint.alerts_url, checkpoint)

    assert load_result.not_modified is True
    assert tuple(load_result.entries) == ()
    assert load_result.etag == '"etag-1"'


class _FakeResponse:
    def __init__(self, body: bytes, *, headers: dict[str, str] | None = None) -> None:
        self._stream = BytesIO(body)
        self.headers = headers or {}
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise AssertionError("response was closed before the loader finished reading it")
        return self._stream.read(size)

    def close(self) -> None:
        self.closed = True
