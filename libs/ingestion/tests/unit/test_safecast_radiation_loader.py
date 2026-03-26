from __future__ import annotations

import json
from datetime import timezone
from io import BytesIO

from ingestion.providers.safecast_radiation import loader
from ingestion.providers.safecast_radiation.loader import (
    SafecastRadiationRequest,
    load_measurements,
)


def test_load_measurements_paginates_and_parses_captured_at(monkeypatch) -> None:
    responses = [
        _FakeResponse(
            [
                {
                    "id": 101,
                    "value": 0.12,
                    "unit": "uSv/h",
                    "captured_at": "2026-03-25T10:15:00Z",
                    "latitude": 35.6895,
                    "longitude": 139.6917,
                },
                {
                    "id": 102,
                    "value": 0.10,
                    "unit": "uSv/h",
                    "captured_at": "2026-03-25T10:10:00Z",
                    "latitude": 35.6896,
                    "longitude": 139.6918,
                },
            ]
        ),
        _FakeResponse(
            [
                {
                    "id": 103,
                    "value": 0.08,
                    "unit": "uSv/h",
                    "captured_at": "2026-03-25T10:05:00Z",
                    "latitude": 35.6897,
                    "longitude": 139.6919,
                }
            ]
        ),
    ]
    all_responses = list(responses)
    requested_urls: list[str] = []

    def fake_urlopen(request):
        requested_urls.append(request.full_url)
        return responses.pop(0)

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    load_result = load_measurements(
        request=SafecastRadiationRequest(
            since="2026-03-25 10:00:00",
            until="2026-03-25 11:00:00",
            page_size=2,
        )
    )
    measurements = list(load_result.measurements)

    assert [measurement["id"] for measurement in measurements] == [101, 102, 103]
    assert measurements[0]["captured_at"].tzinfo == timezone.utc
    assert "page=1" in requested_urls[0]
    assert "per_page=2" in requested_urls[0]
    assert "since=2026-03-25+10%3A00%3A00" in requested_urls[0]
    assert "until=2026-03-25+11%3A00%3A00" in requested_urls[0]
    assert "page=2" in requested_urls[1]
    assert all(response.closed is True for response in all_responses)


def test_load_measurements_accepts_measurements_wrapper(monkeypatch) -> None:
    response = _FakeResponse(
        {
            "measurements": [
                {
                    "id": 201,
                    "value": 0.09,
                    "captured_at": "2026-03-25T12:00:00Z",
                }
            ]
        }
    )

    monkeypatch.setattr(loader, "urlopen", lambda request: response)

    load_result = load_measurements(request=SafecastRadiationRequest(page_size=1))
    measurements = list(load_result.measurements)

    assert measurements[0]["id"] == 201
    assert response.closed is True


class _FakeResponse:
    def __init__(self, payload: object) -> None:
        self._stream = BytesIO(json.dumps(payload).encode("utf-8"))
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)

    def close(self) -> None:
        self.closed = True
