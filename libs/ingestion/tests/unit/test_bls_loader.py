from __future__ import annotations

import json
from io import BytesIO

import pytest

from ingestion.providers.bls_indicators import loader
from ingestion.providers.bls_indicators.loader import (
    load_series_observations,
    parse_series_observations,
)


def test_parse_series_observations_flattens_series_data() -> None:
    payload = {
        "status": "REQUEST_SUCCEEDED",
        "message": [],
        "Results": {
            "series": [
                {
                    "seriesID": "LNS14000000",
                    "catalog": {"series_title": "Unemployment rate"},
                    "data": [
                        {
                            "year": "2026",
                            "period": "M02",
                            "periodName": "February",
                            "value": "4.1",
                            "latest": "true",
                            "footnotes": [{"code": "P", "text": "Preliminary."}, {}],
                        }
                    ],
                }
            ]
        },
    }

    observations = list(parse_series_observations(json.dumps(payload).encode("utf-8")))

    assert observations == [
        {
            "series_id": "LNS14000000",
            "year": "2026",
            "period": "M02",
            "period_name": "February",
            "value": "4.1",
            "latest": True,
            "footnotes": [{"code": "P", "text": "Preliminary."}],
            "catalog": {"series_title": "Unemployment rate"},
        }
    ]


def test_parse_series_observations_accepts_results_wrapped_in_a_list() -> None:
    payload = {
        "status": "REQUEST_SUCCEEDED",
        "message": [],
        "Results": [
            {
                "series": [
                    {
                        "seriesID": "CES0000000001",
                        "data": [{"year": "2026", "period": "M01", "value": "100"}],
                    }
                ]
            }
        ],
    }

    observations = list(parse_series_observations(BytesIO(json.dumps(payload).encode("utf-8"))))

    assert observations[0]["series_id"] == "CES0000000001"
    assert observations[0]["period"] == "M01"


def test_load_series_observations_chunks_requests_to_bls_limits(monkeypatch) -> None:
    requests: list[dict[str, object]] = []

    def fake_urlopen(request, timeout: float = 30.0):  # type: ignore[no-untyped-def]
        payload = json.loads(request.data.decode("utf-8"))
        requests.append(
            {
                "url": request.full_url,
                "headers": dict(request.header_items()),
                "payload": payload,
                "timeout": timeout,
            }
        )
        body = json.dumps(
            {
                "status": "REQUEST_SUCCEEDED",
                "message": [],
                "Results": {
                    "series": [
                        {
                            "seriesID": series_id,
                            "data": [{"year": "2026", "period": "M01", "value": "1"}],
                        }
                        for series_id in payload["seriesid"]
                    ]
                },
            }
        ).encode("utf-8")
        return _FakeResponse(body)

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    series_ids = [f"SERIES{i:02d}" for i in range(26)]
    load_result = load_series_observations(series_ids=series_ids)

    assert len(requests) == 2
    assert len(requests[0]["payload"]["seriesid"]) == 25
    assert len(requests[1]["payload"]["seriesid"]) == 1
    assert requests[0]["url"] == loader.BLS_API_URL
    assert requests[0]["headers"]["Content-type"] == "application/json"
    assert requests[0]["timeout"] == 30.0
    assert len(list(load_result.observations)) == 26


def test_load_series_observations_requires_registration_for_catalog_access() -> None:
    with pytest.raises(ValueError, match="registration_key is required"):
        load_series_observations(
            series_ids=["LNS14000000"],
            catalog=True,
        )


def test_load_series_observations_validates_requested_year_window() -> None:
    with pytest.raises(ValueError, match="at most 10 years"):
        load_series_observations(
            series_ids=["LNS14000000"],
            start_year=2010,
            end_year=2020,
        )


class _FakeResponse:
    def __init__(self, body: bytes) -> None:
        self._stream = BytesIO(body)

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)

    def close(self) -> None:
        return None
