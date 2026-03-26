from __future__ import annotations

import json
from datetime import timezone
from io import BytesIO

from ingestion.providers.reliefweb_reports import loader
from ingestion.providers.reliefweb_reports.loader import (
    ReliefWebReportsRequest,
    load_reports_page,
    parse_reports_page,
)


def test_parse_reports_page_extracts_common_fields_and_pagination() -> None:
    payload = {
        "totalCount": 3,
        "count": 2,
        "data": [
            {
                "id": 123,
                "href": "https://api.reliefweb.int/v2/reports/123",
                "fields": {
                    "title": "Flood update",
                    "url": "https://reliefweb.int/report/example/flood-update",
                    "date": {
                        "created": "2026-03-25T10:15:00+00:00",
                        "changed": "2026-03-25T11:45:00+00:00",
                    },
                },
            },
            {
                "id": 122,
                "href": "https://api.reliefweb.int/v2/reports/122",
                "fields": {
                    "title": "Situation report",
                    "url": "https://reliefweb.int/report/example/sitrep",
                    "date": {
                        "created": "2026-03-24T09:00:00+00:00",
                    },
                },
            },
        ],
    }

    page = parse_reports_page(
        json.dumps(payload).encode("utf-8"),
        request_offset=0,
        request_limit=2,
    )

    assert [report["id"] for report in page.reports] == ["123", "122"]
    assert page.reports[0]["title"] == "Flood update"
    assert page.reports[0]["date_created"].tzinfo == timezone.utc
    assert page.reports[0]["date_changed"].tzinfo == timezone.utc
    assert page.next_offset == 2


def test_load_reports_page_posts_expected_request_body_and_closes_response(monkeypatch) -> None:
    response = _FakeResponse(
        {
            "totalCount": 1,
            "count": 1,
            "data": [
                {
                    "id": 123,
                    "fields": {
                        "title": "Flood update",
                        "url": "https://reliefweb.int/report/example/flood-update",
                        "date": {
                            "created": "2026-03-25T10:15:00+00:00",
                        },
                    },
                }
            ],
        }
    )
    captured_request = None

    def fake_urlopen(request):
        nonlocal captured_request
        captured_request = request
        return response

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    page = load_reports_page(
        ReliefWebReportsRequest(
            appname="example-codex",
            offset=10,
            limit=25,
            query={"value": "earthquake"},
            filter={"field": "country", "value": "Kenya"},
        )
    )

    assert [report["id"] for report in page.reports] == ["123"]
    assert response.closed is True
    assert captured_request is not None
    assert captured_request.full_url == "https://api.reliefweb.int/v2/reports?appname=example-codex"
    assert captured_request.get_method() == "POST"

    request_body = json.loads(captured_request.data.decode("utf-8"))
    assert request_body["offset"] == 10
    assert request_body["limit"] == 25
    assert request_body["preset"] == "latest"
    assert request_body["sort"] == ["date.created:desc", "id:desc"]
    assert request_body["query"] == {"value": "earthquake"}
    assert request_body["filter"] == {"field": "country", "value": "Kenya"}


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._stream = BytesIO(json.dumps(payload).encode("utf-8"))
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)

    def close(self) -> None:
        self.closed = True
