from __future__ import annotations

import json
from io import BytesIO
from urllib.parse import parse_qs, urlparse

from ingestion.providers.acled_events import loader
from ingestion.providers.acled_events.loader import AcledEventsPageRequest, load_event_page


def test_load_event_page_extracts_events_and_pagination(monkeypatch) -> None:
    response = _FakeResponse(
        {
            "data": [
                {"event_id_cnty": "111", "event_date": "2026-03-25"},
                {"event_id_cnty": "110", "event_date": "2026-03-24"},
            ],
            "pagination": {"next_page": 2},
        }
    )

    monkeypatch.setattr(loader, "urlopen", lambda request: response)

    result = load_event_page(
        AcledEventsPageRequest(page=1, limit=2),
        endpoint="https://api.acleddata.com/acled/read/",
        email="analyst@example.com",
        api_key="secret",
        params={"country": "Sudan"},
    )

    assert [event["event_id_cnty"] for event in result.events] == ["111", "110"]
    assert result.next_page == 2
    assert response.closed is True


def test_load_event_page_builds_expected_query_string(monkeypatch) -> None:
    captured_urls: list[str] = []

    def fake_urlopen(request):
        captured_urls.append(request.full_url)
        return _FakeResponse({"data": []})

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    load_event_page(
        AcledEventsPageRequest(page=3, limit=250),
        endpoint="https://api.acleddata.com/acled/read/",
        email="analyst@example.com",
        api_key="secret",
        params={"country": "Sudan", "event_type": "Battles"},
    )

    parsed = urlparse(captured_urls[0])
    query = parse_qs(parsed.query)

    assert query["email"] == ["analyst@example.com"]
    assert query["key"] == ["secret"]
    assert query["page"] == ["3"]
    assert query["limit"] == ["250"]
    assert query["format"] == ["json"]
    assert query["country"] == ["Sudan"]
    assert query["event_type"] == ["Battles"]


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._stream = BytesIO(json.dumps(payload).encode("utf-8"))
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise AssertionError("response was closed before the loader finished reading it")
        return self._stream.read(size)

    def close(self) -> None:
        self.closed = True
