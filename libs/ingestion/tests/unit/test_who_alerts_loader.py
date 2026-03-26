from __future__ import annotations

import json
from io import BytesIO
from urllib.error import HTTPError

from ingestion.providers.who_alerts import loader
from ingestion.providers.who_alerts.checkpoint import WhoAlertsCheckpoint
from ingestion.providers.who_alerts.loader import (
    DEFAULT_WHO_ALERTS_API_URL,
    load_alert_entries,
    parse_alert_entries,
)


def test_parse_alert_entries_extracts_common_fields() -> None:
    payload = _payload(
        [
            {
                "Id": "alert-1",
                "DonId": "2026-DON596",
                "Title": "Title",
                "OverrideTitle": "Override",
                "UseOverrideTitle": True,
                "Summary": "Summary",
                "PublicationDateAndTime": "2026-03-13T19:00:00Z",
                "LastModified": "2026-03-13T19:05:00Z",
                "FormattedDate": "13 March 2026",
                "UrlName": "2026-DON596",
                "ItemDefaultUrl": "/2026-DON596",
            }
        ]
    )

    entries = list(parse_alert_entries(payload))

    assert len(entries) == 1
    assert entries[0]["id"] == "alert-1"
    assert entries[0]["don_id"] == "2026-DON596"
    assert entries[0]["title"] == "Override"
    assert entries[0]["url"] == (
        "https://www.who.int/emergencies/disease-outbreak-news/item/2026-DON596"
    )
    assert entries[0]["published_at"].isoformat() == "2026-03-13T19:00:00+00:00"
    assert entries[0]["updated_at"].isoformat() == "2026-03-13T19:05:00+00:00"


def test_parse_alert_entries_falls_back_to_title_and_item_default_url() -> None:
    payload = _payload(
        [
            {
                "Id": "alert-2",
                "Title": "Fallback title",
                "UseOverrideTitle": False,
                "ItemDefaultUrl": "/2008_01_21-en",
            }
        ]
    )

    entries = list(parse_alert_entries(payload))

    assert entries[0]["title"] == "Fallback title"
    assert entries[0]["url"] == (
        "https://www.who.int/emergencies/disease-outbreak-news/item/2008_01_21-en"
    )


def test_load_alert_entries_paginates_and_sends_cache_headers(monkeypatch) -> None:
    requests: list[object] = []
    responses = {
        "0": _FakeResponse(
            _payload(
                [
                    {"Id": "alert-3", "Title": "Newest", "PublicationDate": "2026-03-14T00:00:00Z"},
                    {"Id": "alert-2", "Title": "Middle", "PublicationDate": "2026-03-13T00:00:00Z"},
                ]
            ),
            headers={"ETag": '"etag-1"', "Last-Modified": "Fri, 14 Mar 2026 10:00:00 GMT"},
        ),
        "2": _FakeResponse(
            _payload(
                [
                    {"Id": "alert-1", "Title": "Oldest", "PublicationDate": "2026-03-12T00:00:00Z"},
                ]
            )
        ),
    }

    def fake_urlopen(request):
        requests.append(request)
        if (
            request.full_url.startswith(DEFAULT_WHO_ALERTS_API_URL)
            and "%24skip=0" in request.full_url
        ):
            return responses["0"]
        if (
            request.full_url.startswith(DEFAULT_WHO_ALERTS_API_URL)
            and "%24skip=2" in request.full_url
        ):
            return responses["2"]
        raise AssertionError(f"unexpected URL: {request.full_url}")

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    checkpoint = WhoAlertsCheckpoint(
        api_url=DEFAULT_WHO_ALERTS_API_URL,
        last_alert_id="alert-0",
        etag='"etag-0"',
        last_modified="Thu, 13 Mar 2026 10:00:00 GMT",
    )

    load_result = load_alert_entries(checkpoint=checkpoint, page_size=2)
    entries = list(load_result.entries)

    assert [entry["id"] for entry in entries] == ["alert-3", "alert-2", "alert-1"]
    assert load_result.etag == '"etag-1"'
    assert load_result.last_modified == "Fri, 14 Mar 2026 10:00:00 GMT"
    assert requests[0].headers["If-none-match"] == '"etag-0"'
    assert requests[0].headers["If-modified-since"] == "Thu, 13 Mar 2026 10:00:00 GMT"
    assert requests[1].headers == {"Accept": "application/json"}


def test_load_alert_entries_returns_not_modified_result_on_http_304(monkeypatch) -> None:
    checkpoint = WhoAlertsCheckpoint(
        api_url=DEFAULT_WHO_ALERTS_API_URL,
        last_alert_id="alert-3",
        etag='"etag-2"',
        last_modified="Fri, 14 Mar 2026 10:00:00 GMT",
    )

    def fake_urlopen(request):
        raise HTTPError(request.full_url, 304, "Not Modified", hdrs=None, fp=None)

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    load_result = load_alert_entries(checkpoint=checkpoint)

    assert list(load_result.entries) == []
    assert load_result.not_modified is True
    assert load_result.etag == '"etag-2"'
    assert load_result.last_modified == "Fri, 14 Mar 2026 10:00:00 GMT"


def _payload(items: list[dict[str, object]]) -> bytes:
    return json.dumps({"value": items}).encode("utf-8")


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
