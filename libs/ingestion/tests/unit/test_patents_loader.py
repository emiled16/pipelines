from __future__ import annotations

import json
from io import BytesIO

from ingestion.providers.patents import loader
from ingestion.providers.patents.checkpoint import PatentsCheckpoint
from ingestion.providers.patents.loader import DEFAULT_PATENT_FIELDS, load_patents


def test_load_patents_posts_expected_payload_and_headers(monkeypatch) -> None:
    requests: list[object] = []
    response = _FakeResponse(
        {
            "error": False,
            "count": 1,
            "total_hits": 1,
            "patents": [
                {
                    "patent_id": "01234567",
                    "patent_date": "2026-03-25",
                    "patent_title": "Example",
                }
            ],
        }
    )

    def fake_urlopen(request):
        requests.append(request)
        return response

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    load_result = load_patents(
        api_key="test-key",
        query={"patent_type": "utility"},
        page_size=250,
    )

    assert [patent["patent_id"] for patent in load_result.patents] == ["01234567"]
    assert response.closed is True
    assert len(requests) == 1

    request = requests[0]
    assert request.get_method() == "POST"
    assert request.full_url == loader.PATENTS_ENDPOINT
    assert request.get_header("X-api-key") == "test-key"
    assert request.get_header("Content-type") == "application/json"

    payload = json.loads(request.data.decode("utf-8"))
    assert payload["q"] == {"patent_type": "utility"}
    assert payload["f"] == list(DEFAULT_PATENT_FIELDS)
    assert payload["s"] == [{"patent_date": "desc"}, {"patent_id": "desc"}]
    assert payload["o"] == {
        "exclude_withdrawn": True,
        "pad_patent_id": True,
        "size": 250,
    }


def test_load_patents_uses_checkpoint_floor_and_cursor_pagination(monkeypatch) -> None:
    requests: list[object] = []
    responses = iter(
        [
            _FakeResponse(
                {
                    "error": False,
                    "count": 2,
                    "total_hits": 3,
                    "patents": [
                        {"patent_id": "01234568", "patent_date": "2026-03-26"},
                        {"patent_id": "01234567", "patent_date": "2026-03-25"},
                    ],
                }
            ),
            _FakeResponse(
                {
                    "error": False,
                    "count": 1,
                    "total_hits": 3,
                    "patents": [
                        {"patent_id": "01234566", "patent_date": "2026-03-24"},
                    ],
                }
            ),
        ]
    )

    def fake_urlopen(request):
        requests.append(request)
        return next(responses)

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    load_result = load_patents(
        api_key="test-key",
        query={"patent_type": "utility"},
        checkpoint=PatentsCheckpoint(
            query_hash="unused-in-loader",
            last_patent_id="01234565",
            last_patent_date="2026-03-24",
        ),
        page_size=2,
    )

    assert [patent["patent_id"] for patent in load_result.patents] == [
        "01234568",
        "01234567",
        "01234566",
    ]
    assert len(requests) == 2

    first_payload = json.loads(requests[0].data.decode("utf-8"))
    second_payload = json.loads(requests[1].data.decode("utf-8"))

    assert first_payload["q"] == {
        "_and": [
            {"patent_type": "utility"},
            {"_gte": {"patent_date": "2026-03-24"}},
        ]
    }
    assert "after" not in first_payload["o"]
    assert second_payload["o"]["after"] == ["2026-03-25", "01234567"]


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
