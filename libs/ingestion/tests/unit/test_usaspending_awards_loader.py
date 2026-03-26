from __future__ import annotations

import json
from io import BytesIO

from ingestion.providers.usaspending_awards import loader
from ingestion.providers.usaspending_awards.loader import load_awards_page


def test_load_awards_page_posts_json_body_and_parses_results(monkeypatch) -> None:
    captured_request: dict[str, object] = {}

    def fake_urlopen(request, timeout: float):
        captured_request["full_url"] = request.full_url
        captured_request["method"] = request.get_method()
        captured_request["headers"] = {
            key.lower(): value for key, value in request.header_items()
        }
        captured_request["body"] = json.loads(request.data.decode("utf-8"))
        captured_request["timeout"] = timeout
        return _FakeResponse(
            {
                "results": [{"generated_internal_id": "award-1"}],
                "page_metadata": {"hasNext": True},
            }
        )

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    result = load_awards_page(
        page=2,
        limit=50,
        search_body={"filters": {"award_type_codes": ["02"]}},
        timeout=12.5,
    )

    assert captured_request["full_url"] == (
        "https://api.usaspending.gov/api/v2/search/spending_by_award/"
    )
    assert captured_request["method"] == "POST"
    assert captured_request["headers"]["content-type"] == "application/json"
    assert captured_request["body"] == {
        "filters": {"award_type_codes": ["02"]},
        "sort": "Last Modified Date",
        "order": "desc",
        "page": 2,
        "limit": 50,
    }
    assert captured_request["timeout"] == 12.5
    assert list(result.awards) == [{"generated_internal_id": "award-1"}]
    assert result.has_next_page is True


def test_load_awards_page_falls_back_to_result_count_when_metadata_is_missing(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        loader,
        "urlopen",
        lambda request, timeout: _FakeResponse({"results": [{"id": 1}, {"id": 2}]}),
    )

    result = load_awards_page(page=1, limit=2)

    assert list(result.awards) == [{"id": 1}, {"id": 2}]
    assert result.has_next_page is True


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._stream = BytesIO(json.dumps(payload).encode("utf-8"))

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)

    def close(self) -> None:
        self._stream.close()
