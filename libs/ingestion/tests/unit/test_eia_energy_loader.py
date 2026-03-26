from __future__ import annotations

import json
from urllib.parse import parse_qs, urlparse

from ingestion.providers.eia_energy.loader import EiaSort, load_data_rows


def test_load_data_rows_builds_expected_query_and_parses_response(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _Response:
        def __enter__(self) -> _Response:
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def read(self, *args, **kwargs) -> bytes:
            return json.dumps(
                {
                    "response": {
                        "total": "2",
                        "frequency": "monthly",
                        "dateFormat": "YYYY-MM",
                        "data": [
                            {"period": "2026-02", "stateid": "CO", "price": "7.47"},
                            {"period": "2026-01", "stateid": "CO", "price": "7.37"},
                        ],
                    }
                }
            ).encode("utf-8")

    def fake_urlopen(request, timeout=None):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        return _Response()

    monkeypatch.setattr("ingestion.providers.eia_energy.loader.urlopen", fake_urlopen)

    result = load_data_rows(
        api_key="secret",
        route="electricity/retail-sales",
        data=["price"],
        facets={"sectorid": ["RES"], "stateid": ["CO"]},
        frequency="monthly",
        start="2025-12-31",
        end="2026-02-01",
        sort=[EiaSort(column="period", direction="desc")],
        offset=25,
        length=50,
        timeout=3.5,
    )

    parsed = urlparse(str(captured["url"]))
    params = parse_qs(parsed.query)

    assert parsed.path == "/v2/electricity/retail-sales/data"
    assert params["api_key"] == ["secret"]
    assert params["data[]"] == ["price"]
    assert params["facets[sectorid][]"] == ["RES"]
    assert params["facets[stateid][]"] == ["CO"]
    assert params["frequency"] == ["monthly"]
    assert params["start"] == ["2025-12-31"]
    assert params["end"] == ["2026-02-01"]
    assert params["sort[0][column]"] == ["period"]
    assert params["sort[0][direction]"] == ["desc"]
    assert params["offset"] == ["25"]
    assert params["length"] == ["50"]
    assert captured["timeout"] == 3.5

    assert result.total == 2
    assert result.frequency == "monthly"
    assert result.date_format == "YYYY-MM"
    assert list(result.rows) == [
        {"period": "2026-02", "stateid": "CO", "price": "7.47"},
        {"period": "2026-01", "stateid": "CO", "price": "7.37"},
    ]


def test_load_data_rows_requires_response_data_array(monkeypatch) -> None:
    class _Response:
        def __enter__(self) -> _Response:
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def read(self, *args, **kwargs) -> bytes:
            return json.dumps({"response": {"total": "0"}}).encode("utf-8")

    monkeypatch.setattr(
        "ingestion.providers.eia_energy.loader.urlopen",
        lambda request, timeout=None: _Response(),
    )

    try:
        load_data_rows(api_key="secret", route="electricity/retail-sales")
    except ValueError as exc:
        assert "response.data" in str(exc)
    else:
        raise AssertionError("Expected ValueError when response.data is missing")
