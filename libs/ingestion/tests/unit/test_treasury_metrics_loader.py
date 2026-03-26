from __future__ import annotations

import json
from io import BytesIO

from ingestion.providers.treasury_metrics import loader
from ingestion.providers.treasury_metrics.checkpoint import TreasuryMetricsCheckpoint
from ingestion.providers.treasury_metrics.loader import load_treasury_metric_rows


def test_load_treasury_metric_rows_paginates_through_all_pages(monkeypatch) -> None:
    responses = [
        _FakeResponse(
            {
                "data": [
                    {"record_date": "2026-03-25", "metric": "10.1"},
                    {"record_date": "2026-03-24", "metric": "10.0"},
                ],
                "meta": {"total-pages": 2},
            }
        ),
        _FakeResponse(
            {
                "data": [
                    {"record_date": "2026-03-23", "metric": "9.9"},
                ],
                "meta": {"total-pages": 2},
            }
        ),
    ]
    requested_urls: list[str] = []

    def fake_urlopen(request):
        requested_urls.append(request.full_url)
        return responses.pop(0)

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    load_result = load_treasury_metric_rows(
        endpoint="v1/accounting/od/rates_of_exchange",
        cursor_field="record_date",
        page_size=2,
    )

    rows = list(load_result.rows)

    assert [row["record_date"] for row in rows] == [
        "2026-03-25",
        "2026-03-24",
        "2026-03-23",
    ]
    assert "page%5Bnumber%5D=1" in requested_urls[0]
    assert "page%5Bnumber%5D=2" in requested_urls[1]


def test_load_treasury_metric_rows_applies_checkpoint_and_custom_filters(monkeypatch) -> None:
    requested_urls: list[str] = []

    def fake_urlopen(request):
        requested_urls.append(request.full_url)
        return _FakeResponse({"data": [], "meta": {"total-pages": 1}})

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    checkpoint = TreasuryMetricsCheckpoint(
        endpoint="v1/accounting/od/rates_of_exchange",
        cursor="2026-03-24",
    )
    load_result = load_treasury_metric_rows(
        endpoint="v1/accounting/od/rates_of_exchange",
        cursor_field="record_date",
        checkpoint=checkpoint,
        filters=("country_currency_desc:eq:Canada-Dollar",),
    )

    assert list(load_result.rows) == []
    assert (
        "filter=country_currency_desc%3Aeq%3ACanada-Dollar%2Crecord_date%3Agt%3A2026-03-24"
        in requested_urls[0]
    )


def test_load_treasury_metric_rows_normalizes_null_strings(monkeypatch) -> None:
    def fake_urlopen(request):
        return _FakeResponse(
            {
                "data": [
                    {
                        "record_date": "2026-03-25",
                        "exchange_rate": "null",
                        "nested": {"comment": "null"},
                    }
                ],
                "meta": {"total-pages": 1},
            }
        )

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    load_result = load_treasury_metric_rows(
        endpoint="v1/accounting/od/rates_of_exchange",
        cursor_field="record_date",
    )

    rows = list(load_result.rows)

    assert rows == [
        {
            "record_date": "2026-03-25",
            "exchange_rate": None,
            "nested": {"comment": None},
        }
    ]


class _FakeResponse(BytesIO):
    def __init__(self, payload: dict[str, object]) -> None:
        super().__init__(json.dumps(payload).encode("utf-8"))
