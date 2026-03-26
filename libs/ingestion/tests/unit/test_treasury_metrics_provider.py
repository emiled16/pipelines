from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import UTC, datetime

from ingestion.providers.treasury_metrics.checkpoint import TreasuryMetricsCheckpoint
from ingestion.providers.treasury_metrics.provider import TreasuryMetricsProvider


def test_treasury_metrics_provider_yields_records_with_batch_timestamp() -> None:
    async def run() -> None:
        provider = TreasuryMetricsProvider(
            endpoint="v1/accounting/od/rates_of_exchange",
            cursor_field="record_date",
            key_fields=("country_currency_desc", "record_date"),
            rows_loader=lambda checkpoint: _rows(
                [
                    {
                        "country_currency_desc": "Canada-Dollar",
                        "record_date": "2026-03-25",
                        "exchange_rate": "1.41",
                    },
                    {
                        "country_currency_desc": "Mexico-Peso",
                        "record_date": "2026-03-24",
                        "exchange_rate": "18.5",
                    },
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert [record.payload["country_currency_desc"] for record in records] == [
            "Canada-Dollar",
            "Mexico-Peso",
        ]
        assert len({record.fetched_at for record in records}) == 1
        assert records[0].occurred_at == datetime(2026, 3, 25, tzinfo=UTC)
        assert records[0].metadata["endpoint"] == "v1/accounting/od/rates_of_exchange"

    asyncio.run(run())


def test_treasury_metrics_provider_builds_checkpoint_from_latest_cursor() -> None:
    async def run() -> None:
        provider = TreasuryMetricsProvider(
            endpoint="v1/accounting/od/rates_of_exchange",
            cursor_field="record_date",
            rows_loader=lambda checkpoint: _rows(
                [
                    {"record_date": "2026-03-25", "exchange_rate": "1.41"},
                    {"record_date": "2026-03-24", "exchange_rate": "1.40"},
                ]
            ),
        )

        records = [record async for record in provider.fetch()]
        checkpoint = provider.build_checkpoint(previous_checkpoint=None)

        assert len(records) == 2
        assert checkpoint == TreasuryMetricsCheckpoint(
            endpoint="v1/accounting/od/rates_of_exchange",
            cursor="2026-03-25",
        )

    asyncio.run(run())


def test_treasury_metrics_provider_preserves_existing_checkpoint_without_new_rows() -> None:
    async def run() -> None:
        provider = TreasuryMetricsProvider(
            endpoint="v1/accounting/od/rates_of_exchange",
            cursor_field="record_date",
            rows_loader=lambda checkpoint: _rows([]),
        )
        previous_checkpoint = TreasuryMetricsCheckpoint(
            endpoint="v1/accounting/od/rates_of_exchange",
            cursor="2026-03-24",
        )

        records = [record async for record in provider.fetch(checkpoint=previous_checkpoint)]
        checkpoint = provider.build_checkpoint(previous_checkpoint=previous_checkpoint)

        assert records == []
        assert checkpoint == previous_checkpoint

    asyncio.run(run())


def test_treasury_metrics_provider_closes_rows_iterator() -> None:
    async def run() -> None:
        rows = _ClosableRows(
            [
                {"record_date": "2026-03-25", "exchange_rate": "1.41"},
            ]
        )
        provider = TreasuryMetricsProvider(
            endpoint="v1/accounting/od/rates_of_exchange",
            cursor_field="record_date",
            rows_loader=lambda checkpoint: rows,
        )

        records = [record async for record in provider.fetch()]

        assert len(records) == 1
        assert rows.closed is True

    asyncio.run(run())


def test_treasury_metrics_provider_adds_required_fields_to_explicit_field_list() -> None:
    provider = TreasuryMetricsProvider(
        endpoint="v1/accounting/od/rates_of_exchange",
        cursor_field="record_date",
        occurred_at_field="effective_date",
        key_fields=("country_currency_desc",),
        fields=("exchange_rate",),
        rows_loader=lambda checkpoint: _rows([]),
    )

    assert provider.fields == (
        "exchange_rate",
        "record_date",
        "effective_date",
        "country_currency_desc",
    )


def _rows(items: list[dict[str, object]]) -> Iterable[dict[str, object]]:
    for item in items:
        yield item


class _ClosableRows:
    def __init__(self, items: list[dict[str, object]]) -> None:
        self._items = iter(items)
        self.closed = False

    def __iter__(self) -> _ClosableRows:
        return self

    def __next__(self) -> dict[str, object]:
        return next(self._items)

    def close(self) -> None:
        self.closed = True
