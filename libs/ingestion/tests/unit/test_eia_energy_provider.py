from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime, timezone

from ingestion.providers.eia_energy.checkpoint import EiaEnergyCheckpoint
from ingestion.providers.eia_energy.loader import EiaEnergyLoadResult, EiaSort
from ingestion.providers.eia_energy.provider import EiaEnergyProvider


def test_eia_energy_provider_paginates_and_yields_all_rows() -> None:
    calls: list[tuple[int, int]] = []

    async def run() -> None:
        provider = EiaEnergyProvider(
            api_key="secret",
            route="electricity/retail-sales",
            data=["price"],
            facets={"sectorid": ["RES"], "stateid": ["CO"]},
            key_fields=["period", "stateid", "sectorid"],
            page_size=2,
            rows_loader=lambda checkpoint, offset, length: _load_result(
                rows=_page(
                    [
                        {"period": "2026-03", "stateid": "CO", "sectorid": "RES", "price": "8.1"},
                        {"period": "2026-02", "stateid": "CO", "sectorid": "RES", "price": "7.8"},
                    ]
                    if offset == 0
                    else [{"period": "2026-01", "stateid": "CO", "sectorid": "RES", "price": "7.5"}]
                ),
                frequency="monthly",
                date_format="YYYY-MM",
                recorder=calls,
                offset=offset,
                length=length,
            ),
        )

        records = [record async for record in provider.fetch()]

        assert [record.payload["period"] for record in records] == ["2026-03", "2026-02", "2026-01"]
        assert all(record.metadata["route"] == "electricity/retail-sales" for record in records)
        assert records[0].occurred_at == datetime(2026, 3, 1, tzinfo=timezone.utc)
        assert len({record.fetched_at for record in records}) == 1
        assert calls == [(0, 2), (2, 2)]

    asyncio.run(run())


def test_eia_energy_provider_stops_at_checkpoint_cursor() -> None:
    async def run() -> None:
        provider = EiaEnergyProvider(
            api_key="secret",
            route="electricity/retail-sales",
            data=["price"],
            key_fields=["period", "stateid"],
            rows_loader=lambda checkpoint, offset, length: _load_result(
                rows=_page(
                    [
                        {"period": "2026-03", "stateid": "CO", "price": "8.1"},
                        {"period": "2026-02", "stateid": "CO", "price": "7.8"},
                        {"period": "2026-01", "stateid": "CO", "price": "7.5"},
                    ]
                ),
                frequency="monthly",
                date_format="YYYY-MM",
            ),
        )
        checkpoint = EiaEnergyCheckpoint(
            route="electricity/retail-sales",
            cursor='period="2026-02"|stateid="CO"',
            last_period="2026-02",
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ['period="2026-03"|stateid="CO"']

    asyncio.run(run())


def test_eia_energy_provider_builds_checkpoint_from_latest_record() -> None:
    async def run() -> None:
        provider = EiaEnergyProvider(
            api_key="secret",
            route="electricity/retail-sales",
            data=["price"],
            sort=[EiaSort(column="period", direction="desc")],
            key_fields=["period", "stateid"],
            rows_loader=lambda checkpoint, offset, length: _load_result(
                rows=_page([{"period": "2026-03", "stateid": "CO", "price": "8.1"}]),
                frequency="monthly",
                date_format="YYYY-MM",
            ),
        )

        records = [record async for record in provider.fetch()]
        checkpoint = provider.build_checkpoint(
            previous_checkpoint=None,
            cursor=records[0].key,
            last_period=str(records[0].payload["period"]),
        )

        assert checkpoint == EiaEnergyCheckpoint(
            route="electricity/retail-sales",
            cursor='period="2026-03"|stateid="CO"',
            last_period="2026-03",
        )

    asyncio.run(run())


def test_eia_energy_provider_closes_rows_iterator_when_checkpoint_stops_iteration() -> None:
    async def run() -> None:
        rows = _ClosableRows(
            [
                {"period": "2026-03", "stateid": "CO", "price": "8.1"},
                {"period": "2026-02", "stateid": "CO", "price": "7.8"},
                {"period": "2026-01", "stateid": "CO", "price": "7.5"},
            ]
        )
        provider = EiaEnergyProvider(
            api_key="secret",
            route="electricity/retail-sales",
            data=["price"],
            key_fields=["period", "stateid"],
            rows_loader=lambda checkpoint, offset, length: EiaEnergyLoadResult(
                rows=rows,
                frequency="monthly",
                date_format="YYYY-MM",
            ),
        )
        checkpoint = EiaEnergyCheckpoint(
            route="electricity/retail-sales",
            cursor='period="2026-02"|stateid="CO"',
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ['period="2026-03"|stateid="CO"']
        assert rows.closed is True

    asyncio.run(run())


def _page(items: list[dict[str, object]]) -> Iterable[dict[str, object]]:
    for item in items:
        yield item


def _load_result(
    *,
    rows: Iterable[dict[str, object]],
    frequency: str | None = None,
    date_format: str | None = None,
    recorder: list[tuple[int, int]] | None = None,
    offset: int | None = None,
    length: int | None = None,
) -> EiaEnergyLoadResult:
    if recorder is not None and offset is not None and length is not None:
        recorder.append((offset, length))
    return EiaEnergyLoadResult(
        rows=rows,
        frequency=frequency,
        date_format=date_format,
    )


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
