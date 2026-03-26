from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime, timezone

from ingestion.providers.comtrade_trade.checkpoint import ComtradeTradeCheckpoint
from ingestion.providers.comtrade_trade.loader import ComtradeTradeLoadResult, ComtradeTradeQuery
from ingestion.providers.comtrade_trade.provider import (
    ComtradeTradeProvider,
    build_trade_query_signature,
    build_trade_row_key,
)


def test_comtrade_trade_provider_yields_all_rows_without_checkpoint() -> None:
    async def run() -> None:
        query = ComtradeTradeQuery(period="2024", reporter_code="840", flow_code="X")
        provider = ComtradeTradeProvider(
            query=query,
            rows_loader=lambda checkpoint: _rows(
                [
                    {"period": 2024, "reporterCode": 840, "flowCode": "X", "cmdCode": "01"},
                    {"period": 2023, "reporterCode": 840, "flowCode": "X", "cmdCode": "02"},
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert len(records) == 2
        assert records[0].metadata["period"] == "2024"
        assert records[0].occurred_at == datetime(2024, 1, 1, tzinfo=timezone.utc)

    asyncio.run(run())


def test_comtrade_trade_provider_stops_once_checkpoint_record_is_reached() -> None:
    async def run() -> None:
        query = ComtradeTradeQuery(period="2024", reporter_code="840", flow_code="X")
        first_row = {"period": 2024, "reporterCode": 840, "flowCode": "X", "cmdCode": "01"}
        second_row = {"period": 2024, "reporterCode": 840, "flowCode": "X", "cmdCode": "02"}
        provider = ComtradeTradeProvider(
            query=query,
            rows_loader=lambda checkpoint: _rows([first_row, second_row]),
        )
        checkpoint = ComtradeTradeCheckpoint(
            query_signature=build_trade_query_signature(query),
            cursor=build_trade_row_key(second_row),
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == [build_trade_row_key(first_row)]

    asyncio.run(run())


def test_comtrade_trade_provider_assigns_one_fetched_at_per_batch() -> None:
    async def run() -> None:
        provider = ComtradeTradeProvider(
            query=ComtradeTradeQuery(period="2024"),
            rows_loader=lambda checkpoint: _rows(
                [
                    {"period": 2024, "reporterCode": 840, "cmdCode": "01"},
                    {"period": 2024, "reporterCode": 840, "cmdCode": "02"},
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert len({record.fetched_at for record in records}) == 1

    asyncio.run(run())


def test_comtrade_trade_provider_builds_checkpoint() -> None:
    query = ComtradeTradeQuery(period="2024", reporter_code="840", flow_code="X")
    provider = ComtradeTradeProvider(query=query)

    checkpoint = provider.build_checkpoint(
        previous_checkpoint=None,
        last_record_key="row-1",
    )

    assert checkpoint == ComtradeTradeCheckpoint(
        query_signature=build_trade_query_signature(query),
        cursor="row-1",
    )


def test_comtrade_trade_provider_preserves_matching_cursor_when_no_rows_are_returned() -> None:
    query = ComtradeTradeQuery(period="2024")
    provider = ComtradeTradeProvider(
        query=query,
        rows_loader=lambda checkpoint: ComtradeTradeLoadResult(rows=[]),
    )
    previous_checkpoint = ComtradeTradeCheckpoint(
        query_signature=build_trade_query_signature(query),
        cursor="row-1",
    )

    checkpoint = provider.build_checkpoint(
        previous_checkpoint=previous_checkpoint,
        last_record_key=None,
    )

    assert checkpoint == previous_checkpoint


def test_comtrade_trade_provider_closes_rows_iterator_when_checkpoint_stops_iteration() -> None:
    async def run() -> None:
        query = ComtradeTradeQuery(period="2024")
        first_row = {"period": 2024, "reporterCode": 840, "cmdCode": "01"}
        second_row = {"period": 2024, "reporterCode": 840, "cmdCode": "02"}
        rows = _ClosableRows([first_row, second_row])
        provider = ComtradeTradeProvider(
            query=query,
            rows_loader=lambda checkpoint: ComtradeTradeLoadResult(rows=rows),
        )
        checkpoint = ComtradeTradeCheckpoint(
            query_signature=build_trade_query_signature(query),
            cursor=build_trade_row_key(second_row),
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == [build_trade_row_key(first_row)]
        assert rows.closed is True

    asyncio.run(run())


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
