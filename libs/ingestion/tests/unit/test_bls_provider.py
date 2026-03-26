from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime, timezone

from ingestion.providers.bls_indicators.checkpoint import BlsIndicatorsCheckpoint
from ingestion.providers.bls_indicators.provider import BlsIndicatorsProvider


def test_bls_provider_yields_all_observations_without_checkpoint() -> None:
    async def run() -> None:
        provider = BlsIndicatorsProvider(
            series_ids=["LNS14000000", "CES0000000001"],
            observations_loader=lambda checkpoint: _observations(
                [
                    {
                        "series_id": "LNS14000000",
                        "year": "2026",
                        "period": "M02",
                        "value": "4.1",
                    },
                    {
                        "series_id": "CES0000000001",
                        "year": "2026",
                        "period": "M02",
                        "value": "160000",
                    },
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == [
            "LNS14000000:2026:M02",
            "CES0000000001:2026:M02",
        ]
        assert records[0].metadata["series_id"] == "LNS14000000"
        assert records[0].occurred_at == datetime(2026, 2, 1, tzinfo=timezone.utc)

    asyncio.run(run())


def test_bls_provider_uses_series_specific_checkpoints() -> None:
    async def run() -> None:
        provider = BlsIndicatorsProvider(
            series_ids=["LNS14000000", "CES0000000001"],
            observations_loader=lambda checkpoint: _observations(
                [
                    {
                        "series_id": "LNS14000000",
                        "year": "2026",
                        "period": "M03",
                        "value": "4.0",
                    },
                    {
                        "series_id": "LNS14000000",
                        "year": "2026",
                        "period": "M02",
                        "value": "4.1",
                    },
                    {
                        "series_id": "CES0000000001",
                        "year": "2026",
                        "period": "M03",
                        "value": "161000",
                    },
                    {
                        "series_id": "CES0000000001",
                        "year": "2026",
                        "period": "M02",
                        "value": "160000",
                    },
                ]
            ),
        )
        checkpoint = BlsIndicatorsCheckpoint(
            series_ids=("LNS14000000", "CES0000000001"),
            cursor="LNS14000000:2026:M02",
            series_cursors={
                "LNS14000000": "LNS14000000:2026:M02",
                "CES0000000001": "CES0000000001:2026:M02",
            },
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == [
            "LNS14000000:2026:M03",
            "CES0000000001:2026:M03",
        ]

    asyncio.run(run())


def test_bls_provider_assigns_one_fetched_at_per_batch() -> None:
    async def run() -> None:
        provider = BlsIndicatorsProvider(
            series_ids=["LNS14000000"],
            observations_loader=lambda checkpoint: _observations(
                [
                    {
                        "series_id": "LNS14000000",
                        "year": "2026",
                        "period": "M02",
                        "value": "4.1",
                    },
                    {
                        "series_id": "LNS14000000",
                        "year": "2026",
                        "period": "M01",
                        "value": "4.2",
                    },
                ]
            ),
        )

        records = [record async for record in provider.fetch()]

        assert len({record.fetched_at for record in records}) == 1

    asyncio.run(run())


def test_bls_provider_builds_checkpoint_from_latest_seen_observation_per_series() -> None:
    async def run() -> None:
        provider = BlsIndicatorsProvider(
            series_ids=["LNS14000000", "CES0000000001"],
            observations_loader=lambda checkpoint: _load_result(
                observations=[
                    {
                        "series_id": "LNS14000000",
                        "year": "2026",
                        "period": "M03",
                        "value": "4.0",
                    },
                    {
                        "series_id": "CES0000000001",
                        "year": "2026",
                        "period": "M03",
                        "value": "161000",
                    },
                ],
                messages=["partial warning"],
            ),
        )

        records = [record async for record in provider.fetch()]
        checkpoint = provider.build_checkpoint(
            previous_checkpoint=None,
            last_entry_id=records[0].key,
        )

        assert provider.messages == ("partial warning",)
        assert checkpoint == BlsIndicatorsCheckpoint(
            series_ids=("LNS14000000", "CES0000000001"),
            cursor="LNS14000000:2026:M03",
            series_cursors={
                "LNS14000000": "LNS14000000:2026:M03",
                "CES0000000001": "CES0000000001:2026:M03",
            },
        )

    asyncio.run(run())


def test_bls_provider_preserves_previous_checkpoint_when_no_new_observations_arrive() -> None:
    async def run() -> None:
        provider = BlsIndicatorsProvider(
            series_ids=["LNS14000000"],
            observations_loader=lambda checkpoint: _load_result(observations=[]),
        )
        previous_checkpoint = BlsIndicatorsCheckpoint(
            series_ids=("LNS14000000",),
            cursor="LNS14000000:2026:M02",
            series_cursors={"LNS14000000": "LNS14000000:2026:M02"},
        )

        records = [record async for record in provider.fetch(checkpoint=previous_checkpoint)]
        checkpoint = provider.build_checkpoint(
            previous_checkpoint=previous_checkpoint,
            last_entry_id=None,
        )

        assert records == []
        assert checkpoint == previous_checkpoint

    asyncio.run(run())


def _observations(items: list[dict[str, object]]) -> Iterable[dict[str, object]]:
    for item in items:
        yield item


def _load_result(
    *,
    observations: Iterable[dict[str, object]],
    messages: Iterable[str] = (),
):
    from ingestion.providers.bls_indicators.loader import BlsLoadResult

    return BlsLoadResult(
        observations=_observations(list(observations)),
        messages=tuple(messages),
    )
