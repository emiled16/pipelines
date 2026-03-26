from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from ingestion.models.record import Record
from ingestion.providers.cloudflare_radar.checkpoint import CloudflareRadarCheckpoint
from ingestion.providers.cloudflare_radar.loader import CloudflareRadarLoadResult
from ingestion.providers.cloudflare_radar.provider import CloudflareRadarProvider


def test_cloudflare_radar_provider_yields_records_across_pages() -> None:
    async def run() -> None:
        requests = []

        def loader(request):
            requests.append(request)
            if request.offset == 0:
                return CloudflareRadarLoadResult(
                    anomalies=[
                        _anomaly("a-2", "2026-03-25T11:00:00Z"),
                        _anomaly("a-1", "2026-03-25T12:00:00Z"),
                    ]
                )
            if request.offset == 2:
                return CloudflareRadarLoadResult(
                    anomalies=[
                        _anomaly("a-3", "2026-03-25T10:00:00Z"),
                    ]
                )
            return CloudflareRadarLoadResult(anomalies=[])

        provider = CloudflareRadarProvider(
            api_token="secret-token",
            page_size=2,
            anomalies_loader=loader,
            location="US",
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == ["a-1", "a-2", "a-3"]
        assert records[0].metadata == {"dataset": "traffic_anomalies", "location": "US"}
        assert len({record.fetched_at for record in records}) == 1
        assert [request.offset for request in requests] == [0, 2]

    asyncio.run(run())


def test_cloudflare_radar_provider_skips_seen_anomalies_at_checkpoint_cursor() -> None:
    async def run() -> None:
        provider = CloudflareRadarProvider(
            api_token="secret-token",
            page_size=10,
            anomalies_loader=lambda request: CloudflareRadarLoadResult(
                anomalies=[
                    _anomaly("a-new", "2026-03-25T12:00:00Z"),
                    _anomaly("a-seen", "2026-03-25T12:00:00Z"),
                    _anomaly("older", "2026-03-25T11:00:00Z"),
                ]
            ),
            location="US",
        )
        checkpoint = CloudflareRadarCheckpoint(
            scope=provider.name,
            cursor_started_at=datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
            anomaly_ids_at_cursor=("a-seen",),
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert [record.key for record in records] == ["a-new"]

    asyncio.run(run())


def test_cloudflare_radar_provider_uses_checkpoint_date_start_for_incremental_fetch() -> None:
    async def run() -> None:
        seen_request = None

        def loader(request):
            nonlocal seen_request
            seen_request = request
            return CloudflareRadarLoadResult(anomalies=[])

        provider = CloudflareRadarProvider(
            api_token="secret-token",
            date_range="7d",
            page_size=10,
            anomalies_loader=loader,
            location="US",
        )
        checkpoint = CloudflareRadarCheckpoint(
            scope=provider.name,
            cursor_started_at=datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
            anomaly_ids_at_cursor=("a-seen",),
        )

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert records == []
        assert seen_request is not None
        assert seen_request.date_range is None
        assert seen_request.date_start == checkpoint.cursor_started_at

    asyncio.run(run())


def test_cloudflare_radar_provider_builds_checkpoint_from_newest_timestamp() -> None:
    provider = CloudflareRadarProvider(api_token="secret-token", location="US")

    checkpoint = provider.build_checkpoint(
        previous_checkpoint=None,
        records=[
            _record(provider, "a-1", "2026-03-25T12:00:00Z"),
            _record(provider, "a-2", "2026-03-25T12:00:00Z"),
            _record(provider, "a-3", "2026-03-25T11:00:00Z"),
        ],
    )

    assert checkpoint == CloudflareRadarCheckpoint(
        scope=provider.name,
        cursor_started_at=datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
        anomaly_ids_at_cursor=("a-1", "a-2"),
    )


def test_cloudflare_radar_provider_merges_checkpoint_ids_when_cursor_does_not_advance() -> None:
    provider = CloudflareRadarProvider(api_token="secret-token", location="US")
    previous_checkpoint = CloudflareRadarCheckpoint(
        scope=provider.name,
        cursor_started_at=datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
        anomaly_ids_at_cursor=("a-1",),
    )

    checkpoint = provider.build_checkpoint(
        previous_checkpoint=previous_checkpoint,
        records=[_record(provider, "a-2", "2026-03-25T12:00:00Z")],
    )

    assert checkpoint == CloudflareRadarCheckpoint(
        scope=provider.name,
        cursor_started_at=datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
        anomaly_ids_at_cursor=("a-1", "a-2"),
    )


def _anomaly(anomaly_id: str, started_at: str) -> dict[str, object]:
    return {
        "id": anomaly_id,
        "uuid": anomaly_id,
        "start_date": datetime.fromisoformat(started_at.replace("Z", "+00:00")),
        "status": "VERIFIED",
        "type": "LOCATION",
        "end_date": None,
        "asn_details": None,
        "location_details": {"code": "US", "name": "United States"},
        "origin_details": None,
        "visible_in_data_sources": ["radar"],
    }


def _record(provider: CloudflareRadarProvider, anomaly_id: str, started_at: str) -> Record:
    occurred_at = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    return Record(
        provider=provider.name,
        key=anomaly_id,
        payload={"uuid": anomaly_id, "start_date": occurred_at},
        occurred_at=occurred_at,
    )
