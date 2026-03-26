from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from urllib.parse import urlencode

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.cloudflare_radar.checkpoint import CloudflareRadarCheckpoint
from ingestion.providers.cloudflare_radar.loader import (
    DEFAULT_CLOUDFLARE_API_BASE_URL,
    CloudflareRadarLoadResult,
    TrafficAnomaliesRequest,
    TrafficAnomalyStatus,
    TrafficAnomalyType,
    load_traffic_anomalies,
)
from ingestion.utils.time import utc_now

TrafficAnomaly = dict[str, object]
TrafficAnomaliesLoader = Callable[
    [TrafficAnomaliesRequest],
    CloudflareRadarLoadResult
    | Iterable[TrafficAnomaly]
    | Awaitable[CloudflareRadarLoadResult | Iterable[TrafficAnomaly]],
]


@dataclass(slots=True, frozen=True)
class CloudflareRadarQuery:
    asn: int | None = None
    location: str | None = None
    origin: str | None = None
    status: TrafficAnomalyStatus | None = None
    types: tuple[TrafficAnomalyType, ...] = ()


class CloudflareRadarProvider(BatchProvider[CloudflareRadarCheckpoint]):
    dataset = "traffic_anomalies"

    def __init__(
        self,
        *,
        api_token: str,
        date_range: str | None = None,
        date_start: datetime | None = None,
        date_end: datetime | None = None,
        asn: int | None = None,
        location: str | None = None,
        origin: str | None = None,
        status: TrafficAnomalyStatus | None = None,
        types: Iterable[TrafficAnomalyType] = (),
        page_size: int = 100,
        anomalies_loader: TrafficAnomaliesLoader | None = None,
        name: str | None = None,
        base_url: str = DEFAULT_CLOUDFLARE_API_BASE_URL,
    ) -> None:
        if not api_token:
            raise ValueError("api_token must not be empty")
        if page_size <= 0:
            raise ValueError("page_size must be greater than 0")
        if date_range is not None and (date_start is not None or date_end is not None):
            raise ValueError("date_range cannot be combined with date_start or date_end")

        normalized_types = tuple(dict.fromkeys(types))

        self._date_range = date_range
        self._date_start = date_start
        self._date_end = date_end
        self._page_size = page_size
        self._query = CloudflareRadarQuery(
            asn=asn,
            location=location,
            origin=origin,
            status=status,
            types=normalized_types,
        )
        self.name = name or _default_name(self._query)
        self._anomalies_loader = anomalies_loader or partial(
            load_traffic_anomalies,
            api_token=api_token,
            base_url=base_url,
        )

    async def fetch(
        self,
        *,
        checkpoint: CloudflareRadarCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        active_checkpoint = (
            checkpoint if checkpoint is not None and checkpoint.scope == self.name else None
        )
        batch_fetched_at = utc_now()
        offset = 0

        while True:
            load_result = await self._load_anomalies(
                self._request_for_page(checkpoint=active_checkpoint, offset=offset)
            )
            anomalies = sorted(load_result.anomalies, key=_anomaly_sort_key, reverse=True)
            if not anomalies:
                return

            for anomaly in anomalies:
                if _is_older_than_checkpoint(anomaly, active_checkpoint):
                    return
                if _is_seen_at_checkpoint_cursor(anomaly, active_checkpoint):
                    continue

                yield Record(
                    provider=self.name,
                    key=_anomaly_id(anomaly),
                    payload=dict(anomaly),
                    occurred_at=_anomaly_started_at(anomaly),
                    fetched_at=batch_fetched_at,
                    metadata=self._metadata(),
                )

            if len(anomalies) < self._page_size:
                return
            offset += self._page_size

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: CloudflareRadarCheckpoint | None,
        records: Iterable[Record],
    ) -> CloudflareRadarCheckpoint | None:
        records = list(records)
        if not records:
            if previous_checkpoint is not None and previous_checkpoint.scope == self.name:
                return previous_checkpoint
            return None

        occurred_at_values = [
            record.occurred_at for record in records if record.occurred_at is not None
        ]
        if not occurred_at_values:
            if previous_checkpoint is not None and previous_checkpoint.scope == self.name:
                return previous_checkpoint
            return None

        newest_started_at = max(occurred_at_values)
        newest_ids = {
            record.key
            for record in records
            if record.key is not None and record.occurred_at == newest_started_at
        }
        if not newest_ids:
            if previous_checkpoint is not None and previous_checkpoint.scope == self.name:
                return previous_checkpoint
            return None

        if (
            previous_checkpoint is not None
            and previous_checkpoint.scope == self.name
            and previous_checkpoint.cursor_started_at == newest_started_at
        ):
            newest_ids.update(previous_checkpoint.anomaly_ids_at_cursor)

        return CloudflareRadarCheckpoint(
            scope=self.name,
            cursor_started_at=newest_started_at,
            anomaly_ids_at_cursor=tuple(sorted(newest_ids)),
        )

    async def _load_anomalies(
        self,
        request: TrafficAnomaliesRequest,
    ) -> CloudflareRadarLoadResult:
        result = self._anomalies_loader(request)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, CloudflareRadarLoadResult):
            return result
        return CloudflareRadarLoadResult(anomalies=list(result))

    def _metadata(self) -> dict[str, object]:
        metadata: dict[str, object] = {"dataset": self.dataset}
        if self._query.asn is not None:
            metadata["asn"] = self._query.asn
        if self._query.location is not None:
            metadata["location"] = self._query.location
        if self._query.origin is not None:
            metadata["origin"] = self._query.origin
        if self._query.status is not None:
            metadata["status"] = self._query.status
        if self._query.types:
            metadata["types"] = list(self._query.types)
        return metadata

    def _request_for_page(
        self,
        *,
        checkpoint: CloudflareRadarCheckpoint | None,
        offset: int,
    ) -> TrafficAnomaliesRequest:
        return TrafficAnomaliesRequest(
            date_range=self._date_range if checkpoint is None else None,
            date_start=_effective_date_start(self._date_start, checkpoint),
            date_end=self._date_end,
            asn=self._query.asn,
            location=self._query.location,
            origin=self._query.origin,
            status=self._query.status,
            types=self._query.types,
            limit=self._page_size,
            offset=offset,
        )


def _default_name(query: CloudflareRadarQuery) -> str:
    params: list[tuple[str, str]] = []
    if query.asn is not None:
        params.append(("asn", str(query.asn)))
    if query.location is not None:
        params.append(("location", query.location))
    if query.origin is not None:
        params.append(("origin", query.origin))
    if query.status is not None:
        params.append(("status", query.status))
    for anomaly_type in query.types:
        params.append(("type", anomaly_type))

    if not params:
        return "cloudflare_radar:traffic_anomalies"
    return f"cloudflare_radar:traffic_anomalies?{urlencode(params, doseq=True)}"


def _effective_date_start(
    configured_date_start: datetime | None,
    checkpoint: CloudflareRadarCheckpoint | None,
) -> datetime | None:
    if checkpoint is None:
        return configured_date_start
    if configured_date_start is None:
        return checkpoint.cursor_started_at
    return max(configured_date_start, checkpoint.cursor_started_at)


def _anomaly_sort_key(anomaly: dict[str, object]) -> tuple[datetime, str]:
    started_at = _anomaly_started_at(anomaly)
    if started_at is None:
        raise ValueError("Cloudflare Radar anomaly is missing start_date")
    return started_at, _anomaly_id(anomaly)


def _anomaly_started_at(anomaly: dict[str, object]) -> datetime | None:
    value = anomaly.get("start_date")
    return value if isinstance(value, datetime) else None


def _anomaly_id(anomaly: dict[str, object]) -> str:
    value = anomaly.get("uuid")
    if not isinstance(value, str) or not value:
        raise ValueError("Cloudflare Radar anomaly is missing uuid")
    return value


def _is_older_than_checkpoint(
    anomaly: dict[str, object],
    checkpoint: CloudflareRadarCheckpoint | None,
) -> bool:
    if checkpoint is None:
        return False
    started_at = _anomaly_started_at(anomaly)
    return started_at is not None and started_at < checkpoint.cursor_started_at


def _is_seen_at_checkpoint_cursor(
    anomaly: dict[str, object],
    checkpoint: CloudflareRadarCheckpoint | None,
) -> bool:
    if checkpoint is None:
        return False
    started_at = _anomaly_started_at(anomaly)
    if started_at != checkpoint.cursor_started_at:
        return False
    return _anomaly_id(anomaly) in set(checkpoint.anomaly_ids_at_cursor)
