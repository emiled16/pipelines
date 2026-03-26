from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DEFAULT_CLOUDFLARE_API_BASE_URL = "https://api.cloudflare.com/client/v4"

TrafficAnomalyStatus = Literal["VERIFIED", "UNVERIFIED"]
TrafficAnomalyType = Literal["LOCATION", "AS", "ORIGIN"]


@dataclass(slots=True, frozen=True)
class TrafficAnomaliesRequest:
    date_range: str | None = None
    date_start: datetime | None = None
    date_end: datetime | None = None
    asn: int | None = None
    location: str | None = None
    origin: str | None = None
    status: TrafficAnomalyStatus | None = None
    types: tuple[TrafficAnomalyType, ...] = ()
    limit: int = 100
    offset: int = 0

    def __post_init__(self) -> None:
        if self.limit <= 0:
            raise ValueError("limit must be greater than 0")
        if self.offset < 0:
            raise ValueError("offset must be greater than or equal to 0")
        if self.date_range is not None and (
            self.date_start is not None or self.date_end is not None
        ):
            raise ValueError("date_range cannot be combined with date_start or date_end")


@dataclass(slots=True, frozen=True)
class CloudflareRadarLoadResult:
    anomalies: list[dict[str, object]]


class CloudflareRadarApiError(RuntimeError):
    pass


def load_traffic_anomalies(
    *,
    api_token: str,
    request: TrafficAnomaliesRequest,
    base_url: str = DEFAULT_CLOUDFLARE_API_BASE_URL,
) -> CloudflareRadarLoadResult:
    http_request = Request(
        _build_url(base_url=base_url, request=request),
        headers={
            "Authorization": f"Bearer {api_token}",
            "Accept": "application/json",
        },
    )
    with urlopen(http_request) as response:
        payload = json.load(response)

    if payload.get("success") is not True:
        raise CloudflareRadarApiError(_format_errors(payload.get("errors")))

    result = payload.get("result")
    if not isinstance(result, dict):
        raise CloudflareRadarApiError("Cloudflare Radar response did not include an object result")

    traffic_anomalies = result.get("trafficAnomalies")
    if traffic_anomalies is None:
        return CloudflareRadarLoadResult(anomalies=[])
    if not isinstance(traffic_anomalies, list):
        raise CloudflareRadarApiError(
            "Cloudflare Radar response result.trafficAnomalies was not a list"
        )

    return CloudflareRadarLoadResult(
        anomalies=[_normalize_traffic_anomaly(anomaly) for anomaly in traffic_anomalies]
    )


def _build_url(*, base_url: str, request: TrafficAnomaliesRequest) -> str:
    params: list[tuple[str, str]] = [("format", "JSON"), ("limit", str(request.limit))]
    if request.offset:
        params.append(("offset", str(request.offset)))
    if request.date_range is not None:
        params.append(("dateRange", request.date_range))
    if request.date_start is not None:
        params.append(("dateStart", _format_datetime(request.date_start)))
    if request.date_end is not None:
        params.append(("dateEnd", _format_datetime(request.date_end)))
    if request.asn is not None:
        params.append(("asn", str(request.asn)))
    if request.location is not None:
        params.append(("location", request.location))
    if request.origin is not None:
        params.append(("origin", request.origin))
    if request.status is not None:
        params.append(("status", request.status))
    for anomaly_type in request.types:
        params.append(("type", anomaly_type))
    return f"{base_url.rstrip('/')}/radar/traffic_anomalies?{urlencode(params, doseq=True)}"


def _normalize_traffic_anomaly(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise CloudflareRadarApiError("Cloudflare Radar anomaly payload was not an object")

    uuid = payload.get("uuid")
    start_date = _parse_datetime(payload.get("startDate"))
    if not isinstance(uuid, str) or not uuid:
        raise CloudflareRadarApiError("Cloudflare Radar anomaly payload did not include a UUID")
    if start_date is None:
        raise CloudflareRadarApiError("Cloudflare Radar anomaly payload did not include startDate")

    anomaly: dict[str, object] = {
        "id": uuid,
        "uuid": uuid,
        "start_date": start_date,
        "status": payload.get("status"),
        "type": payload.get("type"),
        "end_date": _parse_datetime(payload.get("endDate")),
        "asn_details": _coerce_object(payload.get("asnDetails")),
        "location_details": _coerce_object(payload.get("locationDetails")),
        "origin_details": _coerce_object(payload.get("originDetails")),
    }

    visible_in_data_sources = payload.get("visibleInDataSources")
    if isinstance(visible_in_data_sources, list):
        anomaly["visible_in_data_sources"] = [
            value for value in visible_in_data_sources if isinstance(value, str)
        ]
    else:
        anomaly["visible_in_data_sources"] = None

    return anomaly


def _coerce_object(value: object) -> dict[str, object] | None:
    return dict(value) if isinstance(value, dict) else None


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None

    normalized_value = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized_value)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _format_datetime(value: datetime) -> str:
    utc_value = value.astimezone(timezone.utc)
    return utc_value.isoformat().replace("+00:00", "Z")


def _format_errors(errors: object) -> str:
    if not isinstance(errors, list) or not errors:
        return "Cloudflare Radar API request failed"

    messages: list[str] = []
    for error in errors:
        if not isinstance(error, dict):
            continue
        code = error.get("code")
        message = error.get("message")
        if message is None:
            continue
        if code is None:
            messages.append(str(message))
            continue
        messages.append(f"{code}: {message}")

    return "; ".join(messages) if messages else "Cloudflare Radar API request failed"
