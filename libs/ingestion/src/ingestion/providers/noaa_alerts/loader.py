from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypeAlias
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ingestion.providers.noaa_alerts.checkpoint import NoaaAlertsCheckpoint

DEFAULT_ALERTS_URL = "https://api.weather.gov/alerts/active"
DEFAULT_ACCEPT_HEADER = "application/geo+json"
DEFAULT_USER_AGENT = "ingestion-noaa-alerts/0.1"

QueryScalar: TypeAlias = str | int | float | bool
QueryValue: TypeAlias = QueryScalar | Sequence[QueryScalar]


@dataclass(slots=True, frozen=True)
class NoaaAlertsLoadResult:
    entries: Iterable[dict[str, object]]
    etag: str | None = None
    last_modified: str | None = None
    not_modified: bool = False


def build_alerts_url(
    alerts_url: str = DEFAULT_ALERTS_URL,
    query: Mapping[str, QueryValue] | None = None,
) -> str:
    if not query:
        return alerts_url
    return f"{alerts_url}?{urlencode(list(_iter_query_items(query)), doseq=True)}"


def load_alert_entries(
    alerts_url: str = DEFAULT_ALERTS_URL,
    checkpoint: NoaaAlertsCheckpoint | None = None,
    *,
    user_agent: str = DEFAULT_USER_AGENT,
) -> NoaaAlertsLoadResult:
    request = Request(alerts_url, headers=_request_headers(checkpoint, user_agent=user_agent))

    try:
        response = urlopen(request)
        try:
            payload = json.load(response)
            return NoaaAlertsLoadResult(
                entries=tuple(parse_alert_entries(payload)),
                etag=response.headers.get("ETag"),
                last_modified=response.headers.get("Last-Modified"),
            )
        finally:
            response.close()
    except HTTPError as exc:
        if exc.code == 304:
            return NoaaAlertsLoadResult(
                entries=(),
                etag=checkpoint.etag if checkpoint is not None else None,
                last_modified=checkpoint.last_modified if checkpoint is not None else None,
                not_modified=True,
            )
        raise


def parse_alert_entries(payload: Mapping[str, Any]) -> Iterator[dict[str, object]]:
    features = payload.get("features")
    if not isinstance(features, list):
        return

    for feature in features:
        if not isinstance(feature, Mapping):
            continue

        properties = feature.get("properties")
        if not isinstance(properties, Mapping):
            properties = {}

        alert_id = _coerce_text(feature.get("id"))
        if alert_id is None:
            alert_id = _coerce_text(properties.get("@id")) or _coerce_text(properties.get("id"))
        if alert_id is None:
            continue

        sent_at = _parse_datetime(properties.get("sent"))
        effective_at = _parse_datetime(properties.get("effective"))
        onset_at = _parse_datetime(properties.get("onset"))
        expires_at = _parse_datetime(properties.get("expires"))
        ends_at = _parse_datetime(properties.get("ends"))

        yield {
            "id": alert_id,
            "area_desc": _coerce_text(properties.get("areaDesc")),
            "geocode": _coerce_mapping(properties.get("geocode")),
            "affected_zones": _coerce_text_list(properties.get("affectedZones")),
            "references": _parse_references(properties.get("references")),
            "sent_at": sent_at,
            "effective_at": effective_at,
            "onset_at": onset_at,
            "expires_at": expires_at,
            "ends_at": ends_at,
            "status": _coerce_text(properties.get("status")),
            "message_type": _coerce_text(properties.get("messageType")),
            "category": _coerce_text(properties.get("category")),
            "severity": _coerce_text(properties.get("severity")),
            "certainty": _coerce_text(properties.get("certainty")),
            "urgency": _coerce_text(properties.get("urgency")),
            "event": _coerce_text(properties.get("event")),
            "sender": _coerce_text(properties.get("sender")),
            "sender_name": _coerce_text(properties.get("senderName")),
            "headline": _coerce_text(properties.get("headline")),
            "description": _coerce_text(properties.get("description")),
            "instruction": _coerce_text(properties.get("instruction")),
            "response": _coerce_text(properties.get("response")),
            "parameters": _coerce_mapping(properties.get("parameters")),
            "geometry": feature.get("geometry"),
        }


def _iter_query_items(query: Mapping[str, QueryValue]) -> Iterator[tuple[str, QueryScalar]]:
    for key, value in query.items():
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            for item in value:
                yield key, item
            continue
        yield key, value


def _request_headers(
    checkpoint: NoaaAlertsCheckpoint | None,
    *,
    user_agent: str,
) -> dict[str, str]:
    headers = {
        "Accept": DEFAULT_ACCEPT_HEADER,
        "User-Agent": user_agent,
    }
    if checkpoint is None:
        return headers
    if checkpoint.etag:
        headers["If-None-Match"] = checkpoint.etag
    if checkpoint.last_modified:
        headers["If-Modified-Since"] = checkpoint.last_modified
    return headers


def _coerce_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def _coerce_mapping(value: object) -> Mapping[str, object] | None:
    if not isinstance(value, Mapping):
        return None
    return {str(key): item for key, item in value.items()}


def _coerce_text_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    items: list[str] = []
    for item in value:
        text = _coerce_text(item)
        if text is not None:
            items.append(text)
    return items


def _parse_references(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []

    references: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        references.append(
            {
                "id": _coerce_text(item.get("@id")),
                "identifier": _coerce_text(item.get("identifier")),
                "sender": _coerce_text(item.get("sender")),
                "sent_at": _parse_datetime(item.get("sent")),
            }
        )
    return references


def _parse_datetime(value: object) -> datetime | None:
    text = _coerce_text(value)
    if text is None:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None
