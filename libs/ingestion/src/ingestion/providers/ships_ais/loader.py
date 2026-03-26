from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

DEFAULT_AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"
DEFAULT_MESSAGE_TYPES = ("PositionReport", "ShipStaticData")


@dataclass(slots=True, frozen=True)
class ShipsAisSubscription:
    api_key: str
    filters: Mapping[str, Any] = field(default_factory=dict)
    message_types: tuple[str, ...] | None = DEFAULT_MESSAGE_TYPES
    url: str = DEFAULT_AISSTREAM_URL


def build_subscription_payload(subscription: ShipsAisSubscription) -> dict[str, Any]:
    if "APIKey" in subscription.filters or "FilterMessageTypes" in subscription.filters:
        raise ValueError("filters must not override APIKey or FilterMessageTypes")

    payload = {
        "APIKey": subscription.api_key,
        **_json_compatible_mapping(subscription.filters),
    }
    if subscription.message_types is not None:
        payload["FilterMessageTypes"] = list(subscription.message_types)
    return payload


async def open_ais_stream(subscription: ShipsAisSubscription) -> AsyncIterator[dict[str, Any]]:
    connect = _load_websocket_connect()

    async with connect(subscription.url) as websocket:
        await websocket.send(json.dumps(build_subscription_payload(subscription)))
        async for raw_message in websocket:
            parsed_message = parse_ais_message(raw_message)
            if parsed_message is not None:
                yield parsed_message


def parse_ais_message(raw_message: str | bytes | Mapping[str, Any]) -> dict[str, Any] | None:
    message = _decode_message(raw_message)
    message_type = _coerce_str(message.get("MessageType"))
    if message_type is None:
        return None

    metadata = _mapping_copy(message.get("MetaData"))
    message_envelope = _mapping_copy(message.get("Message"))
    message_body = _extract_message_body(message_envelope, message_type)
    if message_body is None:
        return None

    occurred_at = _parse_datetime(
        metadata.get("time_utc")
        or metadata.get("timeUtc")
        or metadata.get("timestamp")
        or message_body.get("time_utc")
        or message_body.get("timeUtc")
    )
    mmsi = _coerce_int(
        message_body.get("UserID") or message_body.get("MMSI") or metadata.get("MMSI")
    )
    latitude = _coerce_float(message_body.get("Latitude") or metadata.get("latitude"))
    longitude = _coerce_float(message_body.get("Longitude") or metadata.get("longitude"))

    return {
        "message_type": message_type,
        "mmsi": mmsi,
        "ship_name": _coerce_str(metadata.get("ShipName") or metadata.get("shipName")),
        "imo": _coerce_int(message_body.get("ImoNumber") or message_body.get("IMO")),
        "call_sign": _coerce_str(message_body.get("CallSign")),
        "latitude": latitude,
        "longitude": longitude,
        "navigational_status": _coerce_int(message_body.get("NavigationalStatus")),
        "speed_over_ground": _coerce_float(message_body.get("Sog")),
        "course_over_ground": _coerce_float(message_body.get("Cog")),
        "true_heading": _coerce_float(message_body.get("TrueHeading")),
        "destination": _coerce_str(message_body.get("Destination")),
        "eta": _parse_datetime(message_body.get("Eta")),
        "ship_type": _coerce_int(message_body.get("Type")),
        "draught": _coerce_float(message_body.get("MaximumStaticDraught")),
        "dimension": _mapping_copy(message_body.get("Dimension")),
        "occurred_at": occurred_at,
        "metadata": metadata,
        "message": message_body,
    }


def _load_websocket_connect():
    try:
        from websockets.asyncio.client import connect
    except ModuleNotFoundError:
        try:
            from websockets import connect
        except ModuleNotFoundError as exc:  # pragma: no cover - depends on optional dependency
            raise ModuleNotFoundError(
                "websockets is required for live AIS streaming; inject a custom message_source "
                "or install the optional client dependency."
            ) from exc
    return connect


def _decode_message(raw_message: str | bytes | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(raw_message, Mapping):
        return dict(raw_message)
    if isinstance(raw_message, bytes):
        raw_message = raw_message.decode("utf-8")
    loaded = json.loads(raw_message)
    if not isinstance(loaded, dict):
        raise ValueError("AIS messages must decode to JSON objects")
    return loaded


def _extract_message_body(
    message_envelope: Mapping[str, Any],
    message_type: str,
) -> dict[str, Any] | None:
    typed_message = message_envelope.get(message_type)
    if isinstance(typed_message, Mapping):
        return dict(typed_message)
    if len(message_envelope) == 1:
        only_value = next(iter(message_envelope.values()))
        if isinstance(only_value, Mapping):
            return dict(only_value)
    if message_envelope:
        return dict(message_envelope)
    return None


def _json_compatible_mapping(mapping: Mapping[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in mapping.items():
        payload[str(key)] = _json_compatible_value(value)
    return payload


def _json_compatible_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return _json_compatible_mapping(value)
    if isinstance(value, tuple):
        return [_json_compatible_value(item) for item in value]
    if isinstance(value, list):
        return [_json_compatible_value(item) for item in value]
    return value


def _mapping_copy(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _coerce_str(value: Any) -> str | None:
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return None


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _coerce_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None

    candidate = value.strip()
    if not candidate:
        return None
    if candidate.endswith("Z"):
        candidate = f"{candidate[:-1]}+00:00"
    try:
        return datetime.fromisoformat(candidate)
    except ValueError:
        return None
