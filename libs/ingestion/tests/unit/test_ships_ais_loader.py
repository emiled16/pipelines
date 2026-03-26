from __future__ import annotations

import json
from datetime import timezone

import pytest

from ingestion.providers.ships_ais.loader import (
    ShipsAisSubscription,
    build_subscription_payload,
    parse_ais_message,
)


def test_parse_ais_message_extracts_position_report_fields() -> None:
    raw_message = json.dumps(
        {
            "MessageType": "PositionReport",
            "MetaData": {
                "MMSI": 367763050,
                "ShipName": "EXAMPLE VESSEL",
                "latitude": 36.9,
                "longitude": -122.1,
                "time_utc": "2026-03-25T10:15:00Z",
            },
            "Message": {
                "PositionReport": {
                    "UserID": 367763050,
                    "Latitude": 36.9,
                    "Longitude": -122.1,
                    "Sog": 12.4,
                    "Cog": 85.7,
                    "TrueHeading": 84,
                    "NavigationalStatus": 0,
                }
            },
        }
    )

    parsed = parse_ais_message(raw_message)

    assert parsed is not None
    assert parsed["message_type"] == "PositionReport"
    assert parsed["mmsi"] == 367763050
    assert parsed["ship_name"] == "EXAMPLE VESSEL"
    assert parsed["latitude"] == 36.9
    assert parsed["longitude"] == -122.1
    assert parsed["occurred_at"].tzinfo == timezone.utc


def test_parse_ais_message_extracts_static_data() -> None:
    parsed = parse_ais_message(
        {
            "MessageType": "ShipStaticData",
            "MetaData": {"MMSI": 244750489, "ShipName": "NORDIC LIGHT"},
            "Message": {
                "ShipStaticData": {
                    "UserID": 244750489,
                    "ImoNumber": 9876543,
                    "CallSign": "PCST",
                    "Destination": "ROTTERDAM",
                    "MaximumStaticDraught": 11.8,
                    "Type": 70,
                }
            },
        }
    )

    assert parsed is not None
    assert parsed["message_type"] == "ShipStaticData"
    assert parsed["imo"] == 9876543
    assert parsed["call_sign"] == "PCST"
    assert parsed["destination"] == "ROTTERDAM"
    assert parsed["draught"] == 11.8
    assert parsed["ship_type"] == 70


def test_build_subscription_payload_includes_api_key_and_filters() -> None:
    subscription = ShipsAisSubscription(
        api_key="secret",
        filters={"BoundingBoxes": [[[-90.0, -180.0], [90.0, 180.0]]]},
    )

    payload = build_subscription_payload(subscription)

    assert payload == {
        "APIKey": "secret",
        "BoundingBoxes": [[[-90.0, -180.0], [90.0, 180.0]]],
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }


def test_build_subscription_payload_rejects_reserved_filter_keys() -> None:
    subscription = ShipsAisSubscription(api_key="secret", filters={"APIKey": "override"})

    with pytest.raises(ValueError, match="must not override"):
        build_subscription_payload(subscription)
