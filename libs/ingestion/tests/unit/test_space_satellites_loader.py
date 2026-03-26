from __future__ import annotations

from datetime import timezone
from io import BytesIO

from ingestion.providers.space_satellites import loader
from ingestion.providers.space_satellites.loader import load_satellite_entries, parse_satellite_entries


def test_parse_satellite_entries_normalizes_celestrak_fields() -> None:
    json_bytes = b"""
    [
      {
        "OBJECT_NAME": "ISS (ZARYA)",
        "OBJECT_ID": "1998-067A",
        "NORAD_CAT_ID": 25544,
        "EPOCH": "2026-03-24T10:15:00Z"
      }
    ]
    """

    entries = list(parse_satellite_entries(json_bytes))

    assert len(entries) == 1
    assert entries[0]["object_name"] == "ISS (ZARYA)"
    assert entries[0]["object_id"] == "1998-067A"
    assert entries[0]["norad_cat_id"] == 25544
    assert entries[0]["epoch"].tzinfo == timezone.utc
    assert entries[0]["id"] == "25544:2026-03-24T10:15:00+00:00"


def test_parse_satellite_entries_falls_back_to_object_name_when_norad_id_is_missing() -> None:
    json_bytes = b"""
    [
      {
        "OBJECT_NAME": "UNKNOWN PAYLOAD",
        "EPOCH": "2026-03-24T10:15:00"
      }
    ]
    """

    entries = list(parse_satellite_entries(json_bytes))

    assert entries[0]["id"] == "UNKNOWN PAYLOAD:2026-03-24T10:15:00+00:00"


def test_load_satellite_entries_keeps_response_open_until_entries_are_consumed(monkeypatch) -> None:
    json_bytes = b"""
    [
      {
        "OBJECT_NAME": "ISS (ZARYA)",
        "NORAD_CAT_ID": 25544,
        "EPOCH": "2026-03-24T10:15:00Z"
      }
    ]
    """
    response = _FakeResponse(json_bytes)

    monkeypatch.setattr(loader, "urlopen", lambda request: response)

    load_result = load_satellite_entries("active")

    assert response.closed is False
    assert [entry["norad_cat_id"] for entry in load_result.entries] == [25544]
    assert response.closed is True
    assert load_result.source_url.endswith("GROUP=ACTIVE&FORMAT=JSON")


class _FakeResponse:
    def __init__(self, body: bytes) -> None:
        self._stream = BytesIO(body)
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise AssertionError("response was closed before the loader finished reading it")
        return self._stream.read(size)

    def close(self) -> None:
        self.closed = True

