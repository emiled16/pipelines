from __future__ import annotations

from datetime import UTC
from io import BytesIO

from ingestion.providers.firms_fires import loader
from ingestion.providers.firms_fires.loader import (
    build_area_csv_url,
    load_fire_entries,
    parse_fire_entries,
)


def test_build_area_csv_url_preserves_area_commas() -> None:
    url = build_area_csv_url(
        api_key="secret/key",
        source="VIIRS_SNPP_NRT",
        area="-124.5,32.5,-113.5,42.0",
        days=3,
    )

    assert url == (
        "https://firms.modaps.eosdis.nasa.gov/api/area/csv/secret%2Fkey/VIIRS_SNPP_NRT/"
        "-124.5,32.5,-113.5,42.0/3"
    )


def test_parse_fire_entries_extracts_timestamp_and_stable_id() -> None:
    csv_bytes = b"""latitude,longitude,acq_date,acq_time,frp,satellite\n"""
    csv_bytes += b"""34.12,-118.45,2026-03-24,0315,2.1,N\n"""

    entries = list(parse_fire_entries(csv_bytes))

    assert len(entries) == 1
    assert entries[0]["latitude"] == "34.12"
    assert entries[0]["longitude"] == "-118.45"
    assert entries[0]["frp"] == "2.1"
    assert entries[0]["satellite"] == "N"
    assert entries[0]["occurred_at"].tzinfo is UTC
    assert entries[0]["occurred_at"].isoformat() == "2026-03-24T03:15:00+00:00"
    assert len(str(entries[0]["id"])) == 64


def test_parse_fire_entries_skips_blank_rows() -> None:
    csv_bytes = b"""latitude,longitude,acq_date,acq_time\n"""
    csv_bytes += b""",,,\n"""

    assert list(parse_fire_entries(csv_bytes)) == []


def test_load_fire_entries_keeps_response_open_until_rows_are_consumed(monkeypatch) -> None:
    csv_bytes = b"""latitude,longitude,acq_date,acq_time\n34,-118,2026-03-24,0315\n"""
    response = _FakeResponse(csv_bytes)

    monkeypatch.setattr(loader, "urlopen", lambda request: response)

    load_result = load_fire_entries(
        api_key="secret",
        source="VIIRS_SNPP_NRT",
        area="-124,32,-113,42",
        days=1,
    )

    assert response.closed is False
    assert len(list(load_result.entries)) == 1
    assert response.closed is True


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
