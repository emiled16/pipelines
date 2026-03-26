from __future__ import annotations

import json
from datetime import datetime
from io import BytesIO

from ingestion.providers.opensanctions_entities import loader
from ingestion.providers.opensanctions_entities.checkpoint import (
    OpenSanctionsEntitiesCheckpoint,
)
from ingestion.providers.opensanctions_entities.loader import (
    load_entities,
    parse_entity_entries,
)


def test_parse_entity_entries_reads_line_delimited_json() -> None:
    payload = b"""
    {"id":"os-1","caption":"Alice","schema":"Person"}

    {"id":"os-2","caption":"Bob","schema":"Person"}
    """

    entries = list(parse_entity_entries(payload))

    assert [entry["id"] for entry in entries] == ["os-1", "os-2"]
    assert entries[0]["caption"] == "Alice"


def test_load_entities_keeps_response_open_until_entries_are_consumed(monkeypatch) -> None:
    dataset_index = {
        "version": "20260326125427-hxr",
        "updated_at": "2026-03-26T12:54:27",
        "last_change": "2026-03-26T12:36:01",
        "resources": [
            {
                "name": "entities.ftm.json",
                "url": "https://data.opensanctions.org/datasets/latest/default/entities.ftm.json",
                "checksum": "abc123",
            }
        ],
    }
    responses = {
        "https://data.opensanctions.org/datasets/latest/default/index.json": _JsonResponse(
            dataset_index
        ),
        "https://data.opensanctions.org/datasets/latest/default/entities.ftm.json": _BytesResponse(
            b'{"id":"os-1","caption":"Alice"}\n'
        ),
    }

    monkeypatch.setattr(loader, "urlopen", lambda request: responses[request.full_url])

    load_result = load_entities("default")
    entity_response = responses["https://data.opensanctions.org/datasets/latest/default/entities.ftm.json"]

    assert entity_response.closed is False
    assert [entry["id"] for entry in load_result.entries] == ["os-1"]
    assert entity_response.closed is True
    assert load_result.updated_at == datetime(2026, 3, 26, 12, 54, 27)


def test_load_entities_skips_entity_download_when_version_matches_checkpoint(monkeypatch) -> None:
    dataset_index = {
        "version": "20260326125427-hxr",
        "resources": [
            {
                "name": "entities.ftm.json",
                "url": "https://data.opensanctions.org/datasets/latest/default/entities.ftm.json",
                "checksum": "abc123",
            }
        ],
    }
    checkpoint = OpenSanctionsEntitiesCheckpoint(
        dataset="default",
        version="20260326125427-hxr",
        entities_url="https://data.opensanctions.org/datasets/latest/default/entities.ftm.json",
        checksum="abc123",
    )

    def fake_urlopen(request):
        if request.full_url.endswith("/entities.ftm.json"):
            raise AssertionError("entities payload should not be fetched when the version is unchanged")
        return _JsonResponse(dataset_index)

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    load_result = load_entities("default", checkpoint=checkpoint)

    assert load_result.not_modified is True
    assert list(load_result.entries) == []
    assert load_result.version == checkpoint.version


class _JsonResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._stream = BytesIO(json.dumps(payload).encode("utf-8"))
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise AssertionError("response was closed before the loader finished reading it")
        return self._stream.read(size)

    def close(self) -> None:
        self.closed = True


class _BytesResponse:
    def __init__(self, payload: bytes) -> None:
        self._stream = BytesIO(payload)
        self.closed = False

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        line = self.readline()
        if line == b"":
            raise StopIteration
        return line

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise AssertionError("response was closed before the loader finished reading it")
        return self._stream.read(size)

    def readline(self, size: int = -1) -> bytes:
        if self.closed:
            raise AssertionError("response was closed before the loader finished reading it")
        return self._stream.readline(size)

    def close(self) -> None:
        self.closed = True
