from __future__ import annotations

from io import BytesIO

import pytest

from ingestion.providers.cisa_kev import loader
from ingestion.providers.cisa_kev.checkpoint import CisaKevCheckpoint
from ingestion.providers.cisa_kev.loader import DEFAULT_CISA_KEV_URL, load_catalog, parse_catalog


def test_parse_catalog_extracts_top_level_metadata_and_vulnerabilities() -> None:
    payload = b"""
    {
      "title": "CISA Catalog of Known Exploited Vulnerabilities",
      "catalogVersion": "2026.03.25",
      "dateReleased": "2026-03-25T10:15:00Z",
      "count": 1,
      "vulnerabilities": [
        {
          "cveID": "CVE-2026-0001",
          "vendorProject": "Example Vendor",
          "product": "Example Product",
          "vulnerabilityName": "Example Vulnerability",
          "dateAdded": "2026-03-24",
          "shortDescription": "Remote code execution.",
          "requiredAction": "Apply the vendor update.",
          "dueDate": "2026-04-14",
          "knownRansomwareCampaignUse": "Known",
          "notes": "Actively exploited."
        }
      ]
    }
    """

    catalog = parse_catalog(payload)

    assert catalog.title == "CISA Catalog of Known Exploited Vulnerabilities"
    assert catalog.catalog_version == "2026.03.25"
    assert catalog.date_released == "2026-03-25T10:15:00Z"
    assert catalog.count == 1
    assert catalog.vulnerabilities[0]["cveID"] == "CVE-2026-0001"
    assert catalog.vulnerabilities[0]["dueDate"] == "2026-04-14"


def test_parse_catalog_skips_vulnerabilities_without_cve_id() -> None:
    payload = b"""
    {
      "vulnerabilities": [
        {"vendorProject": "Missing CVE"},
        {"cveID": "CVE-2026-0002", "product": "Kept"}
      ]
    }
    """

    catalog = parse_catalog(payload)

    assert catalog.count == 1
    assert [item["cveID"] for item in catalog.vulnerabilities] == ["CVE-2026-0002"]


def test_parse_catalog_rejects_non_object_payloads() -> None:
    with pytest.raises(ValueError):
        parse_catalog(b'["not-an-object"]')


def test_load_catalog_uses_conditional_request_headers(monkeypatch) -> None:
    response = _FakeResponse(
        b'{"catalogVersion": "2026.03.25", "vulnerabilities": [{"cveID": "CVE-2026-0001"}]}',
        headers={"ETag": '"etag-2"', "Last-Modified": "Wed, 25 Mar 2026 10:15:00 GMT"},
    )
    checkpoint = CisaKevCheckpoint(
        catalog_version="2026.03.24",
        etag='"etag-1"',
        last_modified="Tue, 24 Mar 2026 10:15:00 GMT",
    )
    captured_headers: dict[str, str] = {}

    def fake_urlopen(request):
        captured_headers.update(request.headers)
        return response

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    load_result = load_catalog(DEFAULT_CISA_KEV_URL, checkpoint)

    assert captured_headers["If-none-match"] == '"etag-1"'
    assert captured_headers["If-modified-since"] == "Tue, 24 Mar 2026 10:15:00 GMT"
    assert load_result.catalog.catalog_version == "2026.03.25"
    assert load_result.etag == '"etag-2"'


def test_cisa_kev_checkpoint_supports_http_cache_fields() -> None:
    checkpoint = CisaKevCheckpoint(
        catalog_version="2026.03.25",
        etag='"etag-2"',
        last_modified="Wed, 25 Mar 2026 10:15:00 GMT",
    )

    assert checkpoint.catalog_version == "2026.03.25"
    assert checkpoint.etag == '"etag-2"'
    assert checkpoint.last_modified == "Wed, 25 Mar 2026 10:15:00 GMT"


class _FakeResponse:
    def __init__(self, body: bytes, *, headers: dict[str, str]) -> None:
        self._stream = BytesIO(body)
        self.headers = headers

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)

    def close(self) -> None:
        self._stream.close()
