from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, BinaryIO
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from ingestion.providers.cisa_kev.checkpoint import CisaKevCheckpoint

DEFAULT_CISA_KEV_URL = (
    "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"
)


@dataclass(slots=True, frozen=True)
class CisaKevCatalog:
    title: str | None
    catalog_version: str | None
    date_released: str | None
    count: int
    vulnerabilities: tuple[dict[str, object], ...]


@dataclass(slots=True, frozen=True)
class CisaKevLoadResult:
    catalog: CisaKevCatalog
    etag: str | None = None
    last_modified: str | None = None
    not_modified: bool = False


def load_catalog(
    catalog_url: str = DEFAULT_CISA_KEV_URL,
    checkpoint: CisaKevCheckpoint | None = None,
) -> CisaKevLoadResult:
    request = Request(catalog_url, headers=_request_headers(checkpoint))

    try:
        with urlopen(request) as response:
            return CisaKevLoadResult(
                catalog=parse_catalog(response),
                etag=response.headers.get("ETag"),
                last_modified=response.headers.get("Last-Modified"),
            )
    except HTTPError as exc:
        if exc.code == 304:
            return CisaKevLoadResult(
                catalog=CisaKevCatalog(
                    title=None,
                    catalog_version=checkpoint.catalog_version if checkpoint is not None else None,
                    date_released=None,
                    count=0,
                    vulnerabilities=(),
                ),
                etag=checkpoint.etag if checkpoint is not None else None,
                last_modified=checkpoint.last_modified if checkpoint is not None else None,
                not_modified=True,
            )
        raise


def parse_catalog(json_source: bytes | BinaryIO) -> CisaKevCatalog:
    payload = _load_json(json_source)
    if not isinstance(payload, Mapping):
        raise ValueError("CISA KEV payload must be a JSON object")

    vulnerabilities = tuple(_iter_vulnerabilities(payload.get("vulnerabilities")))
    raw_count = payload.get("count")
    count = raw_count if isinstance(raw_count, int) else len(vulnerabilities)

    return CisaKevCatalog(
        title=_coerce_optional_str(payload.get("title")),
        catalog_version=_coerce_optional_str(payload.get("catalogVersion")),
        date_released=_coerce_optional_str(payload.get("dateReleased")),
        count=count,
        vulnerabilities=vulnerabilities,
    )


def _load_json(json_source: bytes | BinaryIO) -> Any:
    if isinstance(json_source, bytes):
        return json.loads(json_source)
    return json.load(json_source)


def _iter_vulnerabilities(value: object) -> Iterable[dict[str, object]]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError("CISA KEV vulnerabilities must be a JSON array")

    normalized: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue

        payload = {str(key): item_value for key, item_value in item.items()}
        cve_id = _coerce_optional_str(payload.get("cveID"))
        if cve_id is None:
            continue
        payload["cveID"] = cve_id
        normalized.append(payload)

    return tuple(normalized)


def _request_headers(checkpoint: CisaKevCheckpoint | None) -> dict[str, str]:
    headers: dict[str, str] = {}
    if checkpoint is None:
        return headers
    if checkpoint.etag:
        headers["If-None-Match"] = checkpoint.etag
    if checkpoint.last_modified:
        headers["If-Modified-Since"] = checkpoint.last_modified
    return headers


def _coerce_optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
