from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from ingestion.providers.cisa_kev.checkpoint import CisaKevCheckpoint
from ingestion.providers.cisa_kev.loader import CisaKevCatalog, CisaKevLoadResult
from ingestion.providers.cisa_kev.provider import CisaKevProvider


def test_cisa_kev_provider_yields_full_catalog_without_checkpoint() -> None:
    async def run() -> None:
        provider = CisaKevProvider(
            catalog_loader=lambda checkpoint: _catalog(
                catalog_version="2026.03.25",
                vulnerabilities=[
                    {
                        "cveID": "CVE-2026-0002",
                        "vendorProject": "Example Vendor",
                        "dateAdded": "2026-03-24",
                    },
                    {
                        "cveID": "CVE-2026-0001",
                        "vendorProject": "Example Vendor",
                        "dateAdded": "2026-03-23",
                    },
                ],
            ),
        )

        records = [record async for record in provider.fetch()]

        assert [record.key for record in records] == ["CVE-2026-0002", "CVE-2026-0001"]
        assert records[0].provider == "cisa_kev"
        assert records[0].metadata["catalog_version"] == "2026.03.25"
        assert records[0].occurred_at == datetime(2026, 3, 24, tzinfo=UTC)

    asyncio.run(run())


def test_cisa_kev_provider_skips_unchanged_catalog_versions() -> None:
    async def run() -> None:
        provider = CisaKevProvider(
            catalog_loader=lambda checkpoint: _catalog(
                catalog_version="2026.03.25",
                vulnerabilities=[{"cveID": "CVE-2026-0001", "dateAdded": "2026-03-24"}],
            ),
        )
        checkpoint = CisaKevCheckpoint(catalog_version="2026.03.25")

        records = [record async for record in provider.fetch(checkpoint=checkpoint)]

        assert records == []

    asyncio.run(run())


def test_cisa_kev_provider_assigns_one_fetched_at_per_batch() -> None:
    async def run() -> None:
        provider = CisaKevProvider(
            catalog_loader=lambda checkpoint: _catalog(
                catalog_version="2026.03.25",
                vulnerabilities=[
                    {"cveID": "CVE-2026-0002"},
                    {"cveID": "CVE-2026-0001"},
                ],
            ),
        )

        records = [record async for record in provider.fetch()]

        assert len({record.fetched_at for record in records}) == 1

    asyncio.run(run())


def test_cisa_kev_provider_builds_checkpoint_with_catalog_version_and_http_cache_metadata() -> None:
    async def run() -> None:
        provider = CisaKevProvider(
            catalog_loader=lambda checkpoint: _load_result(
                catalog_version="2026.03.25",
                vulnerabilities=[{"cveID": "CVE-2026-0001"}],
                etag='"etag-2"',
                last_modified="Wed, 25 Mar 2026 10:15:00 GMT",
            ),
        )

        _ = [record async for record in provider.fetch()]
        checkpoint = provider.build_checkpoint(previous_checkpoint=None)

        assert checkpoint == CisaKevCheckpoint(
            catalog_version="2026.03.25",
            etag='"etag-2"',
            last_modified="Wed, 25 Mar 2026 10:15:00 GMT",
        )

    asyncio.run(run())


def test_cisa_kev_provider_preserves_checkpoint_on_not_modified_response() -> None:
    async def run() -> None:
        previous_checkpoint = CisaKevCheckpoint(
            catalog_version="2026.03.25",
            etag='"etag-2"',
            last_modified="Wed, 25 Mar 2026 10:15:00 GMT",
        )
        provider = CisaKevProvider(
            catalog_loader=lambda checkpoint: _load_result(
                catalog_version=checkpoint.catalog_version if checkpoint is not None else None,
                vulnerabilities=[],
                etag=checkpoint.etag if checkpoint is not None else None,
                last_modified=checkpoint.last_modified if checkpoint is not None else None,
                not_modified=True,
            ),
        )

        records = [record async for record in provider.fetch(checkpoint=previous_checkpoint)]
        checkpoint = provider.build_checkpoint(previous_checkpoint=previous_checkpoint)

        assert records == []
        assert checkpoint == previous_checkpoint

    asyncio.run(run())


def _catalog(
    *,
    catalog_version: str | None,
    vulnerabilities: list[dict[str, object]],
) -> CisaKevCatalog:
    return CisaKevCatalog(
        title="CISA Catalog of Known Exploited Vulnerabilities",
        catalog_version=catalog_version,
        date_released="2026-03-25T10:15:00Z",
        count=len(vulnerabilities),
        vulnerabilities=tuple(vulnerabilities),
    )


def _load_result(
    *,
    catalog_version: str | None,
    vulnerabilities: list[dict[str, object]],
    etag: str | None = None,
    last_modified: str | None = None,
    not_modified: bool = False,
) -> CisaKevLoadResult:
    return CisaKevLoadResult(
        catalog=_catalog(
            catalog_version=catalog_version,
            vulnerabilities=vulnerabilities,
        ),
        etag=etag,
        last_modified=last_modified,
        not_modified=not_modified,
    )
