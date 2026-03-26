from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ingestion.providers.treasury_metrics.checkpoint import TreasuryMetricsCheckpoint

TREASURY_FISCAL_DATA_API_BASE_URL = (
    "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
)


@dataclass(slots=True, frozen=True)
class TreasuryMetricsLoadResult:
    rows: Iterable[dict[str, object]]


def load_treasury_metric_rows(
    *,
    endpoint: str,
    cursor_field: str,
    checkpoint: TreasuryMetricsCheckpoint | None = None,
    base_url: str = TREASURY_FISCAL_DATA_API_BASE_URL,
    fields: Sequence[str] = (),
    filters: Sequence[str] = (),
    sort_fields: Sequence[str] = (),
    page_size: int = 1_000,
) -> TreasuryMetricsLoadResult:
    return TreasuryMetricsLoadResult(
        rows=_iter_rows(
            endpoint=endpoint,
            cursor_field=cursor_field,
            checkpoint=checkpoint,
            base_url=base_url,
            fields=fields,
            filters=filters,
            sort_fields=sort_fields,
            page_size=page_size,
        )
    )


def _iter_rows(
    *,
    endpoint: str,
    cursor_field: str,
    checkpoint: TreasuryMetricsCheckpoint | None,
    base_url: str,
    fields: Sequence[str],
    filters: Sequence[str],
    sort_fields: Sequence[str],
    page_size: int,
) -> Iterator[dict[str, object]]:
    page_number = 1

    while True:
        response = urlopen(
            Request(
                _build_url(
                    endpoint=endpoint,
                    cursor_field=cursor_field,
                    checkpoint=checkpoint,
                    base_url=base_url,
                    fields=fields,
                    filters=filters,
                    sort_fields=sort_fields,
                    page_number=page_number,
                    page_size=page_size,
                ),
                headers={"Accept": "application/json"},
            )
        )
        try:
            payload = json.load(response)
        finally:
            response.close()

        data = payload.get("data")
        if not isinstance(data, list):
            raise ValueError("Treasury Metrics response is missing a list data payload")

        for row in data:
            if not isinstance(row, Mapping):
                raise ValueError("Treasury Metrics response data rows must be objects")
            yield _normalize_row(row)

        total_pages = _total_pages(payload)
        if total_pages is None:
            if len(data) < page_size:
                break
        elif page_number >= total_pages:
            break
        page_number += 1


def _build_url(
    *,
    endpoint: str,
    cursor_field: str,
    checkpoint: TreasuryMetricsCheckpoint | None,
    base_url: str,
    fields: Sequence[str],
    filters: Sequence[str],
    sort_fields: Sequence[str],
    page_number: int,
    page_size: int,
) -> str:
    params: list[tuple[str, str | int]] = [("format", "json")]

    if fields:
        params.append(("fields", ",".join(fields)))

    effective_filters = list(filters)
    if checkpoint is not None and checkpoint.endpoint == endpoint:
        effective_filters.append(f"{cursor_field}:gt:{checkpoint.cursor}")
    if effective_filters:
        params.append(("filter", ",".join(effective_filters)))

    if sort_fields:
        params.append(("sort", ",".join(sort_fields)))

    params.extend(
        [
            ("page[number]", page_number),
            ("page[size]", page_size),
        ]
    )

    query = urlencode(params, doseq=False)
    return f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}?{query}"


def _normalize_row(row: Mapping[str, object]) -> dict[str, object]:
    return {str(key): _normalize_value(value) for key, value in row.items()}


def _normalize_value(value: object) -> object:
    if isinstance(value, str) and value.lower() == "null":
        return None
    if isinstance(value, Mapping):
        return {str(key): _normalize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_value(item) for item in value]
    return value


def _total_pages(payload: Mapping[str, Any]) -> int | None:
    meta = payload.get("meta")
    if not isinstance(meta, Mapping):
        return None

    total_pages = meta.get("total-pages")
    if isinstance(total_pages, int):
        return total_pages
    if isinstance(total_pages, str) and total_pages.isdigit():
        return int(total_pages)
    return None
