from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.request import Request, urlopen

from ingestion.providers.patents.checkpoint import PatentsCheckpoint

PATENTS_ENDPOINT = "https://search.patentsview.org/api/v1/patent/"
DEFAULT_PAGE_SIZE = 1000
MAX_PAGE_SIZE = 1000
DEFAULT_PATENT_FIELDS: tuple[str, ...] = (
    "patent_id",
    "patent_title",
    "patent_date",
    "patent_type",
    "patent_abstract",
    "patent_num_times_cited_by_us_patents",
    "assignees.assignee_id",
    "assignees.assignee_organization",
    "assignees.assignee_country",
    "inventors.inventor_id",
    "inventors.inventor_name_first",
    "inventors.inventor_name_last",
    "cpc_current.cpc_group_id",
    "cpc_current.cpc_subclass_id",
)
DEFAULT_SORT: tuple[dict[str, str], ...] = (
    {"patent_date": "desc"},
    {"patent_id": "desc"},
)

PatentDocument = dict[str, Any]


@dataclass(slots=True, frozen=True)
class PatentsLoadResult:
    patents: Iterable[PatentDocument]


def build_patents_query_hash(query: Mapping[str, Any] | None = None) -> str:
    normalized_query = dict(query or {})
    encoded = json.dumps(normalized_query, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def load_patents(
    *,
    api_key: str,
    query: Mapping[str, Any] | None = None,
    checkpoint: PatentsCheckpoint | None = None,
    fields: Sequence[str] | None = None,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> PatentsLoadResult:
    if not api_key:
        raise ValueError("api_key is required to load patents from PatentsView")

    return PatentsLoadResult(
        patents=_iter_patents(
            api_key=api_key,
            query=_build_query(query, checkpoint),
            fields=fields or DEFAULT_PATENT_FIELDS,
            page_size=_coerce_page_size(page_size),
        )
    )


def _iter_patents(
    *,
    api_key: str,
    query: Mapping[str, Any],
    fields: Sequence[str],
    page_size: int,
) -> Iterator[PatentDocument]:
    after: list[str] | None = None

    while True:
        response_payload = _execute_request(
            api_key=api_key,
            body={
                "q": dict(query),
                "f": list(fields),
                "s": [dict(sort_field) for sort_field in DEFAULT_SORT],
                "o": _build_options(page_size=page_size, after=after),
            },
        )
        patents = _extract_patents(response_payload)
        if not patents:
            return

        yield from patents

        if len(patents) < page_size:
            return

        after = _cursor_after(patents[-1])


def _execute_request(*, api_key: str, body: Mapping[str, Any]) -> dict[str, Any]:
    request = Request(
        PATENTS_ENDPOINT,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "ingestion-patents-provider/0.1",
            "X-Api-Key": api_key,
        },
        method="POST",
    )
    response = urlopen(request)
    try:
        payload = json.load(response)
    finally:
        response.close()

    if not isinstance(payload, dict):
        raise ValueError("PatentsView returned a non-object response")
    if payload.get("error"):
        raise ValueError("PatentsView returned an error response")

    return payload


def _extract_patents(payload: Mapping[str, Any]) -> list[PatentDocument]:
    patents = payload.get("patents")
    if patents is None:
        return []
    if not isinstance(patents, list):
        raise ValueError("PatentsView returned an invalid patents payload")
    return [patent for patent in patents if isinstance(patent, dict)]


def _build_query(
    query: Mapping[str, Any] | None,
    checkpoint: PatentsCheckpoint | None,
) -> dict[str, Any]:
    base_query = dict(query or {})
    if checkpoint is None:
        return base_query

    checkpoint_query = {"_gte": {"patent_date": checkpoint.last_patent_date}}
    if not base_query:
        return checkpoint_query

    return {"_and": [base_query, checkpoint_query]}


def _build_options(*, page_size: int, after: list[str] | None) -> dict[str, Any]:
    options: dict[str, Any] = {
        "exclude_withdrawn": True,
        "pad_patent_id": True,
        "size": page_size,
    }
    if after is not None:
        options["after"] = after
    return options


def _cursor_after(patent: Mapping[str, Any]) -> list[str]:
    patent_date = patent.get("patent_date")
    patent_id = patent.get("patent_id")
    if not isinstance(patent_date, str) or not isinstance(patent_id, str):
        raise ValueError(
            "PatentsView response is missing patent_date or patent_id for pagination"
        )
    return [patent_date, patent_id]


def _coerce_page_size(page_size: int) -> int:
    if page_size < 1:
        return 1
    return min(page_size, MAX_PAGE_SIZE)
