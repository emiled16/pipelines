from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, BinaryIO
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ingestion.providers.who_alerts.checkpoint import WhoAlertsCheckpoint

DEFAULT_WHO_ALERTS_API_URL = "https://www.who.int/api/emergencies/diseaseoutbreaknews"
DEFAULT_WHO_ALERTS_PAGE_URL = "https://www.who.int/emergencies/disease-outbreak-news"
DEFAULT_WHO_ALERTS_ITEM_URL = f"{DEFAULT_WHO_ALERTS_PAGE_URL}/item"
DEFAULT_WHO_ALERTS_PAGE_SIZE = 100
WHO_ALERTS_SELECT_FIELDS = (
    "Id,DonId,Title,OverrideTitle,UseOverrideTitle,Summary,Overview,Assessment,"
    "Advice,Response,FurtherInformation,Epidemiology,PublicationDate,"
    "PublicationDateAndTime,LastModified,FormattedDate,UrlName,ItemDefaultUrl"
)
WHO_ALERTS_ORDER_BY = "PublicationDate desc,LastModified desc,Id desc"


@dataclass(slots=True, frozen=True)
class WhoAlertsLoadResult:
    entries: Iterable[dict[str, object]]
    etag: str | None = None
    last_modified: str | None = None
    not_modified: bool = False


def load_alert_entries(
    api_url: str = DEFAULT_WHO_ALERTS_API_URL,
    checkpoint: WhoAlertsCheckpoint | None = None,
    *,
    page_size: int = DEFAULT_WHO_ALERTS_PAGE_SIZE,
) -> WhoAlertsLoadResult:
    first_page_url = _build_page_url(api_url, page_size=page_size, skip=0)
    request = Request(first_page_url, headers=_request_headers(checkpoint))

    try:
        payload, headers = _load_json(request)
    except HTTPError as exc:
        if exc.code == 304:
            return WhoAlertsLoadResult(
                entries=(),
                etag=checkpoint.etag if checkpoint is not None else None,
                last_modified=checkpoint.last_modified if checkpoint is not None else None,
                not_modified=True,
            )
        raise

    return WhoAlertsLoadResult(
        entries=_iter_entries(
            first_payload=payload,
            api_url=api_url,
            page_size=page_size,
        ),
        etag=headers.get("ETag"),
        last_modified=headers.get("Last-Modified"),
    )


def parse_alert_entries(
    json_source: bytes | BinaryIO | Mapping[str, Any],
) -> Iterator[dict[str, object]]:
    if isinstance(json_source, Mapping):
        payload = dict(json_source)
    elif isinstance(json_source, bytes):
        payload = json.loads(json_source)
    else:
        payload = json.load(json_source)

    for item in payload.get("value", []):
        if not isinstance(item, Mapping):
            continue

        alert_id = _string_or_none(item.get("Id"))
        if alert_id is None:
            continue

        url_name = _string_or_none(item.get("UrlName"))
        item_default_url = _string_or_none(item.get("ItemDefaultUrl"))
        yield {
            "id": alert_id,
            "don_id": _string_or_none(item.get("DonId")),
            "title": _resolve_title(item),
            "summary": _string_or_none(item.get("Summary")),
            "overview": _string_or_none(item.get("Overview")),
            "assessment": _string_or_none(item.get("Assessment")),
            "advice": _string_or_none(item.get("Advice")),
            "response": _string_or_none(item.get("Response")),
            "further_information": _string_or_none(item.get("FurtherInformation")),
            "epidemiology": _string_or_none(item.get("Epidemiology")),
            "published_at": _parse_datetime(
                _string_or_none(item.get("PublicationDateAndTime"))
                or _string_or_none(item.get("PublicationDate"))
            ),
            "updated_at": _parse_datetime(_string_or_none(item.get("LastModified"))),
            "formatted_date": _string_or_none(item.get("FormattedDate")),
            "url_name": url_name,
            "item_default_url": item_default_url,
            "url": _build_alert_url(url_name=url_name, item_default_url=item_default_url),
        }


def _iter_entries(
    *,
    first_payload: Mapping[str, Any],
    api_url: str,
    page_size: int,
) -> Iterator[dict[str, object]]:
    payload: Mapping[str, Any] | None = first_payload
    skip = 0

    while payload is not None:
        items = list(parse_alert_entries(payload))
        if not items:
            return

        yield from items

        next_link = _string_or_none(payload.get("@odata.nextLink"))
        if next_link:
            payload, _ = _load_json(Request(next_link, headers={"Accept": "application/json"}))
            continue

        if len(items) < page_size:
            return

        skip += page_size
        payload, _ = _load_json(
            Request(
                _build_page_url(api_url, page_size=page_size, skip=skip),
                headers={"Accept": "application/json"},
            )
        )


def _load_json(request: Request) -> tuple[dict[str, Any], Mapping[str, str]]:
    response = urlopen(request)
    try:
        payload = json.load(response)
        if not isinstance(payload, dict):
            raise ValueError("WHO alerts API returned a non-object JSON payload")
        return payload, response.headers
    finally:
        response.close()


def _build_page_url(api_url: str, *, page_size: int, skip: int) -> str:
    query = urlencode(
        {
            "$orderby": WHO_ALERTS_ORDER_BY,
            "$select": WHO_ALERTS_SELECT_FIELDS,
            "$top": page_size,
            "$skip": skip,
        }
    )
    return f"{api_url}?{query}"


def _request_headers(checkpoint: WhoAlertsCheckpoint | None) -> dict[str, str]:
    headers = {"Accept": "application/json"}
    if checkpoint is None:
        return headers
    if checkpoint.etag:
        headers["If-None-Match"] = checkpoint.etag
    if checkpoint.last_modified:
        headers["If-Modified-Since"] = checkpoint.last_modified
    return headers


def _resolve_title(item: Mapping[str, Any]) -> str | None:
    override_title = _string_or_none(item.get("OverrideTitle"))
    if item.get("UseOverrideTitle") and override_title is not None:
        return override_title
    return _string_or_none(item.get("Title")) or override_title


def _build_alert_url(*, url_name: str | None, item_default_url: str | None) -> str | None:
    slug = url_name or _strip_slashes(item_default_url)
    if slug is None:
        return None
    return f"{DEFAULT_WHO_ALERTS_ITEM_URL}/{slug}"


def _strip_slashes(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip().strip("/")
    return stripped or None


def _string_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
