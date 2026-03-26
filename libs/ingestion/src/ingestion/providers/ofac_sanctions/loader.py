from __future__ import annotations

import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from io import BytesIO
from typing import Any, BinaryIO
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from xml.etree import ElementTree

from ingestion.providers.ofac_sanctions.checkpoint import OfacSanctionsCheckpoint

DEFAULT_SOURCE_URL = (
    "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML"
)
USER_AGENT = "ingestion/0.1"
NON_ALNUM_PATTERN = re.compile(r"[^a-z0-9]+")
CAMEL_TO_SNAKE_PATTERN = re.compile(r"(?<!^)(?=[A-Z])")


@dataclass(slots=True, frozen=True)
class OfacSanctionsLoadResult:
    entries: Iterable[dict[str, object]]
    etag: str | None = None
    last_modified: str | None = None
    not_modified: bool = False


class OfacSanctionsEntries(Iterator[dict[str, object]]):
    def __init__(self, stream: BinaryIO, *, close_stream: bool) -> None:
        self.publish_date: str | None = None
        self.record_count: int | None = None
        self._stream = stream
        self._close_stream = close_stream
        self._iterator = self._iter_entries()

    def __iter__(self) -> OfacSanctionsEntries:
        return self

    def __next__(self) -> dict[str, object]:
        return next(self._iterator)

    def close(self) -> None:
        self._stream.close()

    def _iter_entries(self) -> Iterator[dict[str, object]]:
        parser = ElementTree.XMLPullParser(events=("end",))
        try:
            while True:
                chunk = self._stream.read(8192)
                if not chunk:
                    break
                parser.feed(chunk)
                yield from self._drain(parser)

            yield from self._drain(parser)
            parser.close()
        finally:
            if self._close_stream:
                self._stream.close()

    def _drain(self, parser: ElementTree.XMLPullParser) -> Iterator[dict[str, object]]:
        for _, element in parser.read_events():
            tag = _normalized_tag_name(element.tag)

            if tag in {"publishinformation", "publshinformation"}:
                self.publish_date = _first_text(
                    element,
                    "publish_date",
                    "publishdate",
                    "date_of_record",
                    "dateofrecord",
                )
                self.record_count = _parse_int(
                    _first_text(element, "record_count", "recordcount")
                )
                element.clear()
                continue

            if tag == "sdnentry":
                yield _parse_sdn_entry(element)
                element.clear()


def load_sanctions_entries(
    source_url: str = DEFAULT_SOURCE_URL,
    checkpoint: OfacSanctionsCheckpoint | None = None,
) -> OfacSanctionsLoadResult:
    request = Request(source_url, headers=_request_headers(checkpoint))

    try:
        response = urlopen(request)
        return OfacSanctionsLoadResult(
            entries=_iter_response_entries(response),
            etag=response.headers.get("ETag"),
            last_modified=response.headers.get("Last-Modified"),
        )
    except HTTPError as exc:
        if exc.code == 304:
            return OfacSanctionsLoadResult(
                entries=(),
                etag=checkpoint.etag if checkpoint is not None else None,
                last_modified=(
                    checkpoint.last_modified if checkpoint is not None else None
                ),
                not_modified=True,
            )
        raise


def parse_sanctions_entries(xml_source: bytes | BinaryIO) -> OfacSanctionsEntries:
    if isinstance(xml_source, bytes):
        return OfacSanctionsEntries(BytesIO(xml_source), close_stream=True)
    return OfacSanctionsEntries(xml_source, close_stream=False)


def _iter_response_entries(stream: BinaryIO) -> OfacSanctionsEntries:
    return OfacSanctionsEntries(stream, close_stream=True)


def _request_headers(checkpoint: OfacSanctionsCheckpoint | None) -> dict[str, str]:
    headers = {"User-Agent": USER_AGENT}
    if checkpoint is None:
        return headers
    if checkpoint.etag:
        headers["If-None-Match"] = checkpoint.etag
    if checkpoint.last_modified:
        headers["If-Modified-Since"] = checkpoint.last_modified
    return headers


def _parse_sdn_entry(entry: ElementTree.Element) -> dict[str, object]:
    uid = _required_text(entry, "uid")
    first_name = _first_text(entry, "first_name", "firstname")
    last_name = _first_text(entry, "last_name", "lastname")
    primary_name = _compose_name(first_name, last_name)
    title = _first_text(entry, "title")
    sdn_type = _first_text(entry, "sdn_type", "sdntype")
    remarks = _first_text(entry, "remarks")
    programs = _parse_text_list(entry, "program_list", {"program"})
    aliases = _parse_aliases(entry)
    addresses = _parse_direct_field_list(entry, "address_list", {"address"})
    identifiers = _parse_direct_field_list(entry, "id_list", {"id"})
    nationalities = _parse_country_list(entry, "nationality_list", {"nationality"})
    citizenships = _parse_country_list(entry, "citizenship_list", {"citizenship"})
    dates_of_birth = _parse_value_list(
        entry,
        "date_of_birth_list",
        {"date_of_birth_item", "date_of_birth"},
        ("date_of_birth",),
    )
    places_of_birth = _parse_value_list(
        entry,
        "place_of_birth_list",
        {"place_of_birth_item", "place_of_birth"},
        ("place_of_birth",),
    )
    vessel_info = _parse_first_direct_fields(entry, "vessel_info")
    aircraft_info = _parse_first_direct_fields(entry, "aircraft_info")

    return _compact(
        {
            "id": uid,
            "uid": uid,
            "name": primary_name,
            "first_name": first_name,
            "last_name": last_name,
            "title": title,
            "sdn_type": sdn_type,
            "programs": programs,
            "remarks": remarks,
            "aliases": aliases,
            "addresses": addresses,
            "identifiers": identifiers,
            "nationalities": nationalities,
            "citizenships": citizenships,
            "dates_of_birth": dates_of_birth,
            "places_of_birth": places_of_birth,
            "vessel_info": vessel_info,
            "aircraft_info": aircraft_info,
        }
    )


def _parse_aliases(entry: ElementTree.Element) -> list[dict[str, object]]:
    aliases: list[dict[str, object]] = []
    for alias in _iter_list_items(entry, "aka_list", {"aka"}):
        alias_fields = _direct_text_fields(alias)
        alias_name = _compose_name(
            alias_fields.get("first_name"),
            alias_fields.get("last_name"),
        )
        alias_fields["name"] = (
            alias_name
            or alias_fields.get("name")
            or alias_fields.get("last_name")
        )
        aliases.append(_compact(alias_fields))
    return aliases


def _parse_direct_field_list(
    entry: ElementTree.Element,
    list_name: str,
    item_names: set[str],
) -> list[dict[str, object]]:
    return [
        _compact(_direct_text_fields(item))
        for item in _iter_list_items(entry, list_name, item_names)
    ]


def _parse_country_list(
    entry: ElementTree.Element,
    list_name: str,
    item_names: set[str],
) -> list[str]:
    values: list[str] = []
    for item in _iter_list_items(entry, list_name, item_names):
        country = _first_text(item, "country")
        if country is not None:
            values.append(country)
    return values


def _parse_text_list(
    entry: ElementTree.Element,
    list_name: str,
    item_names: set[str],
) -> list[str]:
    values: list[str] = []
    for item in _iter_list_items(entry, list_name, item_names):
        value = _element_text(item)
        if value is not None:
            values.append(value)
    return values


def _parse_value_list(
    entry: ElementTree.Element,
    list_name: str,
    item_names: set[str],
    field_names: tuple[str, ...],
) -> list[str]:
    values: list[str] = []
    for item in _iter_list_items(entry, list_name, item_names):
        value = _first_text(item, *field_names) or _element_text(item)
        if value is not None:
            values.append(value)
    return values


def _parse_first_direct_fields(
    entry: ElementTree.Element,
    child_name: str,
) -> dict[str, object] | None:
    child = _first_child(entry, child_name)
    if child is None:
        return None
    return _compact(_direct_text_fields(child))


def _iter_list_items(
    entry: ElementTree.Element,
    list_name: str,
    item_names: set[str],
) -> Iterator[ElementTree.Element]:
    normalized_item_names = {_normalized_field_name(item_name) for item_name in item_names}
    for child in entry:
        if _normalized_tag_name(child.tag) != _normalized_field_name(list_name):
            continue
        for item in child:
            if _normalized_tag_name(item.tag) in normalized_item_names:
                yield item


def _first_child(entry: ElementTree.Element, child_name: str) -> ElementTree.Element | None:
    normalized_name = _normalized_field_name(child_name)
    for child in entry:
        if _normalized_tag_name(child.tag) == normalized_name:
            return child
    return None


def _direct_text_fields(element: ElementTree.Element) -> dict[str, str]:
    fields: dict[str, str] = {}
    for child in element:
        value = _element_text(child)
        if value is None:
            continue
        fields[_snake_case_tag(child.tag)] = value
    return fields


def _required_text(element: ElementTree.Element, *field_names: str) -> str:
    value = _first_text(element, *field_names)
    if value is None:
        raise ValueError(f"missing required field: {field_names[0]}")
    return value


def _first_text(element: ElementTree.Element, *field_names: str) -> str | None:
    normalized_names = {_normalized_field_name(field_name) for field_name in field_names}
    for child in element:
        if _normalized_tag_name(child.tag) not in normalized_names:
            continue
        value = _element_text(child)
        if value is not None:
            return value
    return None


def _element_text(element: ElementTree.Element) -> str | None:
    if element.text is None:
        return None
    value = element.text.strip()
    return value or None


def _compose_name(first_name: str | None, last_name: str | None) -> str | None:
    parts = [part for part in (first_name, last_name) if part]
    if not parts:
        return None
    return " ".join(parts)


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _compact(payload: dict[str, Any]) -> dict[str, object]:
    return {
        key: value
        for key, value in payload.items()
        if value is not None and value != [] and value != {}
    }


def _normalized_tag_name(tag: str) -> str:
    return _normalized_field_name(_local_name(tag))


def _normalized_field_name(value: str) -> str:
    return NON_ALNUM_PATTERN.sub("", value.casefold())


def _snake_case_tag(tag: str) -> str:
    return CAMEL_TO_SNAKE_PATTERN.sub("_", _local_name(tag)).replace("-", "_").casefold()


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", maxsplit=1)[-1]
    return tag
