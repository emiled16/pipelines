from __future__ import annotations

import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from io import BytesIO
from typing import BinaryIO
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from xml.etree import ElementTree

from ingestion.providers.rss.checkpoint import RssCheckpoint

INVALID_AMPERSAND_PATTERN = re.compile(
    rb"&(?!#\d+;|#x[0-9A-Fa-f]+;|[A-Za-z][A-Za-z0-9._-]*;)"
)


@dataclass(slots=True, frozen=True)
class RssLoadResult:
    entries: Iterable[dict[str, object]]
    etag: str | None = None
    last_modified: str | None = None
    not_modified: bool = False


def load_feed_entries(feed_url: str, checkpoint: RssCheckpoint | None = None) -> RssLoadResult:
    request = Request(feed_url, headers=_request_headers(checkpoint))

    try:
        response = urlopen(request)
        return RssLoadResult(
            entries=_iter_response_entries(response),
            etag=response.headers.get("ETag"),
            last_modified=response.headers.get("Last-Modified"),
        )
    except HTTPError as exc:
        if exc.code == 304:
            return RssLoadResult(
                entries=(),
                etag=checkpoint.etag if checkpoint is not None else None,
                last_modified=checkpoint.last_modified if checkpoint is not None else None,
                not_modified=True,
            )
        raise


def parse_feed_entries(xml_source: bytes | BinaryIO) -> Iterator[dict[str, object]]:
    if isinstance(xml_source, bytes):
        stream: BinaryIO = BytesIO(xml_source)
    else:
        stream = xml_source
    yield from _iter_items(stream)


def _iter_response_entries(stream: BinaryIO) -> Iterator[dict[str, object]]:
    try:
        yield from parse_feed_entries(stream)
    finally:
        stream.close()


def _request_headers(checkpoint: RssCheckpoint | None) -> dict[str, str]:
    headers: dict[str, str] = {}
    if checkpoint is None:
        return headers
    if checkpoint.etag:
        headers["If-None-Match"] = checkpoint.etag
    if checkpoint.last_modified:
        headers["If-Modified-Since"] = checkpoint.last_modified
    return headers


def _iter_items(stream: BinaryIO) -> Iterator[dict[str, object]]:
    parser = ElementTree.XMLPullParser(events=("end",))
    sanitizer = _AmpersandSanitizer()

    while True:
        chunk = stream.read(8192)
        if not chunk:
            break
        parser.feed(sanitizer.feed(chunk))
        yield from _drain_items(parser)

    parser.feed(sanitizer.flush())
    yield from _drain_items(parser)
    parser.close()


def _drain_items(parser: ElementTree.XMLPullParser) -> Iterator[dict[str, object]]:
    for _, element in parser.read_events():
        if _local_name(element.tag) != "item":
            continue

        guid = _first_text(element, "guid")
        link = _first_text(element, "link")
        title = _first_text(element, "title")
        published_at = _parse_pub_date(_first_text(element, "pubDate"))
        entry_id = guid or link or title

        if entry_id is not None:
            yield {
                "id": entry_id,
                "guid": guid,
                "link": link,
                "title": title,
                "published_at": published_at,
            }

        element.clear()


class _AmpersandSanitizer:
    def __init__(self) -> None:
        self._pending = b""

    def feed(self, chunk: bytes) -> bytes:
        return self._sanitize(self._pending + chunk, eof=False)

    def flush(self) -> bytes:
        return self._sanitize(self._pending, eof=True)

    def _sanitize(self, data: bytes, *, eof: bool) -> bytes:
        output = bytearray()
        index = 0
        self._pending = b""

        while index < len(data):
            if data[index] != ord("&"):
                output.append(data[index])
                index += 1
                continue

            decision = _classify_entity(data, index, eof=eof)
            if decision is None:
                self._pending = data[index:]
                break

            if decision:
                entity_end = data.index(b";", index) + 1
                output.extend(data[index:entity_end])
                index = entity_end
                continue

            output.extend(b"&amp;")
            index += 1

        return bytes(output)


def _classify_entity(data: bytes, start: int, *, eof: bool) -> bool | None:
    index = start + 1
    if index >= len(data):
        return False if eof else None

    first = data[index]
    if first == ord("#"):
        index += 1
        if index >= len(data):
            return False if eof else None

        if data[index] in (ord("x"), ord("X")):
            index += 1
            if index >= len(data):
                return False if eof else None
            digit_start = index
            while index < len(data) and _is_hex_digit(data[index]):
                index += 1
            if index == len(data):
                return False if eof else None
            return index > digit_start and data[index] == ord(";")

        digit_start = index
        while index < len(data) and _is_decimal_digit(data[index]):
            index += 1
        if index == len(data):
            return False if eof else None
        return index > digit_start and data[index] == ord(";")

    if _is_name_start(first):
        index += 1
        while index < len(data) and _is_name_char(data[index]):
            index += 1
        if index == len(data):
            return False if eof else None
        return data[index] == ord(";")

    return False


def _is_decimal_digit(value: int) -> bool:
    return ord("0") <= value <= ord("9")


def _is_hex_digit(value: int) -> bool:
    return (
        _is_decimal_digit(value)
        or ord("a") <= value <= ord("f")
        or ord("A") <= value <= ord("F")
    )


def _is_name_start(value: int) -> bool:
    return ord("a") <= value <= ord("z") or ord("A") <= value <= ord("Z")


def _is_name_char(value: int) -> bool:
    return (
        _is_name_start(value)
        or _is_decimal_digit(value)
        or value in (ord("."), ord("_"), ord("-"))
    )


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", maxsplit=1)[-1]
    return tag


def _first_text(item: ElementTree.Element, tag: str) -> str | None:
    for child in item:
        if _local_name(child.tag) != tag or child.text is None:
            continue
        value = child.text.strip()
        return value or None
    return None


def _parse_pub_date(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        return parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
