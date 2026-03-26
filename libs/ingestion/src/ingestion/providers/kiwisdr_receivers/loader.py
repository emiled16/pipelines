from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import BinaryIO
from urllib.error import HTTPError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen

from ingestion.providers.kiwisdr_receivers.checkpoint import KiwiSdrReceiversCheckpoint

TIMESTAMP_PATTERN = re.compile(
    r"^(?P<stamp>[A-Z][a-z]{2} [A-Z][a-z]{2} [ 0-9]\d \d{2}:\d{2}:\d{2} UTC \d{4})"
    r"(?:\s+(?P<summary>.*))?$"
)
URL_PATTERN = re.compile(r"https?://[^\s<>'\"]+")


@dataclass(slots=True, frozen=True)
class KiwiSdrParseResult:
    entries: list[dict[str, object]]
    directory_generated_at: datetime | None = None
    directory_summary: str | None = None


@dataclass(slots=True, frozen=True)
class KiwiSdrLoadResult:
    entries: Iterable[dict[str, object]]
    etag: str | None = None
    last_modified: str | None = None
    not_modified: bool = False
    directory_generated_at: datetime | None = None
    directory_summary: str | None = None


def load_receivers(
    directory_url: str,
    checkpoint: KiwiSdrReceiversCheckpoint | None = None,
) -> KiwiSdrLoadResult:
    request = Request(directory_url, headers=_request_headers(checkpoint))

    try:
        with urlopen(request) as response:
            parsed = parse_receivers(response)
            return KiwiSdrLoadResult(
                entries=parsed.entries,
                etag=response.headers.get("ETag"),
                last_modified=response.headers.get("Last-Modified"),
                directory_generated_at=parsed.directory_generated_at,
                directory_summary=parsed.directory_summary,
            )
    except HTTPError as exc:
        if exc.code == 304:
            return KiwiSdrLoadResult(
                entries=(),
                etag=checkpoint.etag if checkpoint is not None else None,
                last_modified=checkpoint.last_modified if checkpoint is not None else None,
                not_modified=True,
            )
        raise


def parse_receivers(html_source: bytes | BinaryIO) -> KiwiSdrParseResult:
    if isinstance(html_source, bytes):
        document = html_source.decode("utf-8", errors="replace")
    else:
        document = html_source.read().decode("utf-8", errors="replace")

    text = _DirectoryTextExtractor().extract(document)
    lines = _normalized_lines(text)
    directory_generated_at, directory_summary = _parse_directory_header(lines)
    entries = _parse_entries(lines, directory_generated_at, directory_summary)
    return KiwiSdrParseResult(
        entries=entries,
        directory_generated_at=directory_generated_at,
        directory_summary=directory_summary,
    )


def _request_headers(checkpoint: KiwiSdrReceiversCheckpoint | None) -> dict[str, str]:
    headers: dict[str, str] = {}
    if checkpoint is None:
        return headers
    if checkpoint.etag:
        headers["If-None-Match"] = checkpoint.etag
    if checkpoint.last_modified:
        headers["If-Modified-Since"] = checkpoint.last_modified
    return headers


def _normalized_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split())
        if line:
            lines.append(line)
    return lines


def _parse_directory_header(lines: list[str]) -> tuple[datetime | None, str | None]:
    for line in lines[:5]:
        match = TIMESTAMP_PATTERN.match(line)
        if match is None:
            continue

        generated_at = datetime.strptime(match.group("stamp"), "%a %b %d %H:%M:%S UTC %Y")
        return generated_at.replace(tzinfo=timezone.utc), match.group("summary")

    return None, None


def _parse_entries(
    lines: list[str],
    directory_generated_at: datetime | None,
    directory_summary: str | None,
) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    seen_urls: set[str] = set()

    for index, line in enumerate(lines):
        for match in URL_PATTERN.finditer(line):
            url = _normalize_url(match.group(0))
            if url in seen_urls:
                continue

            status = _find_status_line(lines, index)
            if status is None:
                continue

            name = _find_name_line(lines, index, status=status)
            parsed_status = _parse_status_line(status)
            entry: dict[str, object] = {
                "id": url,
                "url": url,
                "status": status,
                "directory_generated_at": directory_generated_at,
            }
            if directory_summary is not None:
                entry["directory_summary"] = directory_summary
            if name is not None:
                entry["name"] = name
            entry.update(parsed_status)

            entries.append(entry)
            seen_urls.add(url)

    return entries


def _find_status_line(lines: list[str], url_index: int) -> str | None:
    candidates: list[tuple[int, int, str]] = []
    for offset in range(1, 4):
        after_index = url_index + offset
        if after_index < len(lines) and _is_status_line(lines[after_index]):
            candidates.append((offset, 0, lines[after_index]))

        before_index = url_index - offset
        if before_index >= 0 and _is_status_line(lines[before_index]):
            candidates.append((offset, 1, lines[before_index]))

    if not candidates:
        return None

    _, _, line = min(candidates)
    return line


def _find_name_line(lines: list[str], url_index: int, *, status: str) -> str | None:
    candidates: list[tuple[int, int, str]] = []
    ignored_lines = {status}

    for offset in range(1, 4):
        before_index = url_index - offset
        if before_index >= 0:
            candidate = lines[before_index]
            if _is_name_candidate(candidate, ignored_lines=ignored_lines):
                candidates.append((offset, 0, candidate))

        after_index = url_index + offset
        if after_index < len(lines):
            candidate = lines[after_index]
            if _is_name_candidate(candidate, ignored_lines=ignored_lines):
                candidates.append((offset, 1, candidate))

    if not candidates:
        return None

    _, _, line = min(candidates)
    return line


def _is_name_candidate(line: str, *, ignored_lines: set[str]) -> bool:
    if line in ignored_lines:
        return False
    if line == "Image":
        return False
    if URL_PATTERN.search(line):
        return False
    if TIMESTAMP_PATTERN.match(line):
        return False
    return not _is_status_line(line)


def _is_status_line(line: str) -> bool:
    return "KiwiSDR" in line and "users" in line and "SNR" in line


def _parse_status_line(line: str) -> dict[str, object]:
    segments = [segment.strip() for segment in line.split(",") if segment.strip()]
    parsed: dict[str, object] = {"software": segments[0]} if segments else {"software": line}

    if not segments:
        return parsed

    software_match = re.match(r"^(?P<model>KiwiSDR(?:\s+\d+)?)\s+v(?P<version>\S+)$", segments[0])
    if software_match is not None:
        parsed["model"] = software_match.group("model")
        parsed["software_version"] = software_match.group("version")

    for segment in segments[1:]:
        users_match = re.match(r"^(?P<current>\d+)\s*/\s*(?P<maximum>\d+)\s+users$", segment)
        if users_match is not None:
            parsed["users_current"] = int(users_match.group("current"))
            parsed["users_max"] = int(users_match.group("maximum"))
            continue

        snr_match = re.match(r"^SNR\s+(?P<value>.+)$", segment)
        if snr_match is not None:
            snr_value = snr_match.group("value")
            parsed["snr"] = snr_value
            parsed["snr_values_db"] = [int(value) for value in re.findall(r"\d+", snr_value)]
            continue

        range_match = re.match(r"^(?P<start>\d+(?:\.\d+)?)-(?P<end>\d+(?:\.\d+)?)\s*MHz$", segment)
        if range_match is not None:
            parsed["frequency_range_mhz"] = {
                "start": float(range_match.group("start")),
                "end": float(range_match.group("end")),
            }
            parsed["frequency_range_label"] = segment
            continue

        if segment == "GPS":
            parsed["has_gps"] = True
            continue

        if "Limits" in segment:
            parsed["has_time_limits"] = True
            parsed["time_limits_label"] = segment
            continue

        if segment == "DRM":
            parsed["has_drm"] = True
            continue

        if segment == "antenna switch":
            parsed["has_antenna_switch"] = True
            continue

        tdoa_match = re.match(r"^TDoA\s+(?P<value>.+)$", segment)
        if tdoa_match is not None:
            parsed["tdoa"] = tdoa_match.group("value")
            continue

        features = parsed.setdefault("extra_features", [])
        if isinstance(features, list):
            features.append(segment)

    return parsed


def _normalize_url(url: str) -> str:
    parts = urlsplit(url)
    path = parts.path or "/"
    normalized = parts._replace(path=path, fragment="")
    return urlunsplit(normalized)


class _DirectoryTextExtractor(HTMLParser):
    _BLOCK_TAGS = {
        "article",
        "body",
        "br",
        "div",
        "footer",
        "h1",
        "h2",
        "h3",
        "h4",
        "header",
        "html",
        "li",
        "main",
        "p",
        "pre",
        "section",
        "table",
        "td",
        "th",
        "title",
        "tr",
        "ul",
    }

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []
        self._anchor_href: str | None = None

    def extract(self, document: str) -> str:
        self.feed(document)
        self.close()
        return "".join(self._parts)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in self._BLOCK_TAGS:
            self._parts.append("\n")
        if tag == "a":
            attributes = dict(attrs)
            self._anchor_href = attributes.get("href")

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._anchor_href and self._anchor_href.startswith(("http://", "https://")):
            self._parts.extend(("\n", self._anchor_href, "\n"))
        if tag == "a":
            self._anchor_href = None
        if tag in self._BLOCK_TAGS and tag != "br":
            self._parts.append("\n")

    def handle_data(self, data: str) -> None:
        self._parts.append(data)
