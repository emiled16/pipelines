from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from html.parser import HTMLParser
from io import BytesIO
from typing import BinaryIO
from urllib.parse import urljoin
from urllib.request import Request, urlopen

BASE_CHANNEL_URL = "https://t.me/s/"


@dataclass(slots=True, frozen=True)
class TelegramChannelLoadResult:
    entries: Iterable[dict[str, object]]


def load_channel_messages(channel_name: str) -> TelegramChannelLoadResult:
    normalized_channel_name = normalize_channel_name(channel_name)
    if normalized_channel_name is None:
        raise ValueError("channel_name must not be empty")
    request = Request(
        build_channel_url(normalized_channel_name),
        headers={"User-Agent": "Mozilla/5.0"},
    )
    response = urlopen(request)
    return TelegramChannelLoadResult(
        entries=_iter_response_messages(response, channel_name=normalized_channel_name)
    )


def parse_channel_messages(
    html_source: bytes | BinaryIO,
    *,
    channel_name: str | None = None,
) -> Iterator[dict[str, object]]:
    if isinstance(html_source, bytes):
        stream: BinaryIO = BytesIO(html_source)
    else:
        stream = html_source

    html = stream.read().decode("utf-8", errors="replace")
    parser = _TelegramChannelHTMLParser(channel_name=normalize_channel_name(channel_name))
    parser.feed(html)
    parser.close()

    for message in sorted(parser.messages, key=_sort_key, reverse=True):
        yield message


def normalize_channel_name(channel_name: str | None) -> str | None:
    if channel_name is None:
        return None

    normalized = channel_name.strip()
    if not normalized:
        return None
    if normalized.startswith("@"):
        normalized = normalized[1:]

    for prefix in ("https://t.me/s/", "https://t.me/", "http://t.me/s/", "http://t.me/"):
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix) :]
            break

    return normalized.strip("/")


def build_channel_url(channel_name: str) -> str:
    normalized_channel_name = normalize_channel_name(channel_name)
    if normalized_channel_name is None:
        raise ValueError("channel_name must not be empty")
    return f"{BASE_CHANNEL_URL}{normalized_channel_name}"


def _iter_response_messages(stream: BinaryIO, *, channel_name: str) -> Iterator[dict[str, object]]:
    try:
        yield from parse_channel_messages(stream, channel_name=channel_name)
    finally:
        stream.close()


def _sort_key(message: dict[str, object]) -> tuple[int, str]:
    message_id = str(message["id"])
    if message_id.isdigit():
        return (1, f"{int(message_id):020d}")
    return (0, message_id)


@dataclass(slots=True)
class _PendingMessage:
    message_id: str
    channel_name: str | None
    div_depth: int = 1
    url: str | None = None
    published_at: datetime | None = None
    author_fragments: list[str] = field(default_factory=list)
    text_fragments: list[str] = field(default_factory=list)
    views_fragments: list[str] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class _TagState:
    is_message_div: bool = False
    enters_author: bool = False
    enters_text: bool = False
    enters_views: bool = False


class _TelegramChannelHTMLParser(HTMLParser):
    def __init__(self, *, channel_name: str | None) -> None:
        super().__init__(convert_charrefs=True)
        self._default_channel_name = channel_name
        self._stack: list[_TagState] = []
        self._pending_message: _PendingMessage | None = None
        self._author_depth = 0
        self._text_depth = 0
        self._views_depth = 0
        self.messages: list[dict[str, object]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = {key: value for key, value in attrs if value is not None}
        classes = set(attributes.get("class", "").split())

        state = _TagState()
        data_post = attributes.get("data-post")

        if tag == "div" and data_post is not None:
            self._start_message(data_post)
            state = _TagState(is_message_div=True)
        elif self._pending_message is not None and tag == "div":
            self._pending_message.div_depth += 1

        if self._pending_message is not None:
            author_classes = {
                "tgme_widget_message_author",
                "tgme_widget_message_owner_name",
            }
            enters_author = bool(
                classes.intersection(author_classes)
            )
            enters_text = "tgme_widget_message_text" in classes
            enters_views = "tgme_widget_message_views" in classes

            if enters_author:
                self._author_depth += 1
            if enters_text:
                self._text_depth += 1
            if enters_views:
                self._views_depth += 1

            state = _TagState(
                is_message_div=state.is_message_div,
                enters_author=enters_author,
                enters_text=enters_text,
                enters_views=enters_views,
            )

            if (
                tag == "a"
                and "tgme_widget_message_date" in classes
                and self._pending_message.url is None
            ):
                href = attributes.get("href")
                if href:
                    self._pending_message.url = urljoin("https://t.me/", href)

            if tag == "time":
                self._pending_message.published_at = _parse_datetime(attributes.get("datetime"))

            if tag == "br" and self._text_depth > 0:
                self._pending_message.text_fragments.append("\n")

        self._stack.append(state)

    def handle_endtag(self, tag: str) -> None:
        state = self._stack.pop() if self._stack else _TagState()

        if self._pending_message is not None and tag == "div":
            self._pending_message.div_depth -= 1
            if self._pending_message.div_depth == 0:
                self._finish_message()

        if state.enters_author and self._author_depth > 0:
            self._author_depth -= 1
        if state.enters_text and self._text_depth > 0:
            self._text_depth -= 1
        if state.enters_views and self._views_depth > 0:
            self._views_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._pending_message is None:
            return
        if self._author_depth > 0:
            self._pending_message.author_fragments.append(data)
        if self._text_depth > 0:
            self._pending_message.text_fragments.append(data)
        if self._views_depth > 0:
            self._pending_message.views_fragments.append(data)

    def _start_message(self, data_post: str) -> None:
        channel_name, _, message_id = data_post.rpartition("/")
        if not channel_name or not message_id:
            return
        self._pending_message = _PendingMessage(
            message_id=message_id,
            channel_name=normalize_channel_name(channel_name) or self._default_channel_name,
        )

    def _finish_message(self) -> None:
        assert self._pending_message is not None
        channel_name = self._pending_message.channel_name or self._default_channel_name
        author = _clean_text(self._pending_message.author_fragments)
        text = _clean_text(self._pending_message.text_fragments, preserve_newlines=True)
        views = _clean_text(self._pending_message.views_fragments)

        message = {
            "id": self._pending_message.message_id,
            "channel_name": channel_name,
            "url": self._pending_message.url
            or (
                urljoin(
                    "https://t.me/",
                    f"{channel_name}/{self._pending_message.message_id}",
                )
                if channel_name is not None
                else None
            ),
            "author": author,
            "text": text,
            "published_at": self._pending_message.published_at,
            "views": views,
        }
        self.messages.append(message)
        self._pending_message = None


def _clean_text(fragments: list[str], *, preserve_newlines: bool = False) -> str | None:
    if preserve_newlines:
        value = "".join(fragments)
        lines = [" ".join(part.split()) for part in value.splitlines()]
        value = "\n".join(line for line in lines if line)
    else:
        value = " ".join("".join(fragments).split())
    return value or None


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None
