from __future__ import annotations

from io import BytesIO
from urllib.error import HTTPError

from ingestion.providers.kiwisdr_receivers import loader
from ingestion.providers.kiwisdr_receivers.checkpoint import KiwiSdrReceiversCheckpoint
from ingestion.providers.kiwisdr_receivers.loader import load_receivers, parse_receivers


def test_parse_receivers_extracts_receiver_fields_from_directory_html() -> None:
    html_bytes = b"""
    <html>
      <body>
        <pre>
Tue Mar 25 03:08:49 UTC 2026 658 receivers online 210 support TDoA 18 antennas available
KFS NW, California, United States
https://kfs.kiwisdr.example:8073/
KiwiSDR 2 v1.840, 2/4 users, SNR 25/25, 0-30 MHz, GPS, Limits, DRM, antenna switch, TDoA 3

Image

ZL/KFS, New Zealand
https://zl.kiwisdr.example:8073/
KiwiSDR 1 v1.828, 0/4 users, SNR 21, 0-30 MHz, GPS
        </pre>
      </body>
    </html>
    """

    result = parse_receivers(html_bytes)

    assert result.directory_generated_at is not None
    assert result.directory_generated_at.isoformat() == "2026-03-25T03:08:49+00:00"
    assert result.directory_summary == "658 receivers online 210 support TDoA 18 antennas available"
    assert [entry["id"] for entry in result.entries] == [
        "https://kfs.kiwisdr.example:8073/",
        "https://zl.kiwisdr.example:8073/",
    ]
    assert result.entries[0]["name"] == "KFS NW, California, United States"
    assert result.entries[0]["model"] == "KiwiSDR 2"
    assert result.entries[0]["software_version"] == "1.840"
    assert result.entries[0]["users_current"] == 2
    assert result.entries[0]["users_max"] == 4
    assert result.entries[0]["snr_values_db"] == [25, 25]
    assert result.entries[0]["has_gps"] is True
    assert result.entries[0]["has_time_limits"] is True
    assert result.entries[0]["has_drm"] is True
    assert result.entries[0]["has_antenna_switch"] is True
    assert result.entries[0]["tdoa"] == "3"
    assert result.entries[1]["snr_values_db"] == [21]


def test_parse_receivers_ignores_links_without_receiver_status_context() -> None:
    html_bytes = b"""
    <html>
      <body>
        <a href="https://kiwisdr.com/">KiwiSDR home</a>
        <div>Directory docs</div>
        <a href="https://rx.kiwisdr.com/">Receiver list</a>
        <div>Another line without receiver status</div>
      </body>
    </html>
    """

    result = parse_receivers(html_bytes)

    assert result.entries == []


def test_load_receivers_sends_http_cache_headers(monkeypatch) -> None:
    captured_request = None

    class _Response:
        def __init__(self) -> None:
            self.headers = {"ETag": '"etag-2"', "Last-Modified": "Wed, 25 Mar 2026 10:15:00 GMT"}
            self._stream = BytesIO(
                b"""
                <pre>
                Station
                https://kiwi.example:8073/
                KiwiSDR 2 v1.840, 0/4 users, SNR 20, 0-30 MHz
                </pre>
                """
            )

        def __enter__(self) -> _Response:
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def read(self, size: int = -1) -> bytes:
            return self._stream.read(size)

    def fake_urlopen(request):
        nonlocal captured_request
        captured_request = request
        return _Response()

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    result = load_receivers(
        "https://kiwisdr.com/.public/",
        KiwiSdrReceiversCheckpoint(
            directory_url="https://kiwisdr.com/.public/",
            etag='"etag-1"',
            last_modified="Tue, 24 Mar 2026 10:15:00 GMT",
        ),
    )

    assert captured_request is not None
    assert captured_request.headers["If-none-match"] == '"etag-1"'
    assert captured_request.headers["If-modified-since"] == "Tue, 24 Mar 2026 10:15:00 GMT"
    assert result.etag == '"etag-2"'
    assert result.last_modified == "Wed, 25 Mar 2026 10:15:00 GMT"


def test_load_receivers_returns_not_modified_result_for_http_304(monkeypatch) -> None:
    def fake_urlopen(request):
        raise HTTPError(request.full_url, 304, "Not Modified", hdrs=None, fp=None)

    monkeypatch.setattr(loader, "urlopen", fake_urlopen)

    checkpoint = KiwiSdrReceiversCheckpoint(
        directory_url="https://kiwisdr.com/.public/",
        etag='"etag-1"',
        last_modified="Wed, 25 Mar 2026 10:15:00 GMT",
    )

    result = load_receivers("https://kiwisdr.com/.public/", checkpoint)

    assert result.not_modified is True
    assert list(result.entries) == []
    assert result.etag == '"etag-1"'
    assert result.last_modified == "Wed, 25 Mar 2026 10:15:00 GMT"


def test_kiwisdr_checkpoint_supports_http_cache_fields() -> None:
    checkpoint = KiwiSdrReceiversCheckpoint(
        directory_url="https://kiwisdr.com/.public/",
        etag='"etag-1"',
        last_modified="Wed, 25 Mar 2026 10:15:00 GMT",
    )

    assert checkpoint.etag == '"etag-1"'
    assert checkpoint.last_modified == "Wed, 25 Mar 2026 10:15:00 GMT"
