from __future__ import annotations

import json
import re
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from io import BytesIO
from typing import Any, BinaryIO
from urllib.request import Request, urlopen

BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
SERIES_ID_PATTERN = re.compile(r"^[A-Z0-9_#-]+$")


class BlsApiError(RuntimeError):
    """Raised when the BLS API returns an application-level failure."""


@dataclass(slots=True, frozen=True)
class BlsLoadResult:
    observations: Iterable[dict[str, object]]
    messages: tuple[str, ...] = ()


def load_series_observations(
    *,
    series_ids: Sequence[str],
    registration_key: str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    catalog: bool = False,
    calculations: bool = False,
    annualaverage: bool = False,
    aspects: bool = False,
    timeout: float = 30.0,
) -> BlsLoadResult:
    normalized_series_ids = _normalize_series_ids(series_ids)
    _validate_request(
        series_ids=normalized_series_ids,
        registration_key=registration_key,
        start_year=start_year,
        end_year=end_year,
        catalog=catalog,
        calculations=calculations,
        annualaverage=annualaverage,
        aspects=aspects,
    )

    series_limit = 50 if registration_key else 25
    observations: list[dict[str, object]] = []
    messages: list[str] = []

    for series_chunk in _chunked(normalized_series_ids, size=series_limit):
        payload = _request_payload(
            series_ids=series_chunk,
            registration_key=registration_key,
            start_year=start_year,
            end_year=end_year,
            catalog=catalog,
            calculations=calculations,
            annualaverage=annualaverage,
            aspects=aspects,
        )
        request = Request(
            BLS_API_URL,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        response = urlopen(request, timeout=timeout)
        try:
            body = response.read()
        finally:
            response.close()

        parsed_payload = _decode_response(body)
        messages.extend(_extract_messages(parsed_payload))
        observations.extend(_parse_payload_observations(parsed_payload))

    return BlsLoadResult(observations=observations, messages=tuple(messages))


def parse_series_observations(json_source: bytes | BinaryIO) -> Iterator[dict[str, object]]:
    payload = _decode_response(json_source)
    yield from _parse_payload_observations(payload)


def _decode_response(json_source: bytes | BinaryIO) -> Mapping[str, Any]:
    if isinstance(json_source, bytes):
        stream: BinaryIO = BytesIO(json_source)
    else:
        stream = json_source

    payload = json.load(stream)
    if not isinstance(payload, Mapping):
        raise BlsApiError("BLS API response must be a JSON object")

    status = payload.get("status")
    if status == "REQUEST_SUCCEEDED":
        return payload

    messages = _extract_messages(payload)
    if messages:
        raise BlsApiError("; ".join(messages))
    raise BlsApiError(f"unexpected BLS API status: {status!r}")


def _parse_payload_observations(payload: Mapping[str, Any]) -> Iterator[dict[str, object]]:
    for series in _extract_series(payload.get("Results")):
        series_id = str(series.get("seriesID", "")).strip()
        if not series_id:
            continue

        catalog = _normalize_mapping(series.get("catalog"))

        for data_point in _ensure_iterable(series.get("data")):
            if not isinstance(data_point, Mapping):
                continue

            year = _string_or_none(data_point.get("year"))
            period = _string_or_none(data_point.get("period"))
            if year is None or period is None:
                continue

            record: dict[str, object] = {
                "series_id": series_id,
                "year": year,
                "period": period,
                "period_name": _string_or_none(data_point.get("periodName")),
                "value": _string_or_none(data_point.get("value")),
                "latest": _coerce_bool(data_point.get("latest")),
                "footnotes": _normalize_named_items(data_point.get("footnotes")),
            }

            calculations = _normalize_mapping(data_point.get("calculations"))
            if calculations is not None:
                record["calculations"] = calculations

            aspects = _normalize_named_items(data_point.get("aspects"))
            if aspects:
                record["aspects"] = aspects

            if catalog is not None:
                record["catalog"] = catalog

            yield record


def _extract_messages(payload: Mapping[str, Any]) -> list[str]:
    raw_messages = payload.get("message")
    if not isinstance(raw_messages, list):
        return []
    return [str(message) for message in raw_messages if str(message).strip()]


def _extract_series(results: Any) -> Iterator[Mapping[str, Any]]:
    if isinstance(results, Mapping):
        raw_series = results.get("series")
        if isinstance(raw_series, list):
            for series in raw_series:
                if isinstance(series, Mapping):
                    yield series
        return

    if isinstance(results, list):
        for item in results:
            if not isinstance(item, Mapping):
                continue
            yield from _extract_series(item)


def _request_payload(
    *,
    series_ids: Sequence[str],
    registration_key: str | None,
    start_year: int | None,
    end_year: int | None,
    catalog: bool,
    calculations: bool,
    annualaverage: bool,
    aspects: bool,
) -> dict[str, object]:
    payload: dict[str, object] = {"seriesid": list(series_ids)}

    if start_year is not None and end_year is not None:
        payload["startyear"] = str(start_year)
        payload["endyear"] = str(end_year)

    if registration_key:
        payload["registrationkey"] = registration_key

    if catalog:
        payload["catalog"] = True
    if calculations:
        payload["calculations"] = True
    if annualaverage:
        payload["annualaverage"] = True
    if aspects:
        payload["aspects"] = True

    return payload


def _validate_request(
    *,
    series_ids: Sequence[str],
    registration_key: str | None,
    start_year: int | None,
    end_year: int | None,
    catalog: bool,
    calculations: bool,
    annualaverage: bool,
    aspects: bool,
) -> None:
    if not series_ids:
        raise ValueError("series_ids must not be empty")

    if (start_year is None) != (end_year is None):
        raise ValueError("start_year and end_year must be provided together")

    if start_year is not None and end_year is not None:
        if start_year > end_year:
            raise ValueError("start_year must be less than or equal to end_year")

        years_requested = end_year - start_year + 1
        years_limit = 20 if registration_key else 10
        if years_requested > years_limit:
            raise ValueError(
                f"BLS allows at most {years_limit} years per request for this access level"
            )

    if (catalog or calculations or annualaverage or aspects) and not registration_key:
        raise ValueError(
            "registration_key is required for catalog, calculations, annualaverage, or aspects"
        )


def _normalize_series_ids(series_ids: Sequence[str]) -> tuple[str, ...]:
    normalized: list[str] = []

    for series_id in series_ids:
        candidate = str(series_id).strip().upper()
        if not candidate:
            raise ValueError("series_ids must not contain blank values")
        if not SERIES_ID_PATTERN.fullmatch(candidate):
            raise ValueError(f"invalid BLS series ID: {series_id!r}")
        normalized.append(candidate)

    return tuple(normalized)


def _chunked(values: Sequence[str], *, size: int) -> Iterator[tuple[str, ...]]:
    for index in range(0, len(values), size):
        yield tuple(values[index : index + size])


def _ensure_iterable(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return []


def _normalize_mapping(value: Any) -> dict[str, object] | None:
    if not isinstance(value, Mapping):
        return None

    normalized = {
        str(key): item
        for key, item in value.items()
        if item is not None and (not isinstance(item, str) or item.strip())
    }
    return normalized or None


def _normalize_named_items(value: Any) -> list[dict[str, object]]:
    items = _ensure_iterable(value)
    normalized: list[dict[str, object]] = []

    for item in items:
        entry = _normalize_mapping(item)
        if entry is None:
            continue
        normalized.append(entry)

    return normalized


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() == "true"
