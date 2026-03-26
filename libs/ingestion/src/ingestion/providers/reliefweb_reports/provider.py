from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ingestion.abstractions.provider import BatchProvider
from ingestion.models.record import Record
from ingestion.providers.reliefweb_reports.checkpoint import ReliefWebReportsCheckpoint
from ingestion.providers.reliefweb_reports.loader import (
    DEFAULT_PAGE_SIZE,
    DEFAULT_REPORT_FIELDS,
    ReliefWebReportsPage,
    ReliefWebReportsRequest,
    load_reports_page,
)
from ingestion.utils.time import utc_now

ReportsPageLoader = Callable[
    [ReliefWebReportsRequest],
    ReliefWebReportsPage | Awaitable[ReliefWebReportsPage],
]


@dataclass(slots=True, frozen=True)
class ReliefWebReportsBatchState:
    newest_created_at: datetime | None = None


class ReliefWebReportsProvider(BatchProvider[ReliefWebReportsCheckpoint]):
    def __init__(
        self,
        *,
        appname: str,
        page_size: int = DEFAULT_PAGE_SIZE,
        query: Mapping[str, Any] | None = None,
        filter: Mapping[str, Any] | None = None,
        fields: tuple[str, ...] = DEFAULT_REPORT_FIELDS,
        preset: str | None = "latest",
        page_loader: ReportsPageLoader | None = None,
        name: str = "reliefweb_reports",
    ) -> None:
        if not appname.strip():
            raise ValueError("appname is required")
        if not 1 <= page_size <= 1000:
            raise ValueError("page_size must be between 1 and 1000")

        self.appname = appname.strip()
        self.page_size = page_size
        self.query = deepcopy(query)
        self.filter = deepcopy(filter)
        self.fields = tuple(fields)
        self.preset = preset
        self.name = name
        self._page_loader = page_loader or load_reports_page
        self._batch_state = ReliefWebReportsBatchState()

    async def fetch(
        self,
        *,
        checkpoint: ReliefWebReportsCheckpoint | None = None,
    ) -> AsyncIterator[Record]:
        self._batch_state = ReliefWebReportsBatchState()
        batch_fetched_at = utc_now()
        offset = 0

        while True:
            page = await self._load_page(self._build_request(checkpoint=checkpoint, offset=offset))
            if not page.reports:
                return

            for report in page.reports:
                report_id = str(report["id"])
                if checkpoint is not None and checkpoint.cursor == report_id:
                    return

                occurred_at = _coerce_datetime(report.get("date_created"))
                if self._batch_state.newest_created_at is None:
                    self._batch_state = ReliefWebReportsBatchState(newest_created_at=occurred_at)

                yield Record(
                    provider=self.name,
                    key=report_id,
                    payload=dict(report),
                    occurred_at=occurred_at,
                    fetched_at=batch_fetched_at,
                    metadata={
                        "endpoint": "reports",
                        "appname": self.appname,
                    },
                )

            if page.next_offset is None:
                return
            offset = page.next_offset

    def build_checkpoint(
        self,
        *,
        previous_checkpoint: ReliefWebReportsCheckpoint | None,
        last_report_id: str | None,
    ) -> ReliefWebReportsCheckpoint | None:
        cursor = last_report_id or (previous_checkpoint.cursor if previous_checkpoint is not None else None)
        if cursor is None:
            return None

        return ReliefWebReportsCheckpoint(
            cursor=cursor,
            created_at=(
                self._batch_state.newest_created_at
                if self._batch_state.newest_created_at is not None
                else (previous_checkpoint.created_at if previous_checkpoint is not None else None)
            ),
        )

    def _build_request(
        self,
        *,
        checkpoint: ReliefWebReportsCheckpoint | None,
        offset: int,
    ) -> ReliefWebReportsRequest:
        return ReliefWebReportsRequest(
            appname=self.appname,
            offset=offset,
            limit=self.page_size,
            preset=self.preset,
            fields=self.fields,
            query=deepcopy(self.query),
            filter=_merge_filters(
                base_filter=self.filter,
                checkpoint=checkpoint,
            ),
        )

    async def _load_page(self, request: ReliefWebReportsRequest) -> ReliefWebReportsPage:
        result = self._page_loader(request)
        if inspect.isawaitable(result):
            result = await result
        return result


def _merge_filters(
    *,
    base_filter: Mapping[str, Any] | None,
    checkpoint: ReliefWebReportsCheckpoint | None,
) -> Mapping[str, Any] | None:
    checkpoint_filter = None
    if checkpoint is not None and checkpoint.created_at is not None:
        checkpoint_filter = {
            "field": "date.created",
            "value": {"from": checkpoint.created_at.isoformat()},
        }

    if base_filter is None:
        return checkpoint_filter
    if checkpoint_filter is None:
        return deepcopy(base_filter)

    return {
        "operator": "AND",
        "conditions": [
            deepcopy(base_filter),
            checkpoint_filter,
        ],
    }


def _coerce_datetime(value: Any) -> datetime | None:
    return value if isinstance(value, datetime) else None
