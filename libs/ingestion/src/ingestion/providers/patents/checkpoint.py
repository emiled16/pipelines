from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class PatentsCheckpoint:
    query_hash: str
    last_patent_id: str
    last_patent_date: str
