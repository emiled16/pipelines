from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ingestion.abstractions import RecordSink
from ingestion.database import StoredRecordORM
from ingestion.models import Record, WriteResult
from ingestion.stores._serialization import to_json_compatible


class SqlAlchemyRecordStore(RecordSink):
    def __init__(self, *, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def write(self, record: Record) -> None:
        async with self._session_factory() as session:
            session.add(self._to_orm(record))
            await session.commit()

    async def write_many(self, records: Iterable[Record]) -> WriteResult:
        orm_records = [self._to_orm(record) for record in records]

        async with self._session_factory() as session:
            session.add_all(orm_records)
            await session.commit()

        return WriteResult(records_written=len(orm_records))

    def _to_orm(self, record: Record) -> StoredRecordORM:
        return StoredRecordORM(
            provider=record.provider,
            record_key=record.key,
            payload=to_json_compatible(record.payload),
            metadata_=to_json_compatible(dict(record.metadata)),
            occurred_at=record.occurred_at,
            fetched_at=record.fetched_at,
        )
