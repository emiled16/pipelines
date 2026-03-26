from __future__ import annotations

from collections.abc import Iterable
from uuid import uuid4

from sqlalchemy import select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ingestion.abstractions.sink import RecordSink
from ingestion.database.record import StoredRecordORM
from ingestion.models.record import Record, WriteResult
from ingestion.utils.serialization import to_json_compatible
from ingestion.utils.time import utc_now


class SqlAlchemyRecordSink(RecordSink):
    def __init__(self, *, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def write(self, record: Record) -> None:
        async with self._session_factory() as session:
            await self._upsert_many(session, [record])
            await session.commit()

    async def write_many(self, records: Iterable[Record]) -> WriteResult:
        async with self._session_factory() as session:
            records_written = await self._upsert_many(session, records)
            await session.commit()

        return WriteResult(records_written=records_written)

    async def _upsert_many(self, session: AsyncSession, records: Iterable[Record]) -> int:
        normalized_records, upsertable_records, append_only_records = self._normalize_records(records)

        if upsertable_records:
            native_upsert_applied = await self._upsert_many_with_native_conflict_clause(
                session,
                upsertable_records,
            )
            if not native_upsert_applied:
                existing_records = await self._load_existing_records(session, upsertable_records)
                for record in upsertable_records:
                    record_id = (record.provider, record.key)
                    existing_record = existing_records.get(record_id)
                    if existing_record is None:
                        session.add(self._to_orm(record))
                    else:
                        self._update_orm(existing_record, record)

        for record in append_only_records:
            session.add(self._to_orm(record))

        return len(normalized_records)

    async def _upsert_many_with_native_conflict_clause(
        self,
        session: AsyncSession,
        records: list[Record],
    ) -> bool:
        dialect_name = session.get_bind().dialect.name

        if dialect_name == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as dialect_insert
        elif dialect_name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as dialect_insert
        else:
            return False

        table = StoredRecordORM.__table__
        statement = dialect_insert(table).values([self._to_row(record) for record in records])
        statement = statement.on_conflict_do_update(
            index_elements=[table.c.provider, table.c.record_key],
            set_={
                table.c.payload: statement.excluded.payload,
                table.c.metadata: statement.excluded.metadata,
                table.c.occurred_at: statement.excluded.occurred_at,
                table.c.fetched_at: statement.excluded.fetched_at,
            },
        )
        await session.execute(statement)
        return True

    async def _load_existing_records(
        self,
        session: AsyncSession,
        records: list[Record],
    ) -> dict[tuple[str, str], StoredRecordORM]:
        record_ids = [(record.provider, record.key) for record in records if record.key is not None]
        rows = (
            await session.execute(
                select(StoredRecordORM).where(
                    tuple_(StoredRecordORM.provider, StoredRecordORM.record_key).in_(record_ids)
                )
            )
        ).scalars()
        return {(row.provider, row.record_key): row for row in rows if row.record_key is not None}

    def _normalize_records(self, records: Iterable[Record]) -> tuple[list[Record], list[Record], list[Record]]:
        deduped_records: dict[tuple[str, str], Record] = {}
        keyed_order: list[tuple[str, str]] = []
        upsertable_records: list[Record] = []
        append_only_records: list[Record] = []

        for record in records:
            if record.key is None:
                append_only_records.append(record)
                continue

            record_id = (record.provider, record.key)
            if record_id not in deduped_records:
                keyed_order.append(record_id)
            deduped_records[record_id] = record

        upsertable_records = [deduped_records[record_id] for record_id in keyed_order]
        return (
            upsertable_records + append_only_records,
            upsertable_records,
            append_only_records,
        )

    def _to_row(self, record: Record) -> dict[str, object]:
        return {
            "id": uuid4(),
            "provider": record.provider,
            "record_key": record.key,
            "payload": to_json_compatible(record.payload),
            "metadata": to_json_compatible(dict(record.metadata)),
            "occurred_at": record.occurred_at,
            "fetched_at": record.fetched_at,
            "created_at": utc_now(),
        }

    def _to_orm(self, record: Record) -> StoredRecordORM:
        return StoredRecordORM(
            provider=record.provider,
            record_key=record.key,
            payload=to_json_compatible(record.payload),
            metadata_=to_json_compatible(dict(record.metadata)),
            occurred_at=record.occurred_at,
            fetched_at=record.fetched_at,
        )

    def _update_orm(self, orm_record: StoredRecordORM, record: Record) -> None:
        orm_record.payload = to_json_compatible(record.payload)
        orm_record.metadata_ = to_json_compatible(dict(record.metadata))
        orm_record.occurred_at = record.occurred_at
        orm_record.fetched_at = record.fetched_at
