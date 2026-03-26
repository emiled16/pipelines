from __future__ import annotations

from dataclasses import asdict, fields, is_dataclass
from typing import Generic, Mapping

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ingestion.abstractions.checkpoint_store import CheckpointCodec, CheckpointStore, CheckpointT
from ingestion.database.checkpoint import CheckpointRecordORM
from ingestion.utils.serialization import from_json_compatible, to_json_compatible


class DataclassCheckpointCodec(Generic[CheckpointT]):
    def __init__(self, checkpoint_type: type[CheckpointT]) -> None:
        if not is_dataclass(checkpoint_type):
            raise TypeError("checkpoint_type must be a dataclass type")
        self._checkpoint_type = checkpoint_type
        checkpoint_fields = {field.name for field in fields(checkpoint_type)}
        self._cursor_field_name = next(
            (
                field_name
                for field_name in ("cursor", "last_entry_id")
                if field_name in checkpoint_fields
            ),
            None,
        )

    def dump(self, checkpoint: CheckpointT) -> dict[str, object]:
        if not is_dataclass(checkpoint):
            raise TypeError("checkpoint must be a dataclass instance")
        return to_json_compatible(asdict(checkpoint))

    def load(self, payload: Mapping[str, object]) -> CheckpointT:
        return self.load_with_cursor(payload, cursor=None)

    def load_with_cursor(
        self,
        payload: Mapping[str, object],
        *,
        cursor: str | None,
    ) -> CheckpointT:
        data = dict(payload)
        if cursor is not None and self._cursor_field_name is not None:
            data[self._cursor_field_name] = cursor
        return self._checkpoint_type(**from_json_compatible(data))

    def cursor(self, checkpoint: CheckpointT) -> str | None:
        if self._cursor_field_name is not None:
            value = getattr(checkpoint, self._cursor_field_name)
            return None if value is None else str(value)
        return None


class SqlAlchemyCheckpointStore(CheckpointStore[CheckpointT], Generic[CheckpointT]):
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        kind: str,
        checkpoint_type: type[CheckpointT] | None = None,
        codec: CheckpointCodec[CheckpointT] | None = None,
    ) -> None:
        if codec is None:
            if checkpoint_type is None:
                raise TypeError("checkpoint_type is required when codec is not provided")
            codec = DataclassCheckpointCodec(checkpoint_type)

        self._session_factory = session_factory
        self._kind = kind
        self._codec = codec

    async def load(self, scope: str) -> CheckpointT | None:
        async with self._session_factory() as session:
            record = await session.scalar(
                select(CheckpointRecordORM).where(
                    CheckpointRecordORM.scope == scope,
                    CheckpointRecordORM.kind == self._kind,
                )
            )
            if record is None:
                return None
            if isinstance(self._codec, DataclassCheckpointCodec):
                return self._codec.load_with_cursor(record.payload, cursor=record.cursor)
            return self._codec.load(record.payload)

    async def save(self, scope: str, checkpoint: CheckpointT) -> None:
        payload = self._codec.dump(checkpoint)
        cursor = self._codec.cursor(checkpoint)

        async with self._session_factory() as session:
            record = await session.scalar(
                select(CheckpointRecordORM).where(
                    CheckpointRecordORM.scope == scope,
                    CheckpointRecordORM.kind == self._kind,
                )
            )
            if record is None:
                session.add(
                    CheckpointRecordORM(
                        scope=scope,
                        kind=self._kind,
                        cursor=cursor,
                        payload=payload,
                    )
                )
            else:
                record.cursor = cursor
                record.payload = payload
            await session.commit()
