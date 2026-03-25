from __future__ import annotations

from typing import Generic, TypeVar

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ingestion.abstractions import CheckpointStore
from ingestion.database import CheckpointRecordORM
from ingestion.stores._serialization import CheckpointCodec

CheckpointT = TypeVar("CheckpointT")


class SqlAlchemyCheckpointStore(CheckpointStore[CheckpointT], Generic[CheckpointT]):
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        kind: str,
        codec: CheckpointCodec[CheckpointT],
    ) -> None:
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
            return self._codec.load(record.payload)

    async def save(self, scope: str, checkpoint: CheckpointT) -> None:
        payload = self._codec.dump(checkpoint)

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
                        payload=payload,
                    )
                )
            else:
                record.payload = payload
            await session.commit()
