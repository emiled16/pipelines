from __future__ import annotations

import asyncio
import importlib.util
from datetime import datetime, timezone

import pytest


def _require_sqlalchemy() -> None:
    if importlib.util.find_spec("sqlalchemy") is None:
        pytest.skip("sqlalchemy is not installed")
    if importlib.util.find_spec("aiosqlite") is None:
        pytest.skip("aiosqlite is not installed")


def test_sqlalchemy_checkpoint_store_round_trips_dataclass_checkpoints() -> None:
    _require_sqlalchemy()

    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from ingestion.database import Base, CheckpointRecordORM
    from ingestion.providers import RssCheckpoint
    from ingestion.stores import DataclassCheckpointCodec, SqlAlchemyCheckpointStore

    async def run() -> None:
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        store = SqlAlchemyCheckpointStore(
            session_factory=session_factory,
            kind="rss",
            codec=DataclassCheckpointCodec(RssCheckpoint),
        )
        checkpoint = RssCheckpoint(
            feed_url="https://example.com/feed.xml",
            last_entry_id="entry-42",
        )

        await store.save("rss:feed", checkpoint)

        loaded = await store.load("rss:feed")

        assert loaded == checkpoint

        async with session_factory() as session:
            rows = (await session.execute(select(CheckpointRecordORM))).scalars().all()
            assert len(rows) == 1
            assert rows[0].kind == "rss"

        await engine.dispose()

    asyncio.run(run())


def test_sqlalchemy_record_store_persists_json_compatible_records() -> None:
    _require_sqlalchemy()

    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from ingestion.database import Base, StoredRecordORM
    from ingestion.models import Record
    from ingestion.stores import SqlAlchemyRecordStore

    async def run() -> None:
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        store = SqlAlchemyRecordStore(session_factory=session_factory)

        await store.write(
            Record(
                provider="rss:test",
                key="entry-1",
                payload={
                    "id": "entry-1",
                    "published_at": datetime(2026, 3, 24, tzinfo=timezone.utc),
                },
                metadata={"feed_url": "https://example.com/feed.xml"},
            )
        )

        async with session_factory() as session:
            rows = (await session.execute(select(StoredRecordORM))).scalars().all()
            assert len(rows) == 1
            assert rows[0].provider == "rss:test"
            assert rows[0].record_key == "entry-1"

        await engine.dispose()

    asyncio.run(run())
