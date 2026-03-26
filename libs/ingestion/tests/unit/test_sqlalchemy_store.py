from __future__ import annotations

import asyncio
import importlib.util
from datetime import datetime, timezone
from uuid import UUID

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

    from ingestion.database.base import Base
    from ingestion.database.checkpoint import CheckpointRecordORM
    from ingestion.providers.rss.checkpoint import RssCheckpoint
    from ingestion.stores.checkpoint_store import SqlAlchemyCheckpointStore

    async def run() -> None:
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        store = SqlAlchemyCheckpointStore(
            session_factory=session_factory,
            kind="rss",
            checkpoint_type=RssCheckpoint,
        )
        checkpoint = RssCheckpoint(
            feed_url="https://example.com/feed.xml",
            last_entry_id="entry-42",
            etag='"etag-1"',
            last_modified="Wed, 25 Mar 2026 10:15:00 GMT",
        )

        await store.save("rss:feed", checkpoint)

        loaded = await store.load("rss:feed")

        assert loaded == checkpoint

        async with session_factory() as session:
            rows = (await session.execute(select(CheckpointRecordORM))).scalars().all()
            assert len(rows) == 1
            assert rows[0].kind == "rss"
            assert rows[0].cursor == "entry-42"
            assert rows[0].payload["etag"] == '"etag-1"'
            assert rows[0].payload["last_modified"] == "Wed, 25 Mar 2026 10:15:00 GMT"

        await engine.dispose()

    asyncio.run(run())


def test_sqlalchemy_checkpoint_store_prefers_cursor_column_on_load() -> None:
    _require_sqlalchemy()

    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from ingestion.database.base import Base
    from ingestion.database.checkpoint import CheckpointRecordORM
    from ingestion.providers.rss.checkpoint import RssCheckpoint
    from ingestion.stores.checkpoint_store import SqlAlchemyCheckpointStore

    async def run() -> None:
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        store = SqlAlchemyCheckpointStore(
            session_factory=session_factory,
            kind="rss",
            checkpoint_type=RssCheckpoint,
        )

        await store.save(
            "rss:feed",
            RssCheckpoint(
                feed_url="https://example.com/feed.xml",
                last_entry_id="entry-42",
                etag='"etag-1"',
                last_modified="Wed, 25 Mar 2026 10:15:00 GMT",
            ),
        )

        async with session_factory() as session:
            row = await session.scalar(select(CheckpointRecordORM))
            assert row is not None
            row.cursor = "entry-41"
            row.payload["last_entry_id"] = "entry-42"
            await session.commit()

        loaded = await store.load("rss:feed")

        assert loaded == RssCheckpoint(
            feed_url="https://example.com/feed.xml",
            last_entry_id="entry-41",
            etag='"etag-1"',
            last_modified="Wed, 25 Mar 2026 10:15:00 GMT",
        )

        await engine.dispose()

    asyncio.run(run())


def test_sqlalchemy_record_sink_persists_json_compatible_records() -> None:
    _require_sqlalchemy()

    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from ingestion.database.base import Base
    from ingestion.database.record import StoredRecordORM
    from ingestion.models.record import Record
    from ingestion.stores.record_sink import SqlAlchemyRecordSink

    async def run() -> None:
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        store = SqlAlchemyRecordSink(session_factory=session_factory)

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
            assert isinstance(rows[0].id, UUID)
            assert rows[0].provider == "rss:test"
            assert rows[0].record_key == "entry-1"

        await engine.dispose()

    asyncio.run(run())


def test_sqlalchemy_record_sink_upserts_by_provider_and_record_key() -> None:
    _require_sqlalchemy()

    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from ingestion.database.base import Base
    from ingestion.database.record import StoredRecordORM
    from ingestion.models.record import Record
    from ingestion.stores.record_sink import SqlAlchemyRecordSink

    async def run() -> None:
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        store = SqlAlchemyRecordSink(session_factory=session_factory)

        async def fail_if_called(*args, **kwargs):
            raise AssertionError("sqlite should use native upsert instead of preloading rows")

        store._load_existing_records = fail_if_called  # type: ignore[method-assign]

        await store.write_many(
            [
                Record(provider="rss:test", key="entry-1", payload={"version": 1}),
                Record(provider="rss:test", key="entry-1", payload={"version": 2}),
            ]
        )

        async with session_factory() as session:
            rows = (await session.execute(select(StoredRecordORM))).scalars().all()
            assert len(rows) == 1
            assert rows[0].record_key == "entry-1"
            assert rows[0].payload["version"] == 2

        await engine.dispose()

    asyncio.run(run())
