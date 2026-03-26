from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
INGESTION_SRC = PROJECT_ROOT / "libs" / "ingestion" / "src"

if str(INGESTION_SRC) not in sys.path:
    sys.path.insert(0, str(INGESTION_SRC))

DEFAULT_FEED_URL = "https://planetpython.org/rss20.xml"
DEFAULT_DB_PATH = PROJECT_ROOT / "rss_ingestion.db"


async def main(*, feed_url: str, db_path: Path) -> None:
    from sqlalchemy import func, select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from ingestion.database.base import Base
    from ingestion.database.checkpoint import CheckpointRecordORM
    from ingestion.database.record import StoredRecordORM
    from ingestion.providers.rss.checkpoint import RssCheckpoint
    from ingestion.providers.rss.provider import RssProvider
    from ingestion.stores.checkpoint_store import SqlAlchemyCheckpointStore
    from ingestion.stores.record_sink import SqlAlchemyRecordSink

    database_url = f"sqlite+aiosqlite:///{db_path.resolve()}"
    engine = create_async_engine(database_url)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    provider = RssProvider(feed_url=feed_url)
    checkpoint_store = SqlAlchemyCheckpointStore(
        session_factory=session_factory,
        kind="rss",
        checkpoint_type=RssCheckpoint,
    )
    record_sink = SqlAlchemyRecordSink(session_factory=session_factory)

    checkpoint = await checkpoint_store.load(provider.name)
    records = [record async for record in provider.fetch(checkpoint=checkpoint)]
    records_fetched = len(records)
    newest_record_key: str | None = None

    if records:
        newest_record_key = records[0].key
        await record_sink.write_many(records)

    next_checkpoint = provider.build_checkpoint(
        previous_checkpoint=checkpoint,
        last_entry_id=newest_record_key,
    )
    if next_checkpoint is not None:
        await checkpoint_store.save(provider.name, next_checkpoint)

    async with session_factory() as session:
        stored_records_count = await session.scalar(
            select(func.count()).select_from(StoredRecordORM).where(
                StoredRecordORM.provider == provider.name
            )
        )
        stored_checkpoint = await session.scalar(
            select(CheckpointRecordORM).where(
                CheckpointRecordORM.scope == provider.name,
                CheckpointRecordORM.kind == "rss",
            )
        )

    print(f"Feed: {provider.feed_url}")
    print(f"SQLite DB: {db_path.resolve()}")
    print(f"Fetched new records this run: {records_fetched}")
    print(f"Total records stored for this feed: {stored_records_count or 0}")
    print(f"Checkpoint stored for this feed: {stored_checkpoint is not None}")
    if stored_checkpoint is not None:
        print(f"Checkpoint cursor: {stored_checkpoint.cursor}")
    if newest_record_key is not None:
        print(f"Newest record key this run: {newest_record_key}")

    await engine.dispose()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch an RSS feed into a SQLite database.")
    parser.add_argument("--feed-url", default=DEFAULT_FEED_URL, help="RSS feed URL to ingest.")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="Path to the SQLite database file.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(feed_url=args.feed_url, db_path=args.db_path))
