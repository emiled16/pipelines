from __future__ import annotations

import asyncio

from ingestion.models import CursorCheckpoint, Record
from ingestion.stores import InMemoryCheckpointStore, InMemoryRecordSink


def test_memory_checkpoint_store_round_trips_values() -> None:
    async def run() -> None:
        store = InMemoryCheckpointStore[CursorCheckpoint]()
        checkpoint = CursorCheckpoint(value="cursor-1")

        await store.save("rss:feed", checkpoint)

        assert await store.load("rss:feed") == checkpoint

    asyncio.run(run())


def test_memory_record_sink_collects_records() -> None:
    async def run() -> None:
        sink = InMemoryRecordSink()
        record = Record(provider="rss:test", key="entry-1", payload={"id": "entry-1"})

        await sink.write(record)

        assert sink.records == [record]

    asyncio.run(run())
