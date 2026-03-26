# ingestion

Small ingestion primitives for providers, checkpoints, and record sinks.

## Package structure

- `ingestion.abstractions`: provider and storage interfaces
- `ingestion.models`: normalized record and result types
- `ingestion.providers`: concrete source implementations and provider-specific checkpoints
- `ingestion.stores`: in-memory and SQLAlchemy-backed persistence
- `ingestion.database`: SQLAlchemy ORM models

## Example

```python
async def run() -> None:
    from ingestion.providers.rss.checkpoint import RssCheckpoint
    from ingestion.providers.rss.provider import RssProvider
    from ingestion.stores.memory import InMemoryCheckpointStore, InMemoryRecordSink

    provider = RssProvider(feed_url="https://example.com/feed.xml")
    sink = InMemoryRecordSink()
    checkpoint_store = InMemoryCheckpointStore[RssCheckpoint]()

    checkpoint = await checkpoint_store.load(provider.name)

    records = [record async for record in provider.fetch(checkpoint=checkpoint)]
    await sink.write_many(records)

    if records:
        await checkpoint_store.save(
            provider.name,
            RssCheckpoint(feed_url=provider.feed_url, last_entry_id=records[0].key or ""),
        )
```

## Development

Run tests from the repository root with:

```bash
pytest -q
```
