from ingestion.stores.memory import InMemoryCheckpointStore, InMemoryRecordSink

__all__ = [
    "InMemoryCheckpointStore",
    "InMemoryRecordSink",
]

try:
    from ingestion.stores._serialization import DataclassCheckpointCodec
    from ingestion.stores.checkpoint_store import SqlAlchemyCheckpointStore
    from ingestion.stores.record_store import SqlAlchemyRecordStore
except ModuleNotFoundError as exc:
    if exc.name != "sqlalchemy":
        raise
else:
    globals().update(
        {
            "DataclassCheckpointCodec": DataclassCheckpointCodec,
            "SqlAlchemyCheckpointStore": SqlAlchemyCheckpointStore,
            "SqlAlchemyRecordStore": SqlAlchemyRecordStore,
        }
    )
    __all__.extend(
        [
            "DataclassCheckpointCodec",
            "SqlAlchemyCheckpointStore",
            "SqlAlchemyRecordStore",
        ]
    )
