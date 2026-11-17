"""Deserialize saved LangGraph state with an explicit model allowlist."""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

from news_futures.adapters.postgres.database import connect, initialize
from news_futures.adapters.postgres.locks import exclusive_operation
from news_futures.domain.company import CompanyContext, CompanyProposal
from news_futures.domain.contracts import Review


def checkpoint_serializer() -> JsonPlusSerializer:
    return JsonPlusSerializer(allowed_msgpack_modules=[
        *((model.__module__, model.__name__) for model in (CompanyContext, CompanyProposal, Review)),
    ])


@contextmanager
def checkpoints(path: Path) -> Iterator[PostgresSaver]:
    with connect(path, client_cursor=False) as db:
        with exclusive_operation(path, 'checkpoint-setup') as acquired:
            if not acquired:
                raise RuntimeError('Concurrent checkpoint setup')
            initialize(db, path, '')
            saver = PostgresSaver(db, serde=checkpoint_serializer())
            saver.setup()
        yield saver
