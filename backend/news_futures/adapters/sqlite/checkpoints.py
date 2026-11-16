"""Deserialize saved LangGraph state with an explicit model allowlist."""

from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path
import sqlite3

from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

from news_futures.domain.company import CompanyContext, CompanyProposal
from news_futures.domain.contracts import Review


def checkpoint_serializer() -> JsonPlusSerializer:
    return JsonPlusSerializer(allowed_msgpack_modules=[
        *((model.__module__, model.__name__) for model in (CompanyContext, CompanyProposal, Review)),
    ])


@contextmanager
def checkpoints(path: Path) -> Iterator[SqliteSaver]:
    with closing(sqlite3.connect(path, check_same_thread=False)) as db:
        yield SqliteSaver(db, serde=checkpoint_serializer())
