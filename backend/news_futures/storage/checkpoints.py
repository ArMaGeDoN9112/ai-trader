"""Deserialize saved LangGraph state with an explicit model allowlist."""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.checkpoint.serde.jsonplus import EXT_PYDANTIC_V2, JsonPlusSerializer
import ormsgpack

from news_futures.common.errors import RuntimeErrorInfo
from news_futures.storage.database import connect, initialize
from news_futures.storage.locks import exclusive_operation
from news_futures.schemas.company import CompanyContext, CompanyProposal
from news_futures.schemas.contracts import Review


MODELS = {(model.__module__, model.__name__) for model in (CompanyContext, CompanyProposal, Review)}
LEGACY_MODELS = {
    (f'news_futures.{package}.company', model.__name__): model.__module__
    for package in ('agents', 'domain') for model in (CompanyContext, CompanyProposal)
} | {(f'news_futures.{package}.contracts', 'Review'): Review.__module__ for package in ('agents', 'domain')}


def migrate_model_paths(kind: str, data: bytes) -> bytes:
    if kind != 'msgpack' or b'news_futures.' not in data:
        return data
    changed = False
    error: str | None = None

    def extension(code: int, payload: bytes) -> ormsgpack.Ext:
        nonlocal changed, error
        if b'news_futures.' not in payload:
            return ormsgpack.Ext(code, payload)
        value = ormsgpack.unpackb(payload, ext_hook=extension, option=ormsgpack.OPT_NON_STR_KEYS)
        if code == EXT_PYDANTIC_V2:
            module, name, *fields = value
            if module.startswith('news_futures.'):
                target = LEGACY_MODELS.get((module, name), module)
                if (target, name) not in MODELS:
                    error = f'Unsupported checkpoint model: {module}.{name}'
                changed |= target != module
                value = [target, name, *fields]
        elif isinstance(value, (list, tuple)) and value and isinstance(value[0], str) and value[0].startswith('news_futures.'):
            error = f'Unsupported checkpoint model encoding: {value[0]}'
        return ormsgpack.Ext(code, ormsgpack.packb(value, option=ormsgpack.OPT_NON_STR_KEYS))

    state = ormsgpack.unpackb(data, ext_hook=extension, option=ormsgpack.OPT_NON_STR_KEYS)
    if error:
        raise RuntimeErrorInfo(error)
    return ormsgpack.packb(state, option=ormsgpack.OPT_NON_STR_KEYS) if changed else data


class CheckpointSerializer(JsonPlusSerializer):
    def loads_typed(self, data: tuple[str, bytes]) -> Any:
        if migrate_model_paths(*data) != data[1]:
            raise RuntimeErrorInfo('Checkpoint model paths require migration. Run migrate-checkpoints with the operator and worker stopped.')
        return super().loads_typed(data)


def checkpoint_serializer() -> JsonPlusSerializer:
    return CheckpointSerializer(allowed_msgpack_modules=MODELS)


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
