from collections.abc import Iterator
from contextlib import contextmanager
from functools import lru_cache, partial
from hashlib import sha256
import os
from pathlib import Path

from psycopg import Connection as DriverConnection
from sqlalchemy import Connection, Engine, create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import CreateSchema

from news_futures.common.paths import persistent_path


def schema_name(path: Path) -> str:
    stored = persistent_path(path)
    key = stored if stored.startswith('/') else os.path.normcase(stored)
    return 'trader_' + sha256(key.encode()).hexdigest()[:32]


@lru_cache(maxsize=64)
def engine(database_url: str, schema: str) -> Engine:
    return create_engine(
        'postgresql+psycopg://',
        creator=partial(DriverConnection.connect, database_url, options=f'-c search_path={schema}', connect_timeout=5),
        isolation_level='AUTOCOMMIT', poolclass=NullPool, hide_parameters=True,
    )


def connect(path: Path) -> Connection:
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        raise ValueError('Set DATABASE_URL to the PostgreSQL connection string')
    return engine(database_url, schema_name(path)).connect()


@contextmanager
def transaction(db: Connection) -> Iterator[None]:
    db.rollback()
    db.execution_options(isolation_level='READ COMMITTED')
    try:
        with db.begin():
            yield
    finally:
        if not db.invalidated:
            db.execution_options(isolation_level='AUTOCOMMIT')


def has_table(db: Connection, table: str) -> bool:
    return db.exec_driver_sql('SELECT to_regclass(%s)', (table,)).scalar_one() is not None


def archive_exists(path: Path, table: str) -> bool:
    with connect(path) as db:
        return has_table(db, table)


def initialize(db: Connection, path: Path, ddl: str = '') -> None:
    lock_writes(db, path)
    db.execute(CreateSchema(schema_name(path), if_not_exists=True))
    if ddl:
        db.exec_driver_sql(ddl)


def lock_key(path: Path, operation: str) -> int:
    digest = sha256(f'{schema_name(path)}:{operation}'.encode()).digest()
    return int.from_bytes(digest[:8], 'big', signed=True)


def lock_writes(db: Connection, path: Path) -> None:
    db.exec_driver_sql('SELECT pg_advisory_xact_lock(%s::bigint)', (lock_key(path, 'write'),))
