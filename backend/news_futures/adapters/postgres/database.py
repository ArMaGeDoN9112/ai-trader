from hashlib import sha256
import os
from pathlib import Path
from typing import Any

from psycopg import ClientCursor, Connection, sql
from psycopg.rows import dict_row


def schema_name(path: Path) -> str:
    key = os.path.normcase(str(path.resolve()))
    return 'trader_' + sha256(key.encode()).hexdigest()[:32]


def connect(path: Path, *, client_cursor: bool = True) -> Connection[dict[str, Any]]:
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        raise ValueError('Set DATABASE_URL to the PostgreSQL connection string')
    return Connection.connect(
        database_url, autocommit=True, row_factory=dict_row,
        cursor_factory=ClientCursor if client_cursor else None,
        options=f'-c search_path={schema_name(path)}', connect_timeout=5,
    )


def has_table(db: Connection[dict[str, Any]], table: str) -> bool:
    row = db.execute('SELECT to_regclass(%s) AS name', (table,)).fetchone()
    assert row is not None
    return row['name'] is not None


def archive_exists(path: Path, table: str) -> bool:
    with connect(path) as db:
        return has_table(db, table)


def initialize(db: Connection[dict[str, Any]], path: Path, ddl: str) -> None:
    with db.transaction():
        lock_writes(db, path)
        db.execute(sql.SQL('CREATE SCHEMA IF NOT EXISTS {}').format(sql.Identifier(schema_name(path))))
        if ddl:
            db.execute(ddl)


def lock_key(path: Path, operation: str) -> int:
    digest = sha256(f'{schema_name(path)}:{operation}'.encode()).digest()
    return int.from_bytes(digest[:8], 'big', signed=True)


def lock_writes(db: Connection[dict[str, Any]], path: Path) -> None:
    db.execute('SELECT pg_advisory_xact_lock(%s::bigint)', (lock_key(path, 'write'),))
