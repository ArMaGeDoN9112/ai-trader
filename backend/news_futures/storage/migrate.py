"""Import the runtime's SQLite archives during the PostgreSQL cutover."""

from contextlib import closing
import json
from pathlib import Path
import sqlite3
from typing import Any, cast

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import Checkpoint

from langgraph.checkpoint.postgres import PostgresSaver
from psycopg import Connection, sql

from news_futures.storage.checkpoints import checkpoint_serializer, checkpoints, migrate_model_paths
from news_futures.storage.database import archive_exists, connect, has_table, lock_writes, schema_name
from news_futures.storage.events import EventArchive
from news_futures.storage.locks import exclusive_operation
from news_futures.storage.operator_state import OperatorState
from news_futures.storage.publications import Archive
from news_futures.common.errors import RuntimeErrorInfo


ARCHIVES: dict[str, dict[str, str | None]] = {
    'publications': {'sessions': None, 'channels': None, 'revisions': 'ordinal', 'notices': 'id',
                     'runtime_decisions': 'ordinal', 'runtime_status': None},
    'events': {'event_revisions': None, 'event_decisions': 'id', 'position_entry_reasons': None,
               'sandbox_cycles': 'ordinal', 'sandbox_journal': 'seq', 'entry_reservations': None},
    'operator': {'preferences': None, 'close_intents': 'ordinal'},
    'checkpoints': {'checkpoints': None, 'checkpoint_blobs': None, 'checkpoint_writes': None},
}


def migrate_checkpoints(directory: Path) -> dict[str, int]:
    path = directory / 'checkpoints'
    with (exclusive_operation(directory / 'runtime-lock') as runtime,
          exclusive_operation(directory / 'events', 'sandbox') as sandbox,
          exclusive_operation(path, 'checkpoint-setup') as setup):
        if not runtime or not sandbox or not setup:
            raise ValueError('Stop the runtime and sandbox executor before checkpoint migration')
        with connect(path, client_cursor=False) as db, db.transaction():
            report = {'checkpoint_blobs': 0, 'checkpoint_writes': 0}
            if not has_table(db, 'checkpoints'):
                return report
            lock_writes(db, path)
            db.execute('LOCK TABLE checkpoints, checkpoint_blobs, checkpoint_writes IN EXCLUSIVE MODE')
            serializer = checkpoint_serializer()
            for table, keys in (
                ('checkpoint_blobs', ('thread_id', 'checkpoint_ns', 'channel', 'version')),
                ('checkpoint_writes', ('thread_id', 'checkpoint_ns', 'checkpoint_id', 'task_id', 'idx')),
            ):
                rows = db.execute(sql.SQL('SELECT * FROM {} WHERE blob IS NOT NULL').format(sql.Identifier(table)))
                for row in rows.fetchall():
                    original = row['blob']
                    migrated = migrate_model_paths(row['type'], original)
                    serializer.loads_typed((row['type'], migrated))
                    if migrated != original:
                        condition = sql.SQL(' AND ').join(sql.SQL('{} = %s').format(sql.Identifier(key)) for key in keys)
                        db.execute(sql.SQL('UPDATE {} SET blob = %s WHERE {}').format(sql.Identifier(table), condition),
                                   (migrated, *(row[key] for key in keys)))
                        report[table] += 1
            return report


def require_migration(directory: Path) -> None:
    if any((directory / f'{name}.sqlite3').is_file() for name in ARCHIVES):
        if not archive_exists(directory / 'publications', 'sqlite_import'):
            raise RuntimeErrorInfo('SQLite runtime data found. Run migrate-sqlite with the operator and worker stopped.')


def import_tables(source: sqlite3.Connection, db: Connection[dict[str, Any]],
                  tables: dict[str, str | None]) -> dict[str, int]:
    source_tables = {name for name, in source.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    counts = {}
    for table, sequence in tables.items():
        if table not in source_tables:
            continue
        selection = 'rowid AS ordinal, *' if sequence == 'ordinal' else '*'
        rows = source.execute(f'SELECT {selection} FROM "{table}" ORDER BY rowid')
        columns = sql.SQL(', ').join(sql.Identifier(column[0]) for column in rows.description)
        count = 0
        with db.cursor().copy(sql.SQL('COPY {} ({}) FROM STDIN').format(sql.Identifier(table), columns)) as copy:
            for row in rows:
                copy.write_row(row)
                count += 1
        counts[table] = count
        if sequence and count:
            maximum = db.execute(sql.SQL('SELECT max({}) AS value FROM {}').format(
                sql.Identifier(sequence), sql.Identifier(table),
            )).fetchone()
            assert maximum is not None
            db.execute('SELECT setval(pg_get_serial_sequence(%s, %s), %s)', (table, sequence, maximum['value']))
    return counts


def import_checkpoints(source: sqlite3.Connection, db: Connection[dict[str, Any]]) -> dict[str, int]:
    saver = PostgresSaver(db, serde=checkpoint_serializer())
    count = 0
    for thread, namespace, parent, kind, data, metadata in source.execute('''
        SELECT thread_id, checkpoint_ns, parent_checkpoint_id, type, checkpoint, metadata
        FROM checkpoints ORDER BY checkpoint_id
    '''):
        config: RunnableConfig = {'configurable': {'thread_id': thread, 'checkpoint_ns': namespace,
                                                   'checkpoint_id': parent}}
        checkpoint = cast(Checkpoint, saver.serde.loads_typed((kind, migrate_model_paths(kind, data))))
        saver.put(config, checkpoint, json.loads(metadata), checkpoint['channel_versions'])
        count += 1

    write_columns = {row[1] for row in source.execute('PRAGMA table_info(writes)')}
    task_path = 'task_path' if 'task_path' in write_columns else "''"
    writes = 0
    with db.cursor().copy('''
        COPY checkpoint_writes (thread_id, checkpoint_ns, checkpoint_id, task_id, task_path, idx, channel, type, blob)
        FROM STDIN
    ''') as copy:
        for *columns, kind, data in source.execute(f'''
            SELECT thread_id, checkpoint_ns, checkpoint_id, task_id, {task_path}, idx, channel, type, value FROM writes
        '''):
            copy.write_row((*columns, kind, migrate_model_paths(kind, data)))
            writes += 1
    return {'checkpoints': count, 'checkpoint_writes': writes}


def migrate(directory: Path) -> dict[str, Any]:
    sources = {name: directory / f'{name}.sqlite3' for name in ARCHIVES}
    sources = {name: path for name, path in sources.items() if path.is_file()}
    if not sources:
        raise ValueError(f'No runtime SQLite archives in {directory}')

    with (exclusive_operation(directory / 'runtime-lock') as runtime,
          exclusive_operation(directory / 'events', 'sandbox') as sandbox):
        if not runtime or not sandbox:
            raise ValueError('Stop the runtime and sandbox executor before migration')

        publications = directory / 'publications'
        if archive_exists(publications, 'sqlite_import'):
            raise ValueError('SQLite runtime was already imported')

        with Archive(publications) as archive:
            archive.initialize_runtime()
        with EventArchive(directory / 'events') as events:
            events.trades.initialize()
        state = OperatorState(directory)
        with connect(state.path) as db:
            state.initialize(db)
        with checkpoints(directory / 'checkpoints'):
            pass

        with connect(publications, client_cursor=False) as db, db.transaction():
            report = {}
            for name, tables in ARCHIVES.items():
                path = directory / name
                lock_writes(db, path)
                db.execute(sql.SQL('SET LOCAL search_path TO {}').format(sql.Identifier(schema_name(path))))
                for table in tables:
                    data = db.execute(sql.SQL('SELECT 1 FROM {} LIMIT 1').format(sql.Identifier(table))).fetchone()
                    if data:
                        raise ValueError(f'Target PostgreSQL table {name}.{table} already contains data')
                if name in sources:
                    with closing(sqlite3.connect(sources[name].resolve().as_uri() + '?mode=ro', uri=True)) as source:
                        source.execute('BEGIN')
                        report[name] = (import_checkpoints(source, db) if name == 'checkpoints'
                                        else import_tables(source, db, tables))

            marker = sql.Identifier(schema_name(publications), 'sqlite_import')
            db.execute(sql.SQL('CREATE TABLE {} (imported_at timestamptz NOT NULL DEFAULT now())').format(marker))
            db.execute(sql.SQL('INSERT INTO {} DEFAULT VALUES').format(marker))
            return report
