"""SQLite publication archive and atomic source cursor updates."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3
from typing import Any, cast
from uuid import uuid4

from news_futures.common.errors import StorageError
from news_futures.domain.publications import Publication, TEST_SOURCE, utc_now


TEST_TEXT_LIMIT = 4000


class Archive:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY, account_id INTEGER NOT NULL, state TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS channels (
                source TEXT PRIMARY KEY, pts INTEGER NOT NULL, checked_at TEXT NOT NULL,
                history_since TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS revisions (
                revision TEXT PRIMARY KEY, publication TEXT NOT NULL,
                historical INTEGER NOT NULL, status TEXT NOT NULL, trace TEXT
            );
            CREATE TABLE IF NOT EXISTS notices (
                id INTEGER PRIMARY KEY, source TEXT NOT NULL,
                observed_at TEXT NOT NULL, code TEXT NOT NULL
            );
        ''')

    def bind_session(self, session_id: str, account_id: int, state: dict[str, Any]) -> None:
        existing = self.db.execute('SELECT * FROM sessions').fetchall()
        if existing and (existing[0]['id'] != session_id or existing[0]['account_id'] != account_id):
            raise ValueError('Archive belongs to another MTProto session; use a separate archive')
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO sessions VALUES (?, ?, ?)',
                            (session_id, account_id, json.dumps(state)))

    def cursor(self, source: str) -> sqlite3.Row | None:
        return cast(sqlite3.Row | None, self.db.execute(
            'SELECT * FROM channels WHERE source=?', (source,),
        ).fetchone())

    def save_batch(
        self, source: str, pts: int, publications: list[Publication], *,
        historical: bool = False, notices: tuple[str, ...] = (), checked_at: datetime | None = None,
        history_since: datetime | None = None, history_before: datetime | None = None,
    ) -> None:
        if pts <= 0 or any(item.source != source for item in publications):
            raise ValueError('Invalid channel batch')
        now = utc_now()
        with self.db:
            current = self.cursor(source)
            if current and pts < current['pts']:
                raise ValueError('Channel pts cannot move backwards')
            for item in publications:
                context_only = historical or (
                    history_before is not None and (item.edited_at or item.published_at) < history_before
                )
                if context_only or now - item.published_at > timedelta(minutes=15):
                    status = 'context_only'
                elif item.change == 'delete' or not item.text.strip():
                    status = 'no_text'
                else:
                    status = 'pending'
                self.db.execute('INSERT OR IGNORE INTO revisions VALUES (?, ?, ?, ?, NULL)', (
                    item.revision_key(), json.dumps(item.record(), ensure_ascii=False),
                    context_only, status,
                ))
            self.db.execute('INSERT OR REPLACE INTO channels VALUES (?, ?, ?, ?)', (
                source, pts, (checked_at or now).isoformat(),
                current['history_since'] if current else (history_since or now).isoformat(),
            ))
            for code in notices:
                self.db.execute('INSERT INTO notices(source, observed_at, code) VALUES (?, ?, ?)',
                                (source, now.isoformat(), code))

    def notice(self, source: str, code: str) -> None:
        with self.db:
            self.db.execute('INSERT INTO notices(source, observed_at, code) VALUES (?, ?, ?)',
                            (source, utc_now().isoformat(), code))

    def enqueue_test(self, text: object) -> Publication:
        if not isinstance(text, str) or not 0 < len(text.strip()) <= TEST_TEXT_LIMIT:
            raise ValueError('invalid_test_news')
        now = utc_now()
        identity = uuid4().hex
        item = Publication(TEST_SOURCE, identity, f'urn:news-futures:test:{identity}', text.strip(), now, now)
        with self.db:
            self.db.execute("INSERT INTO revisions VALUES (?, ?, 0, 'pending', NULL)", (
                item.revision_key(), json.dumps(item.record(), ensure_ascii=False),
            ))
        return item

    def latest_revision(self, publication_id: str) -> dict[str, Any] | None:
        row = self.db.execute("""
            SELECT revision, historical, publication FROM revisions
            WHERE json_extract(publication, '$.id') = ?
            ORDER BY (json_extract(publication, '$.change') = 'delete') DESC,
                     coalesce(json_extract(publication, '$.edited_at'), json_extract(publication, '$.published_at')) DESC,
                     rowid DESC LIMIT 1
        """, (publication_id,)).fetchone()
        if row is None:
            return None
        return dict(row) | {'publication': json.loads(row['publication'])}

    def process_pending(self, simulate: Callable[[dict[str, Any]], dict[str, Any]]) -> None:
        rows = self.db.execute("SELECT * FROM revisions WHERE status='pending'").fetchall()
        for row in rows:
            publication = json.loads(row['publication'])
            with self.db:
                if utc_now() - datetime.fromisoformat(publication['published_at']) > timedelta(minutes=15):
                    self.db.execute("UPDATE revisions SET status='context_only' WHERE revision=?",
                                    (row['revision'],))
                else:
                    trace = simulate(publication)
                    self.db.execute("UPDATE revisions SET status='simulated', trace=? WHERE revision=?",
                                    (json.dumps(trace, ensure_ascii=False), row['revision']))

    def report(self) -> dict[str, Any]:
        return {
            'mode': 'simulation',
            'channels': [dict(row) for row in self.db.execute('SELECT * FROM channels')],
            'revisions': [dict(row) | {
                'publication': json.loads(row['publication']),
                'trace': json.loads(row['trace']) if row['trace'] else None,
            } for row in self.db.execute('SELECT * FROM revisions ORDER BY rowid')],
            'notices': [dict(row) for row in self.db.execute('SELECT * FROM notices ORDER BY id')],
        }

    def __enter__(self) -> 'Archive':
        return self

    def __exit__(self, *args: object) -> None:
        self.db.close()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        try:
            with self.db:
                yield
        except sqlite3.DatabaseError as error:
            raise StorageError(type(error).__name__) from error

    def lock_writes(self) -> None:
        self.db.execute('BEGIN IMMEDIATE')

    def historical_reprint(self, text: str, published_at: str) -> bool:
        return self.db.execute("""
            SELECT 1 FROM revisions
            WHERE historical = 1 AND trim(json_extract(publication, '$.text')) = trim(?)
              AND coalesce(json_extract(publication, '$.edited_at'),
                           json_extract(publication, '$.published_at')) <= ?
            LIMIT 1
        """, (text, published_at)).fetchone() is not None

    def initialize_runtime(self) -> None:
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS runtime_decisions (
                revision TEXT PRIMARY KEY REFERENCES revisions(revision),
                result TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS runtime_status (id INTEGER PRIMARY KEY CHECK (id = 1), data TEXT NOT NULL);
        """)


    def save_result(self, result: dict[str, Any]) -> None:
        result['at'] = utc_now().isoformat()
        with self.db:
            self.db.execute('''
                INSERT INTO runtime_decisions VALUES (?, ?)
                ON CONFLICT (revision) DO UPDATE SET result = excluded.result
            ''', (result['revision'], json.dumps(result)))


    def pending_revisions(self) -> list[dict[str, Any]]:
        rows = self.db.execute("""
            SELECT revisions.revision, publication, result FROM revisions
            LEFT JOIN runtime_decisions USING (revision)
            WHERE historical = 0 AND revisions.status IN ('pending', 'context_only')
              AND json_extract(publication, '$.change') != 'delete'
              AND trim(json_extract(publication, '$.text')) != ''
              AND (result IS NULL OR json_extract(result, '$.status') = 'processing'
                   OR json_extract(result, '$.reconcile_pending') = 1)
            ORDER BY revisions.rowid
        """).fetchall()
        return [dict(row) for row in rows]


    def save_broker_status(self, status: dict[str, Any], last_confirmed: dict[str, Any] | None) -> None:
        with self.db:
            # Preserve the last successful snapshot even if this attempt failed.
            self.db.execute('BEGIN IMMEDIATE')
            previous = self.db.execute('SELECT data FROM runtime_status WHERE id = 1').fetchone()
            if previous and last_confirmed is None:
                old = json.loads(previous['data'])
                if all(old.get(key) == status[key] for key in ('mode', 'account_id')):
                    last_confirmed = old.get('last_confirmed')
            status['last_confirmed'] = last_confirmed
            self.db.execute('INSERT OR REPLACE INTO runtime_status VALUES (1, ?)', (json.dumps(status),))
