"""Source-neutral publications and an atomic archive/outbox, using only SQLite."""

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any, cast

from news_futures.local import process_publication


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Publication:
    source: str
    external_id: str
    url: str
    text: str
    published_at: datetime
    received_at: datetime
    edited_at: datetime | None = None
    unread_media: bool = False
    change: str = 'new'

    def __post_init__(self) -> None:
        if not self.source or not self.external_id or self.change not in {'new', 'edit', 'delete'}:
            raise ValueError('Invalid publication identity or change')
        for value in (self.published_at, self.received_at, self.edited_at):
            if value is not None and (value.tzinfo is None or value.utcoffset() is None):
                raise ValueError('Publication times must have a timezone')

    def record(self) -> dict[str, Any]:
        result = asdict(self)
        result['id'] = f'{self.source}:{self.external_id}'
        for field in ('published_at', 'received_at', 'edited_at'):
            value = result[field]
            result[field] = value.astimezone(timezone.utc).isoformat() if value else None
        return result

    def revision_key(self) -> str:
        record = self.record()
        if self.change == 'delete':
            return hashlib.sha256(f'delete:{record["id"]}'.encode()).hexdigest()
        # Delivery time, username URLs and new/edit envelopes are not a new revision.
        content = [record[key] for key in (
            'source', 'external_id', 'published_at', 'edited_at', 'text', 'unread_media',
        )]
        return hashlib.sha256(json.dumps(content, ensure_ascii=False).encode()).hexdigest()


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
        history_since: datetime | None = None,
    ) -> None:
        if pts <= 0 or any(item.source != source for item in publications):
            raise ValueError('Invalid channel batch')
        now = utc_now()
        with self.db:
            current = self.cursor(source)
            if current and pts < current['pts']:
                raise ValueError('Channel pts cannot move backwards')
            for item in publications:
                if historical or now - item.published_at > timedelta(minutes=15):
                    status = 'context_only'
                elif item.change == 'delete' or not item.text.strip():
                    status = 'no_text'
                else:
                    status = 'pending'
                self.db.execute('INSERT OR IGNORE INTO revisions VALUES (?, ?, ?, ?, NULL)', (
                    item.revision_key(), json.dumps(item.record(), ensure_ascii=False),
                    historical, status,
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

    def latest_revision(self, publication_id: str) -> dict[str, Any] | None:
        row = self.db.execute("""
            SELECT revision, historical, publication FROM revisions
            WHERE json_extract(publication, '$.id') = ?
            ORDER BY (json_extract(publication, '$.change') = 'delete') DESC, rowid DESC LIMIT 1
        """, (publication_id,)).fetchone()
        if row is None:
            return None
        return dict(row) | {'publication': json.loads(row['publication'])}

    def process_pending(self) -> None:
        rows = self.db.execute("SELECT * FROM revisions WHERE status='pending'").fetchall()
        for row in rows:
            publication = json.loads(row['publication'])
            with self.db:
                if utc_now() - datetime.fromisoformat(publication['published_at']) > timedelta(minutes=15):
                    self.db.execute("UPDATE revisions SET status='context_only' WHERE revision=?",
                                    (row['revision'],))
                else:
                    trace = process_publication(publication)
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
