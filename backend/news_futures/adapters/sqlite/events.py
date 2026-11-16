"""SQLite event history, preserving matching and entry evidence transactions."""

from collections.abc import Iterator
from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
from typing import Any

from news_futures.adapters.sqlite.trades import TradeJournal
from news_futures.common.errors import StorageError
from news_futures.domain.company import EntryReason
from news_futures.domain.publications import TEST_SOURCE


class EventArchive:
    def __init__(self, path: Path, *, testing_revision: str | None = None):
        self.testing_revision = testing_revision
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.trades = TradeJournal(self.db)
        try:
            self.db.executescript('''
                PRAGMA journal_mode=WAL;
                PRAGMA foreign_keys=ON;
                CREATE TABLE IF NOT EXISTS event_revisions (
                    revision TEXT PRIMARY KEY NOT NULL, publication TEXT NOT NULL, links TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS event_decisions (
                    id INTEGER PRIMARY KEY, revision TEXT NOT NULL REFERENCES event_revisions(revision),
                    details TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS event_revision_history ON event_decisions(revision, id);
            ''')
        except BaseException:
            self.db.close()
            raise

    def __enter__(self) -> 'EventArchive':
        return self

    def __exit__(self, *args: object) -> None:
        self.db.close()

    def history(self, *, current_only: bool = False) -> list[dict[str, Any]]:
        current = 'AND d.id IN (SELECT MAX(id) FROM event_decisions GROUP BY revision)' if current_only else ''
        rows = self.db.execute(f'''
            SELECT d.id, d.revision, d.details, r.publication, r.links
            FROM event_decisions d JOIN event_revisions r USING (revision)
            WHERE (json_extract(r.publication, '$.source') IS NOT ? OR d.revision = ?) {current}
            ORDER BY d.id
        ''', (TEST_SOURCE, self.testing_revision))
        return [json.loads(row['details']) | {
            'decision_id': row['id'], 'revision': row['revision'],
            'publication': json.loads(row['publication']), 'links': json.loads(row['links']),
        } for row in rows]

    def append_decision(self, revision: str, details: dict[str, Any],
                        publication: dict[str, Any], links: list[dict[str, Any]]) -> dict[str, Any]:
        cursor = self.db.execute('INSERT INTO event_decisions(revision, details) VALUES (?, ?)', (
            revision, json.dumps(details, ensure_ascii=False, allow_nan=False),
        ))
        return details | {'decision_id': cursor.lastrowid, 'revision': revision,
                          'publication': publication, 'links': links}

    def append_revision(self, revision: str, details: dict[str, Any],
                       publication: dict[str, Any], links: list[dict[str, Any]]) -> dict[str, Any]:
        self.db.execute('INSERT INTO event_revisions VALUES (?, ?, ?)', (
            revision, json.dumps(publication, ensure_ascii=False), json.dumps(links, ensure_ascii=False),
        ))
        return self.append_decision(revision, details, publication, links)

    @contextmanager
    def transaction(self, *, immediate: bool = False) -> Iterator[None]:
        try:
            with self.db:
                self.db.execute('BEGIN IMMEDIATE' if immediate else 'BEGIN')
                yield
        except sqlite3.DatabaseError as error:
            raise StorageError(type(error).__name__) from error

    def latest_decision(self, revision: str) -> dict[str, Any] | None:
        row = self.db.execute("""
            SELECT id, details, publication, links
            FROM event_decisions JOIN event_revisions USING (revision)
            WHERE revision = ? ORDER BY id DESC LIMIT 1
        """, (revision,)).fetchone()
        if row is None:
            return None
        details: dict[str, Any] = json.loads(row['details'])
        return details | {
            'decision_id': row['id'], 'revision': revision,
            'publication': json.loads(row['publication']), 'links': json.loads(row['links']),
        }

    def last_decision_id(self) -> int | None:
        value: int | None = self.db.execute('SELECT MAX(id) FROM event_decisions').fetchone()[0]
        return value

    def current_decision_id(self, revision: str) -> int | None:
        value: int | None = self.db.execute(
            'SELECT MAX(id) FROM event_decisions WHERE revision = ?', (revision,),
        ).fetchone()[0]
        return value

    def save_entry_reason(self, record: EntryReason) -> None:
        self.db.execute('''
            CREATE TABLE IF NOT EXISTS position_entry_reasons (
                account_id TEXT NOT NULL, position_id TEXT NOT NULL,
                contract_id TEXT NOT NULL, reason TEXT NOT NULL, sources TEXT NOT NULL DEFAULT '[]',
                PRIMARY KEY (account_id, position_id)
            )
        ''')
        existing = self.entry_reason(record.account_id, record.position_id)
        if existing is None:
            self.db.execute('INSERT INTO position_entry_reasons VALUES (?, ?, ?, ?, ?)', (
                record.account_id, record.position_id, record.contract_id, record.reason,
                json.dumps(record.model_dump(mode='json')['sources'], ensure_ascii=False),
            ))
        elif existing != record:
            raise ValueError('Причина входа уже зафиксирована для этой позиции')


    def entry_reason(self, account_id: str, position_id: str) -> EntryReason | None:
        if not self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'position_entry_reasons'"
        ).fetchone():
            return None
        row = self.db.execute(
            'SELECT * FROM position_entry_reasons WHERE account_id = ? AND position_id = ?',
            (account_id, position_id),
        ).fetchone()
        if row is None:
            return None
        data = dict(row)
        data['sources'] = json.loads(data['sources'])
        return EntryReason.model_validate(data)
