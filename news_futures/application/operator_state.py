"""Durable operator intents shared by the panel and runtime."""

from collections.abc import Iterator
from contextlib import closing, contextmanager
import json
from pathlib import Path
import sqlite3
from typing import Any

from news_futures.news.publications import utc_now
from news_futures.trading.terms import CloseIntent


class OperatorState:
    def __init__(self, directory: Path):
        self.path = directory / 'operator.sqlite3'

    @contextmanager
    def _read(self) -> Iterator[sqlite3.Connection | None]:
        try:
            db = sqlite3.connect(self.path.as_uri() + '?mode=ro', uri=True)
        except sqlite3.OperationalError:
            if not self.path.exists():
                yield None
                return
            raise
        with closing(db):
            db.row_factory = sqlite3.Row
            yield db

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('CREATE TABLE IF NOT EXISTS preferences (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
            db.execute("""CREATE TABLE IF NOT EXISTS close_intents (
                request_id TEXT PRIMARY KEY, cycle_id TEXT NOT NULL, contract_id TEXT NOT NULL, side TEXT NOT NULL,
                quantity INTEGER NOT NULL, at TEXT NOT NULL, status TEXT NOT NULL, reason TEXT NOT NULL DEFAULT ''
            )""")
            db.execute('BEGIN IMMEDIATE')
            db.row_factory = sqlite3.Row
            yield db

    def _get(self, key: str, default: dict[str, Any]) -> dict[str, Any]:
        with self._read() as db:
            if db is not None and db.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'preferences'",
            ).fetchone():
                row = db.execute('SELECT value FROM preferences WHERE key = ?', (key,)).fetchone()
                if row:
                    return dict(json.loads(row['value']))
        return default

    def pause(self) -> dict[str, Any]:
        return self._get('pause', {'paused': False, 'changed_at': None})

    def set_pause(self, paused: bool) -> None:
        with self._write() as db:
            row = db.execute("SELECT value FROM preferences WHERE key = 'pause'").fetchone()
            current = json.loads(row['value'])['paused'] if row else False
            if current != paused:
                db.execute("INSERT OR REPLACE INTO preferences VALUES ('pause', ?)", (json.dumps({
                    'paused': paused, 'changed_at': utc_now().isoformat(),
                }),))

    def saved(self) -> dict[str, Any]:
        return self._get('settings', {})

    def save_settings(self, values: dict[str, Any], *, expected: dict[str, Any] | None = None) -> None:
        with self._write() as db:
            row = db.execute("SELECT value FROM preferences WHERE key = 'settings'").fetchone()
            if expected is not None and expected != (json.loads(row['value']) if row else {}):
                raise ValueError('settings_conflict')
            db.execute("INSERT OR REPLACE INTO preferences VALUES ('settings', ?)", (
                json.dumps(values | {'changed_at': utc_now().isoformat()}),
            ))

    def close_intent(self, *, pending: bool = False, request_id: str | None = None) -> dict[str, Any] | None:
        with self._read() as db:
            if db is None or not db.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'close_intents'",
            ).fetchone():
                return None
            row = db.execute("""SELECT * FROM close_intents WHERE (NOT ? OR status = 'queued')
                             AND (? IS NULL OR request_id = ?) ORDER BY rowid DESC LIMIT 1""",
                             (pending, request_id, request_id)).fetchone()
            return dict(row) if row else None

    def queue(self, intent: CloseIntent) -> tuple[int, str]:
        with self._write() as db:
            existing = db.execute('SELECT * FROM close_intents WHERE request_id = ?', (intent.request_id,)).fetchone()
            if existing:
                same = (existing['cycle_id'], existing['contract_id'], existing['side'], existing['quantity']) == (
                    intent.cycle_id, intent.contract_id, intent.side, intent.quantity)
                return (202, 'close_queued') if same else (409, 'invalid_request')
            if db.execute("SELECT 1 FROM close_intents WHERE status = 'queued' OR (cycle_id = ? AND status = 'accepted')",
                          (intent.cycle_id,)).fetchone():
                return 409, 'close_already_requested'
            db.execute('INSERT INTO close_intents VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                       (intent.request_id, intent.cycle_id, intent.contract_id, intent.side, intent.quantity,
                        utc_now().isoformat(), 'queued', ''))
        return 202, 'close_queued'

    def finish_close(self, request_id: str, status: str, reason: str) -> None:
        with self._write() as db:
            db.execute('UPDATE close_intents SET status = ?, reason = ? WHERE request_id = ?', (status, reason, request_id))
