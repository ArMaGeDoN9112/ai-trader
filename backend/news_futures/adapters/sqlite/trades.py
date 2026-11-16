"""SQLite journal for sandbox cycles and evidence needed for broker reconciliation."""

from collections.abc import Iterator
from contextlib import AbstractContextManager
import json
from pathlib import Path
import sqlite3
from typing import Any

from news_futures.adapters.sqlite.locks import exclusive_operation
from news_futures.application.ports.storage import TradeEvidence
from news_futures.common.errors import Rejected
from news_futures.domain.publications import utc_now
from news_futures.domain.terms import CloseIntent, EntryReservation


def initialize_journal(db: sqlite3.Connection) -> None:
    db.executescript("""
        CREATE TABLE IF NOT EXISTS sandbox_cycles (
            id TEXT PRIMARY KEY NOT NULL, account_id TEXT NOT NULL, proposal_id TEXT NOT NULL,
            terms TEXT NOT NULL, approval TEXT NOT NULL, source TEXT NOT NULL,
            status TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '', close_requested INTEGER NOT NULL DEFAULT 0,
            UNIQUE(account_id, proposal_id)
        );
        CREATE TABLE IF NOT EXISTS sandbox_journal (
            seq INTEGER PRIMARY KEY, cycle_id TEXT NOT NULL REFERENCES sandbox_cycles(id),
            at TEXT NOT NULL, kind TEXT NOT NULL, details TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS sandbox_cycle_evidence ON sandbox_journal(cycle_id, kind, seq);
    """)


def active_cycles(db: sqlite3.Connection, account_id: str) -> list[sqlite3.Row]:
    if db.execute("SELECT 1 FROM sqlite_master WHERE name = 'sandbox_cycles'").fetchone() is None:
        return []
    return db.execute("""
        SELECT * FROM sandbox_cycles WHERE account_id = ? AND status NOT IN ('closed', 'no_fill')
    """, (account_id,)).fetchall()


def journal_events(db: sqlite3.Connection, cycle_id: str) -> Iterator[dict[str, Any]]:
    for row in db.execute('SELECT seq, at, kind, details FROM sandbox_journal WHERE cycle_id = ? ORDER BY seq',
                          (cycle_id,)):
        yield dict(row) | {'details': json.loads(row['details'])}


def append_event(db: sqlite3.Connection, cycle_id: str, kind: str, **details: Any) -> None:
    """Append inside the caller's transaction."""
    db.execute('INSERT INTO sandbox_journal (cycle_id, at, kind, details) VALUES (?, ?, ?, ?)',
               (cycle_id, utc_now().isoformat(), kind, json.dumps(details, default=str)))


def read_evidence(db: sqlite3.Connection, cycle_id: str) -> TradeEvidence:
    evidence = TradeEvidence()
    for row in db.execute("""
        SELECT kind, details FROM sandbox_journal
        WHERE cycle_id = ? AND kind IN ('intent', 'response_received', 'response_rejected', 'confirmed_fills')
        ORDER BY seq
    """, (cycle_id,)):
        details = json.loads(row['details'])
        kind = row['kind']
        if kind == 'intent':
            action = details['action']
            evidence.operations[action] = details['operation_id']
            if action.partition(':')[0] == 'close':
                evidence.close_lots = details['lots']
        elif kind == 'confirmed_fills':
            evidence.confirmed_fills = details['fills']
        elif kind == 'response_rejected' or details.get('broker_id'):
            evidence.receipts[details['action']] = details.get('broker_id')
    return evidence


def trade_journal(
    db: sqlite3.Connection, *, cycle_id: str | None = None, approval: Path | None = None,
) -> list[dict[str, Any]]:
    approval_path = str(approval.resolve()) if approval else None
    if db.execute("SELECT 1 FROM sqlite_master WHERE name = 'sandbox_cycles'").fetchone() is None:
        return []
    return [
        {'reservation': json.loads(row['terms']), 'approval': row['approval'],
         'source': row['source'], 'status': row['status'], 'reason': row['reason'],
         'events': [{'at': event['at'], 'kind': event['kind'], 'details': event['details']}
                    for event in journal_events(db, row['id'])]}
        for row in db.execute("""
            SELECT * FROM sandbox_cycles
            WHERE (? IS NULL OR id = ?)
              AND (? IS NULL OR approval = ? OR EXISTS (
                  SELECT 1 FROM sandbox_journal WHERE cycle_id = sandbox_cycles.id
                  AND kind = 'reviewed_close' AND json_extract(details, '$.approval') = ?
              ))
            ORDER BY rowid
        """, (cycle_id, cycle_id, approval_path, approval_path, approval_path))
    ]


class TradeJournal:
    """Journal operations share the event archive's transaction and connection."""

    def __init__(self, db: sqlite3.Connection):
        self.db = db

    def initialize(self) -> None:
        initialize_journal(self.db)

    def exclusive(self) -> AbstractContextManager[bool]:
        # Broker operations serialize while intents commit on the archive connection.
        database = self.db.execute('PRAGMA database_list').fetchone()['file']
        return exclusive_operation(database + '.sandbox-lock')

    def active(self, account_id: str) -> list[dict[str, Any]]:
        return [dict(row) for row in active_cycles(self.db, account_id)]

    def evidence(self, cycle_id: str) -> TradeEvidence:
        return read_evidence(self.db, cycle_id)

    def report(self, *, approval: Path) -> list[dict[str, Any]]:
        return trade_journal(self.db, approval=approval)

    def require_available(self, account_id: str) -> None:
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS entry_reservations (
                account_id TEXT PRIMARY KEY NOT NULL, id TEXT NOT NULL, terms TEXT NOT NULL
            )
        """)
        if self.db.execute('SELECT 1 FROM entry_reservations WHERE account_id = ?', (account_id,)).fetchone():
            raise Rejected('entry_already_reserved')

    def reserve(self, entry: EntryReservation) -> None:
        self.db.execute('INSERT INTO entry_reservations VALUES (?, ?, ?)',
                        (entry.account_id, entry.id, entry.model_dump_json()))

    def release(self, entry: EntryReservation) -> bool:
        result = self.db.execute('DELETE FROM entry_reservations WHERE account_id = ? AND id = ?',
                                 (entry.account_id, entry.id))
        return result.rowcount == 1

    def open_cycle(self, entry: EntryReservation, approval: Path, source: str) -> None:
        if self.db.execute('SELECT 1 FROM sandbox_cycles WHERE account_id = ? AND proposal_id = ?',
                           (entry.account_id, entry.proposal_id)).fetchone():
            raise Rejected('proposal_already_reserved')
        self.db.execute("""
            INSERT INTO sandbox_cycles (id, account_id, proposal_id, terms, approval, source, status)
            VALUES (?, ?, ?, ?, ?, ?, 'reserved')
        """, (entry.id, entry.account_id, entry.proposal_id, entry.model_dump_json(),
              str(approval.resolve()), source))

    def completed_manual(self, manual: CloseIntent, account_id: str, source: str) -> dict[str, str] | None:
        row = self.db.execute("""SELECT c.status, c.reason FROM sandbox_cycles c
            JOIN sandbox_journal j ON j.cycle_id = c.id
            WHERE c.id = ? AND c.account_id = ? AND c.source = ? AND c.status IN ('closed', 'no_fill')
            AND j.kind = 'manual_close' AND json_extract(j.details, '$.request_id') = ?
        """, (manual.cycle_id, account_id, source, manual.request_id)).fetchone()
        return dict(row) if row is not None else None

    def has_manual_close(self, cycle_id: str, request_id: str) -> bool:
        return self.db.execute("""SELECT 1 FROM sandbox_journal WHERE cycle_id = ?
            AND kind = 'manual_close' AND json_extract(details, '$.request_id') = ?
        """, (cycle_id, request_id)).fetchone() is not None

    def pending_reviewed_close(self, cycle_id: str) -> str | None:
        saved = self.db.execute("""
            SELECT details FROM sandbox_journal WHERE cycle_id = ? AND kind = 'reviewed_close'
            AND seq > coalesce((SELECT max(seq) FROM sandbox_journal
                WHERE cycle_id = ? AND kind = 'reviewed_close_abandoned'), 0)
            ORDER BY seq DESC LIMIT 1
        """, (cycle_id, cycle_id)).fetchone()
        approval: str | None = json.loads(saved['details'])['approval'] if saved else None
        return approval

    def set_close_requested(self, cycle_id: str, requested: bool) -> None:
        self.db.execute('UPDATE sandbox_cycles SET close_requested = ? WHERE id = ?', (requested, cycle_id))

    def append(self, cycle_id: str, kind: str, **details: Any) -> None:
        append_event(self.db, cycle_id, kind, **details)

    def finish(self, cycle_id: str, status: str, reason: str) -> None:
        self.db.execute('UPDATE sandbox_cycles SET status = ?, reason = ? WHERE id = ?', (status, reason, cycle_id))
