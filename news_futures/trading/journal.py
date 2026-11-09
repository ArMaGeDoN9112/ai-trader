"""SQLite journal for sandbox cycles and evidence needed for broker reconciliation."""

from collections.abc import Iterator
from dataclasses import dataclass, field
import json
from pathlib import Path
import sqlite3
from typing import Any

from news_futures.news.publications import utc_now


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


@dataclass
class TradeEvidence:
    operations: dict[str, str] = field(default_factory=dict)
    receipts: dict[str, str | None] = field(default_factory=dict)
    confirmed_fills: dict[str, list[int | str]] = field(default_factory=dict)
    close_lots: int | None = None


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


def close_requests(operations: dict[str, str]) -> list[tuple[str, str]]:
    return [(action, key) for action, key in operations.items() if action.partition(':')[0] == 'close']
