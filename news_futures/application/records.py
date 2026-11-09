"""Durable decisions, broker status and CLI reports over the runtime archive."""

from contextlib import closing
import json
from pathlib import Path
import sqlite3
from typing import Any

from news_futures.agents.events import EventArchive
from news_futures.news.publications import Archive, utc_now
from news_futures.application.config import load_settings
from news_futures.common.errors import RuntimeErrorInfo
from news_futures.trading.journal import trade_journal


def runtime_tables(archive: Archive) -> None:
    archive.db.executescript("""
        CREATE TABLE IF NOT EXISTS runtime_decisions (
            revision TEXT PRIMARY KEY REFERENCES revisions(revision),
            result TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS runtime_status (id INTEGER PRIMARY KEY CHECK (id = 1), data TEXT NOT NULL);
    """)


def save_result(archive: Archive, result: dict[str, Any]) -> None:
    result['at'] = utc_now().isoformat()
    with archive.db:
        archive.db.execute('''
            INSERT INTO runtime_decisions VALUES (?, ?)
            ON CONFLICT (revision) DO UPDATE SET result = excluded.result
        ''', (result['revision'], json.dumps(result)))


def pending_revisions(archive: Archive) -> list[sqlite3.Row]:
    return archive.db.execute("""
        SELECT revisions.revision, publication, result FROM revisions
        LEFT JOIN runtime_decisions USING (revision)
        WHERE historical = 0 AND revisions.status IN ('pending', 'context_only')
          AND json_extract(publication, '$.change') != 'delete'
          AND trim(json_extract(publication, '$.text')) != ''
          AND (result IS NULL OR json_extract(result, '$.status') = 'processing'
               OR json_extract(result, '$.reconcile_pending') = 1
               OR (json_extract(result, '$.inputs_saved') IS NULL
                   AND json_extract(result, '$.status') = 'blocked'
                   AND json_extract(result, '$.execution.cycle_id') IS NOT NULL))
        ORDER BY revisions.rowid
    """).fetchall()


def save_broker_status(archive: Archive, status: dict[str, Any], last_confirmed: dict[str, Any] | None) -> None:
    with archive.db:
        # Preserve the last successful snapshot even if this attempt failed.
        archive.db.execute('BEGIN IMMEDIATE')
        previous = archive.db.execute('SELECT data FROM runtime_status WHERE id = 1').fetchone()
        if previous:
            old = json.loads(previous['data'])
            instruments = old.get('instruments', {})
            for contract, item in status.get('instruments', {}).items():
                if item['ticker'] or contract not in instruments:
                    instruments[contract] = item
            status['instruments'] = instruments
            if last_confirmed is None and all(old.get(key) == status[key] for key in
                                              ('mode', 'account_id', 'contracts')):
                last_confirmed = old.get('last_confirmed')
                if last_confirmed is None and old.get('broker') == 'connected':
                    last_confirmed = {key: old[key] for key in ('checked_at', 'position', 'protection')}
        status['last_confirmed'] = last_confirmed
        archive.db.execute('INSERT OR REPLACE INTO runtime_status VALUES (1, ?)', (json.dumps(status),))


def review_path(directory: Path, attempt: str | None) -> Path:
    review = directory / 'review'
    return review / attempt if attempt else review


def runtime_report(config: Path, decision: str | None) -> dict[str, Any]:
    settings = load_settings(config)
    if not settings.publications.is_file():
        raise RuntimeErrorInfo('Сначала запустите serve для этого каталога данных.')
    archive = Archive(settings.publications)
    with closing(archive.db), EventArchive(settings.events) as events:
        if decision is None:
            status = archive.db.execute('SELECT data FROM runtime_status WHERE id = 1').fetchone()
            return {
                'broker': json.loads(status['data']) if status else None,
                'channels': [dict(row) for row in archive.db.execute('SELECT * FROM channels')],
                'notices': [dict(row) for row in archive.db.execute('SELECT * FROM notices ORDER BY id DESC LIMIT 20')],
                'decisions': [json.loads(row['result']) for row in archive.db.execute(
                    'SELECT result FROM runtime_decisions ORDER BY rowid DESC LIMIT 20',
                )],
            }
        row = archive.db.execute("""
            SELECT runtime_decisions.revision, result, publication FROM runtime_decisions JOIN revisions USING(revision)
            WHERE revision = ? OR CAST(json_extract(result, '$.decision_id') AS TEXT) = ?
        """, (decision, decision)).fetchone()
        if row is None:
            raise RuntimeErrorInfo('Решение не найдено. Возьмите decision_id или revision из runtime-status.')
        directory = settings.data_dir / 'decisions' / row['revision']
        result = json.loads(row['result'])
        if result.get('legacy'):
            directory = directory / 'resumed'
        review = review_path(directory, result.get('review_attempt'))
        report = {'result': result, 'publication': json.loads(row['publication'])}
        for name, file in (
            ('settings', directory / 'runtime.json'), ('route', directory / 'route' / 'trace.json'),
            ('event', directory / 'event.json'), ('context', review / 'context.json'),
            ('review', review / 'trace.json'),
        ):
            if file.is_file():
                report[name] = json.loads(file.read_bytes())
        if cycle_id := result.get('execution', {}).get('cycle_id'):
            report['trades'] = trade_journal(events.db, cycle_id=cycle_id)
        previous_reviews = [str(path) for path in (directory / 'review').glob('*/context.json') if path.parent != review]
        if previous_reviews:
            report['previous_reviews'] = previous_reviews
        if interrupted_routes := sorted(directory.glob('route-interrupted-*')):
            report['interrupted_routes'] = [str(path) for path in interrupted_routes]
        return report
