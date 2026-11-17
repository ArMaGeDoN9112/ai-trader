"""Read decisions, broker status and trace artifacts for the CLI."""

from contextlib import closing
import json
from pathlib import Path
from typing import Any

from news_futures.storage.database import archive_exists
from news_futures.storage.events import EventArchive
from news_futures.storage.publications import Archive
from news_futures.storage.trades import trade_journal
from news_futures.agents.analysis import TEAM_DIRECTORY
from news_futures.config import load_settings
from news_futures.common.errors import RuntimeErrorInfo


def runtime_report(config: Path, decision: str | None) -> dict[str, Any]:
    settings = load_settings(config)
    if not archive_exists(settings.publications, 'runtime_status'):
        raise RuntimeErrorInfo('Сначала запустите serve для этого каталога данных.')
    archive = Archive(settings.publications)
    with closing(archive.db), EventArchive(settings.events) as events:
        if decision is None:
            status = archive.db.exec_driver_sql('SELECT data FROM runtime_status WHERE id = 1').mappings().fetchone()
            return {
                'broker': json.loads(status['data']) if status else None,
                'channels': [dict(row) for row in archive.db.exec_driver_sql('SELECT * FROM channels').mappings()],
                'notices': [dict(row) for row in archive.db.exec_driver_sql('SELECT * FROM notices ORDER BY id DESC LIMIT 20').mappings()],
                'decisions': [json.loads(row['result']) for row in archive.db.exec_driver_sql(
                    'SELECT result FROM runtime_decisions ORDER BY ordinal DESC LIMIT 20',
                ).mappings()],
            }
        row = archive.db.exec_driver_sql("""
            SELECT runtime_decisions.revision, result, publication FROM runtime_decisions JOIN revisions USING(revision)
            WHERE revision = %s OR CAST((result::json ->> 'decision_id') AS TEXT) = %s
        """, (decision, decision)).mappings().fetchone()
        if row is None:
            raise RuntimeErrorInfo('Решение не найдено. Возьмите decision_id или revision из runtime-status.')
        directory = settings.data_dir / 'decisions' / row['revision']
        result = json.loads(row['result'])
        report = {'result': result, 'publication': json.loads(row['publication'])}
        for name, file in (
            ('settings', directory / 'runtime.json'), ('category', directory / 'route' / 'category.json'),
            ('route', directory / 'route' / 'trace.json'),
            ('event', directory / 'event.json'), ('position_check', directory / 'position-status.json'),
            ('instrument', directory / 'instrument.json'),
            ('agent', directory / TEAM_DIRECTORY / 'trace.json'),
        ):
            if file.is_file():
                report[name] = json.loads(file.read_bytes())
        if trades := trade_journal(events.db, approval=directory / TEAM_DIRECTORY):
            report['trades'] = trades
        if interrupted_routes := sorted(directory.glob('route-interrupted-*')):
            report['interrupted_routes'] = [str(path) for path in interrupted_routes]
        return report
