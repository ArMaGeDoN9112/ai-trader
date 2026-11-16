"""Read decisions, broker status and trace artifacts for the CLI."""

from contextlib import closing
import json
from pathlib import Path
from typing import Any

from news_futures.adapters.sqlite.events import EventArchive
from news_futures.adapters.sqlite.publications import Archive
from news_futures.adapters.sqlite.trades import trade_journal
from news_futures.application.analysis.tool_agent import TEAM_DIRECTORY
from news_futures.application.artifact_paths import review_path
from news_futures.application.config import load_settings
from news_futures.common.errors import RuntimeErrorInfo


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
        attempt = result.get('review_attempt')
        review = review_path(directory, attempt) if attempt else None
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
        if review is not None:
            for name, file in (('context', review / 'context.json'), ('review', review / 'trace.json')):
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
