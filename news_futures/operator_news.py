"""Read-only browser projection of publication, event and execution evidence."""

from contextlib import closing, contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3
from typing import Any, Iterator, cast
from urllib.parse import parse_qsl, urlsplit

from news_futures.local import publication_digest


REVISION = re.compile(r'[0-9a-f]{64}')
CALL = re.compile(r'call-\d+(?:-interrupted-[0-9a-f]{32})?\.json')
PRIVATE = re.compile(r'(token|secret|password|authorization|api.?(?:key|hash)|auth.?key|cookie|session|header|^approval$)',
                     re.I)
SOURCE_KEYS = {'url', 'publication_url', 'source_url'}
CREDENTIAL = re.compile(
    r'''(?ix)
    (\b(?:api[-_ ]?(?:key|hash)|auth[-_ ]?key|(?:access[-_ ]|sandbox[-_ ]|session[-_ ])?token|
    password|session[-_ ]?(?:key|string))["']?\s*[:=]\s*)
    (?:"[^"\n]*"|'[^'\n]*'|[^\s,;}"']+)
    '''
)

@contextmanager
def read_archive(path: Path) -> Iterator[sqlite3.Connection | None]:
    if not path.is_file():
        yield None
        return
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=0.2)) as db:
        db.row_factory = sqlite3.Row
        with db:
            db.execute('BEGIN')
            yield db


def has_table(db: sqlite3.Connection | None, table: str) -> bool:
    return db is not None and db.execute('SELECT 1 FROM sqlite_master WHERE name = ?', (table,)).fetchone() is not None


def artifact(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_bytes())
    except (FileNotFoundError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def fields(record: dict[str, Any], *keys: str) -> dict[str, Any]:
    return {key: record[key] for key in keys if key in record}


def visible(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: source_url(item) if key in SOURCE_KEYS else visible(item)
                for key, item in value.items() if not PRIVATE.search(key)}
    if isinstance(value, list):
        return [visible(item) for item in value]
    if isinstance(value, str):
        value = re.sub(r'(?i)Bearer\s+[^\s"\x27,}]+', 'Bearer [скрыто]', value)
        value = re.sub(r'(?i)\bsk-(?:or-v1-)?[a-z\d_-]{20,}', '[скрыто]', value)
        return CREDENTIAL.sub(r'\1[скрыто]', value)
    return value


def source_url(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        url = urlsplit(value)
        if (url.scheme in ('https', 'http') and url.hostname and not url.username and not url.password
                and not any(PRIVATE.search(key) or key.lower() == 'auth' for key, _ in parse_qsl(url.query, True))):
            return cast(str, visible(value))
    except ValueError:
        pass
    return None


def response(record: dict[str, Any]) -> dict[str, Any]:
    output = fields(record, 'output', 'raw_output', 'refusal', 'requested_model', 'actual_model', 'provider', 'error')
    raw = output.get('raw_output')
    if isinstance(raw, str):
        try:
            decoded = json.loads(raw)
        except ValueError:
            pass
        else:
            if visible(decoded) != decoded:
                output['raw_output'] = json.dumps(visible(decoded), ensure_ascii=False)
    return output


def edition(publication: dict[str, Any]) -> dict[str, Any]:
    return fields(publication, 'id', 'source', 'text', 'url', 'published_at', 'edited_at', 'received_at',
                  'change', 'unread_media')


def decision_dirs(root: Path, revision: str) -> tuple[Path, Path]:
    original = root / 'decisions' / revision
    return original, original / 'resumed'


def route_attempts(directory: Path) -> list[Path]:
    return sorted((path for path in directory.glob('route*') if path.is_dir()
                   and (path.name == 'route' or path.name.startswith('route-interrupted-'))),
                  key=lambda path: when(path / 'trace.json', {}) or when(path, {}))


def reviews(directory: Path) -> list[Path]:
    base = directory / 'review'
    return [base, *(path for path in base.glob('*') if path.is_dir() and REVISION.fullmatch(path.name))]


def calls(directory: Path) -> list[tuple[Path, dict[str, Any]]]:
    saved = [(path, artifact(path)) for path in directory.glob('call-*.json') if CALL.fullmatch(path.name)]
    if not saved:
        saved = [(directory / f'call-{index:02}.json', call)
                 for index, call in enumerate(artifact(directory / 'trace.json').get('calls', []), 1)]
    return sorted(saved, key=lambda item: (when(item[0], item[1]), item[0].name))


def when(path: Path, record: dict[str, Any]) -> str:
    if at := record.get('at') or record.get('formed_at') or record.get('started_at'):
        return str(at)
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()
    except FileNotFoundError:
        return ''


def runtime_rows(db: sqlite3.Connection | None, revision: str | None) -> dict[str, tuple[dict[str, Any], Any]]:
    if db is None or not has_table(db, 'runtime_decisions'):
        return {}
    return {row['revision']: (json.loads(row['publication']), json.loads(row['result']))
            for row in db.execute('''
                SELECT revision, publication, result FROM runtime_decisions JOIN revisions USING (revision)
                WHERE (? IS NULL OR revision = ?)
            ''', (revision, revision))}


def event_rows(db: sqlite3.Connection | None, revision: str | None, *,
               include_unlinked: bool = False) -> dict[str, list[dict[str, Any]]]:
    if db is None or not has_table(db, 'event_decisions'):
        return {}
    events: dict[str, list[dict[str, Any]]] = {}
    for row in db.execute('''
        SELECT d.id, r.revision, linked, publication, links, details
        FROM (
            SELECT revision, json_array_length(links) > 0 AS linked,
                   CASE WHEN ? OR json_array_length(links) > 0 THEN publication END AS publication,
                   CASE WHEN ? OR json_array_length(links) > 0 THEN links END AS links
            FROM event_revisions WHERE (? IS NULL OR revision = ?)
        ) r JOIN event_decisions d USING (revision)
        ORDER BY d.id
    ''', (include_unlinked, include_unlinked, revision, revision)):
        history = events.setdefault(row['revision'], [])
        if row['linked'] or include_unlinked:
            history.append(json.loads(row['details']) | {
                'decision_id': row['id'], 'links': json.loads(row['links']), 'publication': json.loads(row['publication']),
            })
    return events


def cycle_rows(db: sqlite3.Connection | None, account: str) -> list[dict[str, Any]]:
    if db is None or not has_table(db, 'sandbox_cycles'):
        return []
    return [dict(row) | {'terms': json.loads(row['terms'])} for row in db.execute(
        'SELECT id, terms, approval FROM sandbox_cycles WHERE account_id = ?', (account,),
    )]


def journal(db: sqlite3.Connection, cycle_id: str) -> Iterator[dict[str, Any]]:
    for row in db.execute('SELECT seq, at, kind, details FROM sandbox_journal WHERE cycle_id = ? ORDER BY seq',
                          (cycle_id,)):
        yield dict(row) | {'details': json.loads(row['details'])}


def archive_news(root: Path, runtime: dict[str, tuple[dict[str, Any], Any]],
                 events: dict[str, list[dict[str, Any]]]) -> Iterator[tuple[str, list[dict[str, Any]], Any]]:
    for key in runtime.keys() | events.keys():
        if not REVISION.fullmatch(key):
            continue
        if key in events:
            if events[key] and events[key][-1]['links']:
                yield key, events[key], runtime.get(key, ({}, {}))[1]
            continue
        publication, result = runtime[key]
        attempts = [path for directory in decision_dirs(root, key) for path in route_attempts(directory)]
        for directory in reversed(attempts):
            route = artifact(directory / 'trace.json')
            if (route.get('status') == 'completed' and route.get('links')
                    and route.get('publication_revision_sha256') == publication_digest(publication)):
                yield key, [{'links': route['links'], 'publication': publication}], result
                break


def summary(key: str, event: dict[str, Any], result: dict[str, Any], contracts: set[str]) -> dict[str, Any]:
    return {'revision': key, 'event_id': event.get('event_id'),
            'at': result.get('at') or event.get('recorded_at') or event['publication'].get('edited_at')
            or event['publication'].get('published_at'),
            'status': result.get('status', event.get('kind', 'processing')), 'reason': result.get('reason'),
            'companies': sorted({link['company_id'] for link in event['links']}), 'contracts': sorted(contracts),
            'title': event['publication'].get('text', '').strip()[:160]}


def company_edition(row: dict[str, Any], event: dict[str, Any], binding: dict[str, Any] | None,
                    assessments: list[dict[str, Any]]) -> dict[str, Any]:
    publication = event['publication']
    return row | {'at': publication.get('edited_at') or publication.get('published_at'),
                  'publication': edition(publication), 'binding': binding, 'event_kind': event.get('kind'),
                  'assessments': assessments, 'has_trace': bool(event['links'])}


def remember_contract(contracts: dict[str, Any], item: dict[str, Any]) -> None:
    row = contracts.setdefault(item['contract_id'], {'company_id': item['company_id'], 'ticker': None})
    if row['company_id'] is None:
        row['company_id'] = item['company_id']
    if not row['ticker'] and item.get('ticker'):
        row['ticker'] = item['ticker']


def catalog_series(report: dict[str, Any]) -> dict[str, Any]:
    """Read the normalized futures and share identities from a diagnostic report."""
    try:
        operations = report['operations']
        future = operations['future']
        if future['availability'] != 'available':
            return {}
        data = future['data']
        uid, ticker, underlying = data['id'], data['ticker'], data['underlying_share_id']
        if not all(isinstance(value, str) and value for value in (uid, ticker, underlying)):
            return {}
        for share in operations['catalog']['data']:
            if share.get('share_id') == underlying and isinstance(share.get('company_id'), str):
                return {uid: {'company_id': share['company_id'], 'ticker': ticker}}
    except (AttributeError, KeyError, TypeError):
        pass
    return {}


def news_view(root: Path, profile: dict[str, Any], catalog: dict[str, Any],
              series: dict[str, Any], company: str) -> dict[str, Any]:
    companies = dict(catalog)
    contracts = {key: {'company_id': value, 'ticker': None} for key, value in profile['contracts'].items()}
    contracts.update({key: dict(item) for key, item in series.items()})
    decisions = []
    publications = []
    orders = []
    linked_companies: set[str] = set()
    with (read_archive(root / 'publications.sqlite3') as db, read_archive(root / 'events.sqlite3') as events):
        if db is not None and has_table(db, 'runtime_status'):
            row = db.execute('SELECT data FROM runtime_status WHERE id = 1').fetchone()
            if row:
                for key, item in json.loads(row['data']).get('instruments', {}).items():
                    if item.get('ticker') or key not in contracts:
                        contracts[key] = item
        cycles = cycle_rows(events, profile['account_id'])
        cycle_contracts = {cycle['id']: cycle['terms']['contract_id'] for cycle in cycles}
        history_by_revision = event_rows(events, None, include_unlinked=bool(company))
        for key, history, result in archive_news(root, runtime_rows(db, None), history_by_revision):
            current = history[-1]
            bindings = {link['company_id']: link for link in current['links']}
            linked_companies.update(bindings)
            related_contracts = set()
            assessments: dict[str, dict[str, Any]] = {}
            used = cycle_contracts.get((result.get('execution') or {}).get('cycle_id'))
            for directory in decision_dirs(root, key):
                if bindings.keys() - companies.keys():
                    saved_catalog = artifact(directory / 'route' / 'catalog.json')
                    for owner in bindings.keys() - companies.keys():
                        if owner in saved_catalog:
                            companies[owner] = fields(saved_catalog[owner], 'names', 'tickers')
                snapshot = artifact(directory / 'snapshot.json')
                for item in snapshot.get('instruments', []):
                    remember_contract(contracts, item)
                    if item['company_id'] in bindings:
                        related_contracts.add(item['contract_id'])
                for review in reviews(directory):
                    context = artifact(review / 'context.json')
                    for item in context.get('candidates', []):
                        remember_contract(contracts, item)
                        if item['company_id'] in bindings:
                            related_contracts.add(item['contract_id'])
                    if company in bindings and bindings[company]['kind'] != 'direct':
                        for path, call in calls(review):
                            if call.get('role') != 'analyst':
                                continue
                            for check in (call.get('output') or {}).get('assessments', []):
                                contract = contracts.get(check['contract_id'], {})
                                if contract.get('company_id') == company:
                                    at = when(path, call)
                                    if check['contract_id'] not in assessments or at >= assessments[check['contract_id']]['at']:
                                        assessments[check['contract_id']] = check | {'at': at}
                    if (result.get('review_attempt') == review.name
                            or (not result.get('review_attempt') and review.name == 'review')):
                        if proposal := artifact(review / 'trace.json').get('approved_proposal'):
                            used = used or proposal.get('contract_id')
                if any(not contracts[contract]['ticker'] for contract in related_contracts):
                    for uid, item in catalog_series(artifact(directory / 'catalog.json')).items():
                        remember_contract(contracts, item | {'contract_id': uid})
            if used:
                related_contracts = {used}
            row = summary(key, current, result, related_contracts)
            decisions.append(row)
            if company in bindings:
                binding = fields(bindings[company], 'kind', 'basis', 'score', 'confidence')
                publications.append(company_edition(row, current, binding, list(assessments.values())))
        if publications:
            event_ids = {row['event_id'] for row in publications if row['event_id']}
            revisions = {row['revision'] for row in publications}
            for key, history in history_by_revision.items():
                if REVISION.fullmatch(key) and key not in revisions and history and history[-1]['event_id'] in event_ids:
                    current = history[-1]
                    publications.append(company_edition(summary(key, current, {}, set()), current, None, []))
        for cycle in cycles:
            terms = cycle['terms']
            contract_id = terms['contract_id']
            owner = contracts.setdefault(contract_id, {'company_id': terms.get('company_id'), 'ticker': None})
            if events is None or not company or owner['company_id'] != company:
                continue
            intents: dict[str, dict[str, Any]] = {}
            for step in journal(events, cycle['id']):
                details = step['details']
                operation = details.get('operation_id')
                if step['kind'] == 'intent':
                    order = {'contract_id': contract_id, 'action': details['action'], 'at': step['at'], 'status': 'intent'}
                    intents[operation] = order
                    orders.append(order)
                elif operation in intents:
                    intents[operation]['status'] = step['kind']
    for instrument in contracts.values():
        if company_id := instrument.get('company_id'):
            linked_companies.add(company_id)
    for company_id in linked_companies:
        companies.setdefault(company_id, {'names': [company_id], 'tickers': []})
    decisions.sort(key=lambda item: (item['at'] or '', item['revision']), reverse=True)
    publications.sort(key=lambda item: (item['at'] or '', item['revision']), reverse=True)
    orders.sort(key=lambda item: item['at'], reverse=True)
    return {'companies': companies, 'contracts': contracts, 'decisions': decisions,
            'publications': publications, 'orders': orders}


def trace_view(root: Path, account: str, key: str) -> dict[str, Any] | None:
    if not REVISION.fullmatch(key):
        return None
    with (read_archive(root / 'publications.sqlite3') as db, read_archive(root / 'events.sqlite3') as events):
        news = next(archive_news(root, runtime_rows(db, key), event_rows(events, key)), None)
        if news is None:
            return None
        _, history, result = news
        event = next((row for row in history if row.get('decision_id') == result.get('decision_id')), history[0])
        publication = edition(event['publication'])
        steps: list[dict[str, Any]] = []

        def step(id: str, kind: str, status: str, *, at: str | None = None, saved_at: str | None = None,
                 input: object = None, output: object = None, sort_at: str | None = None) -> None:
            steps.append({'id': id, 'kind': kind, 'status': status, 'at': at, 'saved_at': saved_at,
                          'input': input, 'output': output, '_sort_at': sort_at or at or saved_at or '\uffff'})

        step('publication', 'publication', 'saved', at=publication.get('received_at'), output=publication)
        approvals = set()
        routed = False
        approved_risk = set()
        for index, directory in enumerate(decision_dirs(root, key)):
            for route in route_attempts(directory):
                path = route / 'trace.json'
                record = artifact(path)
                routed |= bool(record.get('links'))
                batches = [artifact(path) for path in sorted(route.glob('batch-*.json'))]
                catalog = artifact(route / 'catalog.json')
                step(f'{index}/{route.name}', 'route', record.get('status', 'processing'),
                     at=record.get('at'), saved_at=when(path, {}) or None,
                     input={'publication': publication, 'companies': {
                         company: catalog[company] for batch in batches for company in batch.get('company_ids', [])
                         if company in catalog
                     }}, output=fields(record, 'error', 'links', 'requested_model', 'actual_model') | {'batches': batches})
            for name in ('event', 'snapshot', 'review'):
                status_path = directory / f'{name}-status.json'
                if stage_record := artifact(status_path):
                    if name == 'snapshot' or stage_record['status'] == 'processing' or (
                        name == 'event' and stage_record['status'] != 'continue'
                    ):
                        step(f'{index}/{name}-status', name, stage_record['status'], at=stage_record.get('at'),
                             input={'publication': publication} if name == 'snapshot' else None,
                             output=fields(stage_record, 'reason', 'started_at') | (
                                 artifact(directory / 'snapshot.json') if name == 'snapshot' else {}
                             ))
            for review in reviews(directory):
                saved_calls = calls(review)
                approval = str(review.resolve())
                approvals.add(approval)
                for path, call in saved_calls:
                    role = call.get('role')
                    if role not in ('analyst', 'critic'):
                        continue
                    kind = 'revision' if role == 'analyst' and (call.get('input') or {}).get('review') else role
                    output = call.get('output') or {}
                    status = ('failed' if call.get('error') else 'processing' if not output else
                              ('approved' if output.get('approved') else 'rejected') if role == 'critic'
                              else output.get('action', 'completed'))
                    saved_at = when(path, {})
                    attempt = call.get('started_at') or call.get('at') or saved_at
                    step(f'{index}/{review.name}/{path.name.split("-interrupted-")[0].split(".")[0]}/{attempt}',
                         kind, status,
                         at=call.get('at') or call.get('formed_at') or call.get('started_at'), saved_at=saved_at or None,
                         input=call.get('input'), output=response(call))
                for risk_path in [review / 'risk.json', *review.glob('risk-*.json')]:
                    if risk := artifact(risk_path):
                        if risk['status'] == 'approved':
                            approved_risk.add(approval)
                        step(f'{index}/{review.name}/{risk_path.stem}', 'risk', risk['status'], at=risk.get('at'),
                             saved_at=when(risk_path, {}) or None,
                             input=(risk.get('input') or {}) | {'context': artifact(review / 'context.json') or None},
                             output=risk.get('output'))
        if not routed:
            step('bindings', 'route', 'saved', at=event.get('recorded_at'), output={'links': event['links']})
        if event.get('event_id'):
            call = event.get('call') or {}
            step('event', 'event', event['kind'], at=event.get('recorded_at'),
                 input=call.get('input') or {'publication': publication, 'search': event.get('search')},
                 output=fields(event, 'event_id', 'assessment', 'search', 'correction') | response(call))
        cycle_id = (result.get('execution') or {}).get('cycle_id')
        if events is not None and (cycle_id or result.get('stage') == 'execution'):
            for cycle in cycle_rows(events, account):
                if cycle_id and cycle['id'] != cycle_id:
                    continue
                related = cycle['approval'] in approvals
                records = list(journal(events, cycle['id']))
                if related and cycle['approval'] not in approved_risk:
                    step('entry-reservation', 'risk', 'approved', output=cycle['terms'],
                         sort_at=records[0]['at'] if records else result.get('at'))
                for item in records:
                    kind, details = item['kind'], item['details']
                    if kind == 'reviewed_close':
                        related = details.get('approval') in approvals
                        if details.get('approval') in approved_risk:
                            continue
                    if related:
                        step(f'execution/{item["seq"]}',
                             'risk' if kind in ('reviewed_close', 'reviewed_close_abandoned') else 'execution',
                             kind, at=item['at'],
                             input=details if kind == 'intent' else None,
                             output=details if kind != 'intent' else None)
                        if kind == 'reviewed_close_abandoned':
                            related = False
        stage = result.get('stage')
        latest_risk = max((item for item in steps if item['kind'] == 'risk'),
                          key=lambda item: item['_sort_at'], default={})
        checking_risk = (stage == 'execution' and result['status'] == 'processing'
                         and latest_risk.get('status') == 'processing')
        if stage and not checking_risk:
            step('outcome', stage, result['status'], at=result.get('at'),
                 output=fields(result, 'reason', 'execution', 'reconcile_pending', 'audit_error'))
        steps[1:] = sorted(steps[1:], key=lambda item: item['_sort_at'])
        for item in steps:
            del item['_sort_at']
        return cast(dict[str, Any], visible(summary(key, event, result, set()) | {
            'publication': publication, 'steps': steps,
        }))
