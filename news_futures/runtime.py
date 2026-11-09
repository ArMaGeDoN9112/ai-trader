"""Compose continuous publication intake, model review and sandbox execution."""

import asyncio
from collections.abc import Callable
from contextlib import closing
from datetime import datetime, timedelta
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from threading import Event
from typing import Any, Literal, Protocol, TypedDict, cast
from uuid import uuid4

from langchain_core.runnables import RunnableConfig, RunnableLambda
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph
from langsmith import tracing_context
from pydantic import Field

from news_futures.analysis import Contract, Review, RoleSettings, Settings
from news_futures.credentials import load_token
from news_futures.decisions import (
    MAX_SNAPSHOT_AGE, Account, CompanyContext, CompanyProposal, Instrument, Market, Nonempty,
    OpenPosition, Snapshot, analyze_event,
)
from news_futures.events import EventArchive, match_route
from news_futures.jev import JevSettings, catalog_rows, load_catalog, route_publication
from news_futures.local import replace_json
from news_futures.publications import Archive, utc_now
from news_futures.risk import MAX_NEWS_AGE, EntryReservation, RiskSnapshot
from news_futures.sandbox_execution import (
    TRADE_STATE, SandboxBroker, SandboxExecutor, reconciled_fills, trade_journal, verified_protection,
)
from news_futures.tbank_execution import TBankReader, TBankSandbox, sandbox_client
from news_futures.telegram import configured_channels, default_session, run_telegram, session_path


PACKAGE = Path(__file__).resolve().parent


class RuntimeErrorInfo(ValueError):
    """A sanitized configuration or operation error suitable for CLI output."""


class Shutdown(Exception):
    pass


class RuntimeSettings(Contract):
    data_dir: Path
    catalog: Path
    account_id: Nonempty
    contracts: dict[Nonempty, Nonempty] = Field(min_length=1)  # contract UID -> company ID
    mode: Literal['observation', 'sandbox'] = 'observation'
    sources: Path = PACKAGE / 'telegram-sources.json'
    session: Path | None = None
    jev: Path = PACKAGE / 'jev-model.json'
    matcher: Path = PACKAGE / 'event-model.json'
    review: Path = PACKAGE / 'openrouter-models.json'
    ca_file: Path | None = None

    @property
    def publications(self) -> Path:
        return self.data_dir / 'publications.sqlite3'

    @property
    def events(self) -> Path:
        return self.data_dir / 'events.sqlite3'


def load_settings(config: Path) -> RuntimeSettings:
    try:
        settings = RuntimeSettings.model_validate_json(config.read_bytes())
    except (OSError, ValueError) as error:
        raise RuntimeErrorInfo('Проверьте путь и поля общей конфигурации.') from error
    return settings.model_copy(update={
        field: (config.parent / value).resolve()
        for field in RuntimeSettings.model_fields
        if isinstance(value := getattr(settings, field), Path)
    })


def cycle_inputs(settings: RuntimeSettings) -> dict[str, bytes]:
    inputs = {}
    for field, validate in (
        ('catalog', load_catalog), ('jev', JevSettings.model_validate_json),
        ('matcher', RoleSettings.model_validate_json), ('review', Settings.model_validate_json),
    ):
        try:
            content = getattr(settings, field).read_bytes()
            validate(content)
        except (OSError, ValueError, KeyError, TypeError) as error:
            raise RuntimeErrorInfo(f'Проверьте файл настройки {field}.') from error
        inputs[field] = content
    if set(settings.contracts.values()) - {row.company_id for row in catalog_rows(inputs['catalog']) if row.share_id}:
        raise RuntimeErrorInfo('Сопоставьте contracts с компаниями из catalog.')
    return inputs


def runtime_tables(archive: Archive) -> None:
    archive.db.executescript("""
        CREATE TABLE IF NOT EXISTS runtime_decisions (
            revision TEXT PRIMARY KEY REFERENCES revisions(revision),
            result TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS runtime_status (id INTEGER PRIMARY KEY CHECK (id = 1), data TEXT NOT NULL);
    """)


def save_result(archive: Archive, result: dict[str, Any]) -> None:
    with archive.db:
        archive.db.execute('''
            INSERT INTO runtime_decisions VALUES (?, ?)
            ON CONFLICT (revision) DO UPDATE SET result = excluded.result
        ''', (result['revision'], json.dumps(result)))


class Reader(Protocol):
    def read_risk(self, account_id: str, contract_id: str) -> object: ...
    def read_trade(self, entry: EntryReservation, operations: dict[str, str],
                   receipts: dict[str, str | None]) -> object: ...


class NewsState(TypedDict, total=False):
    stage: str
    status: str
    reason: str | None
    decision_id: int
    affected: list[str]
    position_id: str | None
    proposal: dict[str, Any] | None
    review_attempt: str | None


class Runtime:
    def __init__(
        self, settings: RuntimeSettings, reader: Callable[[str], Reader],
        broker: Callable[[str], SandboxBroker] | None,
    ):
        self.settings = settings
        self.reader = reader
        self.broker = broker
        self.stop = Event()
        self.failed = False
        self.broker_ready = False
        self.sources_ready = False
        self.position: OpenPosition | None = None

    def read_markets(self, catalog: bytes) -> list[RiskSnapshot]:
        settings = self.settings
        listed = {(row.company_id, row.share_id) for row in catalog_rows(catalog)}
        readings = []
        for contract_id, company_id in settings.contracts.items():
            risk = RiskSnapshot.model_validate(self.reader(contract_id).read_risk(settings.account_id, contract_id))
            contract = risk.contract
            if (risk.account_id != settings.account_id or contract.contract_id != contract_id
                    or contract.company_id != company_id or (company_id, contract.share_id) not in listed
                    or len({contract_id, company_id, contract.share_id}) != 3 or contract.last_trade_at <= utc_now()):
                raise RuntimeErrorInfo('Сверьте счёт, фьючерсы и UID их базовых акций в contracts и catalog.')
            readings.append(risk)
        return readings

    def snapshot(self, events: EventArchive, catalog: bytes) -> tuple[Snapshot, dict[str, Any]]:
        settings = self.settings
        active = trade_journal(events, active_account=settings.account_id)
        position = None
        protection: dict[str, Any] = {'status': 'none'}
        if active:
            if len(active) != 1:
                raise RuntimeErrorInfo('Сверьте конфликтующие циклы счёта.')
            trade = active[0]
            entry = EntryReservation.model_validate_json(json.dumps(trade['reservation']))
            if entry.contract_id not in settings.contracts:
                raise RuntimeErrorInfo('Верните контракт открытого цикла в contracts и сверьте счёт.')
            operations = {}
            receipts = {}
            confirmed = {}
            for event in trade['events']:
                details = event['details']
                if event['kind'] == 'intent':
                    operations[details['action']] = details['operation_id']
                elif event['kind'] == 'response_rejected' or (
                    event['kind'] == 'response_received' and details.get('broker_id')
                ):
                    receipts[details['action']] = details.get('broker_id')
                elif event['kind'] == 'confirmed_fills':
                    confirmed = details['fills']
            if 'enter' not in operations:
                raise RuntimeErrorInfo('Сверьте незавершённый резерв счёта.')
            state = TRADE_STATE.validate_python(self.reader(entry.contract_id).read_trade(entry, operations, receipts))
            fills = reconciled_fills(entry, state)
            if (not state.settled or state.other_exposure or state.active_orders or fills is None
                or any(fills[1].get(order_id) != fill for order_id, fill in confirmed.items())
                or len({stop.id for stop in state.protection}) != len(state.protection)
            ):
                raise RuntimeErrorInfo('Сверьте неподтверждённую позицию или заявки брокера.')
            remaining, _ = fills
            if remaining:
                position = OpenPosition(id=entry.id, contract_id=entry.contract_id,
                                        side=entry.side, quantity=remaining)
                stops = tuple(stop for stop in state.protection if stop.status == 'active')
                protected = (trade['status'] == 'protected' and len(stops) == 2
                             and verified_protection(entry, remaining, stops)
                             == {('sl', entry.stop_loss), ('tp', entry.take_profit)})
                protection = {'status': 'confirmed' if protected else 'unconfirmed',
                              'active_ids': [stop.id for stop in stops]}
        companies = load_catalog(catalog)
        instruments = []
        account = None
        for risk in self.read_markets(catalog):
            contract = risk.contract
            if risk.execution_uncertain or risk.has_position != bool(position) or (
                not position and risk.has_active_orders
            ):
                raise RuntimeErrorInfo('На счёте есть неподтверждённая позиция или заявки. Сверьте брокера.')
            account = Account(id=risk.account_id, observed_at=risk.account_observed_at, currency=risk.currency,
                              equity=float(risk.equity), available_margin=float(risk.available_margin))
            instruments.append(Instrument(
                company_id=contract.company_id, company_name=companies[contract.company_id]['names'][0],
                contract_id=contract.contract_id, share_id=contract.share_id, allowed_for_entry=True,
                market=Market(observed_at=contract.quote_at, last_price=float(contract.last_price)),
            ))
        assert account is not None
        return Snapshot(account=account, position=position, instruments=instruments), protection

    def refresh(self) -> None:
        archive = Archive(self.settings.publications)
        with closing(archive.db), EventArchive(self.settings.events) as events:
            self.refresh_broker(archive, events)

    def refresh_broker(self, archive: Archive, events: EventArchive) -> None:
        settings = self.settings
        status: dict[str, Any] = {
            'mode': settings.mode, 'account_id': settings.account_id, 'contracts': settings.contracts,
            'checked_at': utc_now().isoformat(),
        }
        try:
            active = trade_journal(events, active_account=settings.account_id)
            if self.broker and len(active) == 1:
                entry = active[0]['reservation']
                if entry['contract_id'] in settings.contracts:
                    status['reconciliation'] = SandboxExecutor(events, self.broker(entry['contract_id'])).reconcile(
                        contract_id=entry['contract_id'], cycle_id=entry['id'],
                        publications=archive if self.sources_ready else None,
                    )
            snapshot, protection = self.snapshot(events, settings.catalog.read_bytes())
            self.position = snapshot.position
            status.update(broker='connected', position=snapshot.model_dump(mode='json')['position'],
                          protection=protection)
            reconciliation = status.get('reconciliation', {})
            if reconciliation.get('status') == 'blocked' or protection['status'] == 'unconfirmed':
                status.update(broker='blocked', error=reconciliation.get('reason') or 'protection_unconfirmed')
        except Exception as error:
            status.update(broker='blocked', error=error_code(error), position=None,
                          protection={'status': 'unconfirmed'})
        finally:
            self.broker_ready = status.get('broker') == 'connected'
            with archive.db:
                archive.db.execute('INSERT OR REPLACE INTO runtime_status VALUES (1, ?)', (json.dumps(status),))
            print(json.dumps(status, ensure_ascii=False), flush=True)

    def process(self) -> None:
        settings = self.settings
        archive = Archive(settings.publications)
        with closing(archive.db), EventArchive(settings.events) as events:
            rows = archive.db.execute("""
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
            for row in rows:
                if self.stop.is_set():
                    break
                publication = json.loads(row['publication'])
                if row['result'] is None:
                    if not self.broker_ready:
                        continue
                    result = {'revision': row['revision'], 'mode': settings.mode, 'status': 'processing',
                              'publication_id': publication['id'], 'inputs_saved': False}
                    save_result(archive, result)
                else:
                    result = json.loads(row['result'])
                try:
                    self.process_revision(archive, events, publication, result)
                except Shutdown:
                    break
                except Exception as error:
                    status = 'processing' if result.get('stage') == 'execution' else 'failed'
                    result.update(status=status, reason=error_code(error))
                    self.failed = True
                self.failed |= result['status'] == 'failed' or (
                    result['status'] == 'rejected' and result.get('stage') == 'review'
                )
                result['latency_ms'] = round((utc_now() - datetime.fromisoformat(
                    publication['received_at'],
                )) / timedelta(milliseconds=1))
                save_result(archive, result)
                print(json.dumps(result, ensure_ascii=False), flush=True)

    def process_revision(self, archive: Archive, events: EventArchive, publication: dict[str, Any],
                         result: dict[str, Any]) -> None:
        settings = self.settings
        directory = settings.data_dir / 'decisions' / result['revision']
        legacy = 'inputs_saved' not in result and directory.exists()
        original = directory
        if result.get('legacy'):
            directory = directory / 'resumed'
        approval = directory / 'review'
        if attempt := result.get('review_attempt'):
            approval /= attempt
        if result['mode'] == 'sandbox' and (legacy or result.get('stage') == 'execution'):
            trades = trade_journal(events, approval=approval)
            if trades:
                trade, = trades
                cycle = trade['reservation']['id']
                status, reason = trade['status'], trade['reason']
                approval_path = str(approval.resolve())
                is_close = trade['approval'] != approval_path
                abandoned = next((event['details'] for event in reversed(trade['events'])
                                  if event['kind'] == 'reviewed_close_abandoned'
                                  and event['details']['approval'] == approval_path), None)
                result.pop('reconcile_pending', None)
                if abandoned:
                    status, reason = 'rejected', abandoned['reason']
                elif (status in ('reserved', 'blocked') or (is_close and status != 'closed')
                        or (status == 'protected' and not self.broker_ready)):
                    result['reconcile_pending'] = True
                    status, reason = result['status'], reason or 'broker_reconciliation_required'
                result.update(stage='execution', status=status, reason=reason,
                              execution={'cycle_id': cycle, 'status': status, 'reason': reason})
                return
        if not self.broker_ready:
            return
        frozen = (original if legacy else directory) / 'runtime.json'
        previous = (RuntimeSettings.model_validate_json(frozen.read_bytes())
                    if result.get('inputs_saved') or (legacy and frozen.is_file()) else settings)
        if (result['mode'] != settings.mode or previous.account_id != settings.account_id
                or previous.contracts != settings.contracts):
            result.update(status='skipped', reason='runtime_profile_changed')
            return
        if legacy:
            # Keep ticket 11's partial artifacts and broker approval path available for audit.
            directory = original / 'resumed'
            result['legacy'] = True
        if not result.get('inputs_saved'):
            directory.mkdir(parents=True, exist_ok=True)
            replace_json(directory / 'runtime.json', settings.model_dump(mode='json'))
            replace_json(directory / 'publication.json', publication)
            for field, content in cycle_inputs(settings).items():
                (directory / f'{field}.json').write_bytes(content)
            result['inputs_saved'] = True
            save_result(archive, result)
        state = self.analyze_news(directory, publication, result['revision'], (directory / 'catalog.json').read_bytes())
        result.update(stage=state['stage'], status=state['status'], reason=state.get('reason'))
        if 'decision_id' in state:
            result['decision_id'] = state['decision_id']
        result.pop('review_attempt', None)
        if attempt := state.get('review_attempt'):
            result['review_attempt'] = attempt
        result.pop('reconcile_pending', None)
        if self.broker is None or state['status'] != 'approved_for_checks':
            return
        if self.stop.is_set():
            result.update(status='processing', reason='shutdown_before_execution')
            return
        result.update(stage='execution', status='processing', reason='broker_reconciliation_required')
        save_result(archive, result)
        proposal = state['proposal']
        assert proposal is not None and attempt is not None
        review = directory / 'review' / attempt
        executor = SandboxExecutor(events, self.broker(proposal['contract_id']))
        if proposal['action'] == 'enter':
            # Live T-Bank writes remain gated by the executor's independent-stop policy.
            execution = executor.start(archive, review, allowed_contracts=set(settings.contracts))
            if execution['reason'] == 'independent_stops_require_opt_in':
                execution['reason'] = 'autonomous_protection_unavailable'
        else:
            position_id = state['position_id']
            assert position_id is not None
            execution = executor.reconcile(contract_id=proposal['contract_id'],
                                           cycle_id=position_id, review=(archive, review))
        result.update(status=execution['status'], reason=execution['reason'], execution=execution)
        if execution['status'] == 'blocked':
            result['reconcile_pending'] = True

    def analyze_news(self, directory: Path, publication: dict[str, Any], revision: str, catalog: bytes) -> NewsState:
        settings = self.settings

        def route(_: NewsState) -> NewsState:
            route_dir = directory / 'route'
            if route_dir.exists():
                route_dir.rename(directory / f'route-interrupted-{uuid4().hex}')
            routed = route_publication(directory / 'jev.json', directory / 'publication.json',
                                       directory / 'catalog.json', route_dir)
            if routed['status'] != 'completed':
                return {'status': 'failed', 'reason': routed['error']}
            return {'status': 'continue'}

        def match(_: NewsState) -> NewsState:
            with EventArchive(settings.events) as events:
                event = match_route(events, directory / 'route', directory / 'matcher.json')
            replace_json(directory / 'event.json', event)
            affected = event['analysis_company_ids']
            result: NewsState = {'decision_id': event['decision_id'], 'affected': affected}
            if event['kind'] == 'model_error':
                return result | {'status': 'failed', 'reason': 'event_model_error'}
            if not set(affected).intersection(settings.contracts.values()):
                return result | {'status': 'skipped',
                                 'reason': 'repeat' if event['kind'] == 'repeat' else 'no_allowed_company'}
            return result

        def prepare(state: NewsState) -> NewsState:
            archive = Archive(settings.publications)
            with closing(archive.db), EventArchive(settings.events) as events:
                latest = archive.latest_revision(publication['id'])
                if latest is None or latest['revision'] != revision:
                    return {'status': 'skipped', 'reason': 'publication_revision_superseded'}
                if archive.db.execute("""
                    SELECT 1 FROM revisions
                    WHERE historical = 1 AND trim(json_extract(publication, '$.text')) = trim(?)
                      AND coalesce(json_extract(publication, '$.edited_at'),
                                   json_extract(publication, '$.published_at')) <= ?
                    LIMIT 1
                """, (publication['text'], publication['published_at'])).fetchone():
                    return {'status': 'skipped', 'reason': 'historical_reprint'}
                snapshot, _ = self.snapshot(events, catalog)
            if snapshot.position and settings.contracts[snapshot.position.contract_id] not in state['affected']:
                return {'status': 'skipped', 'reason': 'unrelated_to_position'}
            if not snapshot.position and utc_now() - datetime.fromisoformat(publication['published_at']) > MAX_NEWS_AGE:
                return {'status': 'skipped', 'reason': 'publication_context_only'}
            replace_json(directory / 'snapshot.json', snapshot.model_dump(mode='json'))
            return {}

        def review(state: NewsState) -> NewsState:
            snapshot_path = directory / 'snapshot.json'
            content = snapshot_path.read_bytes()
            snapshot = Snapshot.model_validate_json(content)
            attempt = sha256(content).hexdigest()
            with EventArchive(settings.events) as events:
                trace = analyze_event(events, state['decision_id'], snapshot_path,
                                      directory / 'review.json', directory / 'review' / attempt, mode=settings.mode,
                                      checkpointer=checkpointer, thread_id=f'{revision}:review:{attempt}')
            return {'status': trace['status'], 'reason': trace['error'], 'proposal': trace['approved_proposal'],
                    'position_id': snapshot.position.id if snapshot.position else None, 'review_attempt': attempt}

        def audited(stage: str, node: Callable[[NewsState], NewsState]) -> RunnableLambda[NewsState, dict[str, Any]]:
            def invoke(state: NewsState) -> dict[str, Any]:
                if self.stop.is_set():
                    raise Shutdown
                try:
                    updates = node(state)
                except Exception as error:
                    updates = {'status': 'failed', 'reason': error_code(error)}
                updates['stage'] = stage
                return dict(updates)
            return RunnableLambda(invoke)

        graph: StateGraph[NewsState, None, NewsState, NewsState] = StateGraph(NewsState)
        graph.add_node('route', audited('route', route))
        graph.add_node('event', audited('event', match))
        graph.add_node('snapshot', audited('snapshot', prepare))
        graph.add_node('review', audited('review', review))
        graph.add_edge(START, 'route')
        graph.add_conditional_edges('route', lambda state: 'event' if state['status'] == 'continue' else END)
        graph.add_conditional_edges('event', lambda state: 'snapshot' if state['status'] == 'continue' else END)
        graph.add_conditional_edges('snapshot', lambda state: 'review' if state['status'] == 'continue' else END)
        graph.add_edge('review', END)
        with closing(sqlite3.connect(settings.data_dir / 'checkpoints.sqlite3', check_same_thread=False)) as db:
            checkpointer = SqliteSaver(db, serde=JsonPlusSerializer(allowed_msgpack_modules=[
                (cls.__module__, cls.__name__) for cls in (CompanyContext, CompanyProposal, Review)
            ]))
            config: RunnableConfig = {'configurable': {'thread_id': revision}}
            with tracing_context(enabled=False):
                compiled = graph.compile(checkpointer=checkpointer)
                saved = compiled.get_state(config)
                if saved.created_at and ('review' in saved.next or saved.values.get('stage') == 'review'):
                    snapshot = Snapshot.model_validate_json((directory / 'snapshot.json').read_bytes())
                    timestamps = [snapshot.account.observed_at, *(item.market.observed_at for item in snapshot.instruments)]
                    if snapshot.position != self.position or (saved.next and utc_now() - min(timestamps) > MAX_SNAPSHOT_AGE):
                        compiled.update_state(config, {'status': 'continue', 'reason': None, 'proposal': None,
                                                       'review_attempt': None}, as_node='event')
                        saved = compiled.get_state(config)
                if saved.created_at and not saved.next:
                    return cast(NewsState, saved.values)
                return cast(NewsState, compiled.invoke(
                    None if saved.created_at else {'status': 'continue'}, config, durability='sync',
                ))


def error_code(error: Exception) -> str:
    return str(error) if isinstance(error, RuntimeErrorInfo) else type(error).__name__


async def run_runtime(config: Path, once: bool) -> int:
    settings = load_settings(config)
    inputs = cycle_inputs(settings)
    try:
        channels = configured_channels(settings.sources)
        session = session_path(settings.session or default_session())
    except (OSError, ValueError, TypeError, KeyError) as error:
        raise RuntimeErrorInfo('Проверьте sources и внешний путь к session.') from error
    load_token('openrouter', 'api_key')
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(settings.data_dir / 'runtime-lock.sqlite3', timeout=0)) as lock, sandbox_client(
        settings.ca_file, read_only=settings.mode == 'observation',
    ) as client:
        try:
            lock.execute('BEGIN IMMEDIATE')
        except sqlite3.OperationalError as error:
            if error.sqlite_errorcode != sqlite3.SQLITE_BUSY:
                raise
            raise RuntimeErrorInfo('serve уже работает с этим каталогом данных.') from error
        def reader(contract: str) -> Reader:
            return TBankReader(client, settings.account_id, contract, settings.contracts[contract])

        def broker(contract: str) -> SandboxBroker:
            return TBankSandbox(client, settings.account_id, contract, settings.contracts[contract])

        runtime = Runtime(settings, reader, broker if settings.mode == 'sandbox' else None)
        archive = Archive(settings.publications)
        with closing(archive.db):
            runtime_tables(archive)
            print(json.dumps({'mode': settings.mode, 'sources': channels, 'account_id': settings.account_id,
                              'contracts': settings.contracts}, ensure_ascii=False), flush=True)
            try:
                # Preflight uses the read adapter even when sandbox was selected.
                runtime.read_markets(inputs['catalog'])
            except Exception as error:
                raise RuntimeErrorInfo(
                    f'Брокер: {error_code(error)}. Проверьте токен, счёт, контракт и подключение.',
                ) from error

        async def in_worker(action: Callable[[], None]) -> None:
            task = asyncio.create_task(asyncio.to_thread(action))
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                runtime.stop.set()
                await task
                raise

        async def process() -> None:
            if not runtime.sources_ready:
                runtime.sources_ready = True
                await in_worker(runtime.refresh)
            await in_worker(runtime.process)

        async def monitor() -> None:
            while not runtime.stop.is_set():
                try:
                    await in_worker(runtime.refresh)
                except Exception as error:
                    print(json.dumps({'broker_monitor_error': error_code(error)}), flush=True)
                await asyncio.sleep(15)

        await in_worker(runtime.refresh)
        observer = None if once else asyncio.create_task(monitor())
        try:
            source_code = await run_telegram(settings.sources, settings.publications, session, once, process=process)
            if once:
                await in_worker(runtime.refresh)
            return source_code or int(runtime.failed)
        finally:
            runtime.stop.set()
            if observer is not None:
                observer.cancel()
                await asyncio.gather(observer, return_exceptions=True)


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
        review = directory / 'review'
        if attempt := result.get('review_attempt'):
            review /= attempt
        report = {'result': result, 'publication': json.loads(row['publication'])}
        for name, file in (
            ('settings', directory / 'runtime.json'), ('route', directory / 'route' / 'trace.json'),
            ('event', directory / 'event.json'), ('context', review / 'context.json'),
            ('review', review / 'trace.json'),
        ):
            if file.is_file():
                report[name] = json.loads(file.read_bytes())
        if cycle_id := result.get('execution', {}).get('cycle_id'):
            report['trades'] = trade_journal(events, cycle_id=cycle_id)
        previous_reviews = [str(path) for path in (directory / 'review').glob('*/context.json') if path.parent != review]
        if previous_reviews:
            report['previous_reviews'] = previous_reviews
        if interrupted_routes := sorted(directory.glob('route-interrupted-*')):
            report['interrupted_routes'] = [str(path) for path in interrupted_routes]
        return report
