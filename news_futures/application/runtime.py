"""Run publication processing and broker monitoring for one local profile."""

import asyncio
from collections.abc import Callable
from contextlib import ExitStack, closing
from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3
from threading import Event
from typing import Any
from uuid import uuid4

from news_futures.trading.broker import BrokerReader, SandboxBroker
from news_futures.trading.account import BrokerAccount
from news_futures.common.credentials import load_token
from news_futures.agents.company import OpenPosition, Snapshot
from news_futures.agents.events import EventArchive
from news_futures.common.artifacts import replace_json
from news_futures.application.workflow import Shutdown, analyze_news
from news_futures.news.publications import Archive, utc_now
from news_futures.application.config import RuntimeSettings, cycle_inputs, load_settings
from news_futures.common.errors import RuntimeErrorInfo, error_code
from news_futures.application.records import pending_revisions, review_path, runtime_tables, save_broker_status, save_result
from news_futures.trading.executor import AUTONOMOUS_PROTECTION_UNAVAILABLE, SandboxExecutor
from news_futures.adapters.tbank.rest import TBankReader, TBankSandbox, sandbox_client
from news_futures.adapters.telegram import configured_channels, default_session, run_telegram, session_path
from news_futures.trading.journal import trade_journal


class Runtime:
    def __init__(
        self, settings: RuntimeSettings, reader: Callable[[str], BrokerReader],
        broker: Callable[[str], SandboxBroker] | None, *, run_id: str | None = None,
    ):
        self.settings = settings
        self.account = BrokerAccount(settings.account_id, settings.contracts, reader)
        self.broker = broker
        self.run_id = run_id
        self.stop = Event()
        self.failed = False
        self.broker_ready = False
        self.sources_ready = False
        self.position: OpenPosition | None = None

    def refresh(self) -> None:
        archive = Archive(self.settings.publications)
        with closing(archive.db), EventArchive(self.settings.events) as events:
            self.refresh_broker(archive, events)

    def refresh_broker(self, archive: Archive, events: EventArchive) -> None:
        settings = self.settings
        status: dict[str, Any] = {
            'mode': settings.mode, 'account_id': settings.account_id, 'contracts': settings.contracts,
            'run_id': self.run_id, 'confirmed': False,
        }
        last_confirmed = None
        try:
            if self.broker is not None:
                reconciliation = self.account.reconcile(
                    events, self.broker, archive if self.sources_ready else None,
                )
                if reconciliation is not None:
                    status['reconciliation'] = reconciliation
            snapshot, protection, readings = self.account.snapshot(events, settings.catalog.read_bytes())
            self.position = snapshot.position
            status.update(broker='connected', position=snapshot.model_dump(mode='json')['position'],
                          protection=protection)
            status['instruments'] = {
                reading.contract.contract_id: {'company_id': reading.contract.company_id, 'ticker': reading.contract.ticker}
                for reading in readings
            }
            reconciliation = status.get('reconciliation', {})
            if reconciliation.get('status') == 'blocked' or protection['status'] == 'unconfirmed':
                status.update(broker='blocked', error=reconciliation.get('reason') or 'protection_unconfirmed')
        except Exception as error:
            status.update(broker='blocked', error=error_code(error), position=None,
                          protection={'status': 'unconfirmed'})
        else:
            if status['broker'] == 'connected':
                status['confirmed'] = True
                last_confirmed = {'checked_at': utc_now().isoformat(), 'position': status['position'],
                                  'protection': status['protection']}
        finally:
            status['checked_at'] = utc_now().isoformat()
            self.broker_ready = status.get('broker') == 'connected'
            save_broker_status(archive, status, last_confirmed)
            print(json.dumps(status, ensure_ascii=False), flush=True)

    def process(self) -> None:
        settings = self.settings
        archive = Archive(settings.publications)
        with closing(archive.db), EventArchive(settings.events) as events:
            for row in pending_revisions(archive):
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
        if result['mode'] == 'sandbox' and (legacy or result.get('stage') == 'execution'):
            approval = review_path(directory, result.get('review_attempt'))
            if self._recover_execution(events, approval, result):
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
        catalog = (directory / 'catalog.json').read_bytes()

        def read_snapshot(events: EventArchive) -> Snapshot:
            snapshot, _, _ = self.account.snapshot(events, catalog)
            return snapshot

        state = analyze_news(
            settings, directory, publication, result['revision'], read_snapshot=read_snapshot,
            current_position=lambda: self.position, stop=self.stop,
        )
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
        proposal = state['proposal']
        assert proposal is not None and attempt is not None
        self._execute_review(
            archive, events, review_path(directory, attempt), proposal, state.get('position_id'), result,
        )

    def _recover_execution(self, events: EventArchive, approval: Path, result: dict[str, Any]) -> bool:
        trades = trade_journal(events.db, approval=approval)
        if not trades:
            return False
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
        return True

    def _execute_review(
        self, archive: Archive, events: EventArchive, review: Path, proposal: dict[str, Any],
        position_id: str | None, result: dict[str, Any],
    ) -> None:
        assert self.broker is not None
        result.update(stage='execution', status='processing', reason='broker_reconciliation_required')
        save_result(archive, result)
        executor = SandboxExecutor(events, self.broker(proposal['contract_id']))
        risk_path = review / f'risk-{uuid4().hex}.json'
        risk_audit_error = False

        def record_risk(status: str, inputs: dict[str, Any], output: object) -> None:
            nonlocal risk_audit_error
            try:
                replace_json(risk_path, {'at': utc_now().isoformat(), 'status': status,
                                         'input': inputs, 'output': output})
            except OSError:
                risk_audit_error = True

        if proposal['action'] == 'enter':
            execution = executor.start(archive, review, allowed_contracts=set(self.settings.contracts),
                                       record_risk=record_risk)
            if execution['reason'] == 'independent_stops_require_opt_in':
                execution['reason'] = AUTONOMOUS_PROTECTION_UNAVAILABLE
        else:
            assert position_id is not None
            execution = executor.reconcile(contract_id=proposal['contract_id'], cycle_id=position_id,
                                           review=(archive, review), record_risk=record_risk)
        result.update(status=execution['status'], reason=execution['reason'], execution=execution)
        if risk_audit_error:
            result['audit_error'] = 'risk_audit_unavailable'
        if execution['status'] == 'blocked':
            result['reconcile_pending'] = True


async def run_runtime(
    config: Path | RuntimeSettings, once: bool, *,
    run_id: str | None = None, channels: list[str] | None = None,
    on_phase: Callable[[str], None] | None = None,
    on_source: Callable[[str, bool], None] | None = None,
) -> int:
    settings = load_settings(config) if isinstance(config, Path) else config
    inputs = cycle_inputs(settings)
    try:
        if channels is None:
            channels = configured_channels(settings.sources)
        session = session_path(settings.session or default_session())
    except (OSError, ValueError, TypeError, KeyError) as error:
        raise RuntimeErrorInfo('Проверьте sources и внешний путь к session.') from error
    load_token('openrouter', 'api_key')
    if on_phase is not None:
        on_phase('directory')
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    with ExitStack() as stack:
        lock = stack.enter_context(closing(sqlite3.connect(settings.data_dir / 'runtime-lock.sqlite3', timeout=0)))
        try:
            lock.execute('BEGIN IMMEDIATE')
        except sqlite3.OperationalError as error:
            if error.sqlite_errorcode != sqlite3.SQLITE_BUSY:
                raise
            if on_phase is not None:
                on_phase('directory_busy')
            raise RuntimeErrorInfo('serve уже работает с этим каталогом данных.') from error
        if on_phase is not None:
            on_phase('broker')
        client = stack.enter_context(sandbox_client(settings.ca_file, read_only=settings.mode == 'observation'))
        def reader(contract: str) -> BrokerReader:
            return TBankReader(client, settings.account_id, contract, settings.contracts[contract])

        def broker(contract: str) -> SandboxBroker:
            return TBankSandbox(client, settings.account_id, contract, settings.contracts[contract])

        runtime = Runtime(settings, reader, broker if settings.mode == 'sandbox' else None, run_id=run_id)
        archive = Archive(settings.publications)
        with closing(archive.db):
            runtime_tables(archive)
            print(json.dumps({'mode': settings.mode, 'sources': channels, 'account_id': settings.account_id,
                              'contracts': settings.contracts}, ensure_ascii=False), flush=True)
            try:
                # Preflight uses the read adapter even when sandbox was selected.
                runtime.account.read_markets(inputs['catalog'])
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
                if on_phase is not None:
                    on_phase('running')
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
            if on_phase is not None:
                on_phase('sources')
            source_code = await run_telegram(channels, settings.publications, session, once, process=process,
                                             on_source=on_source)
            if once:
                await in_worker(runtime.refresh)
            return source_code or int(runtime.failed)
        finally:
            runtime.stop.set()
            if observer is not None:
                observer.cancel()
                await asyncio.gather(observer, return_exceptions=True)
