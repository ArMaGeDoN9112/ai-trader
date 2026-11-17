"""Run publication processing and broker monitoring for one local profile."""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import AbstractContextManager
from datetime import datetime, timedelta
import json
from pathlib import Path
from threading import Event
from typing import Any
from uuid import uuid4

from news_futures.application.catalog import catalog_contracts
from news_futures.application.artifact_paths import review_path
from news_futures.application.config import RuntimeSettings, cycle_inputs, groq_models, role_models
from news_futures.application.ports.broker import BrokerReader, MarketReading, SandboxBroker
from news_futures.application.ports.models import AnalysisModels
from news_futures.application.ports.storage import EventStore, RuntimePublications, RuntimeStorage
from news_futures.application.trading.account import BrokerAccount
from news_futures.application.trading.executor import AUTONOMOUS_PROTECTION_UNAVAILABLE, SandboxExecutor
from news_futures.application.workflow import Shutdown, analyze_news, signal_deadline
from news_futures.common.artifacts import replace_json
from news_futures.common.errors import error_code
from news_futures.domain.company import OpenPosition, Snapshot
from news_futures.domain.contracts import Aggregator
from news_futures.domain.publications import TEST_SOURCE, publication_digest, utc_now
from news_futures.domain.terms import CloseIntent


class Runtime:
    def __init__(
        self, settings: RuntimeSettings, reader: Callable[[str, str | None], BrokerReader],
        broker: Callable[[str, str | None], SandboxBroker] | None, *, models: AnalysisModels, storage: RuntimeStorage,
        read_limit: Callable[[float], AbstractContextManager[None]], run_id: str | None = None,
        on_controls: Callable[..., None] | None = None, started_at: datetime | None = None,
    ):
        self.settings = settings
        self.models = models
        self.read_limit = read_limit
        self.started_at = started_at if started_at is not None else utc_now()
        self.account = BrokerAccount(settings.account_id, reader)
        self.broker = broker
        self.run_id = run_id
        self.storage = storage
        self.operator = storage.controls
        self.on_controls = on_controls
        self.pause_version: str | None = None
        self.stop = Event()
        self.failed = False
        self.broker_ready = False
        self.sources_ready = False
        self.position: OpenPosition | None = None

    def _pause(self) -> dict[str, Any]:
        pause = self.operator.pause()
        if self.on_controls is not None and pause['changed_at'] != self.pause_version:
            self.pause_version = pause['changed_at']
            self.on_controls(pause_version=self.pause_version)
        return pause

    def _entry_block(self, result: dict[str, Any]) -> str | None:
        pause = self._pause()
        if utc_now() >= datetime.fromisoformat(result['deadline_at']):
            return 'signal_deadline'
        if pause['paused']:
            return 'entries_paused'
        if pause['changed_at'] != result.get('entry_pause_version'):
            return 'entry_pause_changed'
        return None

    def _manual_close(self, events: EventStore) -> None:
        if self.broker is None or self.on_controls is None or self.stop.is_set():
            return
        row = self.operator.close_intent(pending=True)
        if row is None:
            return
        intent = CloseIntent(**{name: row[name] for name in ('request_id', 'cycle_id', 'contract_id', 'side', 'quantity')})
        reason = 'close_position_changed'
        result = SandboxExecutor(events, self.broker(intent.contract_id, None)).reconcile(
            contract_id=intent.contract_id, cycle_id=intent.cycle_id, manual=intent,
        )
        if result.get('manual') == 'accepted':
            self.operator.finish_close(intent.request_id, 'accepted', result['reason'])
            return
        if result['reason'] == 'executor_busy':
            return
        if result['status'] == 'rejected':
            reason = result['reason']
        self.operator.finish_close(intent.request_id, 'rejected', reason)

    def refresh(self) -> None:
        with self.storage.publications() as archive, self.storage.events() as events:
            self._pause()
            self._manual_close(events)
            self.refresh_broker(archive, events)

    def poll_controls(self) -> None:
        self._pause()
        if self.broker is not None and self.operator.close_intent(pending=True):
            self.refresh()

    def refresh_broker(self, archive: RuntimePublications, events: EventStore) -> None:
        settings = self.settings
        status: dict[str, Any] = {
            'mode': settings.mode, 'account_id': settings.account_id,
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
            self.position, protection = self.account.position(events)
            status.update(broker='connected', position=self.position.model_dump(mode='json') if self.position else None,
                          protection=protection)
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
            archive.save_broker_status(status, last_confirmed)
            print(json.dumps(status, ensure_ascii=False), flush=True)

    def process(self) -> None:
        settings = self.settings
        tool_agent = settings.analysis_strategy == 'tool_agent'
        with self.storage.publications() as archive, self.storage.events() as events:
            for row in archive.pending_revisions():
                if self.stop.is_set():
                    break
                publication = json.loads(row['publication'])
                if row['result'] is None:
                    if not self.broker_ready and not tool_agent and utc_now() < signal_deadline(settings, publication):
                        continue
                    result = {'revision': row['revision'], 'mode': settings.mode, 'status': 'processing',
                              'publication_id': publication['id'], 'inputs_saved': False,
                              'analysis_strategy': settings.analysis_strategy}
                    archive.save_result(result)
                else:
                    result = json.loads(row['result'])
                try:
                    self.process_revision(archive, events, publication, result)
                except Shutdown:
                    break
                except Exception as error:
                    status = 'processing' if result.get('stage') == 'execution' else 'failed'
                    result.update(status=status, reason=error_code(error))
                    if publication['source'] != TEST_SOURCE:
                        self.failed = True
                if publication['source'] != TEST_SOURCE:
                    self.failed |= result['status'] == 'failed' or (
                        result['status'] == 'rejected' and result.get('stage') == 'review'
                    )
                result['latency_ms'] = round((utc_now() - datetime.fromisoformat(
                    publication['received_at'],
                )) / timedelta(milliseconds=1))
                archive.save_result(result)
                print(json.dumps(result, ensure_ascii=False), flush=True)

    def process_revision(self, archive: RuntimePublications, events: EventStore, publication: dict[str, Any],
                         result: dict[str, Any]) -> None:
        settings = self.settings
        testing = publication['source'] == TEST_SOURCE
        directory = settings.data_dir / 'decisions' / result['revision']
        if not testing and result['mode'] == 'sandbox' and result.get('stage') == 'execution':
            approval = review_path(directory, result['review_attempt'])
            if self._recover_execution(events, approval, result):
                return
        if not testing and datetime.fromisoformat(publication['published_at']) < self.started_at:
            result.update(status='skipped', stage='source', reason='publication_before_start')
            return
        tool_agent = settings.analysis_strategy == 'tool_agent'
        frozen = directory / 'runtime.json'
        previous = (RuntimeSettings.model_validate_json(frozen.read_bytes())
                    if result['inputs_saved'] else settings)
        if (result.get('analysis_strategy', 'fixed') != settings.analysis_strategy
                or previous.order_transport != settings.order_transport
                or result['mode'] != settings.mode or previous.account_id != settings.account_id):
            result.update(status='skipped', reason='runtime_profile_changed')
            return
        if not result['inputs_saved']:
        deadline = signal_deadline(previous, publication)
        received_at = datetime.fromisoformat(publication['received_at'])
        now = utc_now()
        result['deadline_at'] = deadline.isoformat()
        if now < deadline and not self.broker_ready and not tool_agent:
            return
        result.setdefault('queue_wait_ms', max(0, round((now - received_at).total_seconds() * 1000)))
        if now >= deadline:
            result.update(status='skipped', stage='freshness', reason='signal_deadline')
            return
            saved = self.operator.saved() if self.on_controls is not None else {}
            if saved.get('account_id') != settings.account_id:
                saved = {}
            models = saved.get('models')
            aggregator: Aggregator = saved.get('aggregator', settings.chat_aggregator)
            if aggregator == 'groq':
                models = saved.get('groq_models') or groq_models(settings)
            inputs = cycle_inputs(settings, models, aggregator=aggregator)
            result['entry_pause_version'] = self._pause()['changed_at']
            directory.mkdir(parents=True, exist_ok=True)
            replace_json(directory / 'runtime.json', settings.model_dump(mode='json'))
            replace_json(directory / 'publication.json', publication)
            for field, content in inputs.items():
                (directory / f'{field}.json').write_bytes(content)
            try:
                futures_catalog = settings.futures_catalog.read_bytes()
            except FileNotFoundError:
                pass
            else:
                (directory / 'futures-catalog.json').write_bytes(futures_catalog)
            result['inputs_saved'] = True
            archive.save_result(result)
            if self.on_controls is not None:
                self.on_controls(models=role_models(inputs), aggregator=aggregator)
        catalog = (directory / 'catalog.json').read_bytes()
        futures_path = directory / 'futures-catalog.json'
        try:
            contracts = catalog_contracts(catalog, futures_path.read_bytes())
        except FileNotFoundError:
            contracts = {}

        def read_market(uid: str, seconds: float) -> MarketReading:
            with self.read_limit(seconds):
                return self.account.observe(catalog, uid, contracts[uid])

        state = analyze_news(
            previous, directory, publication, result['revision'], contracts=contracts, read_market=read_market,
            current_position=lambda: self.position, stop=self.stop, models=self.models, storage=self.storage,
            deadline=deadline,
        )
        result.update(stage=state['stage'], status=state['status'], reason=state.get('reason'))
        if 'decision_id' in state:
            result['decision_id'] = state['decision_id']
        if tool_agent:
        proposal = state.get('proposal')
        if (not testing and state['status'] in ('candidate', 'approved_for_checks')
                and proposal is not None and proposal['action'] == 'enter' and 'observation_id' in state):
            candidate = SignalCandidate(
                revision=result['revision'], event_decision_id=state['decision_id'], strategy=previous.analysis_strategy,
                proposal_id=proposal['id'], proposal=CompanyProposal.model_validate(
                    {name: value for name, value in proposal.items() if name != 'id'},
                ), observation_id=state['observation_id'], received_at=received_at, deadline_at=deadline,
                decided_at=datetime.fromisoformat(state['decided_at']),
            )
            candidates = directory / 'candidates'
            candidates.mkdir(exist_ok=True)
            key = publication_digest({'proposal_id': candidate.proposal_id})
            replace_json(candidates / f'{key}.json', candidate.model_dump(mode='json'))
            if 'proposal' in state:
                result['proposal'] = state['proposal']
            if 'critic_status' in state:
                result['critic_status'] = state['critic_status']
            return
        result.pop('review_attempt', None)
        if attempt := state.get('review_attempt'):
            result['review_attempt'] = attempt
        result.pop('reconcile_pending', None)
        if testing or state['status'] != 'approved_for_checks':
            return
        assert proposal is not None
        if proposal['action'] == 'enter' and (blocked := self._entry_block(result)):
            result.update(status='skipped', reason=blocked)
            return
        if self.broker is None:
            return
        if self.stop.is_set():
            result.update(status='processing', reason='shutdown_before_execution')
            return
        assert attempt is not None
        uid = state['selected_contract_id']
        self._execute_review(
            archive, events, review_path(directory, attempt), proposal, state.get('position_id'), result,
            {uid: contracts[uid]} if uid in contracts else {},
        )

    def _recover_execution(self, events: EventStore, approval: Path, result: dict[str, Any]) -> bool:
        trades = events.trades.report(approval=approval)
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
        self, archive: RuntimePublications, events: EventStore, review: Path, proposal: dict[str, Any],
        position_id: str | None, result: dict[str, Any], contracts: dict[str, str],
    ) -> None:
        assert self.broker is not None
        result.update(stage='execution', status='processing', reason='broker_reconciliation_required')
        archive.save_result(result)
        executor = SandboxExecutor(events, self.broker(proposal['contract_id'], contracts.get(proposal['contract_id'])))
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
            execution = executor.start(archive, review, allowed_contracts=contracts.keys(),
                                       record_risk=record_risk, entry_block=lambda: self._entry_block(result))
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


async def run(
    runtime: Runtime, channels: list[str], once: bool, *,
    consume: Callable[[Callable[[], Awaitable[None]]], Awaitable[int]],
    refresh_futures_catalog: Callable[[], None], on_phase: Callable[[str], None] | None,
) -> int:
    settings = runtime.settings
    on_controls = runtime.on_controls
    with runtime.storage.publications() as archive:
        archive.initialize_runtime()
        print(json.dumps({'mode': settings.mode, 'sources': channels, 'account_id': settings.account_id},
                         ensure_ascii=False), flush=True)

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
        loop = asyncio.get_running_loop()
        next_check = 0.0
        while not runtime.stop.is_set():
            try:
                if loop.time() >= next_check:
                    await in_worker(runtime.refresh)
                    next_check = loop.time() + settings.broker_refresh_seconds
                elif on_controls is not None:
                    await in_worker(runtime.poll_controls)
            except Exception as error:
                print(json.dumps({'broker_monitor_error': error_code(error)}), flush=True)
            await asyncio.sleep(min(1, settings.broker_refresh_seconds) if on_controls is not None
                                else settings.broker_refresh_seconds)

    async def refresh_catalog() -> None:
        while not runtime.stop.is_set():
            await asyncio.sleep(settings.futures_refresh_seconds)
            await in_worker(refresh_futures_catalog)

    await in_worker(runtime.refresh)
    await in_worker(refresh_futures_catalog)
    catalog_task = None if once else asyncio.create_task(refresh_catalog())
    observer = None if once else asyncio.create_task(monitor())
    try:
        if on_phase is not None:
            on_phase('sources')
        source_code = await consume(process)
        if once:
            await in_worker(runtime.refresh)
        return source_code or int(runtime.failed)
    finally:
        runtime.stop.set()
        background_tasks = [task for task in (observer, catalog_task) if task is not None]
        for task in background_tasks:
            task.cancel()
        await asyncio.gather(*background_tasks, return_exceptions=True)
