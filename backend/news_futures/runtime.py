"""Run publication processing and broker monitoring for one local profile."""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import AbstractContextManager
from datetime import datetime, timedelta
import json
from pathlib import Path
from threading import Event
from typing import Any

from news_futures.dataflows.catalog import catalog_contracts
from news_futures.config import RuntimeSettings, cycle_inputs, groq_models, role_models
from news_futures.brokers.contracts import BrokerReader, MarketReading, SandboxBroker
from news_futures.llm_clients.contracts import AnalysisModels
from news_futures.storage.contracts import EventStore, RuntimePublications, RuntimeStorage
from news_futures.trading.account import BrokerAccount
from news_futures.trading.executor import SandboxExecutor
from news_futures.workflow import Shutdown, analyze_news, signal_deadline
from news_futures.common.artifacts import replace_json
from news_futures.common.errors import error_code
from news_futures.schemas.company import CompanyProposal, MinuteCandle, OpenPosition, SignalCandidate
from news_futures.dataflows.catalog import listed_futures
from news_futures.schemas.contracts import Aggregator
from news_futures.dataflows.publications import TEST_SOURCE, publication_digest, utc_now
from news_futures.trading.terms import CloseIntent


IMOEX_UID = '4821c9aa-36e8-4743-b37c-861e58581b25'
SECTOR_INDEX_UIDS = {
    'financial': 'abaaa0fe-472d-4240-9679-144dd382e6ce',
    'consumer': '7ad47d78-8e53-4b52-a2ef-c0b7ab580850',
    'oil_gas': 'cb58b054-84a6-4939-99b1-d0cab7e7d8d1',
    'metals_mining': 'e130b83b-5c97-4b71-b638-f1c7faeaf5ae',
}


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
        self.sources_ready = False
        self.position: OpenPosition | None = None

    def _pause(self) -> dict[str, Any]:
        pause = self.operator.pause()
        if self.on_controls is not None and pause['changed_at'] != self.pause_version:
            self.pause_version = pause['changed_at']
            self.on_controls(pause_version=self.pause_version)
        return pause

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
                reconciliation = self.account.reconcile(events, self.broker)
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
            archive.save_broker_status(status, last_confirmed)
            print(json.dumps(status, ensure_ascii=False), flush=True)

    def process(self) -> None:
        settings = self.settings
        with self.storage.publications() as archive:
            for row in archive.pending_revisions():
                if self.stop.is_set():
                    break
                publication = json.loads(row['publication'])
                if row['result'] is None:
                    result = {'revision': row['revision'], 'mode': settings.mode, 'status': 'processing',
                              'publication_id': publication['id'], 'inputs_saved': False}
                    archive.save_result(result)
                else:
                    result = json.loads(row['result'])
                try:
                    self.process_revision(archive, publication, result)
                except Shutdown:
                    break
                except Exception as error:
                    result.update(status='failed', reason=error_code(error))
                if publication['source'] != TEST_SOURCE and result['status'] == 'failed':
                    self.failed = True
                result['latency_ms'] = round((utc_now() - datetime.fromisoformat(
                    publication['received_at'],
                )) / timedelta(milliseconds=1))
                archive.save_result(result)
                print(json.dumps(result, ensure_ascii=False), flush=True)

    def process_revision(self, archive: RuntimePublications, publication: dict[str, Any],
                         result: dict[str, Any]) -> None:
        settings = self.settings
        testing = publication['source'] == TEST_SOURCE
        directory = settings.data_dir / 'decisions' / result['revision']
        if not testing and datetime.fromisoformat(publication['published_at']) < self.started_at:
            result.update(status='skipped', stage='source', reason='publication_before_start')
            return
        frozen = directory / 'runtime.json'
        previous = (RuntimeSettings.model_validate_json(frozen.read_bytes())
                    if result['inputs_saved'] else settings)
        if (previous.order_transport != settings.order_transport
                or result['mode'] != settings.mode or previous.account_id != settings.account_id):
            result.update(status='skipped', reason='runtime_profile_changed')
            return
        deadline = signal_deadline(previous, publication)
        received_at = datetime.fromisoformat(publication['received_at'])
        now = utc_now()
        result['deadline_at'] = deadline.isoformat()
        result.setdefault('queue_wait_ms', max(0, round((now - received_at).total_seconds() * 1000)))
        if now >= deadline:
            result.update(status='skipped', stage='freshness', reason='signal_deadline')
            return
        if not result['inputs_saved']:
            saved = self.operator.saved() if self.on_controls is not None else {}
            if saved.get('account_id') != settings.account_id:
                saved = {}
            models = saved.get('models')
            aggregator: Aggregator = saved.get('aggregator', settings.chat_aggregator)
            if aggregator == 'groq':
                models = saved.get('groq_models') or groq_models(settings)
            inputs = cycle_inputs(settings, models, aggregator=aggregator)
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

        def read_candles(uid: str, start: datetime, end: datetime, seconds: float) -> list[MinuteCandle]:
            with self.read_limit(seconds):
                return self.account.reader(uid, contracts[uid]).read_candles(start, end)

        def read_comparison(uid: str, start: datetime, end: datetime,
                            seconds: float) -> dict[str, tuple[str, list[MinuteCandle]]]:
            company = contracts[uid]
            contract = listed_futures(json.loads(futures_path.read_bytes()))[uid]
            with self.read_limit(seconds):
                reader = self.account.reader(uid, company)
                references = {'underlying': reader.resolve_share(contract['share_id']), 'IMOEX': IMOEX_UID}
                sectors = SECTOR_INDEX_UIDS.keys() & set(contract['categories'])
                sector = previous.sector_benchmarks.get(company)
                if sector is None and len(sectors) == 1:
                    sector = SECTOR_INDEX_UIDS[sectors.pop()]
                if sector:
                    references['sector'] = sector
                return {'future': (uid, reader.read_candles(start, end))} | {
                    key: (instrument, reader.read_reference_candles(instrument, start, end))
                    for key, instrument in references.items()
                }

        state = analyze_news(
            previous, directory, publication, result['revision'], contracts=contracts, read_market=read_market,
            current_position=lambda: self.position, stop=self.stop, models=self.models, storage=self.storage,
            deadline=deadline, read_candles=read_candles, read_comparison=read_comparison,
        )
        result.update(stage=state['stage'], status=state['status'], reason=state.get('reason'))
        if 'decision_id' in state:
            result['decision_id'] = state['decision_id']
        proposal = state.get('proposal')
        if (not testing and state['status'] == 'candidate'
                and proposal is not None and proposal['action'] == 'enter' and 'observation_id' in state):
            candidate = SignalCandidate(
                revision=result['revision'], event_decision_id=state['decision_id'], strategy='tool_agent',
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
