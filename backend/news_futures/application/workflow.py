"""Checkpointed LangGraph workflow for one publication revision."""

from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta
from hashlib import sha256
import json
from pathlib import Path
from threading import Event
from typing import Any, TypedDict, cast
from uuid import uuid4

from langchain_core.language_models import BaseChatModel
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langgraph.graph import END, START, StateGraph
from langsmith import tracing_context

from news_futures.application.analysis.decisions import analyze_event, news_record
from news_futures.application.analysis.events import match_route
from news_futures.application.analysis.routing import route_publication, select_contract
from news_futures.application.analysis.team_dto import EventEvidence
from news_futures.application.analysis.tool_agent import TEAM_DIRECTORY, run_tool_agent
from news_futures.application.artifact_paths import market_path, review_path
from news_futures.application.config import RuntimeSettings
from news_futures.application.ports.broker import MarketReading, NoSupportedFutures, ReadMarket
from news_futures.application.ports.models import AnalysisModels, BatchDecision, Choose
from news_futures.application.ports.storage import EventStore, PublicationStore, RuntimeStorage
from news_futures.common.artifacts import move_artifact, replace_json
from news_futures.common.errors import Rejected, error_code
from news_futures.domain.company import MAX_SNAPSHOT_AGE, OpenPosition, Snapshot
from news_futures.domain.contracts import RoleSettings
from news_futures.domain.publications import TEST_SOURCE, utc_now
from news_futures.domain.risk import MAX_NEWS_AGE


class Shutdown(Exception):
    pass


class SignalExpired(Exception):
    pass


def signal_deadline(settings: RuntimeSettings, publication: dict[str, Any]) -> datetime:
    return min(datetime.fromisoformat(publication['received_at']) + timedelta(seconds=settings.signal_timeout_seconds),
               datetime.fromisoformat(publication['published_at']) + MAX_NEWS_AGE)


class NewsState(TypedDict, total=False):
    stage: str
    status: str
    reason: str | None
    decision_id: int
    affected: list[str]
    selected_contract_id: str
    held_contract_id: str
    position_id: str | None
    proposal: dict[str, Any] | None
    review_attempt: str | None
    critic_status: str
    observation_id: str
    decided_at: str


def analyze_news(
    settings: RuntimeSettings, directory: Path, publication: dict[str, Any], revision: str, *,
    contracts: dict[str, str], read_market: ReadMarket, deadline: datetime,
    current_position: Callable[[], OpenPosition | None],
    stop: Event, models: AnalysisModels, storage: RuntimeStorage,
) -> NewsState:
    tool_agent = settings.analysis_strategy == 'tool_agent'
    testing_revision = revision if publication['source'] == TEST_SOURCE else None

    def remaining() -> float:
        if stop.is_set():
            raise Shutdown
        seconds = (deadline - utc_now()).total_seconds()
        if seconds <= 0:
            raise SignalExpired
        return seconds

    @contextmanager
    def decisions() -> Iterator[Choose]:
        remaining()
        with models.decisions() as choose:
            def bounded(model: str, questions: dict[str, Any], state: dict[str, Any], keys: dict[str, str], *,
                        timeout: float) -> BatchDecision:
                result = choose(model, questions, state, keys, timeout=min(timeout, remaining()))
                remaining()
                return result
            yield bounded

    def chat(role: RoleSettings) -> BaseChatModel:
        budget_ms = max(1, int(remaining() * 1000))
        return models.chat(role.model_copy(update={'timeout_ms': min(role.timeout_ms, budget_ms)}))

    def check_current(decision_id: int, archive: PublicationStore, events: EventStore) -> str | None:
        latest = archive.latest_revision(publication['id'])
        if latest is None or latest['revision'] != revision:
            return 'publication_revision_superseded'
        if current_position():
            return 'position_already_open'
        if events.current_decision_id(revision) != decision_id:
            return 'event_decision_superseded'
        return None

    def observe(uid: str, timeout: float, current: Callable[[], str | None]) -> MarketReading:
        def capture() -> MarketReading:
            return read_market(uid, min(timeout, remaining()))

        with ThreadPoolExecutor(max_workers=1) as pool:
            market = pool.submit(capture)
            if reason := current():
                market.cancel()
                raise Rejected(reason)
            reading = market.result()
        remaining()
        if reason := current():
            raise Rejected(reason)
        observation = reading.observation
        if (observation.contract_id != uid or len(reading.snapshot.instruments) != 1
                or reading.snapshot.instruments[0].contract_id != uid
                or reading.snapshot.instruments[0].market != observation.market
                or reading.snapshot.instruments[0].market_observation_id != observation.id):
            raise ValueError('market_observation_mismatch')
        path = market_path(directory, observation.id)
        path.parent.mkdir(exist_ok=True)
        replace_json(path, observation.model_dump(mode='json'))
        return reading

    def route(_: NewsState) -> NewsState:
        route_dir = directory / 'route'
        if route_dir.exists():
            move_artifact(route_dir, directory / f'route-interrupted-{uuid4().hex}')
        position = current_position()
        routed = route_publication(directory / 'decisions.json', directory / 'publication.json',
                                   directory / 'catalog.json', route_dir,
                                   decisions=decisions, futures_catalog=directory / 'futures-catalog.json',
                                   position_company=contracts.get(position.contract_id) if position else None)
        if routed['status'] != 'completed':
            return {'status': 'failed', 'reason': routed['error'],
                    'stage': 'category' if routed['candidate_count'] is None else 'route'}
        if routed['candidate_count'] == 0:
            return {'status': 'skipped', 'reason': 'no_category_candidates', 'stage': 'category'}
        return {'status': 'continue'}

    def record_route(_: NewsState) -> NewsState:
        with storage.events(testing_revision=testing_revision) as events:
            event = match_route(events, directory / 'route')
        replace_json(directory / 'event.json', event)
        affected = (event['analysis_company_ids'] if tool_agent
                    else sorted({link['company_id'] for link in event['links']}))
        result: NewsState = {'decision_id': event['decision_id'], 'affected': affected}
        if tool_agent and event['kind'] == 'repeat':
            return result | {'status': 'skipped', 'reason': 'repeat', 'stage': 'event'}
        if not affected:
            return result | {'status': 'skipped', 'reason': 'no_company_link'}
        if not set(affected).intersection(contracts.values()):
            return result | {'status': 'skipped', 'reason': 'no_listed_futures'}
        return result

    def prepare(state: NewsState) -> NewsState:
        with storage.publications() as archive:
            latest = archive.latest_revision(publication['id'])
            if latest is None or latest['revision'] != revision:
                return {'status': 'skipped', 'reason': 'publication_revision_superseded'}
            if testing_revision is None and archive.historical_reprint(publication['text'], publication['published_at']):
                return {'status': 'skipped', 'reason': 'historical_reprint'}
        position = current_position()
        if position:
            contract = position.contract_id
            reason = ('position_already_open' if contracts.get(contract) in state['affected']
                      else 'account_position_limit')
            return {'status': 'skipped', 'reason': reason, 'held_contract_id': contract}
        return {}

    def instrument(state: NewsState) -> NewsState:
        if selected := state.get('selected_contract_id'):
            return {} if selected in contracts else {'status': 'skipped', 'reason': 'no_suitable_contract'}
        selection = select_contract(directory, publication['text'], {
            uid: company for uid, company in contracts.items() if company in state['affected']
        }, decisions)
        if selection['error']:
            return {'status': 'failed', 'reason': selection['error']}
        if selection['selected'] is None:
            return {'status': 'skipped', 'reason': 'no_suitable_contract'}
        return {'selected_contract_id': selection['selected']['contract_id']}

    def capture_snapshot(state: NewsState) -> NewsState:
        uid = state['selected_contract_id']
        try:
            with storage.publications() as archive, storage.events(testing_revision=testing_revision) as events:
                reading = observe(uid, remaining(), lambda: check_current(state['decision_id'], archive, events))
        except (NoSupportedFutures, Rejected) as error:
            return {'status': 'skipped', 'reason': str(error)}
        replace_json(directory / 'snapshot.json', reading.snapshot.model_dump(mode='json'))
        return {}

    def agent(state: NewsState) -> NewsState:
        event = EventEvidence.model_validate(news_record(json.loads((directory / 'event.json').read_bytes())))
        uid = state['selected_contract_id']
        with storage.events(testing_revision=testing_revision) as events, storage.publications() as archive:
            def current() -> str | None:
                return check_current(event.decision_id, archive, events)

            def check_stop() -> None:
                if stop.is_set():
                    raise Shutdown

            outcome = run_tool_agent(
                directory, revision, event, settings.account_id, {uid: contracts[uid]}, events,
                lambda contract, timeout: observe(contract, timeout, current), current,
                check_stop, checkpointer, settings.coordinator,
                settings.agent_limits, chat, deadline=deadline,
            )
            result: NewsState = {'status': outcome.status, 'reason': outcome.reason}
            if outcome.proposal is not None:
                result['proposal'] = outcome.proposal.model_dump(mode='json')
            if outcome.critic_status is not None:
                result['critic_status'] = outcome.critic_status
            if outcome.observation_id is not None:
                result['observation_id'] = outcome.observation_id
            return result

    def review(state: NewsState) -> NewsState:
        uid = state['selected_contract_id']
        snapshot_path = directory / 'snapshot.json'
        content = snapshot_path.read_bytes()
        snapshot = Snapshot.model_validate_json(content)
        if [item.contract_id for item in snapshot.instruments] != [uid]:
            return {'status': 'failed', 'reason': 'instrument_selection_mismatch'}
        attempt = sha256(content).hexdigest()
        with storage.events(testing_revision=testing_revision) as events, storage.publications() as archive:
            trace = analyze_event(events, state['decision_id'], snapshot_path,
                                  directory / 'review.json', review_path(directory, attempt), mode=settings.mode, model_factory=chat,
                                  checkpointer=checkpointer, thread_id=f'{revision}:review:{attempt}')
            if not snapshot.position and trace['approved_proposal'] is not None:
                if reason := check_current(state['decision_id'], archive, events):
                    return {'status': 'skipped', 'reason': reason, 'proposal': None}
        result: NewsState = {'status': trace['status'], 'reason': trace['error'], 'proposal': trace['approved_proposal'],
                             'position_id': snapshot.position.id if snapshot.position else None, 'review_attempt': attempt}
        if observation_id := snapshot.instruments[0].market_observation_id:
            result['observation_id'] = observation_id
        return result

    def audited(stage: str, node: Callable[[NewsState], NewsState]) -> RunnableLambda[NewsState, dict[str, Any]]:
        def note(record: dict[str, Any]) -> None:
            if stage not in ('route', 'instrument'):
                try:
                    replace_json(directory / f'{stage}-status.json', record)
                except OSError:
                    # Stage markers are supplementary to the checkpoint and the decision record.
                    pass

        def invoke(state: NewsState) -> dict[str, Any]:
            if stop.is_set():
                raise Shutdown
            started = utc_now().isoformat()
            note({'at': started, 'status': 'processing'})
            try:
                remaining()
                updates = node(state)
                completed = utc_now()
                if completed >= deadline:
                    raise SignalExpired
                if updates.get('proposal') is not None:
                    updates['decided_at'] = completed.isoformat()
            except Shutdown:
                raise
            except SignalExpired:
                updates = {'status': 'skipped', 'reason': 'signal_deadline'}
            except Exception as error:
                updates = {'status': 'failed', 'reason': error_code(error)}
            updates.setdefault('stage', stage)
            note({'at': utc_now().isoformat(), 'started_at': started,
                  'status': updates.get('status', 'completed'), 'reason': updates.get('reason'),
                  **({'held_contract_id': updates['held_contract_id']} if 'held_contract_id' in updates else {})})
            return dict(updates)
        return RunnableLambda(invoke)

    graph: StateGraph[NewsState, None, NewsState, NewsState] = StateGraph(NewsState)
    graph.add_node('route', audited('route', route))
    graph.add_node('event', audited('route', record_route))
    graph.add_node('position', audited('position', prepare))
    graph.add_node('instrument', audited('instrument', instrument))
    final_node = 'agent' if tool_agent else 'review'
    if not tool_agent:
        graph.add_node('snapshot', audited('snapshot', capture_snapshot))
        graph.add_conditional_edges('snapshot', lambda state: final_node if state['status'] == 'continue' else END)
    graph.add_node(final_node, audited(final_node, agent if tool_agent else review))
    graph.add_edge(START, 'route')
    graph.add_conditional_edges('route', lambda state: 'event' if state['status'] == 'continue' else END)
    graph.add_conditional_edges('event', lambda state: 'position' if state['status'] == 'continue' else END)
    graph.add_conditional_edges('position', lambda state: 'instrument' if state['status'] == 'continue' else END)
    graph.add_conditional_edges('instrument', lambda state: ('agent' if tool_agent else 'snapshot')
                                if state['status'] == 'continue' else END)
    graph.add_edge(final_node, END)
    with storage.checkpoints() as checkpointer:
        config: RunnableConfig = {
            'configurable': {'thread_id': f'{revision}:workflow:{TEAM_DIRECTORY}' if tool_agent else revision},
        }
        with tracing_context(enabled=False):
            compiled = graph.compile(checkpointer=checkpointer)
            saved = compiled.get_state(config)
            if saved.created_at and ('review' in saved.next or saved.values.get('stage') == 'review'):
                snapshot = Snapshot.model_validate_json((directory / 'snapshot.json').read_bytes())
                timestamps = [snapshot.account.observed_at, *(item.market.observed_at for item in snapshot.instruments)]
                if (snapshot.position is not None or current_position() is not None
                        or (saved.next and utc_now() - min(timestamps) > MAX_SNAPSHOT_AGE)):
                    compiled.update_state(config, {'status': 'continue', 'reason': None, 'proposal': None,
                                                   'review_attempt': None}, as_node='event')
                    saved = compiled.get_state(config)
            if saved.created_at and not saved.next:
                return cast(NewsState, saved.values)
            return cast(NewsState, compiled.invoke(
                None if saved.created_at else {'status': 'continue'}, config, durability='sync',
            ))
