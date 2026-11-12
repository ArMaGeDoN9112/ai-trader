"""Checkpointed LangGraph workflow for one publication revision."""

from collections.abc import Callable
from contextlib import closing
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from threading import Event
from typing import Any, TypedDict, cast
from uuid import uuid4

from langchain_core.runnables import RunnableConfig, RunnableLambda
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph
from langsmith import tracing_context

from news_futures.agents.company import MAX_SNAPSHOT_AGE, OpenPosition, Snapshot
from news_futures.agents.decisions import analyze_event
from news_futures.agents.events import EventArchive, match_route
from news_futures.agents.routing import route_publication
from news_futures.agents.tool_agent import run_tool_agent, VERSION
from news_futures.application.checkpoints import checkpoint_serializer
from news_futures.common.artifacts import move_artifact, replace_json
from news_futures.news.publications import Archive, TEST_SOURCE, utc_now
from news_futures.trading.risk import MAX_NEWS_AGE
from news_futures.trading.terms import ContractSnapshot
from news_futures.application.config import RuntimeSettings
from news_futures.common.errors import error_code
from news_futures.application.records import review_path


class Shutdown(Exception):
    pass


class NewsState(TypedDict, total=False):
    stage: str
    status: str
    reason: str | None
    decision_id: int
    affected: list[str]
    held_contract_id: str
    position_id: str | None
    proposal: dict[str, Any] | None
    review_attempt: str | None
    critic_status: str


def analyze_news(
    settings: RuntimeSettings, directory: Path, publication: dict[str, Any], revision: str, *,
    read_snapshot: Callable[[EventArchive], Snapshot], current_position: Callable[[], OpenPosition | None],
    stop: Event, read_market: Callable[[EventArchive, str, float], tuple[Snapshot, ContractSnapshot]] | None = None,
) -> NewsState:
    tool_agent = settings.analysis_strategy == 'tool_agent'
    testing_revision = revision if publication['source'] == TEST_SOURCE else None

    def route(_: NewsState) -> NewsState:
        route_dir = directory / 'route'
        if route_dir.exists():
            move_artifact(route_dir, directory / f'route-interrupted-{uuid4().hex}')
        position = current_position()
        routed = route_publication(directory / 'jev.json', directory / 'publication.json',
                                   directory / 'catalog.json', route_dir,
                                   futures_catalog=directory / 'futures-catalog.json',
                                   position_company=settings.contracts.get(position.contract_id) if position else None)
        if routed['status'] != 'completed':
            return {'status': 'failed', 'reason': routed['error'],
                    'stage': 'category' if routed['candidate_count'] is None else 'route'}
        if routed['candidate_count'] == 0:
            return {'status': 'skipped', 'reason': 'no_category_candidates', 'stage': 'category'}
        return {'status': 'continue'}

    def record_route(_: NewsState) -> NewsState:
        with EventArchive(settings.events, testing_revision=testing_revision) as events:
            event = match_route(events, directory / 'route')
        replace_json(directory / 'event.json', event)
        affected = (event['analysis_company_ids'] if tool_agent
                    else sorted({link['company_id'] for link in event['links']}))
        result: NewsState = {'decision_id': event['decision_id'], 'affected': affected}
        if tool_agent and event['kind'] == 'repeat':
            return result | {'status': 'skipped', 'reason': 'repeat', 'stage': 'event'}
        if not set(affected).intersection(settings.contracts.values()):
            return result | {'status': 'skipped', 'reason': 'no_allowed_company'}
        return result

    def prepare(state: NewsState) -> NewsState:
        archive = Archive(settings.publications)
        with closing(archive.db), EventArchive(settings.events) as events:
            latest = archive.latest_revision(publication['id'])
            if latest is None or latest['revision'] != revision:
                return {'status': 'skipped', 'reason': 'publication_revision_superseded'}
            if testing_revision is None and archive.db.execute("""
                SELECT 1 FROM revisions
                WHERE historical = 1 AND trim(json_extract(publication, '$.text')) = trim(?)
                  AND coalesce(json_extract(publication, '$.edited_at'),
                               json_extract(publication, '$.published_at')) <= ?
                LIMIT 1
            """, (publication['text'], publication['published_at'])).fetchone():
                return {'status': 'skipped', 'reason': 'historical_reprint'}
            snapshot = None if tool_agent else read_snapshot(events)
        position = snapshot.position if snapshot is not None else current_position()
        if position:
            contract = position.contract_id
            reason = ('position_already_open' if settings.contracts[contract] in state['affected']
                      else 'account_position_limit')
            return {'status': 'skipped', 'reason': reason, 'held_contract_id': contract}
        if utc_now() - datetime.fromisoformat(publication['published_at']) > MAX_NEWS_AGE:
            return {'status': 'skipped', 'reason': 'publication_context_only'}
        if snapshot is not None:
            replace_json(directory / 'snapshot.json', snapshot.model_dump(mode='json'))
        return {}

    def agent(state: NewsState) -> NewsState:
        assert read_market is not None
        event = json.loads((directory / 'event.json').read_bytes())
        contracts = {uid: company for uid, company in settings.contracts.items() if company in state['affected']}
        archive = Archive(settings.publications)
        with EventArchive(settings.events, testing_revision=testing_revision) as events, closing(archive.db):
            def check_current() -> str | None:
                latest = archive.latest_revision(publication['id'])
                if latest is None or latest['revision'] != revision:
                    return 'publication_revision_superseded'
                if utc_now() - datetime.fromisoformat(publication['published_at']) > MAX_NEWS_AGE:
                    return 'publication_context_only'
                if current_position():
                    return 'position_already_open'
                current_event = events.db.execute(
                    'SELECT MAX(id) FROM event_decisions WHERE revision = ?', (event['revision'],),
                ).fetchone()[0]
                return 'event_decision_superseded' if current_event != event['decision_id'] else None

            def check_stop() -> None:
                if stop.is_set():
                    raise Shutdown

            return cast(NewsState, run_tool_agent(
                directory, revision, event, settings.account_id, contracts, events,
                lambda uid, seconds: read_market(events, uid, seconds),
                check_current, check_stop, checkpointer, settings.coordinator,
            ))

    def review(state: NewsState) -> NewsState:
        snapshot_path = directory / 'snapshot.json'
        content = snapshot_path.read_bytes()
        snapshot = Snapshot.model_validate_json(content)
        attempt = sha256(content).hexdigest()
        with EventArchive(settings.events, testing_revision=testing_revision) as events:
            trace = analyze_event(events, state['decision_id'], snapshot_path,
                                  directory / 'review.json', review_path(directory, attempt), mode=settings.mode,
                                  checkpointer=checkpointer, thread_id=f'{revision}:review:{attempt}')
        return {'status': trace['status'], 'reason': trace['error'], 'proposal': trace['approved_proposal'],
                'position_id': snapshot.position.id if snapshot.position else None, 'review_attempt': attempt}

    def audited(stage: str, node: Callable[[NewsState], NewsState]) -> RunnableLambda[NewsState, dict[str, Any]]:
        def note(record: dict[str, Any]) -> None:
            if stage != 'route':
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
                updates = node(state)
            except Shutdown:
                raise
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
    # Keep checkpoint node names so revisions interrupted before the upgrade can resume.
    graph.add_node('event', audited('route', record_route))
    graph.add_node('snapshot', audited('position', prepare))
    final_node = 'agent' if tool_agent else 'review'
    graph.add_node(final_node, audited(final_node, agent if tool_agent else review))
    graph.add_edge(START, 'route')
    graph.add_conditional_edges('route', lambda state: 'event' if state['status'] == 'continue' else END)
    graph.add_conditional_edges('event', lambda state: 'snapshot' if state['status'] == 'continue' else END)
    graph.add_conditional_edges('snapshot', lambda state: final_node if state['status'] == 'continue' else END)
    graph.add_edge(final_node, END)
    with closing(sqlite3.connect(settings.data_dir / 'checkpoints.sqlite3', check_same_thread=False)) as db:
        checkpointer = SqliteSaver(db, serde=checkpoint_serializer())
        config: RunnableConfig = {'configurable': {'thread_id': f'{revision}:workflow:{VERSION}' if tool_agent else revision}}
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
