"""Checkpointed LangGraph workflow for one publication revision."""

from collections.abc import Callable
from contextlib import closing
from datetime import datetime
from hashlib import sha256
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
from news_futures.application.checkpoints import checkpoint_serializer
from news_futures.common.artifacts import move_artifact, replace_json
from news_futures.news.publications import Archive, utc_now
from news_futures.trading.risk import MAX_NEWS_AGE
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
    position_id: str | None
    proposal: dict[str, Any] | None
    review_attempt: str | None


def analyze_news(
    settings: RuntimeSettings, directory: Path, publication: dict[str, Any], revision: str, *,
    read_snapshot: Callable[[EventArchive], Snapshot], current_position: Callable[[], OpenPosition | None],
    stop: Event,
) -> NewsState:

    def route(_: NewsState) -> NewsState:
        route_dir = directory / 'route'
        if route_dir.exists():
            move_artifact(route_dir, directory / f'route-interrupted-{uuid4().hex}')
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
            snapshot = read_snapshot(events)
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
            except Exception as error:
                updates = {'status': 'failed', 'reason': error_code(error)}
            updates['stage'] = stage
            note({'at': utc_now().isoformat(), 'started_at': started,
                  'status': updates.get('status', 'completed'), 'reason': updates.get('reason')})
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
        checkpointer = SqliteSaver(db, serde=checkpoint_serializer())
        config: RunnableConfig = {'configurable': {'thread_id': revision}}
        with tracing_context(enabled=False):
            compiled = graph.compile(checkpointer=checkpointer)
            saved = compiled.get_state(config)
            if saved.created_at and ('review' in saved.next or saved.values.get('stage') == 'review'):
                snapshot = Snapshot.model_validate_json((directory / 'snapshot.json').read_bytes())
                timestamps = [snapshot.account.observed_at, *(item.market.observed_at for item in snapshot.instruments)]
                if snapshot.position != current_position() or (saved.next and utc_now() - min(timestamps) > MAX_SNAPSHOT_AGE):
                    compiled.update_state(config, {'status': 'continue', 'reason': None, 'proposal': None,
                                                   'review_attempt': None}, as_node='event')
                    saved = compiled.get_state(config)
            if saved.created_at and not saved.next:
                return cast(NewsState, saved.values)
            return cast(NewsState, compiled.invoke(
                None if saved.created_at else {'status': 'continue'}, config, durability='sync',
            ))
