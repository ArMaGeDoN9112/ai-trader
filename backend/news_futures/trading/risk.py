"""Deterministic entry sizing and durable reservation for a sandbox account."""

from collections.abc import Callable, Set
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from news_futures.trading.approval import read_approval
from news_futures.agents.events import model_publication
from news_futures.storage.contracts import EventStore, PublicationStore
from news_futures.common.errors import Rejected, StorageError
from news_futures.schemas.company import CompanyProposal
from news_futures.dataflows.publications import TEST_SOURCE, utc_now
from news_futures.trading.sizing import MAX_NEWS_AGE, MAX_PROPOSAL_AGE, fresh, size_entry
from news_futures.trading.terms import EntryReservation, RiskSnapshot


@dataclass(frozen=True)
class Admission:
    reservation: EntryReservation | None = None
    reason: str | None = None


def check_publication(
    events: EventStore, publications: PublicationStore, context: dict[str, Any],
    proposal: CompanyProposal, formed_at: datetime,
) -> datetime:
    latest = publications.latest_revision(proposal.publication_id)
    event = context['event']
    if latest is None or latest['revision'] != event['revision']:
        raise Rejected('publication_revision_unconfirmed')
    publication = latest['publication']
    if publication['source'] == TEST_SOURCE:
        raise Rejected('testing_publication')
    if latest['historical'] != 0 or publication['change'] == 'delete' or not publication['text'].strip():
        raise Rejected('publication_context_only')
    current = events.history(current_only=True)
    if model_publication(publication) != context['publication'] or not any(
        row['decision_id'] == event['decision_id'] and row['revision'] == event['revision']
        and proposal.company_id in row['analysis_company_ids']
        for row in current
    ):
        raise Rejected('publication_revision_unconfirmed')
    reviewed_decisions = {row['decision_id'] for row in context['related_news']}
    if any(row['event_id'] == event['event_id'] and row['kind'] != 'repeat'
           and row['decision_id'] > event['decision_id'] and row['decision_id'] not in reviewed_decisions
           for row in current):
        raise Rejected('event_context_superseded')
    published = datetime.fromisoformat(publication['published_at'])
    received = datetime.fromisoformat(publication['received_at'])
    edited = datetime.fromisoformat(publication['edited_at']) if publication['edited_at'] else published
    now = utc_now()
    if not fresh(formed_at, now, MAX_PROPOSAL_AGE):
        raise Rejected('stale_or_future_proposal')
    if ((proposal.action == 'enter' and not fresh(published, now, MAX_NEWS_AGE))
            or any(at.utcoffset() is None for at in (received, edited))
            or not published <= edited <= received <= formed_at):
        raise Rejected('stale_or_unverified_publication_time')
    return now


def reserve_entry(
    events: EventStore, publications: PublicationStore, directory: Path, *, account_id: str,
    allowed_contracts: Set[str], read_snapshot: Callable[[str, str], object],
    on_reserve: Callable[[EntryReservation, CompanyProposal, dict[str, Any]], None] | None = None,
    risk_input: dict[str, Any] | None = None,
) -> Admission:
    """Re-read risk data and reserve an entry. The callback records intent in the same transaction."""
    try:
        proposal, proposal_id, formed_at, context = read_approval(directory)
        if risk_input is not None:
            risk_input['proposal'] = proposal.model_dump(mode='json')
        if proposal.action != 'enter':
            raise Rejected('entry_proposal_required')
        contract_id = proposal.contract_id
        if contract_id is None or contract_id not in allowed_contracts:
            raise Rejected('contract_not_allowed')
        if context['account']['id'] != account_id:
            raise Rejected('account_mismatch')
        try:
            snapshot = RiskSnapshot.model_validate(read_snapshot(account_id, contract_id))
        except Exception as error:
            raise Rejected('risk_data_unavailable_or_invalid') from error
        if risk_input is not None:
            risk_input['snapshot'] = snapshot.model_dump(mode='json')
        if snapshot.account_id != account_id:
            raise Rejected('account_mismatch')
        with publications.transaction(), events.transaction(immediate=True):
            events.trades.require_available(account_id)
            # Hold the source lock through the reservation commit.
            publications.lock_writes()
            now = check_publication(events, publications, context, proposal, formed_at)
            reservation = size_entry(snapshot, context, proposal, proposal_id, now)
            events.trades.reserve(reservation)
            if on_reserve is not None:
                on_reserve(reservation, proposal, context)
        return Admission(reservation=reservation)
    except Rejected as error:
        return Admission(reason=str(error))
    except StorageError:
        return Admission(reason='admission_storage_unavailable')
    except (ValueError, KeyError, TypeError, AttributeError, OverflowError):
        return Admission(reason='invalid_admission_evidence')


def cancel_unsubmitted(events: EventStore, reservation: EntryReservation) -> bool:
    """Cancel only a reservation whose owner knows no order was submitted."""
    with events.transaction():
        return events.trades.release(reservation)
