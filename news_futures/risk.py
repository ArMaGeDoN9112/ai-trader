"""Deterministic entry sizing and durable reservation for a sandbox account."""

from collections.abc import Callable, Set
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from fractions import Fraction
import json
from pathlib import Path
import sqlite3
from typing import Annotated, Any, Literal
from uuid import uuid4

from pydantic import AwareDatetime, ConfigDict, Field

from news_futures.analysis import Contract, Review, validate_levels
from news_futures.decisions import CompanyProposal, Nonempty
from news_futures.events import EventArchive, model_publication
from news_futures.local import publication_digest
from news_futures.publications import Archive, utc_now


MAX_NEWS_AGE = timedelta(minutes=15)
MAX_PROPOSAL_AGE = timedelta(seconds=60)
MAX_RISK_DATA_AGE = timedelta(seconds=60)
MAX_PLANNED_RISK = 500
MAX_LEVERAGE = 2
Money = Annotated[Decimal, Field(ge=0, max_digits=18, decimal_places=9)]
Positive = Annotated[Money, Field(gt=0)]


class ContractSnapshot(Contract):
    model_config = ConfigDict(revalidate_instances='always')

    contract_id: Nonempty
    company_id: Nonempty
    share_id: Nonempty
    observed_at: AwareDatetime
    quote_at: AwareDatetime
    last_trade_at: AwareDatetime
    last_price: Positive
    currency: Literal['RUB']
    price_unit: Literal['points']
    contracts_per_lot: int = Field(gt=0)
    tick_size: Positive
    tick_value_per_contract: Positive
    margin_per_lot_buy: Positive
    margin_per_lot_sell: Positive
    round_trip_cost_per_lot: Money
    limit_buy_available: bool
    limit_sell_available: bool


class RiskSnapshot(Contract):
    model_config = ConfigDict(revalidate_instances='always')

    account_id: Nonempty
    account_observed_at: AwareDatetime
    currency: Literal['RUB']
    equity: Positive
    available_margin: Money
    has_position: bool
    has_active_orders: bool
    execution_uncertain: bool
    contract: ContractSnapshot


class Rejected(ValueError):
    """Admission failed with a stable reason suitable for the local journal."""


class EntryReservation(Contract):
    id: str
    account_id: str
    proposal_id: str
    contract_id: str
    side: Literal['long', 'short']
    entry_price: Decimal
    stop_loss: Decimal
    take_profit: Decimal
    lots: int
    contracts: int
    planned_risk: Decimal
    notional: Decimal
    initial_margin: Decimal


@dataclass(frozen=True)
class Admission:
    reservation: EntryReservation | None = None
    reason: str | None = None


def _fresh(at: datetime, now: datetime, limit: timedelta) -> bool:
    return at.utcoffset() is not None and timedelta(0) <= now - at <= limit


def read_approval(directory: Path) -> tuple[CompanyProposal, str, datetime, dict[str, Any]]:
    try:
        trace = json.loads((directory / 'trace.json').read_bytes())
        if trace['mode'] != 'sandbox' or trace['status'] != 'approved_for_checks' or trace['error'] is not None:
            raise Rejected('sandbox_approval_required')
        payload = trace['approved_proposal']
        count = len(trace['calls'])
        analyst, critic = [
            json.loads((directory / f'call-{index:02}.json').read_bytes())
            for index in (count - 1, count)
        ]
        proposal_id = payload['id']
        if (not isinstance(proposal_id, str) or not proposal_id.strip()
                or analyst['role'] != 'analyst' or critic['role'] != 'critic'
                or analyst['mode'] != 'sandbox' or critic['mode'] != 'sandbox'
                or analyst['error'] is not None or critic['error'] is not None
                or publication_digest(payload) != publication_digest(analyst['output'])
                or publication_digest(payload) != publication_digest(critic['input']['proposal'])
                or analyst['input']['context'] != critic['input']['context']):
            raise Rejected('proposal_not_reviewed')
        review = Review.model_validate(critic['output'])
        if not review.approved or review.objections or review.proposal_id != proposal_id:
            raise Rejected('proposal_not_reviewed')
        proposal = CompanyProposal.model_validate({key: value for key, value in payload.items() if key != 'id'})
        formed_at = datetime.fromisoformat(analyst['formed_at'])
    except Rejected:
        raise
    except (OSError, ValueError, KeyError, TypeError, IndexError, OverflowError) as error:
        raise Rejected('invalid_approval_record') from error
    return proposal, proposal_id, formed_at, critic['input']['context']


def check_publication(
    events: EventArchive, publications: Archive, context: dict[str, Any],
    proposal: CompanyProposal, formed_at: datetime,
) -> datetime:
    latest = publications.latest_revision(proposal.publication_id)
    event = context['event']
    if latest is None or latest['revision'] != event['revision']:
        raise Rejected('publication_revision_unconfirmed')
    publication = latest['publication']
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
    if not _fresh(formed_at, now, MAX_PROPOSAL_AGE):
        raise Rejected('stale_or_future_proposal')
    if ((proposal.action == 'enter' and not _fresh(published, now, MAX_NEWS_AGE))
            or any(at.utcoffset() is None for at in (received, edited))
            or not published <= edited <= received <= formed_at):
        raise Rejected('stale_or_unverified_publication_time')
    return now


def _size(
    snapshot: RiskSnapshot, context: dict[str, Any], proposal: CompanyProposal,
    proposal_id: str, now: datetime,
) -> EntryReservation:
    contract = snapshot.contract
    if not all(_fresh(at, now, MAX_RISK_DATA_AGE) for at in (
        snapshot.account_observed_at, contract.observed_at, contract.quote_at,
    )):
        raise Rejected('stale_or_future_risk_data')
    if snapshot.execution_uncertain or snapshot.has_position or snapshot.has_active_orders:
        raise Rejected('account_has_exposure_or_unresolved_execution')
    if (contract.last_trade_at <= now or not contract.limit_buy_available
            or not contract.limit_sell_available):
        raise Rejected('contract_unavailable')
    if (contract.contract_id != proposal.contract_id or contract.company_id != proposal.company_id
            or len({contract.contract_id, contract.company_id, contract.share_id}) != 3
            or not any(candidate['contract_id'] == contract.contract_id
                       and candidate['company_id'] == contract.company_id
                       and candidate['share_id'] == contract.share_id
                       and candidate['allowed_for_entry'] is True for candidate in context['candidates'])):
        raise Rejected('contract_specification_mismatch')
    try:
        validate_levels(proposal.model_dump())
    except ValueError as error:
        raise Rejected('invalid_protection_levels') from error
    side = proposal.side
    assert side is not None
    entry, stop, take = (Decimal(str(price)) for price in (
        proposal.entry_price, proposal.stop_loss, proposal.take_profit,
    ))
    tick = Fraction(contract.tick_size)
    ticks = [
        Fraction(price) / tick
        for price in (entry, stop, take, contract.last_price)
    ]
    if any(price.denominator != 1 for price in ticks):
        raise Rejected('price_off_tick')
    if not min(stop, take) < contract.last_price < max(stop, take):
        raise Rejected('market_outside_protection_levels')
    entry_ticks, stop_ticks, _, quote_ticks = ticks
    tick_value_per_lot = Fraction(contract.tick_value_per_contract) * contract.contracts_per_lot
    risk = abs(entry_ticks - stop_ticks) * tick_value_per_lot + Fraction(contract.round_trip_cost_per_lot)
    # A short limit can fill above its limit price, up to the protective range.
    exposure_ticks = max(entry_ticks, quote_ticks, stop_ticks) if side == 'short' else max(entry_ticks, quote_ticks)
    exposure = exposure_ticks * tick_value_per_lot
    margin = contract.margin_per_lot_buy if side == 'long' else contract.margin_per_lot_sell
    limits = {
        'risk_budget_exceeded': MAX_PLANNED_RISK // risk,
        'leverage_limit_exceeded': MAX_LEVERAGE * Fraction(snapshot.equity) // exposure,
        'insufficient_margin': Fraction(snapshot.available_margin) // Fraction(margin),
    }
    lots = min(limits.values())
    if lots < 1:
        raise Rejected(next(reason for reason, maximum in limits.items() if maximum < 1))
    planned_risk = risk * lots
    notional = exposure * lots
    return EntryReservation(
        id=uuid4().hex, account_id=snapshot.account_id, proposal_id=proposal_id,
        contract_id=contract.contract_id, side=side, entry_price=entry, stop_loss=stop, take_profit=take,
        lots=lots, contracts=lots * contract.contracts_per_lot,
        planned_risk=Decimal(planned_risk.numerator) / Decimal(planned_risk.denominator),
        notional=Decimal(notional.numerator) / Decimal(notional.denominator),
        initial_margin=margin * lots,
    )


def reserve_entry(
    events: EventArchive, publications: Archive, directory: Path, *, account_id: str,
    allowed_contracts: Set[str], read_snapshot: Callable[[str, str], object],
    on_reserve: Callable[[EntryReservation, str], None] | None = None,
) -> Admission:
    """Re-read risk data and reserve an entry. The callback records intent in the same transaction."""
    try:
        proposal, proposal_id, formed_at, context = read_approval(directory)
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
        if snapshot.account_id != account_id:
            raise Rejected('account_mismatch')
        with publications.db, events.db:
            events.db.execute('BEGIN IMMEDIATE')
            events.db.execute("""
                CREATE TABLE IF NOT EXISTS entry_reservations (
                    account_id TEXT PRIMARY KEY NOT NULL, id TEXT NOT NULL, terms TEXT NOT NULL
                )
            """)
            if events.db.execute(
                'SELECT 1 FROM entry_reservations WHERE account_id = ?', (account_id,),
            ).fetchone():
                raise Rejected('entry_already_reserved')
            # Keep an edit or deletion from landing between the source check and reservation commit.
            publications.db.execute('BEGIN IMMEDIATE')
            now = check_publication(events, publications, context, proposal, formed_at)
            reservation = _size(snapshot, context, proposal, proposal_id, now)
            events.db.execute('INSERT INTO entry_reservations VALUES (?, ?, ?)', (
                account_id, reservation.id, reservation.model_dump_json(),
            ))
            if on_reserve is not None:
                on_reserve(reservation, proposal.reason)
        return Admission(reservation=reservation)
    except Rejected as error:
        return Admission(reason=str(error))
    except sqlite3.DatabaseError:
        return Admission(reason='admission_storage_unavailable')
    except (ValueError, KeyError, TypeError, AttributeError, OverflowError):
        return Admission(reason='invalid_admission_evidence')


def cancel_unsubmitted(events: EventArchive, reservation: EntryReservation) -> bool:
    """Cancel only a reservation whose owner knows no order was submitted."""
    with events.db:
        result = events.db.execute('DELETE FROM entry_reservations WHERE account_id = ? AND id = ?',
                                   (reservation.account_id, reservation.id))
    return result.rowcount == 1
