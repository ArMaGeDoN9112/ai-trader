"""Risk limits and position sizing for validated proposals."""

from datetime import datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import Field

from news_futures.common.errors import Rejected
from news_futures.domain.company import CompanyProposal
from news_futures.domain.contracts import Contract, validate_levels
from news_futures.domain.terms import EntryReservation, RiskSnapshot


class RiskPolicy(Contract):
    max_news_age_seconds: int = Field(gt=0)
    max_proposal_age_seconds: int = Field(gt=0)
    max_risk_data_age_seconds: int = Field(gt=0)
    max_planned_risk_rub: Decimal = Field(gt=0)
    max_leverage: Decimal = Field(gt=0)


POLICY = RiskPolicy.model_validate_json(Path(__file__).with_name('risk-policy.json').read_bytes())
MAX_NEWS_AGE = timedelta(seconds=POLICY.max_news_age_seconds)
MAX_PROPOSAL_AGE = timedelta(seconds=POLICY.max_proposal_age_seconds)
MAX_RISK_DATA_AGE = timedelta(seconds=POLICY.max_risk_data_age_seconds)


def fresh(at: datetime, now: datetime, limit: timedelta) -> bool:
    return at.utcoffset() is not None and timedelta(0) <= now - at <= limit


def size_entry(
    snapshot: RiskSnapshot, context: dict[str, Any], proposal: CompanyProposal,
    proposal_id: str, now: datetime,
) -> EntryReservation:
    contract = snapshot.contract
    if not all(fresh(at, now, MAX_RISK_DATA_AGE) for at in (
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
        'risk_budget_exceeded': Fraction(POLICY.max_planned_risk_rub) // risk,
        'leverage_limit_exceeded': Fraction(POLICY.max_leverage) * Fraction(snapshot.equity) // exposure,
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
