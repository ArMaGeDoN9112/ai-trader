"""Validated broker data and the immutable terms reserved for an entry."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import AwareDatetime, ConfigDict, Field

from news_futures.agents.company import Nonempty
from news_futures.agents.contracts import Contract


Money = Annotated[Decimal, Field(ge=0, max_digits=18, decimal_places=9)]
Positive = Annotated[Money, Field(gt=0)]


class ContractSnapshot(Contract):
    model_config = ConfigDict(revalidate_instances='always')

    contract_id: Nonempty
    company_id: Nonempty
    share_id: Nonempty
    ticker: Nonempty | None = None
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
class CloseIntent:
    request_id: str
    cycle_id: str
    contract_id: str
    side: str
    quantity: int
