"""Company evidence and market snapshot contracts for the review workflow."""

from datetime import timedelta
from typing import Annotated, Any, Literal

from pydantic import AwareDatetime, Field, model_validator

from news_futures.schemas.contracts import Contract, Position, ProposalTerms
from news_futures.dataflows.publications import publication_digest


Nonempty = Annotated[str, Field(min_length=1)]
ObservationId = Annotated[str, Field(pattern=r'^[0-9a-f]{64}$')]
MAX_SNAPSHOT_AGE = timedelta(minutes=5)


class BookLevel(Contract):
    price: float = Field(gt=0)
    quantity_lots: int = Field(gt=0)


class OrderBook(Contract):
    observed_at: AwareDatetime
    bid: BookLevel | None
    ask: BookLevel | None

    @model_validator(mode='after')
    def ordered_prices(self) -> 'OrderBook':
        if self.bid and self.ask and self.bid.price >= self.ask.price:
            raise ValueError('crossed_order_book')
        return self


class MinuteCandle(Contract):
    started_at: AwareDatetime
    close: float = Field(gt=0)
    volume_lots: int = Field(ge=0)


class Market(Contract):
    observed_at: AwareDatetime
    last_price: float = Field(gt=0)
    order_book: OrderBook | None = Field(default=None, exclude_if=lambda value: value is None)
    trading_status: Nonempty | None = Field(default=None, exclude_if=lambda value: value is None)
    trading_status_observed_at: AwareDatetime | None = Field(default=None, exclude_if=lambda value: value is None)


class MarketObservation(Contract):
    contract_id: Nonempty
    market: Market
    received_at: AwareDatetime
    price_unit: Literal['points']
    limit_buy_available: bool
    limit_sell_available: bool

    @model_validator(mode='after')
    def chronological(self) -> 'MarketObservation':
        if (self.market.observed_at > self.received_at
                or (self.market.order_book and self.market.order_book.observed_at > self.received_at)):
            raise ValueError('quote_observed_after_receipt')
        return self

    @property
    def id(self) -> str:
        return publication_digest(self.model_dump(mode='json'))


class AccountBalance(Contract):
    observed_at: AwareDatetime
    currency: Nonempty
    equity: float
    available_margin: float


class Account(AccountBalance):
    id: Nonempty


class Instrument(Contract):
    company_id: Nonempty
    company_name: Nonempty
    share_id: Nonempty
    contract_id: Nonempty
    allowed_for_entry: bool
    market: Market
    market_observation_id: ObservationId | None = Field(default=None, exclude_if=lambda value: value is None)


class OpenPosition(Position):
    id: Nonempty


class Snapshot(Contract):
    account: Account
    position: OpenPosition | None
    instruments: list[Instrument]


class EntrySource(Contract):
    revision: Nonempty
    url: Nonempty
    quote: Nonempty


class EntryReason(Contract):
    account_id: Nonempty
    position_id: Nonempty
    contract_id: Nonempty
    reason: Nonempty
    sources: list[EntrySource] = Field(default_factory=list)


class Evidence(Contract):
    revision: Nonempty
    quote: Nonempty


class CandidateAssessment(Contract):
    contract_id: Nonempty
    impact: Literal['verified', 'unverified']
    reason: Nonempty
    evidence: list[Evidence]


class LevelRationale(Contract):
    entry: Nonempty
    stop_loss: Nonempty
    take_profit: Nonempty


class CompanyProposal(ProposalTerms):
    company_id: Nonempty | None
    contract_id: Nonempty | None
    assessments: list[CandidateAssessment]
    level_rationale: LevelRationale | None


class SignalCandidate(Contract):
    revision: str = Field(pattern=r'^[0-9a-f]{64}$')
    event_decision_id: int = Field(gt=0)
    strategy: Literal['tool_agent']
    proposal_id: Nonempty
    proposal: CompanyProposal
    observation_id: ObservationId
    received_at: AwareDatetime
    decided_at: AwareDatetime
    deadline_at: AwareDatetime

    @model_validator(mode='after')
    def valid_signal(self) -> 'SignalCandidate':
        if self.proposal.action != 'enter' or self.proposal.contract_id is None or self.proposal.side is None:
            raise ValueError('entry_candidate_required')
        if not self.received_at <= self.decided_at < self.deadline_at:
            raise ValueError('invalid_candidate_timing')
        return self
