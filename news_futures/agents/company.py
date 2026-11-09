"""Company evidence and market snapshot contracts for the review workflow."""

from datetime import timedelta
from typing import Annotated, Any, Literal

from pydantic import AwareDatetime, Field

from news_futures.agents.contracts import Contract, Position, ProposalTerms, ReviewContext


Nonempty = Annotated[str, Field(min_length=1)]
MAX_SNAPSHOT_AGE = timedelta(minutes=5)


class Market(Contract):
    observed_at: AwareDatetime
    last_price: float = Field(gt=0)


class Account(Contract):
    id: Nonempty
    observed_at: AwareDatetime
    currency: Nonempty
    equity: float
    available_margin: float


class Instrument(Contract):
    company_id: Nonempty
    company_name: Nonempty
    share_id: Nonempty
    contract_id: Nonempty
    allowed_for_entry: bool
    market: Market


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


class CompanyContext(ReviewContext):
    event: dict[str, Any]
    affected_company_ids: list[str]
    candidates: list[Instrument]
    account: Account
    position: OpenPosition | None
    entry_reason: EntryReason | None
    related_news: list[dict[str, Any]]
    omitted_related_news_count: int


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
