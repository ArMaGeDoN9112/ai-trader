"""Data transferred between agent roles, tools and the LangGraph checkpoint."""

from datetime import date, datetime
from typing import Any, Literal

from langchain_core.messages import BaseMessage, messages_from_dict, messages_to_dict
from langchain_core.messages.ai import UsageMetadata
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, field_serializer, field_validator

from news_futures.agents.events import EventAssessment, RoutedLink
from news_futures.agents.calendar import ScheduledEvent
from news_futures.schemas.company import AccountBalance, CompanyProposal, Instrument
from news_futures.schemas.contracts import Contract, Review


class DTO(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True, allow_inf_nan=False)


class PublicationEvidence(DTO):
    id: str
    source: str
    url: str
    text: str
    published_at: str
    edited_at: str | None
    unread_media: bool | None
    change: str | None


class EventCorrection(DTO):
    reason: str
    supersedes: int


class EventSummary(DTO):
    decision_id: int
    revision: str
    event_id: str
    kind: str
    basis_at: str | None
    assessment: EventAssessment | None
    links: list[RoutedLink]
    correction: EventCorrection | None


class EventEvidence(EventSummary):
    publication: PublicationEvidence

    def summary(self) -> EventSummary:
        return EventSummary.model_validate(self.model_dump(exclude={'publication'}))


class CoordinatorInput(DTO):
    publication: PublicationEvidence
    event: EventSummary
    contracts: dict[str, str]


class AnalystTask(DTO):
    publication: PublicationEvidence
    event: EventSummary
    company_id: str
    question: str
    contracts: list[str]


class Assignment(Contract):
    status: Literal['delegate', 'skip', 'insufficient_data']
    reason: str = Field(min_length=1, max_length=500)
    company_id: str | None


class AnalystAnswer(Contract):
    proposal: CompanyProposal | None
    missing_data: list[str]


class get_company_news(Contract):
    """Read earlier publications about an allowed company from the local archive."""

    company_id: str


class get_market_context(Contract):
    """Read the quote, top of book and order availability for the selected contract."""

    contract_id: str


class get_market_reaction(Contract):
    """Measure price and volume after publication and receipt using completed minute candles."""

    contract_id: str


class get_event_versions(Contract):
    """Read the archived versions, changed text and explicitly cited links for the triggering event."""


class compare_market(Contract):
    """Compare the selected future with its stock and benchmark on matching completed minutes."""

    contract_id: str


class get_upcoming_events(Contract):
    """Read scheduled disclosures and macroeconomic events for the assigned company and market."""


class ToolRequest(DTO):
    name: str
    args: dict[str, JsonValue]
    id: str


class Observation(DTO):
    company_id: str
    observation_id: str


class NewsObservation(Observation):
    news: list[EventEvidence]


class MarketObservation(Observation):
    instrument: Instrument
    account: AccountBalance
    price_unit: Literal['points']
    limit_buy_available: bool
    limit_sell_available: bool
    spread_points: float | None = None


class ReactionWindow(DTO):
    anchor_at: AwareDatetime
    baseline_close_at: AwareDatetime | None
    latest_close_at: AwareDatetime | None
    price_change_bps: float | None
    baseline_candles: int
    reaction_candles: int
    baseline_avg_lots: float | None
    reaction_volume_lots: int | None
    reaction_avg_lots: float | None


class ReactionObservation(Observation):
    contract_id: str
    as_of: AwareDatetime
    since_publication: ReactionWindow
    since_receipt: ReactionWindow


class EventVersion(DTO):
    decision_id: int
    revision: str
    event_id: str
    publication_url: str
    published_at: str
    edited_at: str | None
    kind: str
    cited_urls: list[str]


class EventVersionsObservation(Observation):
    earliest_archived: EventVersion
    versions: list[EventVersion]
    compared_to: EventVersion | None
    added_text: list[str]
    removed_text: list[str]
    excerpts_truncated: bool


class ComparisonWindow(DTO):
    anchor_at: AwareDatetime
    baseline_close_at: AwareDatetime | None
    latest_close_at: AwareDatetime | None
    change_bps: dict[str, float]
    future_minus_reference_bps: dict[str, float]


class ComparisonObservation(Observation):
    instruments: dict[str, str]
    missing_instruments: list[str]
    as_of: AwareDatetime
    since_publication: ComparisonWindow
    since_receipt: ComparisonWindow


class CalendarObservation(Observation):
    checked_on: date
    through: AwareDatetime
    issuer_covered: bool
    events: list[ScheduledEvent]


ExtraEvidence = EventVersionsObservation | ComparisonObservation | CalendarObservation
ToolObservation = NewsObservation | MarketObservation | ReactionObservation | ExtraEvidence


class ProposalEvidence(DTO):
    publication: PublicationEvidence
    event: EventSummary
    candidates: list[Instrument]
    related_news: list[EventEvidence]


class ProposalContext(ProposalEvidence):
    affected_company_ids: list[str]
    account: AccountBalance | None
    position: None
    entry_reason: None
    market_reaction: ReactionObservation | None = Field(default=None, exclude_if=lambda value: value is None)
    extra_evidence: list[ExtraEvidence] = Field(default_factory=list, exclude_if=lambda value: not value)


class IdentifiedProposal(CompanyProposal):
    id: str


class CriticTask(DTO):
    context: ProposalContext
    proposal: IdentifiedProposal


class RevisionFeedback(DTO):
    proposal: IdentifiedProposal
    review: Review


class AnalystInput(DTO):
    context: ProposalContext
    proposal: IdentifiedProposal | None
    review: Review | None


class ToolRequested(DTO):
    tool: str


class MissingData(DTO):
    missing_data: list[str]


class SkippedStep(DTO):
    reason: str


class ToolError(DTO):
    error: str


OutcomeStatus = Literal['skipped', 'insufficient_data', 'candidate', 'rejected', 'failed']


class AgentOutcome(DTO):
    status: OutcomeStatus
    reason: str
    proposal: IdentifiedProposal | None = None
    critic_status: Literal['approved', 'rejected'] | None = None
    observation_id: str | None = None


class AgentCall(DTO):
    id: str
    kind: str
    started_at: str
    status: Literal['completed', 'skipped', 'failed', 'cached']
    at: str | None = None
    latency_ms: int | None = None
    input: CoordinatorInput | AnalystInput | CriticTask | None = None
    output: (Assignment | IdentifiedProposal | CompanyProposal | Review | ToolRequested
             | MissingData | SkippedStep | ToolObservation | None) = None
    handoff_to: Literal['analyst', 'critic'] | None = None
    requested_model: str | None = None
    actual_model: str | None = None
    provider: str | None = None
    usage: UsageMetadata | dict[str, JsonValue] | None = None
    cost: JsonValue = None
    tool: str | None = None
    tool_call_id: str | None = None
    arguments: dict[str, JsonValue] | None = None
    error: str | None = None


class AgentTrace(DTO):
    revision: str
    strategy: Literal['tool_agent']
    version: str
    account_id: str
    outcome: AgentOutcome | None
    calls: list[AgentCall]


class AgentState(DTO):
    """State of one publication analysis."""

    deadline: datetime
    company_id: str | None = None
    messages: list[BaseMessage] = Field(default_factory=list)
    calls: list[AgentCall] = Field(default_factory=list)
    observations: dict[str, ToolObservation] = Field(default_factory=dict)
    proposal: IdentifiedProposal | None = None
    context: ProposalContext | None = None
    review: Review | None = None
    turns: int = 0
    reads: int = 0
    reviews: int = 0
    input_used: int = 0
    output_used: int = 0
    pending: ToolRequest | None = None
    outcome: AgentOutcome | None = None

    @field_validator('messages', mode='before')
    @classmethod
    def restore_messages(cls, value: list[Any]) -> list[BaseMessage]:
        return messages_from_dict(value)

    @field_serializer('messages')
    def store_messages(self, value: list[BaseMessage]) -> list[dict[str, Any]]:
        return messages_to_dict(value)
