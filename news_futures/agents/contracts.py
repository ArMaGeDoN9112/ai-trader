"""Pydantic contracts for model settings, proposals and reviews."""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from news_futures.common.artifacts import publication_digest


class Contract(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True, frozen=True,
                              str_strip_whitespace=True, allow_inf_nan=False)


class RoleSettings(Contract):
    model: str = Field(min_length=3, pattern=r'^[^\s/:]+/[^\s:]+$')
    max_tokens: int = Field(default=2048, gt=0)
    temperature: float | None = Field(default=None, ge=0, le=2)
    timeout_ms: int = Field(default=60000, gt=0)


class Settings(Contract):
    analyst: RoleSettings
    critic: RoleSettings


class ProposalTerms(Contract):
    action: Literal['enter', 'close', 'skip', 'hold']
    publication_id: str
    side: Literal['long', 'short'] | None
    entry_price: float | None
    stop_loss: float | None
    take_profit: float | None
    reason: str = Field(min_length=1)


class Proposal(ProposalTerms):
    company_id: str
    contract_id: str


class Review(Contract):
    proposal_id: str
    approved: bool
    objections: list[str]
    reason: str = Field(min_length=1)


class Position(Contract):
    contract_id: str
    side: Literal['long', 'short']
    quantity: int = Field(gt=0)


class ReviewContext(Contract):
    publication: dict[str, Any]


class AnalysisContext(ReviewContext):
    catalog: dict[str, Any]
    company_link: dict[str, Any]
    market: dict[str, Any]
    account: dict[str, Any]
    position: Position | None
    entry_reason: str | None

    @model_validator(mode='after')
    def consistent_references(self) -> 'AnalysisContext':
        try:
            company, share, future = (self.catalog[key] for key in ('company', 'share', 'future'))
            for key in ('id', 'external_id', 'source', 'url', 'text', 'published_at', 'received_at'):
                if not isinstance(self.publication[key], str) or not self.publication[key].strip():
                    raise ValueError('invalid_publication')
            identifiers = (company['id'], share['id'], future['id'])
            if (any(not isinstance(value, str) or not value.strip() for value in identifiers)
                    or len(set(identifiers)) != 3
                    or share['company_id'] != company['id']
                    or future['underlying_share_id'] != share['id']
                    or self.company_link['company_id'] != company['id']
                    or self.company_link['publication_id'] != self.publication['id']):
                raise ValueError('inconsistent_context_references')
            if ('publication_revision_sha256' in self.company_link
                    and self.company_link['publication_revision_sha256'] != publication_digest(self.publication)):
                raise ValueError('company_link_publication_revision_mismatch')
            if self.position and (self.position.contract_id != future['id']
                                  or not self.entry_reason or not self.entry_reason.strip()):
                raise ValueError('invalid_position_context')
        except (KeyError, TypeError) as error:
            raise ValueError('incomplete_context') from error
        return self


def validate_levels(proposal: dict[str, Any]) -> None:
    side, entry, stop, take = (proposal[key] for key in (
        'side', 'entry_price', 'stop_loss', 'take_profit'))
    if proposal['action'] == 'enter':
        if side is None or any(price is None or price <= 0 for price in (entry, stop, take)):
            raise ValueError('missing_entry_levels')
        if not (stop < entry < take if side == 'long' else take < entry < stop):
            raise ValueError('invalid_protection_levels')
    elif any(value is not None for value in (side, entry, stop, take)):
        raise ValueError('unexpected_entry_levels')
