"""Pydantic contracts for model settings, proposals and reviews."""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Contract(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True, frozen=True,
                              str_strip_whitespace=True, allow_inf_nan=False)


Aggregator = Literal['openrouter', 'groq']


class RoleSettings(Contract):
    model: str = Field(min_length=3, pattern=r'^[^\s/:]+(?:/[^\s:]+)?(?::free)?$')
    aggregator: Aggregator = 'openrouter'
    max_tokens: int = Field(default=2048, gt=0)
    temperature: float | None = Field(default=None, ge=0, le=2)
    timeout_ms: int = Field(default=60000, gt=0)

    @model_validator(mode='after')
    def valid_model_id(self) -> 'RoleSettings':
        if self.aggregator == 'groq' and self.model.endswith(':free'):
            raise ValueError('invalid_groq_model_id')
        if self.aggregator == 'openrouter' and '/' not in self.model:
            raise ValueError('invalid_openrouter_model_id')
        return self


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


class Review(Contract):
    proposal_id: str
    approved: bool
    objections: list[str]
    reason: str = Field(min_length=1)


class Position(Contract):
    contract_id: str
    side: Literal['long', 'short']
    quantity: int = Field(gt=0)


def validate_levels(proposal: dict[str, Any]) -> None:
    check_levels(proposal['action'], proposal['side'], proposal['entry_price'],
                 proposal['stop_loss'], proposal['take_profit'])


def check_levels(action: str, side: str | None, entry: float | None,
                 stop: float | None, take: float | None) -> None:
    if action == 'enter':
        if side is None or entry is None or stop is None or take is None or min(entry, stop, take) <= 0:
            raise ValueError('missing_entry_levels')
        if not (stop < entry < take if side == 'long' else take < entry < stop):
            raise ValueError('invalid_protection_levels')
    elif any(value is not None for value in (side, entry, stop, take)):
        raise ValueError('unexpected_entry_levels')
