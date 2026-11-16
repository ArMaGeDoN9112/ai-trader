"""Model interfaces shared by routing, event analysis and agent orchestration."""

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from math import isclose
from typing import Annotated, Any, Literal, Protocol

from langchain_core.language_models import BaseChatModel
from pydantic import BaseModel, ConfigDict, Field, ValidationError, ValidatorFunctionWrapHandler
from pydantic import field_validator, model_validator

from news_futures.domain.contracts import RoleSettings


ChatModel = Callable[[RoleSettings], BaseChatModel]
Nonempty = Annotated[str, Field(min_length=1)]
Probability = Annotated[float, Field(ge=0, le=1)]


def optional_metadata(value: object, validate: ValidatorFunctionWrapHandler) -> Any:
    try:
        return validate(value)
    except ValidationError:
        return None


class Input(BaseModel):
    model_config = ConfigDict(strict=True, str_strip_whitespace=True)


class Choice(Input):
    model_config = ConfigDict(extra='forbid')
    type: Literal['choice']
    choice: str
    probabilities: dict[str, Probability]
    confidence: Probability

    @model_validator(mode='after')
    def valid_distribution(self) -> 'Choice':
        if (self.choice not in self.probabilities
                or not isclose(sum(self.probabilities.values()), 1.0, abs_tol=0.01)
                or self.probabilities[self.choice] < max(self.probabilities.values())):
            raise ValueError('invalid_choice_distribution')
        return self


class Usage(Input):
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    cost: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    _optional = field_validator('input_tokens', 'output_tokens', 'cost', mode='wrap')(optional_metadata)


class DecisionMetadata(Input):
    model: Nonempty | None = None
    id: Nonempty | None = None
    provider: Nonempty | None = None
    usage: Usage | None = None
    _optional = field_validator('model', 'id', 'provider', 'usage', mode='wrap')(optional_metadata)


@dataclass(frozen=True)
class BatchDecision:
    metadata: DecisionMetadata | None = None
    answers: dict[str, Choice] = field(default_factory=dict)
    error: str | None = None


class Choose(Protocol):
    def __call__(
        self, model: str, questions: dict[str, Any], state: dict[str, Any], keys: dict[str, str], *,
        timeout: float,
    ) -> BatchDecision: ...


Decisions = Callable[[], AbstractContextManager[Choose]]


@dataclass(frozen=True)
class AnalysisModels:
    chat: ChatModel
    decisions: Decisions
