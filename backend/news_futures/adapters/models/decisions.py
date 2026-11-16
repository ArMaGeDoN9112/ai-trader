"""OpenRouter Decisions transport and response validation."""

from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from functools import partial
from typing import Any

import httpx
from pydantic import TypeAdapter

from news_futures.adapters.credentials import load_token
from news_futures.application.ports.models import BatchDecision, Choice, Choose, DecisionMetadata


DECISIONS_URL = 'https://openrouter.ai/api/alpha/decisions'


def request_choices(client: httpx.Client, token: str, model: str, questions: dict[str, Any],
                    state: dict[str, Any], keys: dict[str, str], *, timeout: float) -> BatchDecision:
    metadata = None
    try:
        response = client.post(
            DECISIONS_URL, headers={'Authorization': f'Bearer {token}'}, timeout=timeout,
            json={'model': model, 'questions': questions, 'state': state},
        )
        response.raise_for_status()
        payload = response.json()
        metadata = DecisionMetadata.model_validate(payload)
        metadata = metadata.model_copy(update={
            key: value.replace(token, '[redacted]') if value else None
            for key, value in (('model', metadata.model), ('id', metadata.id), ('provider', metadata.provider))
        })
        answers = TypeAdapter(dict[str, Choice]).validate_python(payload.get('answers'), strict=True)
        if (answers.keys() != keys.keys() or payload.get('error') is not None
                or not metadata.id or not metadata.model
                or any(answer.probabilities.keys() != questions[key]['criteria'].keys()
                       for key, answer in answers.items())):
            raise ValueError('invalid_decisions_response')
        return BatchDecision(metadata, {company_id: answers[key] for key, company_id in keys.items()})
    except httpx.HTTPStatusError as exc:
        return BatchDecision(error=f'http_{exc.response.status_code}')
    except httpx.RequestError:
        return BatchDecision(error='service_unavailable')
    except ValueError:
        return BatchDecision(metadata, error='invalid_decision')


@contextmanager
def choices(client: httpx.Client | None = None, token: str | None = None) -> Iterator[Choose]:
    key = token if token is not None else load_token('openrouter', 'api_key')
    with httpx.Client() if client is None else nullcontext(client) as transport:
        yield partial(request_choices, transport, key)
