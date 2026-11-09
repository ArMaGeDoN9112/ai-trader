"""Scripted model responses with no network traffic."""

import json
from typing import Any

import httpx
from pydantic import SecretStr

from news_futures.adapters.openrouter import OpenRouterChat
from news_futures.agents.contracts import RoleSettings

class ScriptedEndpoint:
    def __init__(self, answers: list[Any], *, finish_reason: str = 'stop',
                 actual_model: str | None = None, status_code: int = 200,
                 refusal: str | None = None) -> None:
        self.answers = iter(answers)
        self.requests: list[dict[str, Any]] = []
        self.finish_reason = finish_reason
        self.actual_model = actual_model
        self.status_code = status_code
        self.refusal = refusal

    def model(self, settings: RoleSettings) -> OpenRouterChat:
        return OpenRouterChat(
            model=settings.model, api_key=SecretStr('local-test-secret'),
            http_client=httpx.Client(transport=httpx.MockTransport(self.respond)),
            base_url='https://openrouter.ai/api/v1', use_responses_api=False,
            max_retries=0, extra_body={'provider': {'require_parameters': True}},
        )

    def respond(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        self.requests.append(body)
        answer = next(self.answers)
        if isinstance(answer, Exception):
            raise answer
        if callable(answer):
            answer = answer(body)
        return httpx.Response(self.status_code, json={
            'id': 'gen-control', 'object': 'chat.completion', 'created': 1793955602,
            'model': self.actual_model or body['model'], 'provider': 'TestProvider', 'system_fingerprint': None,
            'choices': [{'index': 0, 'finish_reason': self.finish_reason,
                         'message': {'role': 'assistant', 'content':
                                     answer if isinstance(answer, str) else json.dumps(answer),
                                     'refusal': self.refusal}}],
            'usage': {'prompt_tokens': 20, 'completion_tokens': 30,
                      'total_tokens': 50, 'cost': 0.001},
        })


def review(approved: bool = True) -> Any:
    def answer(body: dict[str, Any]) -> dict[str, Any]:
        data = json.loads(body['messages'][-1]['content'])
        return {'proposal_id': data['proposal']['id'], 'approved': approved,
                'objections': [] if approved else ['Insufficient independent evidence.'],
                'reason': 'Checked against the original publication and context.'}
    return answer
