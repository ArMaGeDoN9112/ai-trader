"""Configure chat model clients for the application analysis roles."""

from pydantic import SecretStr

from news_futures.adapters.credentials import load_token
from news_futures.adapters.models.groq import GroqChat
from news_futures.adapters.models.openrouter import OpenRouterChat
from news_futures.domain.contracts import RoleSettings


def chat_model(settings: RoleSettings) -> GroqChat | OpenRouterChat:
    groq = settings.aggregator == 'groq'
    client = GroqChat if groq else OpenRouterChat
    return client(
        model=settings.model, max_completion_tokens=settings.max_tokens,
        api_key=SecretStr(load_token(settings.aggregator, 'api_key')),
        temperature=settings.temperature, timeout=settings.timeout_ms / 1000,
        max_retries=0, base_url='https://api.groq.com/openai/v1' if groq else 'https://openrouter.ai/api/v1',
        use_responses_api=False, extra_body=None if groq else {'provider': {'require_parameters': True}},
    )
