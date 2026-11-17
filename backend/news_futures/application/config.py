"""Validated runtime profile and per-publication configuration inputs."""

import json
from pathlib import Path
from typing import Literal

from pydantic import Field

from news_futures.application.catalog import load_catalog
from news_futures.application.analysis.routing import DecisionsSettings
from news_futures.application.analysis.tool_agent import AgentLimits
from news_futures.common.errors import RuntimeErrorInfo
from news_futures.common.paths import DECISIONS_SETTINGS, FUTURES_CATEGORIES, GROQ_SETTINGS, REVIEW_SETTINGS, TELEGRAM_SOURCES
from news_futures.domain.company import Nonempty
from news_futures.domain.contracts import Aggregator, Contract, RoleSettings, Settings


CHAT_ROLES = ('analyst', 'critic')


class RuntimeSettings(Contract):
    data_dir: Path
    catalog: Path
    account_id: Nonempty
    mode: Literal['observation', 'sandbox'] = 'observation'
    analysis_strategy: Literal['fixed', 'tool_agent'] = 'fixed'
    order_transport: Literal['rest', 'mcp'] = 'rest'
    chat_aggregator: Aggregator = 'openrouter'
    coordinator: RoleSettings | None = None
    sources: Path = TELEGRAM_SOURCES
    session: Path | None = None
    decisions: Path = DECISIONS_SETTINGS
    review: Path = REVIEW_SETTINGS
    groq: Path = GROQ_SETTINGS
    futures_categories: Path = FUTURES_CATEGORIES
    agent_limits: AgentLimits = AgentLimits()
    futures_refresh_seconds: int = Field(default=86400, gt=0)
    broker_refresh_seconds: int = Field(default=15, gt=0)
    ca_file: Path | None = None

    @property
    def publications(self) -> Path:
        return self.data_dir / 'publications'

    @property
    def events(self) -> Path:
        return self.data_dir / 'events'

    @property
    def futures_catalog(self) -> Path:
        return self.data_dir / 'futures-catalog' / 'catalog.json'


def load_settings(config: Path) -> RuntimeSettings:
    try:
        settings = RuntimeSettings.model_validate_json(config.read_bytes())
    except (OSError, ValueError) as error:
        raise RuntimeErrorInfo('Проверьте путь и поля общей конфигурации.') from error
    paths = {
        field: (config.parent / value).resolve()
        for field in RuntimeSettings.model_fields
        if isinstance(value := getattr(settings, field), Path)
    }
    return settings.model_copy(update=paths)


def role_models(inputs: dict[str, bytes]) -> dict[str, str]:
    review = json.loads(inputs['review'])
    return {'decisions': json.loads(inputs['decisions'])['model'],
            **{role: review[role]['model'] for role in CHAT_ROLES}}


def groq_models(settings: RuntimeSettings) -> dict[str, str]:
    try:
        models = json.loads(settings.groq.read_bytes())
        if not isinstance(models, dict) or models.keys() != set(CHAT_ROLES):
            raise ValueError('invalid_groq_models')
        for model in models.values():
            RoleSettings.model_validate({'model': model, 'aggregator': 'groq'})
        return {role: models[role] for role in CHAT_ROLES}
    except (OSError, ValueError) as error:
        raise RuntimeErrorInfo('Проверьте файл настройки groq.') from error


def cycle_inputs(settings: RuntimeSettings, models: dict[str, str] | None = None, *,
                 aggregator: Aggregator | None = None) -> dict[str, bytes]:
    inputs = {}
    for field, validate in (
        ('catalog', load_catalog), ('decisions', DecisionsSettings.model_validate_json),
        ('review', Settings.model_validate_json),
    ):
        try:
            content = getattr(settings, field).read_bytes()
            validate(content)
        except (OSError, ValueError, KeyError, TypeError) as error:
            raise RuntimeErrorInfo(f'Проверьте файл настройки {field}.') from error
        inputs[field] = content
    if models is None and aggregator is None:
        return inputs
    options = json.loads(inputs['review'])
    for role in CHAT_ROLES:
        options[role]['model'] = (models or {}).get(role, options[role]['model'])
        if aggregator is not None:
            options[role]['aggregator'] = aggregator
    Settings.model_validate(options)
    inputs['review'] = json.dumps(options).encode()
    return inputs
