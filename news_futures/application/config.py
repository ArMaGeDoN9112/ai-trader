"""Validated runtime profile and per-publication configuration inputs."""

import json
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from news_futures.agents.company import Nonempty
from news_futures.agents.contracts import Aggregator, Contract, RoleSettings, Settings
from news_futures.agents.routing import JevSettings, catalog_rows, load_catalog
from news_futures.common.errors import RuntimeErrorInfo
from news_futures.common.paths import EVENT_SETTINGS, JEV_SETTINGS, REVIEW_SETTINGS, TELEGRAM_SOURCES, config_path


GROQ_MODELS = {'analyst': 'openai/gpt-oss-120b', 'critic': 'openai/gpt-oss-120b'}


class RuntimeSettings(Contract):
    data_dir: Path
    catalog: Path
    account_id: Nonempty
    contracts: dict[Nonempty, Nonempty] = Field(min_length=1)  # contract UID -> company ID
    mode: Literal['observation', 'sandbox'] = 'observation'
    analysis_strategy: Literal['fixed', 'tool_agent'] = 'fixed'
    order_transport: Literal['rest', 'mcp'] = 'rest'
    coordinator: RoleSettings | None = None
    sources: Path = TELEGRAM_SOURCES
    session: Path | None = None
    jev: Path = JEV_SETTINGS
    matcher: Path = EVENT_SETTINGS
    review: Path = REVIEW_SETTINGS
    ca_file: Path | None = None

    @model_validator(mode='after')
    def analysis_permissions(self) -> 'RuntimeSettings':
        if self.analysis_strategy == 'tool_agent' and self.mode != 'observation':
            raise ValueError('tool_agent_requires_observation')
        return self

    @property
    def publications(self) -> Path:
        return self.data_dir / 'publications.sqlite3'

    @property
    def events(self) -> Path:
        return self.data_dir / 'events.sqlite3'

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
    for field in ('sources', 'jev', 'matcher', 'review'):
        paths[field] = config_path(paths[field])
    return settings.model_copy(update=paths)


def role_models(inputs: dict[str, bytes]) -> dict[str, str]:
    review = json.loads(inputs['review'])
    return {'jev': json.loads(inputs['jev'])['model'],
            **{role: review[role]['model'] for role in ('analyst', 'critic')}}


def cycle_inputs(settings: RuntimeSettings, models: dict[str, str] | None = None, *,
                 aggregator: Aggregator | None = None) -> dict[str, bytes]:
    inputs = {}
    for field, validate in (
        ('catalog', load_catalog), ('jev', JevSettings.model_validate_json),
        ('review', Settings.model_validate_json),
    ):
        try:
            content = getattr(settings, field).read_bytes()
            validate(content)
        except (OSError, ValueError, KeyError, TypeError) as error:
            raise RuntimeErrorInfo(f'Проверьте файл настройки {field}.') from error
        inputs[field] = content
    if set(settings.contracts.values()) - {row.company_id for row in catalog_rows(inputs['catalog']) if row.share_id}:
        raise RuntimeErrorInfo('Сопоставьте contracts с компаниями из catalog.')
    if models is None and aggregator is None:
        return inputs
    models = models or {}
    aggregator = aggregator or 'openrouter'
    for field in ('jev', 'review'):
        options = json.loads(inputs[field])
        if field == 'review':
            for role in ('analyst', 'critic'):
                options[role].update(model=models.get(role, options[role]['model']), aggregator=aggregator)
            Settings.model_validate(options)
        else:
            options['model'] = models.get(field, options['model'])
            JevSettings.model_validate(options)
        inputs[field] = json.dumps(options).encode()
    return inputs
