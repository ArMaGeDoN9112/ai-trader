"""Validated runtime profile and per-publication configuration inputs."""

from pathlib import Path
from typing import Literal

from pydantic import Field

from news_futures.agents.company import Nonempty
from news_futures.agents.contracts import Contract, RoleSettings, Settings
from news_futures.agents.routing import JevSettings, catalog_rows, load_catalog
from news_futures.common.errors import RuntimeErrorInfo
from news_futures.common.paths import EVENT_SETTINGS, JEV_SETTINGS, REVIEW_SETTINGS, TELEGRAM_SOURCES, config_path


class RuntimeSettings(Contract):
    data_dir: Path
    catalog: Path
    account_id: Nonempty
    contracts: dict[Nonempty, Nonempty] = Field(min_length=1)  # contract UID -> company ID
    mode: Literal['observation', 'sandbox'] = 'observation'
    sources: Path = TELEGRAM_SOURCES
    session: Path | None = None
    jev: Path = JEV_SETTINGS
    matcher: Path = EVENT_SETTINGS
    review: Path = REVIEW_SETTINGS
    ca_file: Path | None = None

    @property
    def publications(self) -> Path:
        return self.data_dir / 'publications.sqlite3'

    @property
    def events(self) -> Path:
        return self.data_dir / 'events.sqlite3'


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


def cycle_inputs(settings: RuntimeSettings) -> dict[str, bytes]:
    inputs = {}
    for field, validate in (
        ('catalog', load_catalog), ('jev', JevSettings.model_validate_json),
        ('matcher', RoleSettings.model_validate_json), ('review', Settings.model_validate_json),
    ):
        try:
            content = getattr(settings, field).read_bytes()
            validate(content)
        except (OSError, ValueError, KeyError, TypeError) as error:
            raise RuntimeErrorInfo(f'Проверьте файл настройки {field}.') from error
        inputs[field] = content
    if set(settings.contracts.values()) - {row.company_id for row in catalog_rows(inputs['catalog']) if row.share_id}:
        raise RuntimeErrorInfo('Сопоставьте contracts с компаниями из catalog.')
    return inputs
