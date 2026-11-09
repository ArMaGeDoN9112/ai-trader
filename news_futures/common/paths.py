"""Packaged defaults and compatibility for profiles that name their old paths."""

from pathlib import Path


PACKAGE = Path(__file__).resolve().parents[1]
CONTROL_CASE = PACKAGE / 'simulation/control.json'
SANDBOX_SETTINGS = PACKAGE / 'adapters/tbank/sandbox-settings.json'
TELEGRAM_SOURCES = PACKAGE / 'adapters/telegram-sources.json'
REVIEW_SETTINGS = PACKAGE / 'agents/openrouter-models.json'
EVENT_SETTINGS = PACKAGE / 'agents/event-model.json'
JEV_SETTINGS = PACKAGE / 'agents/jev-model.json'

_MOVED_DEFAULTS = {
    PACKAGE / path.name: path
    for path in (SANDBOX_SETTINGS, TELEGRAM_SOURCES, REVIEW_SETTINGS, EVENT_SETTINGS, JEV_SETTINGS)
}


def config_path(value: str | Path) -> Path:
    path = Path(value)
    replacement = _MOVED_DEFAULTS.get(path.resolve())
    return replacement if replacement is not None and not path.exists() else path
