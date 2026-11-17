"""Packaged configuration paths."""

import os
from pathlib import Path, PurePosixPath


PACKAGE = Path(__file__).resolve().parents[1]
CONTROL_CASE = PACKAGE / 'defaults/control.json'
SANDBOX_SETTINGS = PACKAGE / 'defaults/sandbox-settings.json'
TELEGRAM_SOURCES = PACKAGE / 'defaults/telegram-sources.json'
REVIEW_SETTINGS = PACKAGE / 'defaults/openrouter-models.json'
EVENT_SETTINGS = PACKAGE / 'defaults/event-model.json'
DECISIONS_SETTINGS = PACKAGE / 'defaults/decisions-model.json'
GROQ_SETTINGS = PACKAGE / 'defaults/groq-models.json'
FUTURES_CATEGORIES = PACKAGE / 'defaults/futures-categories.json'


def persistent_path(path: Path) -> str:
    resolved = path.resolve()
    container_root = os.environ.get('TRADER_CONTAINER_LOCAL')
    workspace = (PACKAGE.parents[1] / '.local').resolve()
    if container_root and resolved.is_relative_to(workspace):
        return str(PurePosixPath(container_root).joinpath(*resolved.relative_to(workspace).parts))
    return str(resolved)


def local_path(path: str) -> Path:
    container_root = os.environ.get('TRADER_CONTAINER_LOCAL')
    if container_root and (stored := PurePosixPath(path)).is_relative_to(container_root):
        return PACKAGE.parents[1] / '.local' / Path(*stored.relative_to(container_root).parts)
    return Path(path)
