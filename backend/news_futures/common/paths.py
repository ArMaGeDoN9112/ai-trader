"""Packaged configuration paths."""

from pathlib import Path


PACKAGE = Path(__file__).resolve().parents[1]
CONTROL_CASE = PACKAGE / 'application/control.json'
SANDBOX_SETTINGS = PACKAGE / 'adapters/tbank/sandbox-settings.json'
TELEGRAM_SOURCES = PACKAGE / 'adapters/telegram-sources.json'
REVIEW_SETTINGS = PACKAGE / 'application/analysis/openrouter-models.json'
EVENT_SETTINGS = PACKAGE / 'application/analysis/event-model.json'
DECISIONS_SETTINGS = PACKAGE / 'application/analysis/decisions-model.json'
GROQ_SETTINGS = PACKAGE / 'application/analysis/groq-models.json'
FUTURES_CATEGORIES = PACKAGE / 'adapters/tbank/futures-categories.json'
