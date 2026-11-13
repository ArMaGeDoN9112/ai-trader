"""Packaged configuration paths."""

from pathlib import Path


PACKAGE = Path(__file__).resolve().parents[1]
CONTROL_CASE = PACKAGE / 'simulation/control.json'
SANDBOX_SETTINGS = PACKAGE / 'adapters/tbank/sandbox-settings.json'
TELEGRAM_SOURCES = PACKAGE / 'adapters/telegram-sources.json'
REVIEW_SETTINGS = PACKAGE / 'agents/openrouter-models.json'
EVENT_SETTINGS = PACKAGE / 'agents/event-model.json'
DECISIONS_SETTINGS = PACKAGE / 'agents/decisions-model.json'
GROQ_SETTINGS = PACKAGE / 'agents/groq-models.json'
FUTURES_CATEGORIES = PACKAGE / 'adapters/tbank/futures-categories.json'
