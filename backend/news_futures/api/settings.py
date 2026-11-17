"""Validate and apply local operator profile edits."""

from collections.abc import Iterator
from contextlib import contextmanager
import json
from typing import Any

from news_futures.credentials import load_token
from news_futures.api.news import read_archive
from news_futures.storage.database import has_table
from news_futures.storage.locks import exclusive_operation
from news_futures.storage.operator_state import OperatorState
from news_futures.storage.trades import active_cycles
from news_futures.brokers.tbank import TBankReader, sandbox_client
from news_futures.dataflows.telegram import validate_channels
from news_futures.config import CHAT_ROLES, RuntimeSettings, cycle_inputs, groq_models, role_models
from news_futures.trading.executor import AUTONOMOUS_PROTECTION_UNAVAILABLE
from news_futures.common.errors import CredentialsError
from news_futures.dataflows.publications import utc_now


def profile_view(settings: RuntimeSettings) -> dict[str, Any]:
    return settings.model_dump(include={'mode', 'account_id', 'analysis_strategy'}) | {
        'entry_block': AUTONOMOUS_PROTECTION_UNAVAILABLE if settings.mode == 'sandbox' else None,
    }


class Preferences:
    def __init__(self, settings: RuntimeSettings, channels: list[str], state: OperatorState):
        self.base = settings
        self.state = state
        models = role_models(cycle_inputs(settings))
        self.decisions_model = models.pop('decisions')
        self.default: dict[str, Any] = {'mode': settings.mode, 'channels': channels,
                                       'analysis_strategy': settings.analysis_strategy,
                                       'models': models, 'aggregator': settings.chat_aggregator,
                                       'groq_models': groq_models(settings)}
        self.active = self.initial_active(self.saved())
        self.profile_at: str | None = None
        self.models_at: str | None = None

    def initial_active(self, values: dict[str, Any]) -> dict[str, Any]:
        key = 'groq_models' if values['aggregator'] == 'groq' else 'models'
        return values | {'models': values[key] | {'decisions': self.decisions_model}}

    def saved(self, values: dict[str, Any] | None = None) -> dict[str, Any]:
        if values is None:
            values = self.state.saved()
        if values.get('account_id') != self.base.account_id:
            values = {}
        saved = {key: values.get(key, default) for key, default in self.default.items()}
        for key in ('models', 'groq_models'):
            saved[key] = {role: saved[key].get(role, model) for role, model in self.default[key].items()}
        return saved

    def view(self) -> dict[str, Any]:
        stored = self.state.saved()
        changed_at = stored.get('changed_at') if stored.get('account_id') == self.base.account_id else None
        return {'saved': self.saved(stored), 'active': self.active, 'changed_at': changed_at,
                'models_at': self.models_at, 'profile_at': self.profile_at}

    def runtime(self, values: dict[str, Any]) -> RuntimeSettings:
        return self.base.model_copy(update={key: values[key] for key in ('mode', 'analysis_strategy')})

    def prepare(self) -> tuple[dict[str, Any], list[str], bytes]:
        values = self.saved()
        runtime = self.runtime(values)
        self.active = self.initial_active(values)
        self.models_at = None
        self.profile_at = None
        return profile_view(runtime), values['channels'], json.dumps({
            'settings': runtime.model_dump(mode='json'), 'channels': values['channels'],
        }).encode()

    @contextmanager
    def exclusive_profile(self) -> Iterator[None]:
        with exclusive_operation(self.base.data_dir / 'runtime-lock') as runtime:
            if not runtime:
                raise ValueError('account_unconfirmed')
            with exclusive_operation(self.base.events, 'sandbox') as execution:
                if not execution:
                    raise ValueError('account_unconfirmed')
                yield

    def require_flat(self, settings: RuntimeSettings) -> None:
        with read_archive(settings.events) as db:
            if db is not None:
                if active_cycles(db, settings.account_id):
                    raise ValueError('account_has_operations')
                if has_table(db, 'entry_reservations') and db.execute(
                    'SELECT 1 FROM entry_reservations WHERE account_id = %s', (settings.account_id,),
                ).fetchone():
                    raise ValueError('account_has_operations')
        try:
            with sandbox_client(settings.ca_file, read_only=True) as client:
                position, orders = TBankReader(client, settings.account_id, '', None).account_activity()
        except Exception as error:
            raise ValueError('account_unconfirmed') from error
        if position or orders:
            raise ValueError('account_has_position' if position else 'account_has_operations')

    def save(self, body: dict[str, Any], *, stopped: bool) -> None:
        stored = self.state.saved()
        saved = self.saved(stored)
        if body.get('base') != saved:
            raise ValueError('settings_conflict')
        mode = body.get('mode')
        if mode not in ('sandbox', 'observation'):
            raise ValueError('invalid_mode')
        strategy = body.get('analysis_strategy', saved['analysis_strategy'])
        if strategy not in ('fixed', 'tool_agent'):
            raise ValueError('invalid_analysis_strategy')
        try:
            channels = validate_channels(body.get('channels'))
        except ValueError as error:
            raise ValueError('invalid_channels') from error
        aggregator = body.get('aggregator', saved['aggregator'])
        if aggregator not in ('openrouter', 'groq'):
            raise ValueError('invalid_aggregator')
        models = body.get('models')
        groq_models = body.get('groq_models', saved['groq_models'])
        for choices in (models, groq_models):
            if (not isinstance(choices, dict) or choices.keys() != set(CHAT_ROLES)
                    or not all(isinstance(model, str) and len(model) <= 200 for model in choices.values())):
                raise ValueError('invalid_models')
        values = {'mode': mode, 'analysis_strategy': strategy, 'channels': channels, 'models': models,
                  'aggregator': aggregator, 'groq_models': groq_models, 'account_id': self.base.account_id}
        candidate = self.runtime(values)
        try:
            cycle_inputs(candidate, models, aggregator='openrouter')
            cycle_inputs(candidate, groq_models, aggregator='groq')
        except ValueError as error:
            raise ValueError('invalid_models') from error
        if aggregator == 'groq' and aggregator != saved['aggregator']:
            try:
                load_token('groq', 'api_key')
            except CredentialsError as error:
                raise ValueError('invalid_groq_credentials') from error
        mode_changed = mode != saved['mode']
        if mode_changed and not stopped:
            raise ValueError('mode_requires_stop')
        if mode_changed:
            # Changing the execution profile requires a broker check with no concurrent local executor.
            with self.exclusive_profile():
                self.require_flat(self.runtime(self.active))
                self.state.save_settings(values, expected=stored)
        else:
            self.state.save_settings(values, expected=stored)
        if mode_changed:
            self.active = self.active | {'mode': mode, 'analysis_strategy': strategy}
            self.profile_at = utc_now().isoformat()
