"""Validate and apply local operator profile edits."""

from collections.abc import Iterator
from contextlib import closing, contextmanager
import json
import sqlite3
from typing import Any

from news_futures.adapters.tbank.rest import TBankReader, sandbox_client
from news_futures.adapters.telegram import validate_channels
from news_futures.application.config import RuntimeSettings, cycle_inputs
from news_futures.application.operator_state import OperatorState
from news_futures.common.errors import RuntimeErrorInfo
from news_futures.news.publications import utc_now
from news_futures.operator.news import read_archive
from news_futures.trading.account import BrokerAccount
from news_futures.trading.executor import AUTONOMOUS_PROTECTION_UNAVAILABLE
from news_futures.trading.journal import active_cycles


def role_models(inputs: dict[str, bytes]) -> dict[str, str]:
    review = json.loads(inputs['review'])
    return {'jev': json.loads(inputs['jev'])['model'], 'matcher': json.loads(inputs['matcher'])['model'],
            **{role: review[role]['model'] for role in ('analyst', 'critic')}}


def profile_view(settings: RuntimeSettings) -> dict[str, Any]:
    return settings.model_dump(include={'mode', 'account_id', 'contracts'}) | {
        'entry_block': AUTONOMOUS_PROTECTION_UNAVAILABLE if settings.mode == 'sandbox' else None,
    }


class Preferences:
    def __init__(self, settings: RuntimeSettings, channels: list[str], state: OperatorState):
        self.base = settings
        self.state = state
        self.default: dict[str, Any] = {'mode': settings.mode, 'channels': channels, 'contracts': settings.contracts,
                                       'models': role_models(cycle_inputs(settings))}
        self.active = self.saved()
        self.active['models'] = self.default['models']
        self.profile_at: str | None = None
        self.models_at: str | None = None

    def saved(self, values: dict[str, Any] | None = None) -> dict[str, Any]:
        if values is None:
            values = self.state.saved()
        if values.get('account_id') != self.base.account_id:
            values = {}
        return {key: values.get(key, default) for key, default in self.default.items()}

    def view(self) -> dict[str, Any]:
        stored = self.state.saved()
        changed_at = stored.get('changed_at') if stored.get('account_id') == self.base.account_id else None
        return {'saved': self.saved(stored), 'active': self.active, 'changed_at': changed_at,
                'models_at': self.models_at, 'profile_at': self.profile_at}

    def runtime(self, values: dict[str, Any]) -> RuntimeSettings:
        return self.base.model_copy(update={key: values[key] for key in ('mode', 'contracts')})

    def prepare(self, *, check_cycles: bool = True) -> tuple[dict[str, Any], list[str], bytes]:
        values = self.saved()
        runtime = self.runtime(values)
        if check_cycles:
            with read_archive(runtime.events) as db:
                if db is not None and any(json.loads(row['terms'])['contract_id'] not in runtime.contracts
                                          for row in active_cycles(db, runtime.account_id)):
                    raise ValueError('account_has_operations')
        self.active = values | {'models': self.default['models']}
        self.models_at = None
        self.profile_at = None
        return profile_view(runtime), values['channels'], json.dumps({
            'settings': runtime.model_dump(mode='json'), 'channels': values['channels'],
        }).encode()

    @contextmanager
    def exclusive_profile(self, *, stopped: bool) -> Iterator[None]:
        self.base.data_dir.mkdir(parents=True, exist_ok=True)
        with (closing(sqlite3.connect(self.base.data_dir / 'runtime-lock.sqlite3', timeout=0)) as runtime,
              closing(sqlite3.connect(str(self.base.events) + '.sandbox-lock', timeout=0)) as execution):
            for connection in ((runtime, execution) if stopped else (execution,)):
                try:
                    connection.execute('BEGIN IMMEDIATE')
                except sqlite3.OperationalError as error:
                    raise ValueError('account_unconfirmed') from error
            yield

    def require_flat(self, settings: RuntimeSettings) -> None:
        with read_archive(settings.events) as db:
            if db is not None:
                if active_cycles(db, settings.account_id):
                    raise ValueError('account_has_operations')
                if db.execute("SELECT 1 FROM sqlite_master WHERE name = 'entry_reservations'").fetchone() and db.execute(
                    'SELECT 1 FROM entry_reservations WHERE account_id = ?', (settings.account_id,),
                ).fetchone():
                    raise ValueError('account_has_operations')
        try:
            contract = next(iter(settings.contracts))
            with sandbox_client(settings.ca_file, read_only=True) as client:
                position, orders = TBankReader(client, settings.account_id, contract, None).account_activity()
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
        try:
            channels = validate_channels(body.get('channels'))
        except ValueError as error:
            raise ValueError('invalid_channels') from error
        rows = body.get('contracts')
        if (not isinstance(rows, list) or not rows
                or any(not isinstance(pair, list) or len(pair) != 2 or any(
                    not isinstance(item, str) or not item or item.strip() != item for item in pair) for pair in rows)
                or len(dict(rows)) != len(rows)):
            raise ValueError('invalid_contracts')
        contracts = dict(rows)
        models = body.get('models')
        if (not isinstance(models, dict) or models.keys() != self.default['models'].keys()
                or not all(isinstance(model, str) and len(model) <= 200 for model in models.values())):
            raise ValueError('invalid_models')
        values = {'mode': mode, 'channels': channels, 'contracts': contracts, 'models': models,
                  'account_id': self.base.account_id}
        candidate = self.runtime(values)
        try:
            inputs = cycle_inputs(candidate)
        except (RuntimeErrorInfo, ValueError) as error:
            raise ValueError('invalid_contracts') from error
        try:
            cycle_inputs(candidate, models)
        except ValueError as error:
            raise ValueError('invalid_models') from error
        mode_changed = mode != saved['mode']
        if mode_changed and not stopped:
            raise ValueError('mode_requires_stop')
        removed = saved['contracts'].items() - contracts.items()
        added = contracts.items() - (saved['contracts'].items() | self.active['contracts'].items())
        if added:
            try:
                with sandbox_client(candidate.ca_file, read_only=True) as client:
                    BrokerAccount(candidate.account_id, dict(added), lambda contract: TBankReader(
                        client, candidate.account_id, contract, contracts[contract],
                    )).read_markets(inputs['catalog'])
            except Exception as error:
                raise ValueError('invalid_contracts') from error
        if mode_changed or removed:
            # Changing the execution profile requires a broker check with no concurrent local executor.
            with self.exclusive_profile(stopped=stopped):
                self.require_flat(self.runtime(self.active))
                self.state.save_settings(values, expected=stored)
        else:
            self.state.save_settings(values, expected=stored)
        if mode_changed:
            self.active = self.active | {'mode': mode}
            self.profile_at = utc_now().isoformat()
