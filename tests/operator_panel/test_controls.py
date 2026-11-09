from contextlib import closing
from dataclasses import asdict, replace
from decimal import Decimal
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
import unittest
from unittest.mock import patch
from uuid import uuid4

import httpx

from news_futures.agents.events import EventArchive
from news_futures.application.config import RuntimeSettings, cycle_inputs
from news_futures.application.operator_state import OperatorState
from news_futures.application.records import runtime_tables
from news_futures.application.runtime import Runtime
from news_futures.common.artifacts import write_json
from news_futures.news.publications import Archive
from news_futures.operator.news import trace_view
from news_futures.operator.settings import Preferences, profile_view
from news_futures.operator.supervisor import Worker
from news_futures.operator.trades import trades_view
from news_futures.trading.broker import Fill
from news_futures.trading.executor import SandboxExecutor
from news_futures.trading.terms import CloseIntent, EntryReservation
from tests.support.broker import ControlBroker
from tests.support.risk import RiskFixture


def catalog(path: Path) -> None:
    write_json(path, {'operations': {'catalog': {'availability': 'available', 'data': [{
        'company_id': 'sber', 'company_name': 'Сбербанк', 'ticker': 'SBER', 'share_id': 'sber-share',
    }]}}})


class PreferencesTest(unittest.TestCase):
    def setUp(self) -> None:
        root = Path(self.enterContext(TemporaryDirectory()))
        catalog(root / 'catalog.json')
        self.settings = RuntimeSettings(data_dir=root, catalog=root / 'catalog.json',
                                        account_id='account', contracts={'future': 'sber'})
        self.state = OperatorState(root)
        self.preferences = Preferences(self.settings, ['example'], self.state)

    def form(self, **changes: object) -> dict[str, Any]:
        saved = self.preferences.saved()
        return saved | {'base': saved, 'contracts': [list(item) for item in saved['contracts'].items()]} | changes

    def test_channels_wait_for_restart_and_bad_settings_preserve_working_values(self) -> None:
        original = self.preferences.saved()
        with self.assertRaisesRegex(ValueError, 'invalid_channels'):
            self.preferences.save(self.form(channels=['example', 'EXAMPLE']), stopped=False)
        with self.assertRaisesRegex(ValueError, 'invalid_models'):
            self.preferences.save(self.form(models=original['models'] | {'jev': 'test/no-jev'}), stopped=False)
        self.assertEqual(self.preferences.saved(), original)
        update = self.form(channels=['another'])
        self.preferences.save(update, stopped=False)
        self.assertEqual(self.preferences.active['channels'], ['example'])
        self.assertEqual(self.preferences.prepare()[1], ['another'])
        with self.assertRaisesRegex(ValueError, 'settings_conflict'):
            self.preferences.save(update, stopped=False)

    def test_mode_changes_only_when_the_worker_is_stopped_and_the_account_is_flat(self) -> None:
        update = self.form(mode='sandbox')
        with self.assertRaisesRegex(ValueError, 'mode_requires_stop'):
            self.preferences.save(update, stopped=False)
        with patch('news_futures.operator.settings.sandbox_client', side_effect=lambda *args, **kw: httpx.Client()), patch(
            'news_futures.operator.settings.TBankReader.account_activity',
        ) as activity:
            activity.side_effect = TimeoutError
            with self.assertRaisesRegex(ValueError, 'account_unconfirmed'):
                self.preferences.save(update, stopped=True)
            activity.side_effect = None
            for position, orders, reason in ((True, False, 'account_has_position'),
                                              (False, True, 'account_has_operations')):
                activity.return_value = (position, orders)
                with self.assertRaisesRegex(ValueError, reason):
                    self.preferences.save(update, stopped=True)
            self.assertEqual(self.preferences.saved()['mode'], 'observation')
            activity.return_value = (False, False)
            self.preferences.save(update, stopped=True)
        self.assertEqual(self.preferences.active['mode'], 'sandbox')
        self.assertEqual(Preferences(self.settings, ['example'], self.state).prepare()[0]['mode'], 'sandbox')


class ControlledBroker(ControlBroker):
    source = 'sandbox'

    def enter(self, entry: EntryReservation, operation_id: str) -> str | None:
        result = super().enter(entry, operation_id)
        if entry.side == 'short':
            self.state = replace(self.state, position_lots=-self.entry_lots)
        return result

    def protect(self, entry: EntryReservation, operation_id: str, lots: int, kind: Any) -> str | None:
        result = super().protect(entry, operation_id, lots, kind)
        if entry.side == 'short':
            self.state = replace(self.state, protection=tuple(replace(stop, side='buy') for stop in self.state.protection))
        return result

    def read_risk(self, account_id: str, contract_id: str) -> object:
        return self.risk.snapshot | {'has_position': bool(self.state.position_lots)}


class ManualCloseTest(RiskFixture):
    def setUp(self) -> None:
        super().setUp()
        self.broker = ControlledBroker(self)
        self.controls = OperatorState(self.root)
        self.settings = RuntimeSettings(data_dir=self.root, catalog=self.root / 'catalog.json',
                                        account_id=self.account_id, contracts={self.contract_id: 'sber'}, mode='sandbox',
                                        review=self.config)
        catalog(self.settings.catalog)

    def open_position(self, *, short: bool = False) -> CloseIntent:
        approval = self.approved(side='short', stop_loss=310, take_profit=280) if short else self.approved()
        saved = self.root / 'decisions' / self.publication.revision_key() / 'review'
        saved.parent.mkdir(parents=True)
        approval.rename(saved)
        archive = Archive(self.publications_db)
        with EventArchive(self.events_db) as events, closing(archive.db):
            runtime_tables(archive)
            result = SandboxExecutor(events, self.broker).start(
                archive, saved, allowed_contracts={self.contract_id}, allow_independent_stops=True,
            )
        self.assertEqual(result['status'], 'protected')
        return CloseIntent(str(uuid4()), result['cycle_id'], self.contract_id, 'short' if short else 'long', 3)

    def apply_close(self) -> None:
        runtime = Runtime(self.settings, lambda _: self.broker, lambda _: self.broker,
                          run_id='controlled-run', on_controls=lambda **event: None)
        with patch('builtins.print'):
            runtime.refresh()

    def test_manual_close_while_paused_keeps_confirmed_partial_entry_and_short_exit(self) -> None:
        intent = self.open_position(short=True)
        self.controls.set_pause(True)
        self.assertTrue(OperatorState(self.root).pause()['paused'])
        self.apply_close()
        worker = Worker(self.root, profile_view(self.settings), [], b'')
        worker.state = 'running'
        worker.run_id = 'controlled-run'
        self.assertEqual(worker.configure('close', asdict(intent)), (202, 'close_queued'))
        self.assertEqual(worker.configure('close', asdict(intent)), (202, 'close_queued'))
        self.apply_close()
        self.assertEqual((self.controls.close_intent() or {})['status'], 'accepted')
        self.assertEqual(worker.controls()['close']['status'], 'closed')
        # Simulate a worker restart before its acknowledgement reached the panel store.
        self.controls.finish_close(intent.request_id, 'queued', '')
        self.apply_close()
        self.assertEqual((self.controls.close_intent() or {})['status'], 'accepted')
        self.assertEqual(self.broker.calls.count('close'), 1)
        detail = trades_view(self.root, self.account_id, intent.cycle_id) or {}
        self.assertEqual(detail['status'], 'closed')
        self.assertEqual(detail['exit_reason'], 'manual_exit')
        entry, exit_order = [item for item in detail['orders'] if item['action'] in ('enter', 'close')]
        self.assertGreater(entry['requested'], entry['filled'])
        self.assertEqual((exit_order['side'], exit_order['filled'], exit_order['price']), ('buy', 3, '303'))
        self.assertEqual(detail['traces'], [{'kind': 'entry', 'revision': self.publication.revision_key()}])
        trace = trace_view(self.root, self.account_id, self.publication.revision_key()) or {}
        self.assertEqual(trace['publication']['url'], 'https://t.me/rbc/1')
        self.assertEqual(trace['cycle_id'], intent.cycle_id)

    def test_stop_fill_between_confirmation_and_execution_cannot_trigger_a_manual_order(self) -> None:
        intent = self.open_position()
        self.controls.queue(intent)
        self.broker.state = replace(self.broker.state, position_lots=0,
                                    exits=(Fill('sl-child', 3, Decimal('290')),),
                                    protection=(replace(self.broker.state.protection[0], status='executed'),
                                                self.broker.state.protection[1]))
        self.apply_close()
        self.assertEqual((self.controls.close_intent() or {})['reason'], 'close_position_changed')
        self.assertNotIn('close', self.broker.calls)
        self.assertEqual((trades_view(self.root, self.account_id, intent.cycle_id) or {})['exit_reason'], 'protective_exit')

    def test_changed_volume_keeps_the_existing_protection(self) -> None:
        intent = self.open_position()
        self.controls.queue(replace(intent, quantity=4))
        self.apply_close()
        self.assertEqual((self.controls.close_intent() or {})['status'], 'rejected')
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp'])

    def test_published_model_override_can_be_frozen_without_editing_the_base_config(self) -> None:
        preferences = Preferences(self.settings, ['example'], self.controls)
        active = cycle_inputs(self.settings)
        models = preferences.saved()['models'] | {'analyst': 'test/new-analyst'}
        self.controls.save_settings({'models': models})
        updated = cycle_inputs(self.settings, self.controls.saved()['models'])
        self.assertEqual(json.loads(active['review'])['analyst']['model'], 'test/analyst')
        self.assertEqual(json.loads(updated['review'])['analyst']['model'], 'test/new-analyst')
        self.assertEqual(cycle_inputs(self.settings), active)
