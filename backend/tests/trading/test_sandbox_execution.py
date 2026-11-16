from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from threading import Event
from unittest.mock import patch

from tests.support.risk import RiskFixture

from news_futures.trading.broker import Fill
from news_futures.agents.events import EventArchive
from news_futures.news.publications import Archive
from news_futures.trading.terms import EntryReservation
from news_futures.trading.executor import SandboxExecutor
from news_futures.trading.journal import trade_journal
from tests.support.broker import ControlBroker


class SandboxExecutionTest(RiskFixture):
    def setUp(self) -> None:
        super().setUp()
        self.broker = ControlBroker(self)

    def start(self, approval: Path) -> dict[str, str]:
        archive = Archive(self.publications_db)
        with EventArchive(self.events_db) as events, closing(archive.db):
            return SandboxExecutor(events, self.broker).start(
                archive, approval, allowed_contracts={self.contract_id},
            )

    def reconcile(self, *, close: bool = False) -> dict[str, str]:
        with EventArchive(self.events_db) as events:
            return SandboxExecutor(events, self.broker).reconcile(close=close)

    def test_protects_filled_lots_then_closes_and_prevents_replay(self) -> None:
        approval = self.approved()
        result = self.start(approval)
        self.assertEqual(result['status'], 'protected')
        self.assertEqual([stop.lots for stop in self.broker.state.protection], [3, 3])
        self.assertEqual(self.reconcile(close=True)['status'], 'closed')
        self.assertEqual(self.start(approval)['status'], 'rejected')
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp', 'cancel_protection', 'close'])
        with EventArchive(self.events_db) as events:
            cycle, = trade_journal(events.db)
        self.assertEqual(cycle['reservation']['lots'], 4)
        intents = [event for event in cycle['events'] if event['kind'] == 'intent']
        self.assertEqual(len({event['details']['operation_id'] for event in intents}), 5)

    def test_sandbox_requires_explicit_opt_in_before_reserving_or_submitting(self) -> None:
        self.broker.source = 'sandbox'
        self.assertEqual(self.start(self.approved())['reason'], 'independent_stops_require_opt_in')
        self.assertEqual(self.broker.calls, [])

    def test_conflicting_active_cycles_block_before_contacting_broker(self) -> None:
        approval = self.approved()
        entry = self.start(approval)
        self.assertEqual(entry['status'], 'protected')
        with EventArchive(self.events_db) as events:
            with events.db:
                events.db.execute("""
                    INSERT INTO sandbox_cycles (id, account_id, proposal_id, terms, approval, source, status)
                    SELECT 'conflicting-cycle', account_id, 'other-proposal', terms, approval, source, 'reserved'
                    FROM sandbox_cycles WHERE id = ?
                """, (entry['cycle_id'],))
            executor = SandboxExecutor(events, self.broker)
            with patch.object(self.broker, 'read_trade') as read_trade:
                self.assertEqual(executor.reconcile(close=True, cycle_id=entry['cycle_id']),
                                 {'status': 'blocked', 'reason': 'conflicting_active_cycles'})
                read_trade.assert_not_called()
            self.assertEqual(events.db.execute('SELECT sum(close_requested) FROM sandbox_cycles').fetchone()[0], 0)
        with patch.object(self.broker, 'read_risk') as read_risk:
            self.assertEqual(self.start(approval), {'status': 'blocked', 'reason': 'conflicting_active_cycles'})
            read_risk.assert_not_called()
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp'])

    def test_restart_reconciles_interrupted_entry_before_protection(self) -> None:
        self.broker.crash_after_entry = True
        with self.assertRaises(KeyboardInterrupt):
            self.start(self.approved())
        self.broker.state = replace(self.broker.state, settled=False)
        self.assertEqual(self.reconcile()['status'], 'blocked')
        self.assertEqual(self.broker.calls, ['enter'])
        self.broker.state = replace(self.broker.state, settled=True)
        self.assertEqual(self.reconcile()['status'], 'protected')
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp'])

    def test_confirmed_absence_of_protection_emergency_closes(self) -> None:
        self.broker.protection_rejected = True
        self.assertEqual(self.start(self.approved())['status'], 'closed')
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp', 'cancel_protection', 'close'])

    def test_expired_stop_does_not_count_as_a_fill_or_release_reservation(self) -> None:
        approval = self.approved()
        self.assertEqual(self.start(approval)['status'], 'protected')
        self.broker.state = replace(self.broker.state, protection=(
            replace(self.broker.state.protection[0], status='expired'),
            self.broker.state.protection[1],
        ))
        self.broker.close_unavailable = True
        self.assertEqual(self.reconcile()['status'], 'blocked')
        self.assertEqual(self.broker.state.position_lots, 3)
        self.assertEqual(self.start(approval)['reason'], 'entry_already_reserved')
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp', 'cancel_protection', 'close'])

    def test_reconciles_native_stop_after_disconnection_and_removes_sibling(self) -> None:
        self.assertEqual(self.start(self.approved())['status'], 'protected')
        self.broker.state = replace(self.broker.state, position_lots=0,
                                    exits=(Fill('sl-child', 3, Decimal('289')),),
                                    protection=(replace(self.broker.state.protection[0], status='executed'),
                                                self.broker.state.protection[1]))
        self.assertEqual(self.reconcile()['status'], 'closed')
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp', 'cancel_protection'])

    def test_cancels_sibling_while_native_child_is_pending(self) -> None:
        self.assertEqual(self.start(self.approved())['status'], 'protected')
        self.broker.state = replace(self.broker.state, settled=False, active_orders=True,
                                    protection=(replace(self.broker.state.protection[0], status='executed'),
                                                self.broker.state.protection[1]))
        self.assertEqual(self.reconcile()['status'], 'blocked')
        self.assertEqual(self.broker.state.protection[1].status, 'canceled')
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp', 'cancel_protection'])

    def test_reconciles_before_retrying_only_the_still_active_stop(self) -> None:
        self.assertEqual(self.start(self.approved())['status'], 'protected')
        self.broker.first_cancel_uncertain = True
        self.assertEqual(self.reconcile(close=True)['status'], 'blocked')
        self.assertEqual([stop.status for stop in self.broker.state.protection], ['active', 'canceled'])
        self.assertEqual(self.reconcile()['status'], 'closed')
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp', 'cancel_protection', 'cancel_protection', 'close'])

    def test_a_previously_confirmed_fill_cannot_disappear_at_reconciliation(self) -> None:
        self.assertEqual(self.start(self.approved())['status'], 'protected')
        self.broker.state = replace(self.broker.state, entry=None, position_lots=0, protection=())
        self.assertEqual(self.reconcile()['reason'], 'confirmed_fills_changed')
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp'])

    def test_unrecognized_stop_status_blocks_exit_without_assuming_stop_is_gone(self) -> None:
        self.assertEqual(self.start(self.approved())['status'], 'protected')
        object.__setattr__(self.broker.state.protection[0], 'status', 'ACTIVE')
        self.assertEqual(self.reconcile(close=True)['reason'], 'invalid_broker_state')
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp'])

    def test_parallel_delivery_has_only_one_owner_and_replay_cannot_reenter(self) -> None:
        approval = self.approved()
        submitting = Event()
        resume = Event()
        enter = self.broker.enter

        def pending(entry: EntryReservation, operation_id: str) -> str | None:
            submitting.set()
            if not resume.wait(timeout=5):
                raise TimeoutError
            return enter(entry, operation_id)

        with patch.object(self.broker, 'enter', pending), ThreadPoolExecutor(1) as pool:
            first = pool.submit(self.start, approval)
            try:
                self.assertTrue(submitting.wait(timeout=5))
                self.assertEqual(self.start(approval)['reason'], 'executor_busy')
                self.assertEqual(self.reconcile(close=True)['reason'], 'executor_busy')
            finally:
                resume.set()
            self.assertEqual(first.result()['status'], 'protected')
        self.assertEqual(self.reconcile(close=True)['status'], 'closed')
        self.assertEqual(self.start(approval)['reason'], 'proposal_already_reserved')
        self.assertEqual(self.broker.calls.count('enter'), 1)

    def test_unavailable_read_persists_block_and_close_request_until_broker_recovers(self) -> None:
        approval = self.approved()
        self.assertEqual(self.start(approval)['status'], 'protected')
        another_approval = self.approved(reason='A new approved proposal.')
        with patch.object(self.broker, 'read_trade', side_effect=TimeoutError):
            self.assertEqual(self.reconcile(close=True)['reason'], 'broker_read_unavailable')
            self.assertEqual(self.start(another_approval)['reason'], 'entry_already_reserved')
            with EventArchive(self.events_db) as events:
                cycle, = trade_journal(events.db)
            self.assertEqual((cycle['status'], cycle['reason']), ('blocked', 'broker_read_unavailable'))
            self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp'])
        self.assertEqual(self.reconcile()['status'], 'closed')

    def test_close_for_another_cycle_does_not_touch_current_protection(self) -> None:
        result = self.start(self.approved())
        self.assertEqual(result['status'], 'protected')
        with EventArchive(self.events_db) as events:
            executor = SandboxExecutor(events, self.broker)
            self.assertEqual(executor.reconcile(close=True, cycle_id='old-cycle')['reason'],
                             'sandbox_cycle_mismatch')
            self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp'])
            self.assertEqual(executor.reconcile(close=True, cycle_id=result['cycle_id'])['status'], 'closed')
