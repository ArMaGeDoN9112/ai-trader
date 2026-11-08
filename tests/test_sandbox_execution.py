from contextlib import closing
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Literal

import test_risk

from news_futures.events import EventArchive
from news_futures.publications import Archive
from news_futures.risk import EntryReservation
from news_futures.sandbox_execution import (
    Fill, Protection, SandboxExecutor, TradeState, trade_journal,
)


class ControlBroker:
    source: Literal['control', 'sandbox'] = 'control'
    paired_protection_confirmed = True

    def __init__(self, risk: test_risk.RiskFixture) -> None:
        self.risk = risk
        self.account_id = risk.account_id
        self.state = TradeState(settled=True, position_lots=0, entry=None, exits=(),
                                protection=(), active_orders=False, other_exposure=False)
        self.calls: list[str] = []
        self.crash_after_entry = False
        self.protection_rejected = False
        self.entry_lots = 3
        self.close_unavailable = False
        self.operation_ids: dict[str, str] = {}

    def record(self, action: str, operation_id: str) -> None:
        with EventArchive(self.risk.events_db) as events:
            cycle = trade_journal(events)[-1]
        event = cycle['events'][-1]
        assert event['kind'] == 'intent' and event['details']['operation_id'] == operation_id
        assert action not in self.operation_ids
        self.operation_ids[action] = operation_id
        self.calls.append(action)

    def read_risk(self, account_id: str, contract_id: str) -> object:
        return self.risk.snapshot

    def read_trade(self, entry: EntryReservation, operations: dict[str, str]) -> TradeState:
        assert operations == self.operation_ids
        return self.state

    def enter(self, entry: EntryReservation, operation_id: str) -> None:
        self.record('enter', operation_id)
        self.state = replace(self.state, position_lots=self.entry_lots,
                             entry=Fill('entry-order', self.entry_lots, entry.entry_price))
        if self.crash_after_entry:
            raise KeyboardInterrupt

    def protect(self, entry: EntryReservation, operation_id: str, lots: int) -> None:
        self.record('protect', operation_id)
        if self.protection_rejected:
            raise TimeoutError
        self.state = replace(self.state, protection=(
            Protection('sl-order', 'sl', lots, entry.stop_loss, 'sell'),
            Protection('tp-order', 'tp', lots, entry.take_profit, 'sell'),
        ))

    def cancel_protection(self, entry: EntryReservation, operation_id: str, ids: tuple[str, ...]) -> None:
        self.record('cancel_protection', operation_id)
        self.state = replace(self.state, protection=tuple(
            replace(stop, status='canceled') if stop.id in ids else stop for stop in self.state.protection
        ))

    def close(self, entry: EntryReservation, operation_id: str, lots: int) -> None:
        self.record('close', operation_id)
        if self.close_unavailable:
            self.state = replace(self.state, settled=False)
            raise TimeoutError
        self.state = replace(self.state, position_lots=0, exits=(Fill('exit-order', lots, Decimal('303')),))


class SandboxExecutionTest(test_risk.RiskFixture):
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
        self.assertEqual(self.broker.calls, ['enter', 'protect', 'cancel_protection', 'close'])
        with EventArchive(self.events_db) as events:
            cycle, = trade_journal(events)
        self.assertEqual(cycle['reservation']['lots'], 4)
        intents = [event for event in cycle['events'] if event['kind'] == 'intent']
        self.assertEqual(len({event['details']['operation_id'] for event in intents}), 4)

    def test_unconfirmed_native_pair_refuses_before_reserving_or_submitting(self) -> None:
        self.broker.paired_protection_confirmed = False
        self.assertEqual(self.start(self.approved())['reason'], 'safe_native_protection_unconfirmed')
        self.assertEqual(self.broker.calls, [])

    def test_restart_reconciles_interrupted_entry_before_protection(self) -> None:
        self.broker.crash_after_entry = True
        with self.assertRaises(KeyboardInterrupt):
            self.start(self.approved())
        self.broker.state = replace(self.broker.state, settled=False)
        self.assertEqual(self.reconcile()['status'], 'blocked')
        self.assertEqual(self.broker.calls, ['enter'])
        self.broker.state = replace(self.broker.state, settled=True)
        self.assertEqual(self.reconcile()['status'], 'protected')
        self.assertEqual(self.broker.calls, ['enter', 'protect'])

    def test_confirmed_absence_of_protection_emergency_closes(self) -> None:
        self.broker.protection_rejected = True
        self.assertEqual(self.start(self.approved())['status'], 'closed')
        self.assertEqual(self.broker.calls, ['enter', 'protect', 'close'])

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
        self.assertEqual(self.broker.calls, ['enter', 'protect', 'cancel_protection', 'close'])

    def test_reconciles_native_stop_after_disconnection_and_removes_sibling(self) -> None:
        self.assertEqual(self.start(self.approved())['status'], 'protected')
        self.broker.state = replace(self.broker.state, position_lots=0,
                                    exits=(Fill('sl-child', 3, Decimal('289')),),
                                    protection=(replace(self.broker.state.protection[0], status='executed'),
                                                self.broker.state.protection[1]))
        self.assertEqual(self.reconcile()['status'], 'closed')
        self.assertEqual(self.broker.calls, ['enter', 'protect', 'cancel_protection'])

    def test_a_previously_confirmed_fill_cannot_disappear_at_reconciliation(self) -> None:
        self.assertEqual(self.start(self.approved())['status'], 'protected')
        self.broker.state = replace(self.broker.state, entry=None, position_lots=0, protection=())
        self.assertEqual(self.reconcile()['reason'], 'confirmed_fills_changed')
        self.assertEqual(self.broker.calls, ['enter', 'protect'])

    def test_unrecognized_stop_status_blocks_exit_without_assuming_stop_is_gone(self) -> None:
        self.assertEqual(self.start(self.approved())['status'], 'protected')
        object.__setattr__(self.broker.state.protection[0], 'status', 'ACTIVE')
        self.assertEqual(self.reconcile(close=True)['reason'], 'invalid_broker_state')
        self.assertEqual(self.broker.calls, ['enter', 'protect'])
