"""In-memory sandbox broker for execution and workflow tests."""

from dataclasses import replace
from decimal import Decimal
from typing import Literal

from news_futures.agents.events import EventArchive
from news_futures.trading.broker import Fill, Protection, TradeState
from news_futures.trading.journal import trade_journal
from news_futures.trading.terms import EntryReservation
from tests.support.risk import RiskFixture

class ControlBroker:
    source: Literal['control', 'sandbox'] = 'control'

    def __init__(self, risk: RiskFixture) -> None:
        self.risk = risk
        self.account_id = risk.account_id
        self.state = TradeState(settled=True, position_lots=0, entry=None, exits=(),
                                protection=(), active_orders=False, other_exposure=False)
        self.calls: list[str] = []
        self.crash_after_entry = False
        self.protection_rejected = False
        self.entry_lots = 3
        self.close_unavailable = False
        self.first_cancel_uncertain = False
        self.operation_ids: dict[str, str] = {}

    def record(self, action: str, operation_id: str) -> None:
        with EventArchive(self.risk.events_db) as events:
            cycle = trade_journal(events.db)[-1]
        event = cycle['events'][-1]
        assert event['kind'] == 'intent' and event['details']['operation_id'] == operation_id
        assert action not in self.operation_ids or (
            action == 'cancel_protection' and operation_id == self.operation_ids[action]
        )
        self.operation_ids[action] = operation_id
        self.calls.append(action)

    def read_risk(self, account_id: str, contract_id: str) -> object:
        return self.risk.snapshot

    def read_trade(self, entry: EntryReservation, operations: dict[str, str],
                   receipts: dict[str, str | None]) -> TradeState:
        assert operations == self.operation_ids
        return self.state

    def enter(self, entry: EntryReservation, operation_id: str) -> str | None:
        self.record('enter', operation_id)
        self.state = replace(self.state, position_lots=self.entry_lots,
                             entry=Fill('entry-order', self.entry_lots, entry.entry_price))
        if self.crash_after_entry:
            raise KeyboardInterrupt
        return 'entry-order'

    def protect(self, entry: EntryReservation, operation_id: str, lots: int,
                kind: Literal['sl', 'tp']) -> str | None:
        self.record(kind, operation_id)
        if self.protection_rejected and kind == 'tp':
            raise TimeoutError
        stop = Protection(f'{kind}-order', kind, lots,
                          entry.stop_loss if kind == 'sl' else entry.take_profit, 'sell')
        self.state = replace(self.state, protection=(*self.state.protection, stop))
        return stop.id

    def cancel_protection(self, entry: EntryReservation, operation_id: str, ids: tuple[str, ...]) -> None:
        self.record('cancel_protection', operation_id)
        uncertain = self.first_cancel_uncertain and len(ids) == 2
        canceled = ids[1:] if uncertain else ids
        self.state = replace(self.state, protection=tuple(
            replace(stop, status='canceled') if stop.id in canceled else stop for stop in self.state.protection
        ))
        if uncertain:
            raise TimeoutError

    def close(self, entry: EntryReservation, operation_id: str, lots: int) -> str | None:
        self.record('close', operation_id)
        if self.close_unavailable:
            self.state = replace(self.state, settled=False)
            raise TimeoutError
        self.state = replace(self.state, position_lots=0, exits=(Fill('exit-order', lots, Decimal('303')),))
        return 'exit-order'

    def cancel_orders(self, entry: EntryReservation, operation_id: str, ids: tuple[str, ...]) -> None:
        raise AssertionError('the control broker exposes no cancellable ordinary orders')
