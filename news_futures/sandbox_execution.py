"""Programmatic sandbox execution. Broker effects are separate from model workflows."""

from collections.abc import Callable, Iterator, Set
from contextlib import closing, contextmanager
from dataclasses import asdict
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
from typing import Annotated, Any, Literal, Protocol
from uuid import UUID, uuid5

from pydantic import ConfigDict, Field, TypeAdapter, ValidationError
from pydantic.dataclasses import dataclass

from news_futures.events import EventArchive
from news_futures.publications import Archive, utc_now
from news_futures.risk import EntryReservation, Rejected, reserve_entry


BROKER_DATA = ConfigDict(strict=True, revalidate_instances='always', extra='forbid')
Identifier = Annotated[str, Field(min_length=1)]
Lots = Annotated[int, Field(gt=0)]
Points = Annotated[Decimal, Field(gt=0, allow_inf_nan=False, max_digits=18, decimal_places=9)]


@dataclass(frozen=True, config=BROKER_DATA)
class Fill:
    order_id: Identifier
    lots: Lots
    price_points: Points


@dataclass(frozen=True, config=BROKER_DATA)
class Protection:
    id: Identifier
    kind: Literal['sl', 'tp']
    lots: Lots
    trigger_points: Points
    side: Literal['buy', 'sell']
    status: Literal['active', 'canceled', 'executed', 'expired'] = 'active'


@dataclass(frozen=True, config=BROKER_DATA)
class TradeState:
    """Normalized account-wide reconciliation of the cycle and its broker effects.

    Settled means every submitted request is accounted for, including rejected
    requests and activated stop children. An absent result alone is insufficient.
    Position is signed in lots. Fills are per exchange order, with average prices.
    Foreign positions, orders and stops are reported as other exposure.
    """

    settled: bool
    position_lots: int
    entry: Fill | None
    exits: tuple[Fill, ...]
    protection: tuple[Protection, ...]
    active_orders: bool
    other_exposure: bool


TRADE_STATE = TypeAdapter(TradeState)


class SandboxBroker(Protocol):
    @property
    def source(self) -> Literal['control', 'sandbox']: ...

    @property
    def account_id(self) -> str: ...

    @property
    def paired_protection_confirmed(self) -> bool:
        """The platform independently enforces that the pair cannot over-close."""
        ...

    def read_risk(self, account_id: str, contract_id: str) -> object: ...
    def read_trade(self, entry: EntryReservation, operations: dict[str, str]) -> TradeState: ...

    def enter(self, entry: EntryReservation, operation_id: str) -> None:
        """Submit a limit order in points, immediate fill-or-cancel in lots."""
        ...

    def protect(self, entry: EntryReservation, operation_id: str, lots: int) -> None:
        """Install position-bound native SL and TP with no application-side timer."""
        ...

    def cancel_protection(self, entry: EntryReservation, operation_id: str, ids: tuple[str, ...]) -> None: ...
    def close(self, entry: EntryReservation, operation_id: str, lots: int) -> None: ...


def trade_journal(events: EventArchive) -> list[dict[str, Any]]:
    with events.db:
        if events.db.execute("SELECT 1 FROM sqlite_master WHERE name = 'sandbox_cycles'").fetchone() is None:
            return []
        return [
            {'reservation': json.loads(row['terms']), 'approval': row['approval'],
             'source': row['source'], 'status': row['status'], 'reason': row['reason'],
             'events': [
                 {'at': event['at'], 'kind': event['kind'], 'details': json.loads(event['details'])}
                 for event in events.db.execute(
                     'SELECT at, kind, details FROM sandbox_journal WHERE cycle_id = ? ORDER BY seq',
                     (row['id'],),
                 )
             ]}
            for row in events.db.execute('SELECT * FROM sandbox_cycles ORDER BY rowid')
        ]


class SandboxExecutor:
    def __init__(self, events: EventArchive, broker: SandboxBroker):
        self.events = events
        self.db = events.db
        self.broker = broker
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS sandbox_cycles (
                id TEXT PRIMARY KEY NOT NULL, account_id TEXT NOT NULL, proposal_id TEXT NOT NULL,
                terms TEXT NOT NULL, approval TEXT NOT NULL, source TEXT NOT NULL,
                status TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '', close_requested INTEGER NOT NULL DEFAULT 0,
                UNIQUE(account_id, proposal_id)
            );
            CREATE TABLE IF NOT EXISTS sandbox_journal (
                seq INTEGER PRIMARY KEY, cycle_id TEXT NOT NULL REFERENCES sandbox_cycles(id),
                at TEXT NOT NULL, kind TEXT NOT NULL, details TEXT NOT NULL
            );
        """)

    @contextmanager
    def _exclusive(self) -> Iterator[bool]:
        # A separate lock database serializes the whole broker sequence while intents commit in the archive.
        database = self.db.execute('PRAGMA database_list').fetchone()['file']
        with closing(sqlite3.connect(database + '.sandbox-lock', timeout=0)) as lock:
            try:
                lock.execute('BEGIN IMMEDIATE')
            except sqlite3.OperationalError as error:
                if error.sqlite_errorcode != sqlite3.SQLITE_BUSY:
                    raise
                yield False
            else:
                yield True

    def start(self, publications: Archive, approval: Path, *, allowed_contracts: Set[str]) -> dict[str, str]:
        if self.broker.source not in ('control', 'sandbox') or self.broker.paired_protection_confirmed is not True:
            return {'status': 'rejected', 'reason': 'safe_native_protection_unconfirmed'}

        def record(entry: EntryReservation) -> None:
            if self.db.execute(
                'SELECT 1 FROM sandbox_cycles WHERE account_id = ? AND proposal_id = ?',
                (entry.account_id, entry.proposal_id),
            ).fetchone():
                raise Rejected('proposal_already_reserved')
            self.db.execute("""
                INSERT INTO sandbox_cycles (id, account_id, proposal_id, terms, approval, source, status)
                VALUES (?, ?, ?, ?, ?, ?, 'reserved')
            """, (entry.id, entry.account_id, entry.proposal_id, entry.model_dump_json(),
                  str(approval.resolve()), self.broker.source))

        with self._exclusive() as acquired:
            if not acquired:
                return {'status': 'blocked', 'reason': 'executor_busy'}
            admission = reserve_entry(
                self.events, publications, approval, account_id=self.broker.account_id,
                allowed_contracts=allowed_contracts, read_snapshot=self.broker.read_risk, on_reserve=record,
            )
            entry = admission.reservation
            if entry is None:
                return {'status': 'rejected', 'reason': admission.reason or 'admission_failed'}
            operations: dict[str, str] = {}
            self._send(entry, operations, 'enter', lambda key: self.broker.enter(entry, key))
            return self._advance(entry, operations, close=False)

    def reconcile(self, *, close: bool = False) -> dict[str, str]:
        with self._exclusive() as acquired:
            if not acquired:
                return {'status': 'blocked', 'reason': 'executor_busy'}
            row = self.db.execute("""
                SELECT * FROM sandbox_cycles WHERE account_id = ? AND status NOT IN ('closed', 'no_fill')
            """, (self.broker.account_id,)).fetchone()
            if row is None:
                return {'status': 'idle', 'reason': ''}
            if row['source'] != self.broker.source:
                return {'status': 'blocked', 'reason': 'broker_source_mismatch'}
            entry = EntryReservation.model_validate_json(row['terms'])
            operations = {
                details['action']: details['operation_id']
                for event in self.db.execute(
                    "SELECT details FROM sandbox_journal WHERE cycle_id = ? AND kind = 'intent'", (entry.id,),
                )
                for details in (json.loads(event['details']),)
            }
            if 'enter' not in operations:
                return self._finish(entry, 'no_fill', 'entry_never_submitted')
            if close:
                with self.db:
                    self.db.execute('UPDATE sandbox_cycles SET close_requested = 1 WHERE id = ?', (entry.id,))
            return self._advance(entry, operations, close=close or bool(row['close_requested']))

    def _log(self, entry: EntryReservation, kind: str, **details: Any) -> None:
        with self.db:
            self.db.execute('INSERT INTO sandbox_journal (cycle_id, at, kind, details) VALUES (?, ?, ?, ?)',
                            (entry.id, utc_now().isoformat(), kind, json.dumps(details, default=str)))

    def _send(
        self, entry: EntryReservation, operations: dict[str, str], action: str,
        submit: Callable[[str], None], **terms: Any,
    ) -> None:
        if action in operations:
            raise ValueError('operation_already_attempted')
        key = uuid5(UUID(entry.id), action).hex
        self._log(entry, 'intent', action=action, operation_id=key, **terms)
        operations[action] = key
        try:
            submit(key)
        except Exception:
            self._log(entry, 'response_uncertain', operation_id=key)
        else:
            self._log(entry, 'response_received', operation_id=key)

    def _finish(self, entry: EntryReservation, status: str, reason: str = '') -> dict[str, str]:
        with self.db:
            self.db.execute('UPDATE sandbox_cycles SET status = ?, reason = ? WHERE id = ?',
                            (status, reason, entry.id))
            if status in ('closed', 'no_fill'):
                self.db.execute('DELETE FROM entry_reservations WHERE account_id = ? AND id = ?',
                                (entry.account_id, entry.id))
        return {'status': status, 'reason': reason, 'cycle_id': entry.id}

    def _advance(
        self, entry: EntryReservation, operations: dict[str, str], *, close: bool,
    ) -> dict[str, str]:
        previous = self.db.execute("""
            SELECT details FROM sandbox_journal
            WHERE cycle_id = ? AND kind = 'confirmed_fills' ORDER BY seq DESC LIMIT 1
        """, (entry.id,)).fetchone()
        confirmed = json.loads(previous['details'])['fills'] if previous else {}
        while True:
            try:
                observation = self.broker.read_trade(entry, dict(operations))
            except Exception:
                return self._finish(entry, 'blocked', 'broker_read_unavailable')
            try:
                state = TRADE_STATE.validate_python(observation)
            except ValidationError:
                return self._finish(entry, 'blocked', 'invalid_broker_state')
            self._log(entry, 'reconciliation', state=asdict(state))
            if not state.settled or state.active_orders or state.other_exposure:
                return self._finish(entry, 'blocked', 'unresolved_broker_state')
            sign = 1 if entry.side == 'long' else -1
            entry_lots = state.entry.lots if state.entry else 0
            remaining = entry_lots - sum(fill.lots for fill in state.exits)
            fills = ((state.entry,) if state.entry else ()) + state.exits
            if (not 0 <= remaining <= entry_lots <= entry.lots or state.position_lots != sign * remaining
                    or len({fill.order_id for fill in fills}) != len(fills)):
                return self._finish(entry, 'blocked', 'fills_and_position_disagree')
            current = {fill.order_id: [fill.lots, str(fill.price_points.normalize())] for fill in fills}
            if any(current.get(order_id) != fill for order_id, fill in confirmed.items()):
                return self._finish(entry, 'blocked', 'confirmed_fills_changed')
            if current != confirmed:
                self._log(entry, 'confirmed_fills', fills=current)
                confirmed = current
            active = tuple(stop for stop in state.protection if stop.status == 'active')
            expected_side = 'sell' if sign == 1 else 'buy'
            expected = {('sl', entry.stop_loss), ('tp', entry.take_profit)}
            if (active and 'protect' not in operations
                    or len({stop.id for stop in state.protection}) != len(state.protection)):
                return self._finish(entry, 'blocked', 'protection_ownership_unconfirmed')
            if not remaining and not active:
                return self._finish(entry, 'closed' if entry_lots else 'no_fill')
            safe_fill = (state.entry is not None
                         and sign * state.entry.price_points <= sign * entry.entry_price
                         and min(entry.stop_loss, entry.take_profit) < state.entry.price_points
                         < max(entry.stop_loss, entry.take_profit))
            if remaining and not close and safe_fill and self.broker.paired_protection_confirmed is True:
                if (len(active) == 2
                        and {(stop.kind, stop.trigger_points) for stop in active} == expected
                        and all(stop.lots == remaining and stop.side == expected_side for stop in active)):
                    return self._finish(entry, 'protected')
                if 'protect' not in operations and not state.exits:
                    self._send(entry, operations, 'protect',
                               lambda key: self.broker.protect(entry, key, remaining), lots=remaining)
                    continue
            if active and 'cancel_protection' not in operations:
                ids = tuple(stop.id for stop in active)
                self._send(entry, operations, 'cancel_protection',
                           lambda key: self.broker.cancel_protection(entry, key, ids), stop_ids=ids)
            elif active or 'close' in operations:
                return self._finish(entry, 'blocked', 'exit_or_cancellation_unconfirmed')
            else:
                self._send(entry, operations, 'close', lambda key: self.broker.close(entry, key, remaining),
                           lots=remaining, reason='control_exit' if close else 'protection_unconfirmed')
