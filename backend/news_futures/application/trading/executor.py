"""Programmatic sandbox execution. Broker effects are separate from model workflows."""

from collections.abc import Callable, Set
from dataclasses import asdict
from pathlib import Path
from typing import Any
from uuid import UUID, uuid5

from pydantic import ValidationError

from news_futures.application.analysis.approval import read_approval
from news_futures.application.analysis.entry_context import entry_sources
from news_futures.application.ports.broker import SandboxBroker, SubmissionRejected
from news_futures.application.ports.storage import EventStore, PublicationStore, checked_publication
from news_futures.application.trading.risk import (
    check_publication, reserve_entry,
)
from news_futures.common.errors import Rejected
from news_futures.domain.company import CompanyProposal, EntryReason
from news_futures.domain.execution import TRADE_STATE, TradeState, close_requests, reconciled_fills, verified_protection
from news_futures.domain.risk import MAX_RISK_DATA_AGE, fresh
from news_futures.domain.terms import CloseIntent, EntryReservation, RiskSnapshot


AUTONOMOUS_PROTECTION_UNAVAILABLE = 'autonomous_protection_unavailable'
SANDBOX_SOURCES = ('sandbox', 'sandbox_mcp')


class SandboxExecutor:
    def __init__(self, events: EventStore, broker: SandboxBroker):
        self.events = events
        self.trades = events.trades
        self.broker = broker
        self.trades.initialize()

    def start(
        self, publications: PublicationStore, approval: Path, *,
        allowed_contracts: Set[str], allow_independent_stops: bool = False,
        record_risk: Callable[[str, dict[str, Any], object], None] | None = None,
        entry_block: Callable[[], str | None] | None = None,
    ) -> dict[str, str]:
        if self.broker.source not in ('control', *SANDBOX_SOURCES):
            return {'status': 'rejected', 'reason': 'unsupported_broker_source'}
        if self.broker.source in SANDBOX_SOURCES and allow_independent_stops is not True:
            return {'status': 'rejected', 'reason': 'independent_stops_require_opt_in'}

        def record(entry: EntryReservation, proposal: CompanyProposal, context: dict[str, Any]) -> None:
            self.trades.open_cycle(entry, approval, self.broker.source)
            self.events.save_entry_reason(EntryReason(account_id=entry.account_id, position_id=entry.id,
                                                       contract_id=entry.contract_id, reason=proposal.reason,
                                                       sources=entry_sources(proposal, context)))

        with self.trades.exclusive() as acquired:
            if not acquired:
                return {'status': 'blocked', 'reason': 'executor_busy'}
            if entry_block is not None and (reason := entry_block()):
                return {'status': 'skipped', 'reason': reason}
            if len(self.trades.active(self.broker.account_id)) > 1:
                return {'status': 'blocked', 'reason': 'conflicting_active_cycles'}
            risk_input: dict[str, Any] = {'allowed_contracts': sorted(allowed_contracts)}
            if record_risk is not None:
                record_risk('processing', risk_input, None)
            admission = reserve_entry(
                self.events, publications, approval, account_id=self.broker.account_id,
                allowed_contracts=allowed_contracts, read_snapshot=self.broker.read_risk, on_reserve=record,
                risk_input=risk_input if record_risk is not None else None,
            )
            entry = admission.reservation
            if record_risk is not None:
                record_risk('approved' if entry else 'rejected', risk_input,
                            entry.model_dump(mode='json') if entry else {'reason': admission.reason})
            if entry is None:
                return {'status': 'rejected', 'reason': admission.reason or 'admission_failed'}
            if entry_block is not None and (reason := entry_block()):
                self._finish(entry, 'no_fill', reason)
                return {'status': 'skipped', 'reason': reason, 'cycle_id': entry.id}
            operations: dict[str, str] = {}
            self._send(entry, operations, 'enter', lambda key: self.broker.enter(entry, key))
            return self._advance(entry, operations, close=False)

    def reconcile(self, *, close: bool = False, contract_id: str | None = None,
                  cycle_id: str | None = None,
                  review: tuple[PublicationStore, Path] | None = None,
                  publications: PublicationStore | None = None,
                  record_risk: Callable[[str, dict[str, Any], object], None] | None = None,
                  manual: CloseIntent | None = None) -> dict[str, str]:
        with self.trades.exclusive() as acquired:
            if not acquired:
                return {'status': 'blocked', 'reason': 'executor_busy'}
            if manual is not None:
                completed = self.trades.completed_manual(manual, self.broker.account_id, self.broker.source)
                if completed:
                    return completed | {'cycle_id': manual.cycle_id, 'manual': 'accepted'}
            active = self.trades.active(self.broker.account_id)
            if not active:
                return {'status': 'idle', 'reason': ''}
            if len(active) != 1:
                return {'status': 'blocked', 'reason': 'conflicting_active_cycles'}
            row, = active
            if cycle_id is not None and row['id'] != cycle_id:
                return {'status': 'blocked', 'reason': 'sandbox_cycle_mismatch'}
            if row['source'] != self.broker.source:
                return {'status': 'blocked', 'reason': 'broker_source_mismatch'}
            entry = EntryReservation.model_validate_json(row['terms'])
            if contract_id is not None and contract_id != entry.contract_id:
                return {'status': 'blocked', 'reason': 'sandbox_selection_mismatch'}
            operations = self.trades.evidence(entry.id).operations
            if manual is not None:
                if self.trades.has_manual_close(entry.id, manual.request_id):
                    return self._advance(entry, operations, close=True) | {'manual': 'accepted'}
                if (self.broker.source not in SANDBOX_SOURCES or manual.cycle_id != entry.id
                        or manual.contract_id != entry.contract_id or manual.side != entry.side
                        or row['status'] != 'protected' or row['close_requested']
                        or close_requests(operations) or 'cancel_protection' in operations):
                    return self._reject_manual(entry, manual, 'close_position_changed')
                return self._confirm_manual_close(entry, operations, manual)
            if 'enter' not in operations:
                return self._finish(entry, 'no_fill', 'entry_never_submitted')
            if review is not None:
                risk_input: dict[str, Any] = {}
                if record_risk is not None:
                    record_risk('processing', risk_input, None)
                try:
                    self._record_reviewed_close(
                        entry, *review, risk_input=risk_input if record_risk is not None else None,
                    )
                except Rejected as error:
                    if record_risk is not None:
                        record_risk('rejected', risk_input, {'reason': str(error)})
                    return {'status': 'rejected', 'reason': str(error), 'cycle_id': entry.id}
                if record_risk is not None:
                    record_risk('approved', risk_input, None)
                close = True
            elif close:
                with self.events.transaction():
                    self.trades.set_close_requested(entry.id, True)
            elif row['close_requested']:
                close = True
                if 'cancel_protection' not in operations and not close_requests(operations):
                    approval = self.trades.pending_reviewed_close(entry.id)
                    if approval is not None:
                        close = False
                        if publications is not None:
                            try:
                                self._record_reviewed_close(entry, publications, Path(approval), persist=False)
                            except Rejected as error:
                                with self.events.transaction():
                                    self.trades.set_close_requested(entry.id, False)
                                    self.trades.append(entry.id, 'reviewed_close_abandoned',
                                                 approval=approval, reason=str(error))
                            else:
                                close = True
            return self._advance(entry, operations, close=close)

    def _record_reviewed_close(self, entry: EntryReservation, publications: PublicationStore, approval: Path, *,
                               persist: bool = True, risk_input: dict[str, Any] | None = None) -> None:
        proposal, proposal_id, formed_at, context = read_approval(approval)
        if risk_input is not None:
            risk_input['proposal'] = proposal.model_dump(mode='json')
        position = context.get('position')
        if (proposal.action != 'close' or proposal.contract_id != entry.contract_id or not position
                or position['id'] != entry.id or position['side'] != entry.side
                or position['contract_id'] != entry.contract_id
                or context['account']['id'] != entry.account_id):
            raise Rejected('close_position_mismatch')
        try:
            snapshot = RiskSnapshot.model_validate(self.broker.read_risk(entry.account_id, entry.contract_id))
        except Exception as error:
            raise Rejected('risk_data_unavailable_or_invalid') from error
        if risk_input is not None:
            risk_input['snapshot'] = snapshot.model_dump(mode='json')
        contract = snapshot.contract
        with checked_publication(publications, self.events):
            now = check_publication(self.events, publications, context, proposal, formed_at)
            if (snapshot.account_id != entry.account_id or contract.contract_id != entry.contract_id
                    or contract.company_id != proposal.company_id or snapshot.execution_uncertain
                    or not snapshot.has_position
                    or not any(row['contract_id'] == entry.contract_id and row['share_id'] == contract.share_id
                               for row in context['candidates'])
                    or not (contract.limit_sell_available if entry.side == 'long' else contract.limit_buy_available)
                    or contract.last_trade_at <= now
                    or not all(fresh(at, now, MAX_RISK_DATA_AGE)
                               for at in (snapshot.account_observed_at, contract.observed_at, contract.quote_at))):
                raise Rejected('close_market_or_position_unconfirmed')
            if persist:
                self.trades.set_close_requested(entry.id, True)
                self.trades.append(entry.id, 'reviewed_close', proposal_id=proposal_id, approval=str(approval.resolve()))

    def _log(self, entry: EntryReservation, kind: str, **details: Any) -> None:
        with self.events.transaction():
            self.trades.append(entry.id, kind, **details)

    def _send(
        self, entry: EntryReservation, operations: dict[str, str], action: str,
        submit: Callable[[str], str | None], **terms: Any,
    ) -> None:
        if action in operations and action not in ('cancel_protection', 'cancel_orders'):
            raise ValueError('operation_already_attempted')
        key = uuid5(UUID(entry.id), action).hex
        self._log(entry, 'intent', action=action, operation_id=key, **terms)
        operations[action] = key
        try:
            broker_id = submit(key)
        except SubmissionRejected as error:
            self._log(entry, 'response_rejected', action=action, operation_id=key, reason=str(error))
        except Exception:
            self._log(entry, 'response_uncertain', operation_id=key)
        else:
            self._log(entry, 'response_received', action=action, operation_id=key, broker_id=broker_id)

    def _finish(self, entry: EntryReservation, status: str, reason: str = '') -> dict[str, str]:
        with self.events.transaction():
            self.trades.finish(entry.id, status, reason)
            if status in ('closed', 'no_fill'):
                self.trades.release(entry)
        return {'status': status, 'reason': reason, 'cycle_id': entry.id}

    def _reject_manual(self, entry: EntryReservation, manual: CloseIntent, reason: str) -> dict[str, str]:
        self._log(entry, 'manual_close_rejected', request_id=manual.request_id, reason=reason)
        return {'status': 'rejected', 'reason': reason, 'cycle_id': entry.id}

    def _confirm_manual_close(self, entry: EntryReservation, operations: dict[str, str],
                              manual: CloseIntent) -> dict[str, str]:
        # Confirm the browser snapshot before recording any actionable close intent.
        evidence = self.trades.evidence(entry.id)
        try:
            state = TRADE_STATE.validate_python(self.broker.read_trade(entry, operations, evidence.receipts))
        except Exception:
            return self._reject_manual(entry, manual, 'broker_read_unavailable')
        fills = reconciled_fills(entry, state)
        active = tuple(stop for stop in state.protection if stop.status == 'active')
        if (not state.settled or state.active_orders or state.other_exposure or fills is None
                or fills[0] != manual.quantity or any(stop.status == 'executed' for stop in state.protection)
                or len(active) != 2 or len({stop.id for stop in state.protection}) != len(state.protection)
                or verified_protection(entry, manual.quantity, active) != {('sl', entry.stop_loss), ('tp', entry.take_profit)}
                or any(fills[1].get(order) != fill for order, fill in evidence.confirmed_fills.items())):
            self._log(entry, 'reconciliation', state=asdict(state))
            return self._reject_manual(entry, manual, 'close_position_changed')
        with self.events.transaction():
            self.trades.set_close_requested(entry.id, True)
            self.trades.append(entry.id, 'manual_close', request_id=manual.request_id,
                         quantity=manual.quantity, side=manual.side, contract_id=manual.contract_id)
        return self._advance(entry, operations, close=True, observation=state) | {'manual': 'accepted'}

    def _advance(
        self, entry: EntryReservation, operations: dict[str, str], *, close: bool,
        observation: TradeState | None = None,
    ) -> dict[str, str]:
        evidence = self.trades.evidence(entry.id)
        confirmed = evidence.confirmed_fills
        close_lots = evidence.close_lots
        closes = close_requests(operations)
        close_count = len(closes)
        close |= close_count > 0 or 'cancel_protection' in operations
        close_attempted = False
        cancel_attempted = False
        cancel_orders_attempted = False
        while True:
            receipts = self.trades.evidence(entry.id).receipts
            if observation is None:
                try:
                    observed = self.broker.read_trade(entry, dict(operations), receipts)
                except Exception:
                    return self._finish(entry, 'blocked', 'broker_read_unavailable')
                try:
                    state = TRADE_STATE.validate_python(observed)
                except ValidationError:
                    return self._finish(entry, 'blocked', 'invalid_broker_state')
            else:
                state, observation = observation, None
            self._log(entry, 'reconciliation', state=asdict(state))
            if self.broker.source == 'sandbox_mcp' and any(
                action not in receipts for action in ('enter', *(key for key, _ in close_requests(operations)))
            ):
                return self._finish(entry, 'blocked', 'mcp_order_submission_unconfirmed')
            active = tuple(stop for stop in state.protection if stop.status == 'active')
            if ((active and not operations.keys() & {'sl', 'tp', 'protect'})
                    or len({stop.id for stop in state.protection}) != len(state.protection)):
                return self._finish(entry, 'blocked', 'protection_ownership_unconfirmed')
            native_exit = any(stop.status == 'executed' for stop in state.protection)
            if not state.settled and not native_exit and (operations.keys() & {'sl', 'tp'}) - receipts.keys():
                return self._finish(entry, 'blocked', 'protection_submission_unconfirmed')
            fills = reconciled_fills(entry, state)
            if close and active and not native_exit and (
                not state.settled or state.other_exposure or state.active_orders
                or fills is None or any(fills[1].get(order_id) != fill for order_id, fill in confirmed.items())
            ):
                return self._finish(entry, 'blocked', 'unresolved_broker_state')
            if active and (native_exit or close) and not cancel_attempted:
                ids = tuple(stop.id for stop in active)
                cancel_attempted = True
                self._send(entry, operations, 'cancel_protection',
                           lambda key: self.broker.cancel_protection(entry, key, ids), stop_ids=ids)
                continue
            if state.active_orders and state.cancellable_orders and not active and not cancel_orders_attempted:
                order_ids = state.cancellable_orders
                cancel_orders_attempted = True
                self._send(entry, operations, 'cancel_orders',
                           lambda key: self.broker.cancel_orders(entry, key, order_ids), order_ids=order_ids)
                continue
            if not state.settled or state.active_orders or state.other_exposure:
                return self._finish(entry, 'blocked', 'unresolved_broker_state')
            if fills is None:
                return self._finish(entry, 'blocked', 'fills_and_position_disagree')
            remaining, current = fills
            if any(current.get(order_id) != fill for order_id, fill in confirmed.items()):
                return self._finish(entry, 'blocked', 'confirmed_fills_changed')
            if current != confirmed:
                self._log(entry, 'confirmed_fills', fills=current)
                confirmed = current
            expected = {('sl', entry.stop_loss), ('tp', entry.take_profit)}
            if not remaining and not active:
                return self._finish(entry, 'closed' if state.entry else 'no_fill')
            sign = 1 if entry.side == 'long' else -1
            safe_fill = (state.entry is not None
                         and sign * state.entry.price_points <= sign * entry.entry_price
                         and min(entry.stop_loss, entry.take_profit) < state.entry.price_points
                         < max(entry.stop_loss, entry.take_profit))
            if remaining and not close and safe_fill and 'cancel_protection' not in operations:
                verified = verified_protection(entry, remaining, active)
                if len(active) == 2 and verified == expected:
                    return self._finish(entry, 'protected')
                if not state.exits and not operations.keys() & {'sl', 'tp', 'protect'}:
                    self._send(entry, operations, 'sl',
                               lambda key: self.broker.protect(entry, key, remaining, 'sl'), lots=remaining)
                    continue
                if (not state.exits and 'sl' in operations and 'tp' not in operations
                        and len(active) == 1 and verified == {('sl', entry.stop_loss)}):
                    self._send(entry, operations, 'tp',
                               lambda key: self.broker.protect(entry, key, remaining, 'tp'), lots=remaining)
                    continue
            if active and not cancel_attempted:
                ids = tuple(stop.id for stop in active)
                cancel_attempted = True
                self._send(entry, operations, 'cancel_protection',
                           lambda key: self.broker.cancel_protection(entry, key, ids), stop_ids=ids)
            elif active or (close_lots is not None and remaining >= close_lots):
                return self._finish(entry, 'blocked', 'exit_or_cancellation_unconfirmed')
            elif close_attempted:
                return self._finish(entry, 'blocked', 'close_residual_pending')
            else:
                action = f'close:{close_count + 1}' if close_count else 'close'
                close_attempted = True
                close_lots = remaining
                self._send(entry, operations, action, lambda key: self.broker.close(entry, key, remaining),
                           lots=remaining, reason='control_exit' if close else 'protection_unconfirmed')
                close = True
