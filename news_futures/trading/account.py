"""Read the configured sandbox account and reconcile its active cycle."""

from collections.abc import Callable
from typing import Any

from news_futures.trading.broker import (
    TRADE_STATE, BrokerReader, SandboxBroker, reconciled_fills, verified_protection,
)
from news_futures.agents.company import Account, Instrument, Market, OpenPosition, Snapshot
from news_futures.agents.events import EventArchive
from news_futures.agents.routing import catalog_rows, load_catalog
from news_futures.news.publications import Archive, utc_now
from news_futures.trading.terms import EntryReservation, RiskSnapshot
from news_futures.common.errors import RuntimeErrorInfo
from news_futures.trading.executor import SandboxExecutor
from news_futures.trading.journal import active_cycles, read_evidence


class BrokerAccount:
    def __init__(self, account_id: str, contracts: dict[str, str], reader: Callable[[str], BrokerReader]):
        self.account_id = account_id
        self.contracts = contracts
        self.reader = reader

    def reconcile(self, events: EventArchive, broker: Callable[[str], SandboxBroker],
                  publications: Archive | None) -> dict[str, str] | None:
        active = active_cycles(events.db, self.account_id)
        if len(active) == 1:
            entry = EntryReservation.model_validate_json(active[0]['terms'])
            contract_id = entry.contract_id
            if contract_id in self.contracts:
                return SandboxExecutor(events, broker(contract_id)).reconcile(
                    contract_id=contract_id, cycle_id=entry.id, publications=publications,
                )
        return None

    def read_markets(self, catalog: bytes) -> list[RiskSnapshot]:
        listed = {(row.company_id, row.share_id) for row in catalog_rows(catalog)}
        readings = []
        for contract_id, company_id in self.contracts.items():
            risk = RiskSnapshot.model_validate(self.reader(contract_id).read_risk(self.account_id, contract_id))
            contract = risk.contract
            if (risk.account_id != self.account_id or contract.contract_id != contract_id
                    or contract.company_id != company_id or (company_id, contract.share_id) not in listed
                    or len({contract_id, company_id, contract.share_id}) != 3 or contract.last_trade_at <= utc_now()):
                raise RuntimeErrorInfo('Сверьте счёт, фьючерсы и UID их базовых акций в contracts и catalog.')
            readings.append(risk)
        return readings

    def snapshot(self, events: EventArchive, catalog: bytes) -> tuple[Snapshot, dict[str, Any], list[RiskSnapshot]]:
        active = active_cycles(events.db, self.account_id)
        position = None
        protection: dict[str, Any] = {'status': 'none'}
        if active:
            if len(active) != 1:
                raise RuntimeErrorInfo('Сверьте конфликтующие циклы счёта.')
            trade = active[0]
            entry = EntryReservation.model_validate_json(trade['terms'])
            if entry.contract_id not in self.contracts:
                raise RuntimeErrorInfo('Верните контракт открытого цикла в contracts и сверьте счёт.')
            evidence = read_evidence(events.db, entry.id)
            if 'enter' not in evidence.operations:
                raise RuntimeErrorInfo('Сверьте незавершённый резерв счёта.')
            observation = self.reader(entry.contract_id).read_trade(entry, evidence.operations, evidence.receipts)
            state = TRADE_STATE.validate_python(observation)
            fills = reconciled_fills(entry, state)
            if (not state.settled or state.other_exposure or state.active_orders or fills is None
                or any(fills[1].get(order_id) != fill for order_id, fill in evidence.confirmed_fills.items())
                or len({stop.id for stop in state.protection}) != len(state.protection)
            ):
                raise RuntimeErrorInfo('Сверьте неподтверждённую позицию или заявки брокера.')
            remaining, _ = fills
            if remaining:
                position = OpenPosition(id=entry.id, contract_id=entry.contract_id,
                                        side=entry.side, quantity=remaining)
                stops = tuple(stop for stop in state.protection if stop.status == 'active')
                protected = (trade['status'] == 'protected' and len(stops) == 2
                             and verified_protection(entry, remaining, stops)
                             == {('sl', entry.stop_loss), ('tp', entry.take_profit)})
                protection = {'status': 'confirmed' if protected else 'unconfirmed',
                              'active_ids': [stop.id for stop in stops]}
        companies = load_catalog(catalog)
        instruments = []
        account = None
        readings = self.read_markets(catalog)
        for risk in readings:
            contract = risk.contract
            if risk.execution_uncertain or risk.has_position != bool(position) or (
                not position and risk.has_active_orders
            ):
                raise RuntimeErrorInfo('На счёте есть неподтверждённая позиция или заявки. Сверьте брокера.')
            account = Account(id=risk.account_id, observed_at=risk.account_observed_at, currency=risk.currency,
                              equity=float(risk.equity), available_margin=float(risk.available_margin))
            instruments.append(Instrument(
                company_id=contract.company_id, company_name=companies[contract.company_id]['names'][0],
                contract_id=contract.contract_id, share_id=contract.share_id, allowed_for_entry=True,
                market=Market(observed_at=contract.quote_at, last_price=float(contract.last_price)),
            ))
        assert account is not None
        return Snapshot(account=account, position=position, instruments=instruments), protection, readings
