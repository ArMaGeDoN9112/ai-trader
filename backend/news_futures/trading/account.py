"""Read the configured sandbox account and reconcile its active cycle."""

from collections.abc import Callable
from typing import Any

from news_futures.dataflows.catalog import catalog_rows, load_catalog
from news_futures.brokers.contracts import (
    BrokerReader, MarketReading, NoSupportedFutures, SandboxBroker, UnsupportedFuturesUnits,
)
from news_futures.storage.contracts import EventStore
from news_futures.trading.executor import SandboxExecutor
from news_futures.common.errors import RuntimeErrorInfo
from news_futures.schemas.company import Account, Instrument, Market, MarketObservation, OpenPosition, Snapshot
from news_futures.trading.execution import TRADE_STATE, reconciled_fills, verified_protection
from news_futures.dataflows.publications import utc_now
from news_futures.trading.terms import EntryReservation, RiskSnapshot


class BrokerAccount:
    def __init__(self, account_id: str, reader: Callable[[str, str | None], BrokerReader]):
        self.account_id = account_id
        self.reader = reader

    def reconcile(self, events: EventStore, broker: Callable[[str, str | None], SandboxBroker]) -> dict[str, str] | None:
        active = events.trades.active(self.account_id)
        if len(active) == 1:
            entry = EntryReservation.model_validate_json(active[0]['terms'])
            contract_id = entry.contract_id
            return SandboxExecutor(events, broker(contract_id, None)).reconcile(
                contract_id=contract_id, cycle_id=entry.id,
            )
        return None

    def position(self, events: EventStore) -> tuple[OpenPosition | None, dict[str, Any]]:
        active = events.trades.active(self.account_id)
        position = None
        protection: dict[str, Any] = {'status': 'none'}
        if active:
            if len(active) != 1:
                raise RuntimeErrorInfo('Сверьте конфликтующие циклы счёта.')
            trade = active[0]
            entry = EntryReservation.model_validate_json(trade['terms'])
            evidence = events.trades.evidence(entry.id)
            if 'enter' not in evidence.operations:
                raise RuntimeErrorInfo('Сверьте незавершённый резерв счёта.')
            observation = self.reader(entry.contract_id, None).read_trade(entry, evidence.operations, evidence.receipts)
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
        else:
            has_position, has_orders = self.reader('', None).account_activity()
            if has_position or has_orders:
                raise RuntimeErrorInfo('На счёте есть неподтверждённая позиция или заявки. Сверьте брокера.')
        return position, protection

    def observe(self, catalog: bytes, contract_id: str, company_id: str) -> MarketReading:
        companies = load_catalog(catalog)
        listed = {(row.company_id, row.share_id) for row in catalog_rows(catalog)}
        reader = self.reader(contract_id, company_id)
        try:
            risk = RiskSnapshot.model_validate(reader.read_risk(self.account_id, contract_id))
        except UnsupportedFuturesUnits as error:
            raise NoSupportedFutures('no_supported_futures') from error
        contract = risk.contract
        if (risk.account_id != self.account_id or contract.contract_id != contract_id
                or contract.company_id != company_id or (company_id, contract.share_id) not in listed
                or len({contract_id, company_id, contract.share_id}) != 3):
            raise RuntimeErrorInfo('market_contract_mismatch')
        if risk.execution_uncertain or risk.has_position or risk.has_active_orders:
            raise RuntimeErrorInfo('На счёте есть неподтверждённая позиция или заявки. Сверьте брокера.')
        if contract.last_trade_at <= utc_now():
            raise NoSupportedFutures('no_supported_futures')
        order_book = reader.read_order_book()
        received_at = utc_now()
        observation = MarketObservation(
            contract_id=contract_id, market=Market(observed_at=contract.quote_at, last_price=float(contract.last_price),
                                                  order_book=order_book, trading_status=contract.trading_status,
                                                  trading_status_observed_at=contract.observed_at if contract.trading_status else None),
            received_at=received_at, price_unit=contract.price_unit, limit_buy_available=contract.limit_buy_available,
            limit_sell_available=contract.limit_sell_available,
        )
        account = Account(id=risk.account_id, observed_at=risk.account_observed_at, currency=risk.currency,
                          equity=float(risk.equity), available_margin=float(risk.available_margin))
        instrument = Instrument(
            company_id=company_id, company_name=companies[company_id]['names'][0],
            contract_id=contract_id, share_id=contract.share_id, allowed_for_entry=True,
            market=observation.market, market_observation_id=observation.id,
        )
        return MarketReading(Snapshot(account=account, position=None, instruments=[instrument]), observation)
