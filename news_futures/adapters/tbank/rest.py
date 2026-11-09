"""Sandbox REST adapter for separately submitted T-Invest stops."""

from datetime import datetime
from decimal import Decimal
from pathlib import Path
import ssl
from typing import Annotated, Any, Literal
from uuid import UUID

import httpx
from pydantic import BaseModel, Field

from news_futures.trading.broker import Fill, Protection, SubmissionRejected, TradeState
from news_futures.common.credentials import load_token
from news_futures.news.publications import utc_now
from news_futures.trading.terms import EntryReservation, RiskSnapshot
from news_futures.trading.journal import close_requests


SANDBOX_REST = 'https://sandbox-invest-public-api.tbank.ru/rest/tinkoff.public.invest.api.contract.v1.'
POINTS = 'PRICE_TYPE_POINT'
ORDER_STATUS = 'EXECUTION_REPORT_STATUS_'
STOP_STATUS = 'STOP_ORDER_STATUS_'
STOP_TYPES = {'sl': 'STOP_ORDER_TYPE_STOP_LOSS', 'tp': 'STOP_ORDER_TYPE_TAKE_PROFIT'}
STOP_VALIDATION_ERRORS = {'30035', '30036', '30037', '30038', '30104', '30105'}
READ_METHODS = {
    ('SandboxService', 'GetSandboxAccounts'), ('SandboxService', 'GetSandboxPositions'),
    ('SandboxService', 'GetSandboxPortfolio'), ('SandboxService', 'GetSandboxWithdrawLimits'),
    ('SandboxService', 'GetSandboxOrders'), ('SandboxService', 'GetSandboxOrderState'),
    ('SandboxService', 'GetSandboxStopOrders'), ('InstrumentsService', 'FutureBy'),
    ('MarketDataService', 'GetLastPrices'), ('MarketDataService', 'GetTradingStatus'),
}
READ_URLS = {f'{SANDBOX_REST}{service}/{method}' for service, method in READ_METHODS}


class Amount(BaseModel):
    units: Annotated[str, Field(pattern=r'^-?[0-9]+$')]
    nano: Annotated[int, Field(strict=True, gt=-1_000_000_000, lt=1_000_000_000)]
    currency: str | None = None


def amount(value: object, *, rubles: bool = False) -> Decimal:
    parsed = Amount.model_validate(value)
    if rubles and (parsed.currency or '').lower() != 'rub':
        raise ValueError('ruble_amount_required')
    return Decimal(parsed.units) + Decimal(parsed.nano).scaleb(-9)


def quotation(value: Decimal) -> dict[str, str | int]:
    units = int(value)
    return {'units': str(units), 'nano': int((value - units).scaleb(9))}


def integer(value: object) -> int:
    if not isinstance(value, str) or not value.lstrip('-').isascii() or not value.lstrip('-').isdigit():
        raise ValueError('invalid_broker_integer')
    return int(value)


def identifier(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError('invalid_broker_identifier')
    return value


def rows(payload: dict[str, Any], field: str) -> list[dict[str, Any]]:
    value = payload[field]
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError('invalid_broker_rows')
    return value


class TBankReader:
    source: Literal['sandbox'] = 'sandbox'

    def __init__(self, client: httpx.Client, account_id: str, contract_id: str, company_id: str | None):
        self.client = client
        self.account_id = account_id
        self.contract_id = contract_id
        self.company_id = company_id

    def call(self, method: str, *, service: str = 'SandboxService', account: bool = True,
             **args: Any) -> dict[str, Any]:
        if (service, method) not in READ_METHODS:
            raise ValueError('sandbox_read_method_required')
        return self._request(method, service=service, account=account, **args)

    def _request(self, method: str, *, service: str = 'SandboxService', account: bool = True,
                 **args: Any) -> dict[str, Any]:
        if account:
            args['accountId'] = self.account_id
        response = self.client.post(f'{SANDBOX_REST}{service}/{method}', json=args)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError('broker_object_required')
        return payload

    def stops(self) -> list[dict[str, Any]]:
        return rows(self.call('GetSandboxStopOrders', status=STOP_STATUS + 'ALL'), 'stopOrders')

    def positions(self) -> tuple[int, bool]:
        state = self.call('GetSandboxPositions')
        if state.get('accountId') != self.account_id or state['limitsLoadingInProgress'] is not False:
            raise ValueError('sandbox_positions_unconfirmed')
        foreign = bool(rows(state, 'securities') or rows(state, 'options'))
        lots = 0
        for position in rows(state, 'futures'):
            if position['instrumentUid'] != self.contract_id:
                foreign = True
            else:
                lots += integer(position['balance'])
                foreign |= integer(position['blocked']) != 0
        return lots, foreign

    def require_open_account(self) -> None:
        accounts = rows(self.call('GetSandboxAccounts', account=False), 'accounts')
        if not any(item['id'] == self.account_id and item['status'] == 'ACCOUNT_STATUS_OPEN' for item in accounts):
            raise ValueError('open_sandbox_account_required')

    def read_risk(self, account_id: str, contract_id: str) -> RiskSnapshot:
        if self.company_id is None or (account_id, contract_id) != (self.account_id, self.contract_id):
            raise ValueError('sandbox_selection_mismatch')
        self.require_open_account()
        future = self.call('FutureBy', service='InstrumentsService', account=False,
                           id=contract_id, idType='INSTRUMENT_ID_TYPE_UID')['instrument']
        tick = amount(future['minPriceIncrement'])
        tick_value = amount(future['minPriceIncrementAmount'])
        # shortcut: only trade lot-one RUB contracts with a verified one-to-one points conversion.
        if (future['uid'] != contract_id or future['currency'].lower() != 'rub' or future['assetType'] != 'TYPE_SECURITY'
                or type(future['lot']) is not int or future['lot'] != 1 or tick <= 0 or tick != tick_value):
            raise ValueError('unsupported_futures_units')
        last, = rows(self.call('GetLastPrices', service='MarketDataService', account=False,
                               instrumentId=[contract_id]), 'lastPrices')
        status = self.call('GetTradingStatus', service='MarketDataService', account=False, instrumentId=contract_id)
        if last['instrumentUid'] != contract_id or status['instrumentUid'] != contract_id:
            raise ValueError('market_contract_mismatch')
        price = amount(last['price'])
        available = (status['apiTradeAvailableFlag'] is True
                     and status['limitOrderAvailableFlag'] is True
                     and status['marketOrderAvailableFlag'] is True)
        lots, foreign = self.positions()
        portfolio = self.call('GetSandboxPortfolio')
        if portfolio['accountId'] != account_id:
            raise ValueError('sandbox_portfolio_mismatch')
        cash = self.call('GetSandboxWithdrawLimits')
        free = sum((amount(item, rubles=True) for item in rows(cash, 'money')
                    if item['currency'].lower() == 'rub'), Decimal(0))
        ordinary = rows(self.call('GetSandboxOrders'), 'orders')
        stops = self.stops()
        open_stops = any(item['status'] not in (STOP_STATUS + 'CANCELED', STOP_STATUS + 'EXPIRED',
                                               STOP_STATUS + 'EXECUTED') for item in stops)
        now = utc_now()
        ticker = future.get('ticker')
        return RiskSnapshot.model_validate({
            'account_id': account_id, 'account_observed_at': now, 'currency': 'RUB',
            'equity': amount(portfolio['totalAmountPortfolio'], rubles=True),
            'available_margin': max(Decimal(0), free), 'has_position': bool(lots) or foreign,
            'has_active_orders': bool(ordinary) or open_stops, 'execution_uncertain': False,
            'contract': {
                'contract_id': contract_id, 'company_id': self.company_id,
                'ticker': ticker if isinstance(ticker, str) and ticker.strip() else None,
                'share_id': future['basicAssetPositionUid'], 'observed_at': now,
                'quote_at': datetime.fromisoformat(last['time']),
                'last_trade_at': datetime.fromisoformat(future['lastTradeDate']),
                'last_price': price, 'currency': 'RUB',
                'price_unit': 'points', 'contracts_per_lot': 1, 'tick_size': tick,
                'tick_value_per_contract': tick_value,
                'margin_per_lot_buy': amount(future['initialMarginOnBuy'], rubles=True),
                'margin_per_lot_sell': amount(future['initialMarginOnSell'], rubles=True),
                'round_trip_cost_per_lot': (price * Decimal('0.001')).quantize(Decimal('0.01'), rounding='ROUND_UP'),
                'limit_buy_available': available and future['buyAvailableFlag'] is True,
                'limit_sell_available': available and future['sellAvailableFlag'] is True,
            },
        })

    def read_order(self, order_id: str, *, request: bool, side: str,
                   receipt: str | None = None) -> tuple[bool, Fill | None, str | None]:
        if request:
            order_id = str(UUID(order_id))
        try:
            row = self.call('GetSandboxOrderState', orderId=order_id, priceType=POINTS,
                            orderIdType='ORDER_ID_TYPE_REQUEST' if request else 'ORDER_ID_TYPE_EXCHANGE')
        except httpx.HTTPError:
            return False, None, None
        broker_id = identifier(row['orderId'])
        echoed_key = row.get('orderRequestId')
        if (row['instrumentUid'] != self.contract_id or row['direction'] != 'ORDER_DIRECTION_' + side
                or (echoed_key not in (None, '', order_id) if request else broker_id != order_id)
                or (receipt is not None and receipt != broker_id)):
            raise ValueError('order_identity_mismatch')
        lots = integer(row['lotsExecuted'])
        status = row['executionReportStatus']
        requested = integer(row['lotsRequested'])
        if not 0 <= lots <= requested or (status == ORDER_STATUS + 'FILL' and not lots):
            raise ValueError('invalid_filled_lots')
        fill = Fill(broker_id, lots, amount(row['averagePositionPrice'])) if lots else None
        terminal = status in {ORDER_STATUS + 'FILL', ORDER_STATUS + 'CANCELLED', ORDER_STATUS + 'REJECTED'}
        owned = not request or echoed_key == order_id or receipt == broker_id
        pending_id = broker_id if owned and status in {ORDER_STATUS + 'NEW', ORDER_STATUS + 'PARTIALLYFILL'} else None
        return terminal, fill, pending_id

    def read_trade(self, entry: EntryReservation, operations: dict[str, str],
                   receipts: dict[str, str | None]) -> TradeState:
        if (entry.account_id, entry.contract_id) != (self.account_id, self.contract_id):
            raise ValueError('sandbox_selection_mismatch')
        entry_side, exit_side = ('BUY', 'SELL') if entry.side == 'long' else ('SELL', 'BUY')
        stop_ids = {broker_id: key for key in ('sl', 'tp') if (broker_id := receipts.get(key)) is not None}
        protection = []
        children: list[str] = []
        seen = set()
        foreign = False
        settled = all(key in receipts for key in ('sl', 'tp') if key in operations)
        for row in self.stops():
            status = row['status']
            broker_id = row['stopOrderId']
            if broker_id not in stop_ids:
                foreign |= status not in {STOP_STATUS + 'CANCELED', STOP_STATUS + 'EXPIRED', STOP_STATUS + 'EXECUTED'}
                continue
            if row['instrumentUid'] != entry.contract_id or row['orderType'] != STOP_TYPES[stop_ids[broker_id]]:
                raise ValueError('stop_identity_mismatch')
            kind: Literal['sl', 'tp'] = 'sl' if stop_ids[broker_id] == 'sl' else 'tp'
            side: Literal['buy', 'sell'] = 'buy' if row['direction'] == 'STOP_ORDER_DIRECTION_BUY' else 'sell'
            if row['direction'] != 'STOP_ORDER_DIRECTION_' + side.upper():
                raise ValueError('unknown_stop_direction')
            phases: dict[str, Literal['active', 'canceled', 'executed', 'expired']] = {
                STOP_STATUS + 'ACTIVE': 'active', STOP_STATUS + 'CANCELED': 'canceled',
                STOP_STATUS + 'EXECUTED': 'executed', STOP_STATUS + 'EXPIRED': 'expired',
            }
            phase = phases[status]
            terms_confirmed = kind == 'sl' or (
                row.get('exchangeOrderType') == 'EXCHANGE_ORDER_TYPE_MARKET'
                and row.get('takeProfitType') == 'TAKE_PROFIT_TYPE_REGULAR'
            )
            protection.append(Protection(broker_id, kind, integer(row['lotsRequested']),
                                         amount(row['stopPrice']), side, phase, terms_confirmed))
            seen.add(broker_id)
            if phase == 'executed':
                child = row.get('exchangeOrderId')
                if not child:
                    settled = False
                else:
                    children.append(child)
        settled &= seen == set(stop_ids)
        known_entry, fill, pending_entry = self.read_order(operations['enter'], request=True, side=entry_side,
                                                           receipt=receipts.get('enter'))
        settled &= known_entry
        pending = {pending_entry} if pending_entry else set()
        exits = []
        requests: list[tuple[str, bool, str | None]] = [(child, False, None) for child in children]
        requests.extend((key, True, receipts.get(action)) for action, key in close_requests(operations))
        for order_id, request, receipt in requests:
            known, exit_fill, pending_id = self.read_order(order_id, request=request, side=exit_side, receipt=receipt)
            settled &= known
            if exit_fill is not None:
                exits.append(exit_fill)
            # A market close has already passed the stop and ordinary order barrier. Let it settle.
            if not request and pending_id is not None:
                pending.add(pending_id)
        ordinary = rows(self.call('GetSandboxOrders'), 'orders')
        active_ids = {identifier(order['orderId']) for order in ordinary}
        lots, foreign_position = self.positions()
        return TradeState(settled=settled, position_lots=lots, entry=fill, exits=tuple(exits),
                          protection=tuple(protection), active_orders=bool(ordinary),
                          other_exposure=foreign or foreign_position,
                          cancellable_orders=tuple(sorted(pending & active_ids)))

class TBankSandbox(TBankReader):
    def enter(self, entry: EntryReservation, operation_id: str) -> str:
        side = 'BUY' if entry.side == 'long' else 'SELL'
        response = self._request('PostSandboxOrder', orderId=str(UUID(operation_id)), instrumentId=entry.contract_id,
                             quantity=str(entry.lots), direction='ORDER_DIRECTION_' + side,
                             orderType='ORDER_TYPE_LIMIT', priceType=POINTS, price=quotation(entry.entry_price),
                             timeInForce='TIME_IN_FORCE_FILL_AND_KILL', confirmMarginTrade=True)
        return identifier(response['orderId'])

    def protect(self, entry: EntryReservation, operation_id: str, lots: int,
                kind: Literal['sl', 'tp']) -> str:
        side = 'SELL' if entry.side == 'long' else 'BUY'
        price = entry.stop_loss if kind == 'sl' else entry.take_profit
        options: dict[str, Any] = {'takeProfitType': 'TAKE_PROFIT_TYPE_REGULAR'} if kind == 'tp' else {}
        try:
            response = self._request('PostSandboxStopOrder', orderId=str(UUID(operation_id)), instrumentId=entry.contract_id,
                                 quantity=str(lots), direction='STOP_ORDER_DIRECTION_' + side,
                                 stopOrderType=STOP_TYPES[kind], stopPrice=quotation(price), priceType=POINTS,
                                 expirationType='STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL',
                                 exchangeOrderType='EXCHANGE_ORDER_TYPE_MARKET', confirmMarginTrade=True, **options)
        except httpx.HTTPStatusError as error:
            if error.response.status_code == 400:
                body = error.response.json()
                if isinstance(body, dict) and body.get('code') == 3 and body.get('description') in STOP_VALIDATION_ERRORS:
                    raise SubmissionRejected(body['description']) from error
            raise
        return identifier(response['stopOrderId'])

    def cancel_protection(self, entry: EntryReservation, operation_id: str, ids: tuple[str, ...]) -> None:
        self.cancel_all('CancelSandboxStopOrder', 'stopOrderId', ids)

    def cancel_orders(self, entry: EntryReservation, operation_id: str, ids: tuple[str, ...]) -> None:
        self.cancel_all('CancelSandboxOrder', 'orderId', ids, orderIdType='ORDER_ID_TYPE_EXCHANGE')

    def cancel_all(self, method: str, id_field: str, ids: tuple[str, ...], **options: str) -> None:
        uncertain = False
        for broker_id in ids:
            args: dict[str, Any] = {id_field: broker_id, **options}
            try:
                self._request(method, **args)
            except (httpx.HTTPError, ValueError):
                uncertain = True
        if uncertain:
            raise ValueError('cancellation_unconfirmed')

    def close(self, entry: EntryReservation, operation_id: str, lots: int) -> str:
        side = 'SELL' if entry.side == 'long' else 'BUY'
        response = self._request('PostSandboxOrder', orderId=str(UUID(operation_id)), instrumentId=entry.contract_id,
                             quantity=str(lots), direction='ORDER_DIRECTION_' + side,
                             orderType='ORDER_TYPE_MARKET', priceType=POINTS, confirmMarginTrade=True)
        return identifier(response['orderId'])


def restrict_to_reads(request: httpx.Request) -> None:
    if request.method != 'POST' or str(request.url) not in READ_URLS:
        raise ValueError('sandbox_read_method_required')


def sandbox_client(ca_file: Path | None, *, read_only: bool = False) -> httpx.Client:
    tls = ssl.create_default_context(cafile=str(ca_file)) if ca_file else True
    headers = {'Authorization': f"Bearer {load_token('sandbox', 'token')}"}
    return httpx.Client(verify=tls, headers=headers, timeout=8, trust_env=False, follow_redirects=False,
                        event_hooks={'request': [restrict_to_reads]} if read_only else None)
