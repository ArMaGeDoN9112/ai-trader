from contextlib import closing
from datetime import timedelta
from decimal import Decimal
from itertools import count
import json
from typing import Any
from unittest.mock import patch
from uuid import UUID

import httpx
from tests.support.risk import RiskFixture

from news_futures.storage.events import EventArchive
from news_futures.storage.publications import Archive
from news_futures.trading.executor import SandboxExecutor
from news_futures.storage.trades import trade_journal
from news_futures.brokers.tbank import TBankSandbox, quotation, restrict_to_reads
from news_futures.brokers.mcp_orders import CANCEL, CANCEL_STOP, CREATE, FIELDS, STOP, TBankMCPSandbox


class TBankExecutionTest(RiskFixture):
    def test_separate_native_requests_protect_actual_fill_and_close(self) -> None:
        self.run_cycle('normal')

    def test_explicit_tp_rejection_cancels_sl_before_emergency_close(self) -> None:
        self.run_cycle('reject_tp')

    def test_partial_close_is_reconciled_before_submitting_only_the_residual(self) -> None:
        self.run_cycle('partial_close')

    def test_lost_entry_response_is_reconciled_by_request_key_without_resubmission(self) -> None:
        self.run_cycle('lost_entry')

    def test_active_partial_entry_remainder_is_canceled_before_protecting(self) -> None:
        self.run_cycle('active_entry')

    def test_unidentified_active_order_is_not_canceled(self) -> None:
        self.run_cycle('unowned_entry')

    def test_pending_market_close_is_allowed_to_finish(self) -> None:
        self.run_cycle('pending_close')

    def test_stop_activating_during_close_is_reconciled_before_closing_residual(self) -> None:
        self.run_cycle('native_race')

    def test_two_activated_stops_block_on_reverse_position_without_sending_another_exit(self) -> None:
        self.run_cycle('dual_race')

    def test_mcp_submits_and_closes_with_exchange_receipts(self) -> None:
        self.run_cycle('mcp')

    def test_lost_mcp_entry_receipt_blocks_without_resubmission(self) -> None:
        self.run_cycle('mcp_lost_entry')

    def test_mcp_cancels_confirmed_entry_remainder_before_stops(self) -> None:
        self.run_cycle('mcp_active_entry')

    def run_cycle(self, scenario: str) -> None:
        approval = self.approved()
        mcp = scenario.startswith('mcp')
        partial_close = scenario == 'partial_close'
        native_race = scenario == 'native_race'
        positions = 0
        writes: list[tuple[str, dict[str, Any]]] = []
        mcp_writes: list[str] = []
        next_key = count(1)
        orders: dict[str, dict[str, Any]] = {}
        stops: dict[str, dict[str, Any]] = {}
        one = {'units': '1', 'nano': 0}
        price = {'units': '300', 'nano': 0}
        cash = {'units': '100000', 'nano': 0, 'currency': 'rub'}
        margin = {'units': '1000', 'nano': 0, 'currency': 'rub'}

        def reply(request: httpx.Request) -> httpx.Response:
            nonlocal positions
            self.assertEqual(request.url.host, 'sandbox-invest-public-api.tbank.ru')
            method = request.url.path.rsplit('/', 1)[1]
            args = json.loads(request.content)
            if method.startswith(('Post', 'Cancel')):
                writes.append((method, args))
            if method.startswith('Post') or args.get('orderIdType') == 'ORDER_ID_TYPE_REQUEST':
                self.assertEqual(args['orderId'], str(UUID(args['orderId'])))
            if method == 'PostSandboxOrder':
                entering = args['orderType'] == 'ORDER_TYPE_LIMIT'
                if entering:
                    self.assertEqual(positions, 0)
                else:
                    self.assertEqual(int(args['quantity']), positions)
                if not entering:
                    self.assertTrue(all(stop['status'] in ('STOP_ORDER_STATUS_CANCELED', 'STOP_ORDER_STATUS_EXECUTED')
                                        for stop in stops.values()))
                filled = (3 if partial_close or native_race else 1) if entering else (1 if partial_close else positions)
                if not entering and scenario == 'pending_close':
                    filled = 0
                positions = filled if entering else positions - filled
                order_id = 'entry' if entering else f'exit-{len(orders)}'
                orders[args['orderId']] = {
                    'orderId': order_id, 'orderRequestId': args['orderId'], 'direction': args['direction'],
                    'instrumentUid': self.contract_id, 'lotsExecuted': str(filled), 'lotsRequested': args['quantity'],
                    'executionReportStatus': ('EXECUTION_REPORT_STATUS_CANCELLED' if filled < int(args['quantity'])
                                              else 'EXECUTION_REPORT_STATUS_FILL'),
                    'averagePositionPrice': price,
                }
                if entering and scenario in ('active_entry', 'unowned_entry', 'mcp_active_entry'):
                    orders[args['orderId']]['executionReportStatus'] = 'EXECUTION_REPORT_STATUS_PARTIALLYFILL'
                if entering and scenario == 'unowned_entry':
                    del orders[args['orderId']]['orderRequestId']
                if entering and scenario in ('lost_entry', 'unowned_entry'):
                    raise httpx.ReadTimeout('entry accepted', request=request)
                if not entering and scenario == 'pending_close':
                    orders[args['orderId']]['executionReportStatus'] = 'EXECUTION_REPORT_STATUS_NEW'
                return httpx.Response(200, json={'orderId': order_id})
            if method == 'PostSandboxStopOrder':
                self.assertEqual(int(args['quantity']), positions)
                stop_id = args['stopOrderType']
                if scenario == 'reject_tp' and stop_id == 'STOP_ORDER_TYPE_TAKE_PROFIT':
                    return httpx.Response(400, json={'code': 3, 'description': '30035'})
                stops[stop_id] = {
                    'stopOrderId': stop_id, 'orderType': args['stopOrderType'], 'direction': args['direction'],
                    'instrumentUid': self.contract_id, 'stopPrice': args['stopPrice'],
                    'lotsRequested': args['quantity'], 'status': 'STOP_ORDER_STATUS_ACTIVE',
                    'exchangeOrderType': args['exchangeOrderType'], 'takeProfitType': args.get('takeProfitType'),
                }
                return httpx.Response(200, json={'stopOrderId': stop_id})
            if method == 'CancelSandboxStopOrder':
                stop = stops[args['stopOrderId']]
                if scenario == 'dual_race' or (native_race and stop['orderType'] == 'STOP_ORDER_TYPE_STOP_LOSS'):
                    child_id = 'child-' + args['stopOrderId']
                    stop.update(status='STOP_ORDER_STATUS_EXECUTED', exchangeOrderId=child_id)
                    positions -= 1
                    orders[child_id] = {
                        'orderId': child_id, 'direction': 'ORDER_DIRECTION_SELL', 'instrumentUid': self.contract_id,
                        'lotsExecuted': '1', 'lotsRequested': stop['lotsRequested'], 'averagePositionPrice': price,
                        'executionReportStatus': ('EXECUTION_REPORT_STATUS_PARTIALLYFILL' if native_race
                                                  else 'EXECUTION_REPORT_STATUS_FILL'),
                    }
                    return httpx.Response(400, json={'code': 3})
                stop['status'] = 'STOP_ORDER_STATUS_CANCELED'
                return httpx.Response(200, json={})
            if method == 'GetSandboxOrderState':
                expected = 'EXCHANGE' if mcp or args['orderId'].startswith('child-') else 'REQUEST'
                self.assertEqual(args['orderIdType'], 'ORDER_ID_TYPE_' + expected)
                if mcp:
                    return httpx.Response(200, json=next(
                        row for row in orders.values() if row['orderId'] == args['orderId']
                    ))
                return httpx.Response(200, json=orders[args['orderId']])
            if method == 'CancelSandboxOrder':
                self.assertEqual(args['orderIdType'], 'ORDER_ID_TYPE_EXCHANGE')
                expected_id = 'child-STOP_ORDER_TYPE_STOP_LOSS' if native_race else 'entry'
                self.assertEqual(args['orderId'], expected_id)
                order = next(row for row in orders.values() if row['orderId'] == expected_id)
                order['executionReportStatus'] = 'EXECUTION_REPORT_STATUS_CANCELLED'
                return httpx.Response(200, json={})
            data = {
                'GetSandboxAccounts': {'accounts': [{'id': self.account_id, 'status': 'ACCOUNT_STATUS_OPEN'}]},
                'FutureBy': {'instrument': {
                    'ticker': 'SRZ6', 'classCode': 'SPBFUT',
                    'uid': self.contract_id, 'lot': 1, 'currency': 'rub', 'assetType': 'TYPE_SECURITY',
                    'basicAssetPositionUid': 'sber-share', 'lastTradeDate': (self.now + timedelta(days=1)).isoformat(),
                    'minPriceIncrement': one, 'minPriceIncrementAmount': one,
                    'initialMarginOnBuy': margin, 'initialMarginOnSell': margin,
                    'buyAvailableFlag': True, 'sellAvailableFlag': True,
                }},
                'GetLastPrices': {'lastPrices': [
                    {'instrumentUid': self.contract_id, 'price': price, 'time': self.now.isoformat()},
                ]},
                'GetTradingStatus': {'instrumentUid': self.contract_id, 'apiTradeAvailableFlag': True,
                                     'tradingStatus': 'SECURITY_TRADING_STATUS_NORMAL_TRADING',
                                     'limitOrderAvailableFlag': True, 'marketOrderAvailableFlag': True},
                'GetSandboxPositions': {
                    'accountId': self.account_id, 'limitsLoadingInProgress': False,
                    'securities': [], 'options': [], 'futures': [
                        {'instrumentUid': self.contract_id, 'balance': str(positions), 'blocked': '0'},
                    ] if positions else [],
                },
                'GetSandboxPortfolio': {'accountId': self.account_id, 'totalAmountPortfolio': cash},
                'GetSandboxWithdrawLimits': {'money': [cash]},
                'GetSandboxOrders': {'orders': [order for order in orders.values()
                                               if order['executionReportStatus'] in ('EXECUTION_REPORT_STATUS_NEW',
                                                                                    'EXECUTION_REPORT_STATUS_PARTIALLYFILL')]},
                'GetSandboxStopOrders': {'stopOrders': list(stops.values())},
            }
            return httpx.Response(200, json=data[method])

        def mcp_reply(request: httpx.Request) -> httpx.Response:
            self.assertEqual(str(request.url), 'https://sandbox-invest-public-api.tbank.ru/mcp')
            message = json.loads(request.content)
            if 'id' not in message:
                return httpx.Response(202)
            if message['method'] == 'initialize':
                result: Any = {'protocolVersion': message['params']['protocolVersion'],
                               'capabilities': {'tools': {}}, 'serverInfo': {'name': 'control', 'version': '1'}}
            elif message['method'] == 'tools/list':
                result = {'tools': [{'name': name, 'inputSchema': {
                    'type': 'object', 'properties': dict.fromkeys(fields, {}),
                }, 'outputSchema': {'type': 'object', 'properties': {key: {'type': 'string'}}}}
                    for name, (fields, key) in FIELDS.items()]}
            else:
                self.assertEqual(message['method'], 'tools/call')
                name, args = (message['params'][key] for key in ('name', 'arguments'))
                mcp_writes.append(name)
                self.assertNotIn('orderId', args if name == CREATE else {})
                if name in (CREATE, STOP):
                    self.assertEqual(args['instrumentId'], 'SRZ6_SPBFUT')
                    self.assertEqual(args['priceType'], 'PRICE_TYPE_POINT')
                    self.assertIs(type(args['quantity']), int)
                    args.update(instrumentId=self.contract_id, quantity=str(args['quantity']),
                                orderId=str(UUID(int=next(next_key))))
                    for field in ('price', 'stopPrice'):
                        if field in args:
                            args[field] = quotation(Decimal(args[field]['value']))
                    if name == CREATE and args['orderType'] == 'ORDER_TYPE_LIMIT':
                        self.assertEqual(args['timeInForce'], 'TIME_IN_FORCE_FILL_AND_KILL')
                method = {CREATE: 'PostSandboxOrder', STOP: 'PostSandboxStopOrder',
                          CANCEL: 'CancelSandboxOrder', CANCEL_STOP: 'CancelSandboxStopOrder'}[name]
                if name == CANCEL:
                    args['orderIdType'] = 'ORDER_ID_TYPE_EXCHANGE'
                response = reply(httpx.Request('POST', f'https://sandbox-invest-public-api.tbank.ru/{method}', json=args))
                if scenario == 'mcp_lost_entry':
                    return httpx.Response(502)
                result = {'content': [], 'structuredContent': response.json() or {'time': self.now.isoformat()}}
            return httpx.Response(200, json={'jsonrpc': '2.0', 'id': message['id'], 'result': result})

        archive = Archive(self.publications_db)
        with (httpx.Client(transport=httpx.MockTransport(reply),
                           event_hooks={'request': [restrict_to_reads]} if mcp else None) as client,
              EventArchive(self.events_db) as events, closing(archive.db)):
            broker: TBankSandbox | TBankMCPSandbox
            if mcp:
                async_client = httpx.AsyncClient
                self.enterContext(patch('news_futures.brokers.mcp_orders.httpx.AsyncClient',
                                        side_effect=lambda **kw: async_client(transport=httpx.MockTransport(mcp_reply), **kw)))
                self.enterContext(patch('news_futures.brokers.mcp_orders.load_token', return_value='test-token'))
                broker = TBankMCPSandbox(client, self.account_id, self.contract_id, 'sber', None)
                broker.prepare()
                self.assertEqual(mcp_writes, [])
            else:
                broker = TBankSandbox(client, self.account_id, self.contract_id, 'sber')
            executor = SandboxExecutor(events, broker)
            result = executor.start(archive, approval, allowed_contracts={self.contract_id},
                                    allow_independent_stops=True)
            if scenario == 'reject_tp':
                self.assertEqual(result['status'], 'closed', result)
            elif scenario in ('unowned_entry', 'mcp_lost_entry'):
                self.assertEqual(result['reason'], 'mcp_order_submission_unconfirmed'
                                 if mcp else 'unresolved_broker_state')
                self.assertEqual(executor.reconcile()['status'], 'blocked')
                self.assertEqual([method for method, _ in writes], ['PostSandboxOrder'])
            else:
                self.assertEqual(result['status'], 'protected', result)
                self.assertEqual(executor.reconcile(close=True, contract_id='other')['reason'],
                                 'sandbox_selection_mismatch')
                self.assertEqual(executor.reconcile(contract_id=self.contract_id)['status'], 'protected')
                result = executor.reconcile(close=True)
                if scenario == 'pending_close':
                    self.assertEqual(result['reason'], 'unresolved_broker_state')
                    self.assertEqual(executor.reconcile()['reason'], 'unresolved_broker_state')
                    order = next(row for row in orders.values() if row['orderId'].startswith('exit-'))
                    order.update(executionReportStatus='EXECUTION_REPORT_STATUS_FILL', lotsExecuted='1')
                    positions = 0
                    result = executor.reconcile()
                if partial_close:
                    self.assertEqual(result['reason'], 'close_residual_pending')
                    self.assertEqual(positions, 2)
                    self.assertEqual(executor.reconcile()['reason'], 'close_residual_pending')
                    result = executor.reconcile()
                if scenario == 'dual_race':
                    self.assertEqual(result['reason'], 'fills_and_position_disagree')
                    self.assertEqual(positions, -1)
                    self.assertEqual(executor.reconcile()['status'], 'blocked')
                else:
                    self.assertEqual(result['status'], 'closed', result)
            cycle, = trade_journal(events.db)
            self.assertEqual(cycle['status'], 'blocked' if scenario in (
                'unowned_entry', 'dual_race', 'mcp_lost_entry',
            ) else 'closed')
            if mcp:
                self.assertEqual(cycle['source'], 'sandbox_mcp')
                expected = [CREATE] if scenario == 'mcp_lost_entry' else [
                    CREATE, *([CANCEL] if scenario == 'mcp_active_entry' else []),
                    STOP, STOP, CANCEL_STOP, CANCEL_STOP, CREATE,
                ]
                self.assertEqual(mcp_writes, expected)

        stop_requests = [args for method, args in writes if method == 'PostSandboxStopOrder']
        if scenario in ('unowned_entry', 'mcp_lost_entry'):
            self.assertEqual(stop_requests, [])
            return
        self.assertEqual([(args['stopOrderType'], args['stopPrice']['units']) for args in stop_requests],
                         [('STOP_ORDER_TYPE_STOP_LOSS', '290'), ('STOP_ORDER_TYPE_TAKE_PROFIT', '320')])
        self.assertEqual({args['quantity'] for args in stop_requests}, {'3' if partial_close or native_race else '1'})
        self.assertEqual(len({args['orderId'] for args in stop_requests}), 2)
        self.assertEqual(sum(method == 'PostSandboxOrder' and args['orderType'] == 'ORDER_TYPE_LIMIT'
                             for method, args in writes), 1)
        if scenario == 'active_entry':
            self.assertEqual([method for method, _ in writes[:3]],
                             ['PostSandboxOrder', 'CancelSandboxOrder', 'PostSandboxStopOrder'])
        if native_race:
            self.assertEqual([method for method, _ in writes[-4:]],
                             ['CancelSandboxStopOrder', 'CancelSandboxStopOrder', 'CancelSandboxOrder',
                              'PostSandboxOrder'])
            self.assertEqual(writes[-1][1]['quantity'], '2')
        if scenario == 'dual_race':
            self.assertEqual(sum(method == 'PostSandboxOrder' for method, _ in writes), 1)
        if scenario == 'pending_close':
            self.assertNotIn('CancelSandboxOrder', [method for method, _ in writes])
