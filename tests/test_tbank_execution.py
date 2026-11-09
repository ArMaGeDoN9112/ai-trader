from contextlib import closing
from datetime import timedelta
import json
from typing import Any
from uuid import UUID

import httpx
import test_risk

from news_futures.events import EventArchive
from news_futures.publications import Archive
from news_futures.sandbox_execution import SandboxExecutor, trade_journal
from news_futures.tbank_execution import TBankSandbox


class TBankExecutionTest(test_risk.RiskFixture):
    def test_separate_native_requests_protect_actual_fill_and_close(self) -> None:
        self.run_cycle(reject_tp=False)

    def test_explicit_tp_rejection_cancels_sl_before_emergency_close(self) -> None:
        self.run_cycle(reject_tp=True)

    def run_cycle(self, *, reject_tp: bool) -> None:
        approval = self.approved()
        positions = 0
        writes: list[tuple[str, dict[str, Any]]] = []
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
            if method.startswith('Post') or method == 'GetSandboxOrderState':
                self.assertEqual(args['orderId'], str(UUID(args['orderId'])))
            if method == 'PostSandboxOrder':
                entering = args['orderType'] == 'ORDER_TYPE_LIMIT'
                self.assertEqual(positions, 0 if entering else 1)
                if not entering:
                    self.assertTrue(all(stop['status'] == 'STOP_ORDER_STATUS_CANCELED'
                                        for stop in stops.values()))
                positions = 1 if entering else 0
                order_id = 'entry' if entering else 'exit'
                orders[args['orderId']] = {
                    'orderId': order_id, 'orderRequestId': args['orderId'], 'direction': args['direction'],
                    'instrumentUid': self.contract_id, 'lotsExecuted': '1', 'lotsRequested': args['quantity'],
                    'executionReportStatus': ('EXECUTION_REPORT_STATUS_CANCELLED' if entering
                                              else 'EXECUTION_REPORT_STATUS_FILL'),
                    'averagePositionPrice': price,
                }
                return httpx.Response(200, json={'orderId': order_id})
            if method == 'PostSandboxStopOrder':
                self.assertEqual(positions, 1)
                stop_id = args['stopOrderType']
                if reject_tp and stop_id == 'STOP_ORDER_TYPE_TAKE_PROFIT':
                    return httpx.Response(400, json={'code': 3, 'description': '30035'})
                stops[stop_id] = {
                    'stopOrderId': stop_id, 'orderType': args['stopOrderType'], 'direction': args['direction'],
                    'instrumentUid': self.contract_id, 'stopPrice': args['stopPrice'],
                    'lotsRequested': args['quantity'], 'status': 'STOP_ORDER_STATUS_ACTIVE',
                    'exchangeOrderType': args['exchangeOrderType'], 'takeProfitType': args.get('takeProfitType'),
                }
                return httpx.Response(200, json={'stopOrderId': stop_id})
            if method == 'CancelSandboxStopOrder':
                stops[args['stopOrderId']]['status'] = 'STOP_ORDER_STATUS_CANCELED'
                return httpx.Response(200, json={})
            if method == 'GetSandboxOrderState':
                self.assertEqual(args['orderIdType'], 'ORDER_ID_TYPE_REQUEST')
                return httpx.Response(200, json=orders[args['orderId']])
            data = {
                'GetSandboxAccounts': {'accounts': [{'id': self.account_id, 'status': 'ACCOUNT_STATUS_OPEN'}]},
                'FutureBy': {'instrument': {
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
                                     'limitOrderAvailableFlag': True, 'marketOrderAvailableFlag': True},
                'GetSandboxPositions': {
                    'accountId': self.account_id, 'limitsLoadingInProgress': False,
                    'securities': [], 'options': [], 'futures': [
                        {'instrumentUid': self.contract_id, 'balance': str(positions), 'blocked': '0'},
                    ] if positions else [],
                },
                'GetSandboxPortfolio': {'accountId': self.account_id, 'totalAmountPortfolio': cash},
                'GetSandboxWithdrawLimits': {'money': [cash]},
                'GetSandboxOrders': {'orders': []},
                'GetSandboxStopOrders': {'stopOrders': list(stops.values())},
            }
            return httpx.Response(200, json=data[method])

        archive = Archive(self.publications_db)
        with (httpx.Client(transport=httpx.MockTransport(reply)) as client,
              EventArchive(self.events_db) as events, closing(archive.db)):
            broker = TBankSandbox(client, self.account_id, self.contract_id, 'sber')
            executor = SandboxExecutor(events, broker)
            result = executor.start(archive, approval, allowed_contracts={self.contract_id},
                                    allow_independent_stops=True)
            if reject_tp:
                self.assertEqual(result['status'], 'closed', result)
            else:
                self.assertEqual(result['status'], 'protected', result)
                self.assertEqual(executor.reconcile(close=True, contract_id='other')['reason'],
                                 'sandbox_selection_mismatch')
                self.assertEqual(executor.reconcile(contract_id=self.contract_id)['status'], 'protected')
                self.assertEqual(executor.reconcile(close=True)['status'], 'closed')
            cycle, = trade_journal(events)
            self.assertEqual(cycle['status'], 'closed')

        stop_requests = [args for method, args in writes if method == 'PostSandboxStopOrder']
        self.assertEqual([(args['stopOrderType'], args['stopPrice']['units']) for args in stop_requests],
                         [('STOP_ORDER_TYPE_STOP_LOSS', '290'), ('STOP_ORDER_TYPE_TAKE_PROFIT', '320')])
        self.assertEqual({args['quantity'] for args in stop_requests}, {'1'})
        self.assertEqual(len({args['orderId'] for args in stop_requests}), 2)
