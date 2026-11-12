from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import httpx

from news_futures.adapters.tbank.futures_catalog import fetch_catalog, group_futures, save_catalog
from news_futures.adapters.tbank.rest import SANDBOX_REST, restrict_to_reads


class FuturesCatalogTest(unittest.TestCase):
    def test_groups_available_contracts_and_replaces_the_snapshot(self) -> None:
        def contract(ticker: str, base: str, kind: str, position_uid: str = '',
                     available: bool = True) -> dict[str, object]:
            return {'uid': ticker, 'ticker': ticker, 'name': ticker, 'classCode': 'SPBFUT',
                    'assetType': kind, 'basicAsset': base, 'basicAssetPositionUid': position_uid,
                    'expirationDate': '2026-12-17T00:00:00Z', 'lastTradeDate': '2026-12-16T21:00:00Z',
                    'apiTradeAvailableFlag': available}

        futures = [
            contract('SRZ6', 'SBER', 'TYPE_SECURITY', 'ordinary'),
            contract('SRU6', 'Сбер', 'TYPE_SECURITY', 'ordinary', False),
            contract('SBPZ6', 'SBERP', 'TYPE_SECURITY', 'preferred'),
            contract('OGZ6', 'Индекс нефти и газа', 'TYPE_INDEX'),
            contract('BTF', 'BTCUSDT', 'TYPE_INDEX'),
            contract('SBERF', 'Фиксинг SBER', 'TYPE_INDEX'),
            contract('EXZ6', 'Новый товар', 'TYPE_COMMODITY'),
            contract('OLD', 'Старый товар', 'TYPE_COMMODITY', available=False),
        ]
        shares = [{'ticker': ticker, 'positionUid': uid, 'sector': 'financial'}
                  for ticker, uid in [('SBER', 'ordinary'), ('SBERP', 'preferred')]]
        listings = {'Futures': futures, 'Shares': shares}
        requests = []

        def respond(request: httpx.Request) -> httpx.Response:
            method = str(request.url).removeprefix(f'{SANDBOX_REST}InstrumentsService/')
            requests.append(method)
            self.assertEqual(request.content, b'{"instrumentStatus":"INSTRUMENT_STATUS_ALL"}')
            return httpx.Response(200, json={'instruments': listings[method]})

        with httpx.Client(transport=httpx.MockTransport(respond), event_hooks={'request': [restrict_to_reads]}) as client:
            catalog = fetch_catalog(client)
        self.assertEqual(requests, ['Futures', 'Shares'])
        assets = {group['name']: group for group in catalog['assets']}
        self.assertEqual([item['ticker'] for item in assets['SBER']['contracts']], ['SRZ6'])
        self.assertNotIn('Старый товар', assets)
        self.assertEqual(assets['SBER']['categories'], ['financial'])
        self.assertEqual(assets['SBERP']['underlying_position_uid'], 'preferred')
        self.assertEqual(assets['Индекс нефти и газа']['categories'], ['index', 'oil_gas'])
        self.assertEqual(assets['BTCUSDT']['categories'], ['crypto'])
        self.assertEqual(assets['Фиксинг SBER']['categories'], ['financial'])
        self.assertEqual(assets['Новый товар']['categories'], ['unclassified'])

        with TemporaryDirectory() as temporary:
            output = Path(temporary) / 'catalog'
            summary = save_catalog(output, catalog)
            self.assertEqual(summary, {'assets': 6, 'contracts': 6, 'api_trade_available': 6})
            self.assertNotIn('SRU6', (output / 'catalog.json').read_text(encoding='utf-8'))
            report = (output / 'groups.md').read_text(encoding='utf-8')
            self.assertIn('| SBER | `SRZ6` | 1 |', report)
            self.assertIn('## Без категории', report)
            self.assertNotIn('SRU6', report)
            refreshed = catalog | {'assets': group_futures(futures[1:], shares)}
            save_catalog(output, refreshed)
            self.assertNotIn('SRZ6', (output / 'catalog.json').read_text(encoding='utf-8'))
            self.assertNotIn('SRZ6', (output / 'groups.md').read_text(encoding='utf-8'))
            self.assertEqual(sorted(path.name for path in output.iterdir()), ['catalog.json', 'groups.md'])
        with self.assertRaisesRegex(ValueError, 'invalid_future_identity'):
            group_futures(futures + futures[:1], shares)
