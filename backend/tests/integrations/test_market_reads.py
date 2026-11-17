from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import json
from collections.abc import AsyncIterator
from unittest import TestCase
from unittest.mock import AsyncMock, patch

import httpx
from jsonschema import ValidationError
from mcp import types

from news_futures.brokers.mcp_orders import TBankMCPReader
from news_futures.brokers.tbank import TBankReader


class MarketReadsTest(TestCase):
    def test_reads_best_levels_and_only_completed_minute_candles(self) -> None:
        now = datetime(2026, 11, 17, 10, tzinfo=timezone.utc)

        def request(message: httpx.Request) -> httpx.Response:
            args = json.loads(message.content)
            self.assertEqual(args['instrumentId'], 'future')
            self.assertNotIn('accountId', args)
            price = {'units': '300', 'nano': 0}
            if message.url.path.endswith('/GetOrderBook'):
                self.assertEqual(args['depth'], 1)
                return httpx.Response(200, json={
                    'instrumentUid': 'future', 'orderbookTs': now.isoformat(), 'priceCurrency': 'pt',
                    'bids': [{'price': price, 'quantity': '5'}],
                    'asks': [{'price': {'units': '301', 'nano': 0}, 'quantity': '7'}],
                })
            self.assertTrue(message.url.path.endswith('/GetCandles'))
            self.assertEqual(args['interval'], 'CANDLE_INTERVAL_1_MIN')
            candles = [{'time': (now + timedelta(minutes=i)).isoformat(), 'isComplete': i != 1,
                        'volume': '42', 'close': price} for i in range(3)]
            return httpx.Response(200, json={'priceCurrency': 'pt', 'candles': candles})

        with httpx.Client(transport=httpx.MockTransport(request)) as client:
            reader = TBankReader(client, 'account', 'future', 'company')
            book = reader.read_order_book()
            assert book.bid is not None and book.ask is not None
            self.assertEqual((book.bid.quantity_lots, book.ask.price, book.observed_at), (5, 301.0, now))
            candle, = reader.read_candles(now, now + timedelta(minutes=2))
            self.assertEqual((candle.started_at, candle.close, candle.volume_lots), (now, 300.0, 42))

    def test_mcp_book_accepts_current_schema_and_requires_valid_read_only_calls(self) -> None:
        name = 'invest_get_orderbook'
        tool = types.Tool(name=name, annotations=types.ToolAnnotations(readOnlyHint=True), inputSchema={
            'type': 'object', 'required': ['instrumentId', 'depth'],
            'properties': {'instrumentId': {'type': 'string'}, 'depth': {'type': 'integer'}},
        })
        session = AsyncMock()
        session.list_tools.return_value = types.ListToolsResult(tools=[tool])
        session.call_tool.return_value = types.CallToolResult(content=[], structuredContent={
            'orderbookTs': '2026-11-17T10:00:00Z', 'priceCurrency': 'pt',
            'bids': [{'price': {'value': '299.5'}, 'quantity': '2'}], 'asks': [],
        })

        @asynccontextmanager
        async def connected(seconds: float) -> AsyncIterator[AsyncMock]:
            self.assertGreater(seconds, 0)
            yield session

        with httpx.Client(trust_env=False) as client:
            reader = TBankMCPReader(client, 'account', 'future', 'company', None)
            with (patch.object(reader, 'instrument', return_value='FUT_TEST'),
                  patch.object(reader, '_session', side_effect=connected)):
                book = reader.read_order_book()
                assert book.bid is not None
                self.assertEqual(book.bid.price, 299.5)
                self.assertIsNone(book.ask)
                session.call_tool.assert_awaited_once_with(name, arguments={'instrumentId': 'FUT_TEST', 'depth': 1})
                tool.description = 'Updated order book description'
                tool.inputSchema['properties']['optionalField'] = {'type': 'string'}
                tool.outputSchema = {'type': 'object', 'properties': {'optionalField': {'type': 'string'}}}
                self.assertEqual(reader.read_order_book(), book)
                self.assertEqual(session.call_tool.await_count, 2)
                tool.inputSchema['properties']['depth'] = {'type': 'string'}
                with self.assertRaises(ValidationError):
                    reader.read_order_book()
                self.assertEqual(session.call_tool.await_count, 2)
                tool.inputSchema['properties']['depth'] = {'type': 'integer'}
                tool.annotations = types.ToolAnnotations(readOnlyHint=False)
                with self.assertRaisesRegex(ValueError, 'mcp_market_tool_not_read_only'):
                    reader.read_order_book()
                self.assertEqual(session.call_tool.await_count, 2)
