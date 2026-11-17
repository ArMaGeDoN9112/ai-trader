"""Official sandbox MCP writes, with receipt-based reconciliation via Sandbox REST."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from decimal import Decimal
import ssl
from pathlib import Path
from typing import Any, Literal

import httpx
from jsonschema import Draft202012Validator
from mcp import ClientSession, types
from mcp.client.streamable_http import streamable_http_client
from referencing import Registry

from news_futures.credentials import load_token
from news_futures.brokers.diagnostic_reads import schema_digest
from news_futures.brokers.diagnostics import SANDBOX_URL
from news_futures.brokers.tbank import POINTS, STOP_TYPES, TBankReader, broker_read_timeout, identifier
from news_futures.trading.execution import Fill
from news_futures.trading.terms import EntryReservation


CREATE = 'invest_create_order'
STOP = 'invest_create_stoporder'
CANCEL = 'invest_cancel_order'
CANCEL_STOP = 'invest_cancel_stoporder'
COMMON = {'accountId', 'instrumentId', 'quantity', 'direction', 'priceType', 'confirmMarginTrade'}
FIELDS = {
    CREATE: (COMMON | {'orderType', 'price', 'timeInForce'}, 'orderId'),
    STOP: (COMMON | {'stopOrderType', 'stopPrice', 'expirationType', 'exchangeOrderType', 'takeProfitType'}, 'stopOrderId'),
    CANCEL: ({'accountId', 'orderId'}, 'time'),
    CANCEL_STOP: ({'accountId', 'stopOrderId'}, 'time'),
}
MARKET_READS = {
    'GetOrderBook': ('invest_get_orderbook', '60d11dbbfe05c213be87fead46e0b5ebae9203ac8925deb96eb05856a9119787'),
    'GetCandles': ('invest_get_candles', '277376f5d15b1526fc8cb627b220c90255f043abae9c6d84642ff00aff45f315'),
}


class TBankMCPReader(TBankReader):
    def __init__(self, client: httpx.Client, account_id: str, contract_id: str, company_id: str | None,
                 ca_file: Path | None):
        super().__init__(client, account_id, contract_id, company_id)
        self.ca_file = ca_file
        self._symbol: str | None = None

    @asynccontextmanager
    async def _session(self, seconds: float) -> AsyncIterator[ClientSession]:
        tls = ssl.create_default_context(cafile=str(self.ca_file)) if self.ca_file else True

        async def sandbox_only(request: httpx.Request) -> None:
            if str(request.url) != SANDBOX_URL:
                raise ValueError('sandbox_endpoint_required')

        async with asyncio.timeout(seconds), httpx.AsyncClient(
            verify=tls, headers={'Authorization': f"Bearer {load_token('sandbox', 'token')}"},
            trust_env=False, follow_redirects=False, timeout=min(10, seconds), event_hooks={'request': [sandbox_only]},
        ) as client:
            async with streamable_http_client(SANDBOX_URL, http_client=client) as (read, write, _):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    yield session

    def _market_data(self, method: str, **args: Any) -> dict[str, Any]:
        name, digest = MARKET_READS[method]
        instrument = self.instrument()
        return asyncio.run(self._read_market_tool(name, digest, {'instrumentId': instrument, **args}))

    def _market_price(self, value: Any) -> float:
        return float(Decimal(identifier(value['value'])))

    async def _read_market_tool(self, name: str, digest: str, args: dict[str, Any]) -> dict[str, Any]:
        async with self._session(broker_read_timeout(10)) as session:
            tools: list[types.Tool] = []
            cursor = None
            for _ in range(10):
                page = await session.list_tools(cursor=cursor)
                tools.extend(tool for tool in page.tools if tool.name == name)
                cursor = page.nextCursor
                if cursor is None:
                    break
            else:
                raise ValueError('mcp_inventory_incomplete')
            if len(tools) != 1:
                raise ValueError('mcp_market_tool_missing_or_ambiguous')
            tool = tools[0]
            if not tool.annotations or tool.annotations.readOnlyHint is not True or schema_digest(tool) != digest:
                raise ValueError('mcp_market_tool_schema_changed')
            Draft202012Validator(tool.inputSchema, registry=Registry()).validate(args)
            result = await session.call_tool(name, arguments=args)
            if result.isError or not isinstance(result.structuredContent, dict):
                raise ValueError('mcp_market_response_unconfirmed')
            return result.structuredContent

    def read_order(self, order_id: str, *, request: bool, side: str,
                   receipt: str | None = None) -> tuple[bool, Fill | None, str | None]:
        if request and not receipt:
            # shortcut: MCP create tools lack a request key, so an absent receipt requires manual broker investigation.
            return False, None, None
        return super().read_order(receipt if request and receipt else order_id, request=False, side=side)

    def instrument(self) -> str:
        if self._symbol is None:
            row = self.call('FutureBy', service='InstrumentsService', account=False,
                            id=self.contract_id, idType='INSTRUMENT_ID_TYPE_UID')['instrument']
            if row['uid'] != self.contract_id:
                raise ValueError('market_contract_mismatch')
            self._symbol = f"{identifier(row['ticker'])}_{identifier(row['classCode'])}"
        return self._symbol


class TBankMCPSandbox(TBankMCPReader):
    source: Literal['sandbox_mcp'] = 'sandbox_mcp'

    async def _invoke(self, name: str | None, args: dict[str, Any]) -> dict[str, Any]:
        async with self._session(20) as session:
            tools: dict[str, types.Tool] = {}
            cursor = None
            for _ in range(10):
                page = await session.list_tools(cursor=cursor)
                for tool in page.tools:
                    if tool.name in FIELDS:
                        if tool.name in tools:
                            raise ValueError('ambiguous_mcp_order_tool')
                        tools[tool.name] = tool
                cursor = page.nextCursor
                if cursor is None:
                    break
            else:
                raise ValueError('mcp_inventory_incomplete')
            for tool_name in (FIELDS if name is None else (name,)):
                reviewed_tool = tools.get(tool_name)
                fields, output_field = FIELDS[tool_name]
                if (reviewed_tool is None or not fields.issubset(reviewed_tool.inputSchema.get('properties', {}))
                        or not set(reviewed_tool.inputSchema.get('required', ())).issubset(fields)
                        or output_field not in (reviewed_tool.outputSchema or {}).get('properties', {})):
                    raise ValueError('mcp_order_tool_schema_changed')
            if name is None:
                return {}
            schema = tools[name].inputSchema
            Draft202012Validator(schema, registry=Registry()).validate(args)
            result = await session.call_tool(name, arguments=args)
            if result.isError or not isinstance(result.structuredContent, dict):
                raise ValueError('mcp_order_response_unconfirmed')
            return result.structuredContent

    def prepare(self) -> None:
        self.instrument()
        asyncio.run(self._invoke(None, {}))

    def _write(self, name: str, **args: Any) -> str:
        response = asyncio.run(self._invoke(name, {'accountId': self.account_id, **args}))
        return identifier(response[FIELDS[name][1]])

    def _order(self, entry: EntryReservation, lots: int, *, entering: bool) -> str:
        side = 'BUY' if (entry.side == 'long') == entering else 'SELL'
        terms = ({'price': {'value': format(entry.entry_price, 'f')},
                  'timeInForce': 'TIME_IN_FORCE_FILL_AND_KILL'} if entering else {})
        return self._write(CREATE, instrumentId=self.instrument(), quantity=lots, priceType=POINTS,
                           direction='ORDER_DIRECTION_' + side, confirmMarginTrade=True,
                           orderType='ORDER_TYPE_LIMIT' if entering else 'ORDER_TYPE_MARKET', **terms)

    def enter(self, entry: EntryReservation, operation_id: str) -> str:
        return self._order(entry, entry.lots, entering=True)

    def protect(self, entry: EntryReservation, operation_id: str, lots: int, kind: Literal['sl', 'tp']) -> str:
        side = 'SELL' if entry.side == 'long' else 'BUY'
        price = entry.stop_loss if kind == 'sl' else entry.take_profit
        extra = {'takeProfitType': 'TAKE_PROFIT_TYPE_REGULAR'} if kind == 'tp' else {}
        return self._write(STOP, instrumentId=self.instrument(), quantity=lots, priceType=POINTS,
                           direction='STOP_ORDER_DIRECTION_' + side, confirmMarginTrade=True,
                           stopOrderType=STOP_TYPES[kind], stopPrice={'value': format(price, 'f')},
                           expirationType='STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL',
                           exchangeOrderType='EXCHANGE_ORDER_TYPE_MARKET', **extra)

    def cancel_protection(self, entry: EntryReservation, operation_id: str, ids: tuple[str, ...]) -> None:
        for order_id in ids:
            self._write(CANCEL_STOP, stopOrderId=order_id)

    def cancel_orders(self, entry: EntryReservation, operation_id: str, ids: tuple[str, ...]) -> None:
        for order_id in ids:
            self._write(CANCEL, orderId=order_id)

    def close(self, entry: EntryReservation, operation_id: str, lots: int) -> str:
        return self._order(entry, lots, entering=False)
