"""Read-only MCP diagnostics. Bindings are reviewed against a discovered schema."""

import asyncio
from datetime import date, datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import re
import ssl
from typing import Any, Final, Literal, Protocol

import httpx
from jsonschema import Draft202012Validator
from mcp import ClientSession, types
from mcp.client.streamable_http import streamable_http_client
from pydantic import BaseModel, ConfigDict, Field, model_validator
from referencing import Registry

from news_futures.common.credentials import load_token
from news_futures.common.artifacts import write_json


SANDBOX_URL: Final = 'https://sandbox-invest-public-api.tbank.ru/mcp'
READS = ('accounts', 'account', 'positions', 'orders', 'stop_orders',
         'catalog', 'future', 'margin', 'quote', 'trading_status')
CAPABILITIES = ('create_order', 'create_sl', 'create_tp', 'read_stops', 'cancel_stop')


class Contract(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True, allow_inf_nan=False)


class Binding(Contract):
    tool: str
    input_schema: dict[str, Any]
    arguments: dict[str, Any] = Field(default_factory=dict)
    result_path: str = ''
    fields: dict[str, str] = Field(default_factory=dict)
    # Exact schema fragments reviewed by the operator, e.g. an SL type enum.
    evidence: dict[str, Any] = Field(default_factory=dict)


class Settings(Contract):
    environment: Literal['sandbox']
    endpoint: Literal['https://sandbox-invest-public-api.tbank.ru/mcp']
    bindings: dict[str, Binding] = Field(default_factory=dict)
    tbank_contract: str | None = Field(default=None, pattern=r'^[A-Z0-9]+_[A-Z0-9]+$')
    ca_file: str | None = None

    @model_validator(mode='after')
    def one_binding_mode(self) -> 'Settings':
        if self.tbank_contract and self.bindings:
            raise ValueError('choose_tbank_contract_or_manual_bindings')
        return self


class CompanyShare(Contract):
    company_id: str = Field(min_length=1)
    company_name: str = Field(min_length=1)
    share_id: str = Field(min_length=1)
    ticker: str = Field(min_length=1)


class Amount(Contract):
    units: str | int
    nano: int = Field(ge=-999999999, le=999999999)
    currency: str | None = None


class Future(Contract):
    id: str = Field(min_length=1)
    figi: str | None = Field(default=None, min_length=1)
    ticker: str = Field(min_length=1)
    underlying_share_id: str = Field(min_length=1)
    expires_at: str
    last_trade_at: str | None = None
    lot: int = Field(gt=0)
    basic_asset_size: str | Amount | None = None
    currency: str = Field(min_length=1)
    price_unit: Literal['points']
    quantity_unit: Literal['lots']


class Margin(Contract):
    contract_id: str
    currency: str
    tick_size: str | Amount
    tick_value: str | Amount
    initial_margin_buy: str | Amount
    initial_margin_sell: str | Amount


class Quote(Contract):
    contract_id: str
    price: str | Amount
    price_unit: Literal['points']
    timestamp: str


class TradingStatus(Contract):
    contract_id: str
    api_trade_available: bool
    buy_available: bool | None = None
    sell_available: bool | None = None
    limit_order_available: bool | None = None


class Session(Protocol):
    async def list_tools(self, cursor: str | None) -> types.ListToolsResult: ...
    async def call_tool(self, name: str, arguments: dict[str, Any]) -> types.CallToolResult: ...


def pointer(value: Any, path: str) -> Any:
    """Read a JSON pointer, never evaluate expressions supplied by configuration."""
    if path and not path.startswith('/'):
        raise ValueError('invalid_json_pointer')
    for part in path.split('/')[1:]:
        key = part.replace('~1', '/').replace('~0', '~')
        value = value[int(key)] if isinstance(value, list) else value[key]
    return value


def redact(value: Any, token: str) -> Any:
    if isinstance(value, dict):
        return {str(redact(key, token)): ('[REDACTED]' if any(
            word in key.lower() for word in ('token', 'authorization', 'password', 'secret',
                                             'api_key', 'apikey', 'cookie', 'headers'))
            else redact(item, token)) for key, item in value.items()}
    if isinstance(value, list):
        return [redact(item, token) for item in value]
    if isinstance(value, str):
        if value.lstrip().startswith(('{', '[')):
            try:
                return json.dumps(redact(json.loads(value), token), ensure_ascii=False)
            except ValueError:
                pass
        value = re.sub(r'(?i)Bearer\s+[^\s"\x27,}]+', 'Bearer [REDACTED]', value)
        return value.replace(token, '[REDACTED]') if token else value
    return value


def timestamp(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('timezone_required')
    return result


def positive(value: str | Amount) -> Decimal:
    if isinstance(value, Amount):
        units = Decimal(value.units)
        if units != units.to_integral_value() or units * value.nano < 0:
            raise ValueError('invalid_quotation')
        number = units + Decimal(value.nano) / Decimal(1000000000)
    else:
        number = Decimal(value)
    if not number.is_finite() or number <= 0:
        raise ValueError('positive_decimal_required')
    return number


def confirm_contract(data: dict[str, Any], now: datetime) -> dict[str, Any]:
    """Confirm data consistency only; this is never permission to trade."""
    limitation = 'missing_or_invalid_fields'
    try:
        catalog = [CompanyShare.model_validate(row) for row in data['catalog']]
        future = Future.model_validate(data['future'])
        margin = Margin.model_validate(data['margin'])
        quote = Quote.model_validate(data['quote'])
        trading = TradingStatus.model_validate(data['trading_status'])
        share = next(row for row in catalog if row.share_id == future.underlying_share_id)
        if len({share.company_id, share.share_id, future.id}) != 3:
            raise ValueError('company_share_contract_identity_conflict')
        if any(identifier != future.id for identifier in
               (margin.contract_id, quote.contract_id, trading.contract_id)):
            raise ValueError('contract_identity_mismatch')
        if future.currency != margin.currency:
            raise ValueError('currency_mismatch')
        for amount in (margin.tick_value, margin.initial_margin_buy, margin.initial_margin_sell):
            if isinstance(amount, Amount) and amount.currency != future.currency:
                raise ValueError('money_currency_missing_or_mismatched')
        for quotation in (quote.price, margin.tick_size, future.basic_asset_size):
            if isinstance(quotation, Amount) and quotation.currency is not None:
                raise ValueError('points_or_quantity_expected')
        if future.lot != 1:
            raise ValueError('lots_to_contracts_unconfirmed')
        if future.last_trade_at is not None:
            last_trade = timestamp(future.last_trade_at)
            if date.fromisoformat(future.expires_at) < last_trade.date():
                raise ValueError('invalid_contract_dates')
        else:
            last_trade = timestamp(future.expires_at)
        if last_trade <= now:
            raise ValueError('expired_contract')
        quote_age_seconds = (now - timestamp(quote.timestamp)).total_seconds()
        if quote_age_seconds < 0:
            raise ValueError('future_quote')
        order_available = (trading.limit_order_available if trading.limit_order_available is not None
                           else trading.buy_available and trading.sell_available)
        if not (trading.api_trade_available and order_available):
            raise ValueError('contract_unavailable')
        tick = positive(margin.tick_size)
        price = positive(quote.price)
        if future.basic_asset_size is not None:
            positive(future.basic_asset_size)
        for value in (margin.tick_value, margin.initial_margin_buy, margin.initial_margin_sell):
            positive(value)
        if price % tick:
            raise ValueError('price_off_tick')
        return {'confirmed': True, 'company_id': share.company_id,
                'share_id': share.share_id, 'contract_id': future.id,
                'order_quantity_unit': 'lots', 'contracts_per_lot': 1,
                'quote_age_seconds': quote_age_seconds,
                'price_unit': 'points', 'point_value': str(positive(margin.tick_value) / tick)}
    except ValueError as error:
        # Do not persist exception values: validation errors can echo server secrets.
        reasons = {'company_share_contract_identity_conflict', 'contract_identity_mismatch',
                   'currency_mismatch', 'money_currency_missing_or_mismatched',
                   'points_or_quantity_expected', 'lots_to_contracts_unconfirmed', 'expired_contract',
                   'invalid_contract_dates', 'future_quote', 'contract_unavailable', 'price_off_tick',
                   'timezone_required', 'positive_decimal_required', 'invalid_quotation'}
        if type(error) is ValueError and str(error) in reasons:
            limitation = str(error)
    except (KeyError, TypeError, StopIteration, ArithmeticError):
        pass
    return {'confirmed': False, 'limitation': limitation}


def extract(result: types.CallToolResult, binding: Binding) -> Any:
    if result.isError:
        raise ValueError('tool_error')
    payload: Any = result.structuredContent
    if payload is None:
        texts = [part.text for part in result.content if isinstance(part, types.TextContent)]
        if len(texts) != 1:
            raise ValueError('structured_json_required')
        payload = json.loads(texts[0])
    payload = pointer(payload, binding.result_path)
    if binding.fields:
        def row(value: Any) -> dict[str, Any]:
            return {field: pointer(value, path) for field, path in binding.fields.items()}
        return [row(value) for value in payload] if isinstance(payload, list) else row(payload)
    return payload


async def diagnose(session: Session, settings: Settings, directory: Path, *,
                   token: str, source: Literal['sandbox_mcp', 'control'],
                   now: datetime | None = None) -> dict[str, Any]:
    started_at = now or datetime.now(timezone.utc)
    directory.mkdir(parents=True, exist_ok=False)
    tools: list[types.Tool] = []
    cursor = None
    cursors: set[str] = set()
    while True:
        page = await session.list_tools(cursor=cursor)
        tools.extend(page.tools)
        cursor = page.nextCursor
        if not cursor:
            break
        if cursor in cursors:
            raise ValueError('repeated_tools_cursor')
        cursors.add(cursor)
    inventory = [tool.model_dump(mode='json', exclude_none=True) for tool in tools]
    write_json(directory / 'tools.json', redact(inventory, token))
    by_name = {tool.name: tool for tool in tools}
    if len(by_name) != len(tools):
        raise ValueError('duplicate_tool_names')
    report: dict[str, Any] = {
        'source': source, 'environment': 'sandbox', 'endpoint': SANDBOX_URL,
        'observed_at': started_at.isoformat(), 'operations': {}, 'capabilities': {},
        'execution_enabled': False,
        'oco': 'not_verified', 'reduce_only': 'not_verified', 'atomic_entry': 'not_verified',
    }
    data: dict[str, Any] = {}
    for operation in (*READS, *CAPABILITIES):
        section = report['operations' if operation in READS else 'capabilities']
        binding = settings.bindings.get(operation)
        section[operation] = {'availability': 'unavailable', 'limitation': 'no_reviewed_binding'}
        if binding is None:
            continue
        tool = by_name.get(binding.tool)
        limitation = 'tool_or_schema_changed'
        try:
            if tool is None or tool.inputSchema != binding.input_schema:
                raise ValueError('tool_or_schema_changed')
            schema = tool.model_dump(mode='json', exclude_none=True)
            limitation = 'schema_evidence_missing_or_changed'
            if not binding.evidence or any(pointer(schema, path) != value
                                           for path, value in binding.evidence.items()):
                raise ValueError('schema_evidence_missing_or_changed')
            limitation = 'invalid_input_schema_or_arguments'
            Draft202012Validator.check_schema(tool.inputSchema)
            Draft202012Validator(tool.inputSchema, registry=Registry()).validate(binding.arguments)
            if operation in CAPABILITIES:
                section[operation] = {'availability': 'schema_reviewed_not_exercised',
                                      'tool': binding.tool, 'evidence': binding.evidence}
                continue
            limitation = 'read_only_not_declared'
            if not tool.annotations or tool.annotations.readOnlyHint is not True:
                raise ValueError('read_only_not_declared')
            limitation = 'tool_call_failed'
            result = await session.call_tool(binding.tool, binding.arguments)
            limitation = 'invalid_or_missing_response_fields'
            value = extract(result, binding)
            if operation in ('accounts', 'positions', 'orders', 'stop_orders', 'catalog'):
                if not isinstance(value, list):
                    raise ValueError('list_required')
            elif not isinstance(value, dict) or not value:
                raise ValueError('object_required')
            data[operation] = value
            section[operation] = {'availability': 'available', 'tool': binding.tool,
                                  'data': value, 'response': result.model_dump(mode='json')}
        except Exception:
            section[operation] = {'availability': 'unavailable', 'tool': binding.tool,
                                  'limitation': limitation}
    if settings.tbank_contract:
        from news_futures.adapters.tbank.diagnostic_reads import read_tbank
        data = await read_tbank(session, by_name, settings.tbank_contract, report)
    checked_at = now or datetime.now(timezone.utc)
    report['checked_at'] = checked_at.isoformat()
    report['contract'] = confirm_contract(data, checked_at)
    report['status'] = 'data_confirmed' if (report['contract']['confirmed'] and all(
        report['operations'][name]['availability'] == 'available' for name in READS)) else 'limited'
    report = redact(report, token)
    write_json(directory / 'diagnostics.json', report)
    return report


async def run_sandbox(config: Path, directory: Path) -> dict[str, Any]:
    settings = Settings.model_validate_json(config.read_text(encoding='utf-8'))
    if set(settings.bindings) - set((*READS, *CAPABILITIES)):
        raise ValueError('unknown_operation')
    token = load_token('sandbox', 'token')
    if directory.exists():
        raise ValueError('new_directory_required')
    tls = ssl.create_default_context(cafile=settings.ca_file) if settings.ca_file else True
    async def sandbox_only(request: httpx.Request) -> None:
        if str(request.url) != SANDBOX_URL:
            raise ValueError('sandbox_endpoint_required')
    # A custom client disables redirects and environment proxies for credentials.
    async with asyncio.timeout(120):
        async with httpx.AsyncClient(verify=tls, headers={'Authorization': f'Bearer {token}'},
                                     follow_redirects=False, trust_env=False, timeout=30,
                                     event_hooks={'request': [sandbox_only]}) as client:
            async with streamable_http_client(SANDBOX_URL, http_client=client) as (read, write, _):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    return await diagnose(session, settings, directory, token=token, source='sandbox_mcp')
