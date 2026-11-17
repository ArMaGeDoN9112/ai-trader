"""Normalize the reviewed T-Invest sandbox MCP responses for ticket 04."""

from typing import Any

from jsonschema import Draft202012Validator
from mcp import types
from referencing import Registry

from news_futures.brokers.diagnostics import Binding, CompanyShare, Session, extract, positive


def one_instrument(rows: list[dict[str, Any]], ticker: str, class_code: str) -> dict[str, Any]:
    matches = [row for row in rows if (row['ticker'], row['classCode']) == (ticker, class_code)]
    if len(matches) != 1:
        raise ValueError('instrument_missing_or_ambiguous')
    return matches[0]


async def read_tbank(session: Session, tools: dict[str, types.Tool], instrument: str,
                     report: dict[str, Any]) -> dict[str, Any]:
    operations = report['operations']
    responses: dict[str, Any] = {}
    report['tbank_responses'] = responses
    data: dict[str, Any] = {}

    def available_tool(name: str) -> types.Tool:
        tool = tools.get(f'invest_{name}')
        if tool is None:
            raise ValueError('tool_missing')
        return tool

    async def read(label: str, name: str, **args: Any) -> dict[str, Any]:
        item = responses[label] = {'tool': f'invest_{name}', 'availability': 'unavailable'}
        limitation = 'tool_missing'
        try:
            tool = available_tool(name)
            limitation = 'read_only_not_declared'
            if not tool.annotations or tool.annotations.readOnlyHint is not True:
                raise ValueError(limitation)
            limitation = 'invalid_arguments'
            Draft202012Validator.check_schema(tool.inputSchema)
            Draft202012Validator(tool.inputSchema, registry=Registry()).validate(args)
            limitation = 'tool_call_or_response_failed'
            result = await session.call_tool(tool.name, args)
            payload = extract(result, Binding(tool=tool.name, input_schema=tool.inputSchema))
            if not isinstance(payload, dict):
                raise ValueError('object_required')
            item.update(availability='available', data=payload)
            return payload
        except Exception:
            item['limitation'] = limitation
            return {}

    def record(operation: str, value: Any, kind: type, *sources: str) -> None:
        if isinstance(value, kind) and (kind is list or value):
            operations[operation] = {'availability': 'available', 'data': value, 'sources': list(sources)}
            data[operation] = value
        else:
            operations[operation] = {'availability': 'unavailable', 'sources': list(sources),
                                     'limitation': 'missing_or_invalid_response_fields'}

    for operation, name in (('create_order', 'create_order'), ('create_sl', 'create_stoporder'),
                            ('create_tp', 'create_stoporder'), ('read_stops', 'get_stoporders'),
                            ('cancel_stop', 'cancel_stoporder')):
        try:
            tool = available_tool(name)
            capability = {'availability': 'schema_reviewed_not_exercised', 'tool': tool.name}
            if operation in ('create_sl', 'create_tp'):
                stop_type = {'create_sl': 'STOP_ORDER_TYPE_STOP_LOSS',
                             'create_tp': 'STOP_ORDER_TYPE_TAKE_PROFIT'}[operation]
                if stop_type not in tool.inputSchema['properties']['stopOrderType']['enum']:
                    raise ValueError('stop_type_not_advertised')
                capability['stop_type'] = stop_type
            report['capabilities'][operation] = capability
        except (KeyError, ValueError):
            report['capabilities'][operation]['limitation'] = 'tool_or_schema_changed'

    accounts = (await read('accounts', 'list_broker_accounts', status='ACCOUNT_STATUS_OPEN')).get('accounts')
    record('accounts', accounts, list, 'accounts')
    account_id = None
    if isinstance(accounts, list) and len(accounts) == 1 and isinstance(accounts[0], dict):
        account_id = accounts[0].get('id')
    if isinstance(account_id, str) and account_id:
        portfolio = await read('portfolio', 'get_portfolio', accountId=account_id, responseView=['FULL'])
        record('account', portfolio if isinstance(portfolio.get('totalAmountPortfolio'), dict) else None,
               dict, 'portfolio')
        positions = await read('positions', 'get_positions', accountId=account_id)
        record('positions', positions if all(isinstance(positions.get(key), list) for key in
               ('money', 'blocked', 'securities', 'futures', 'options')) else None, dict, 'positions')
        orders = await read('orders', 'get_orders', accountId=account_id)
        record('orders', orders.get('orders'), list, 'orders')
        stops = await read('stop_orders', 'get_stoporders', accountId=account_id, status='STOP_ORDER_STATUS_ALL')
        record('stop_orders', stops.get('stopOrders'), list, 'stop_orders')
    else:
        for operation in ('account', 'positions', 'orders', 'stop_orders'):
            operations[operation]['limitation'] = 'one_open_sandbox_account_required'

    assets = await read('assets', 'list_assets', instrumentType='INSTRUMENT_TYPE_SHARE',
                        instrumentStatus='INSTRUMENT_STATUS_ALL')
    shares = await read('shares', 'list_shares', responseView=['IDENTIFIERS'])
    try:
        companies = {row['uid']: row['name'] for row in assets['assets']}
        catalog = [CompanyShare(company_id=row['assetUid'], company_name=companies[row['assetUid']],
                                share_id=row['positionUid'], ticker=row['ticker']).model_dump()
                   for row in shares['instruments']]
        if len(companies) != len(assets['assets']) or not catalog:
            raise ValueError('catalog_empty_or_ambiguous')
        record('catalog', catalog, list, 'assets', 'shares')
    except (KeyError, TypeError, ValueError):
        record('catalog', None, list, 'assets', 'shares')

    ticker, class_code = instrument.split('_')
    lookup = await read('future_lookup', 'find_instrument', query=ticker,
                        instrumentKind='INSTRUMENT_TYPE_FUTURES', responseView=['FULL'])
    try:
        found = one_instrument(lookup['instruments'], ticker, class_code)
        uid = found['uid']
        if not isinstance(uid, str) or not uid:
            raise ValueError('missing_contract_uid')
        future = (await read('future', 'get_future', instrumentId=uid,
                             idType='INSTRUMENT_ID_TYPE_UID', responseView=['FULL']))['instrument']
        if (future['ticker'], future['classCode'], future['positionUid'], future['lot']) != (
                ticker, class_code, found['positionUid'], found['lot']) or future['assetType'] != 'TYPE_SECURITY':
            raise ValueError('contract_identity_mismatch')
        if not any(row['share_id'] == future['basicAssetPositionUid'] and row['ticker'] == future['basicAsset']
                   for row in data.get('catalog', [])):
            raise ValueError('underlying_share_missing')
        record('future', {'id': uid, 'ticker': ticker, 'underlying_share_id': future['basicAssetPositionUid'],
                          'expires_at': future['expirationDate'], 'last_trade_at': future['lastTradeDate'],
                          'lot': future['lot'], 'currency': future['currency'], 'price_unit': 'points',
                          'quantity_unit': 'lots'}, dict, 'future_lookup', 'future')
    except (KeyError, TypeError, ValueError):
        record('future', None, dict, 'future_lookup', 'future')
        return data

    quote = await read('quote', 'get_market_values', instrumentId=[uid], values=['INSTRUMENT_VALUE_LAST_PRICE'])
    status = await read('trading_status', 'get_trading_statuses', instrumentId=[uid])
    try:
        state = one_instrument(status['tradingStatuses'], ticker, class_code)
        if not isinstance(state['tradingStatus'], str) or not state['tradingStatus']:
            raise ValueError('missing_trading_status')
        record('trading_status', {'contract_id': uid, 'api_trade_available': future['apiTradeAvailableFlag'],
                                  'limit_order_available': state['limitOrderAvailableFlag']},
               dict, 'future', 'trading_status')
    except (KeyError, TypeError, ValueError):
        record('trading_status', None, dict, 'future', 'trading_status')
    try:
        market = one_instrument(quote['instruments'], ticker, class_code)
        last, = (value for value in market['values'] if value['type'] == 'INSTRUMENT_VALUE_LAST_PRICE')
        if market['priceCurrency'] not in ('pt', 'pt.'):
            raise ValueError('points_expected')
        price = last['value']['value']
        record('quote', {'contract_id': uid, 'price': price, 'price_unit': 'points',
                         'timestamp': last['time']}, dict, 'quote')
        tick = future['minPriceIncrement']['value']
        tick_value = future['minPriceIncrementAmount']['value']
        # shortcut: estimate only where points and settlement price have the same numeric value.
        if not account_id or future['lot'] != 1 or positive(tick) != positive(tick_value):
            raise ValueError('order_estimate_units_unconfirmed')
        margins = {}
        for side in ('buy', 'sell'):
            estimate = await read('margin_' + side, 'get_order_price', accountId=account_id,
                                  instrumentId=instrument, direction='ORDER_DIRECTION_' + side.upper(),
                                  quantity=1, price={'value': str(positive(price))})
            margins[side] = estimate.get('extraFuture', {}).get('initialMargin', {})
        if any(item.get('currency') != future['currency'] for item in margins.values()):
            raise ValueError('margin_currency_unconfirmed')
        record('margin', {'contract_id': uid, 'currency': future['currency'], 'tick_size': tick,
                          'tick_value': tick_value, 'initial_margin_buy': margins['buy']['value'],
                          'initial_margin_sell': margins['sell']['value']},
               dict, 'future', 'margin_buy', 'margin_sell')
    except (KeyError, TypeError, ValueError, ArithmeticError):
        if 'quote' not in data:
            record('quote', None, dict, 'quote')
        record('margin', None, dict, 'future', 'margin_buy', 'margin_sell')
    return data
