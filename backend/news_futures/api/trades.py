"""Project sandbox journal records into trade details without local paths or credentials."""

from decimal import Decimal
import json
from pathlib import Path
from typing import Any, cast

from sqlalchemy import Connection, RowMapping

from news_futures.api.news import REVISION, archive_news, event_rows, read_archive, runtime_rows, visible
from news_futures.storage.database import has_table
from news_futures.storage.trades import journal_events
from news_futures.common.paths import local_path


def linked_revision(root: Path, approval: str) -> str | None:
    try:
        revision = local_path(approval).relative_to(root / 'decisions').parts[0]
    except (ValueError, IndexError):
        return None
    return revision if REVISION.fullmatch(revision) else None


def average(orders: list[dict[str, Any]], action: str) -> tuple[int | None, str | None]:
    fills = [order for order in orders if order['action'] == action and order['filled']]
    lots = sum(order['filled'] for order in fills)
    if not lots:
        return None, None
    points = sum(Decimal(order['price']) * order['filled'] for order in fills)
    return lots, format((points / lots).quantize(Decimal('.000000001')).normalize(), 'f')


def trade(root: Path, db: Connection, row: RowMapping,
          publications: Connection | None, *, detail: bool) -> dict[str, Any]:
    terms = json.loads(row['terms'])
    entry_side = 'buy' if terms['side'] == 'long' else 'sell'
    exit_side = 'sell' if entry_side == 'buy' else 'buy'
    events = list(journal_events(db, row['id']))
    orders: dict[str, dict[str, Any]] = {}
    entry_id = None
    confirmed: dict[str, list[Any]] = {}
    confirmed_at = None
    protection_orders = []
    exit_reason = None
    for event in events:
        kind, data = event['kind'], event['details']
        operation = data.get('operation_id')
        if kind == 'intent':
            action = data['action'].partition(':')[0]
            orders[operation] = {'id': operation, 'action': action, 'status': kind, 'broker_id': None,
                                 'side': entry_side if action == 'enter' else exit_side if action == 'close' else None,
                                 'requested': terms['lots'] if action == 'enter' else data.get('lots'),
                                 'filled': None, 'price': None}
            if action == 'close':
                exit_reason = exit_reason or data.get('reason')
        elif operation in orders:
            orders[operation].update(status=kind, broker_id=data.get('broker_id'))
        elif kind == 'reconciliation':
            observation = data['state']
            if observation.get('entry'):
                entry_id = observation['entry']['order_id']
            protection_orders = observation.get('protection', [])
            if any(stop['status'] == 'executed' for stop in protection_orders):
                exit_reason = 'protective_exit'
        elif kind == 'confirmed_fills':
            confirmed, confirmed_at = data['fills'], event['at']
        elif kind == 'manual_close':
            exit_reason = 'manual_exit'
    if entry_id:
        for order in orders.values():
            if order['action'] == 'enter' and not order['broker_id']:
                order['broker_id'] = entry_id
    by_order = {order['broker_id']: order for order in orders.values() if order['broker_id']}
    for order_id, (lots, price) in confirmed.items():
        filled_order = by_order.get(order_id)
        if filled_order is None:
            is_entry = order_id == entry_id
            filled_order = orders['fill:' + order_id] = {
                'id': order_id, 'action': 'enter' if is_entry else 'close',
                'side': entry_side if is_entry else exit_side, 'requested': None,
            }
        filled_order.update(status='confirmed_fills', filled=lots, price=format(Decimal(price), 'f'))
    order_list = list(orders.values())
    entry_lots, entry_price = average(order_list, 'enter')
    exit_lots, exit_price = average(order_list, 'close')
    summary = {'id': row['id'], 'mode': 'sandbox', 'account_id': row['account_id'],
               'contract_id': terms['contract_id'], 'side': terms['side'], 'status': (
                   'open' if row['status'] == 'protected' else row['status']),
               'reason': row['reason'], 'at': events[0]['at'] if events else None,
               'entry_lots': entry_lots, 'entry_price': entry_price, 'exit_lots': exit_lots, 'exit_price': exit_price}
    if not detail:
        return summary
    traces = []
    revision = linked_revision(root, row['approval'])
    if revision and next(archive_news(root, runtime_rows(publications, revision), event_rows(db, revision)), None):
        traces.append({'kind': 'entry', 'revision': revision})
    reason = (db.exec_driver_sql('SELECT * FROM position_entry_reasons WHERE account_id = %s AND position_id = %s',
                         (row['account_id'], row['id'])).mappings().fetchone()
              if has_table(db, 'position_entry_reasons') else None)
    protection = {'protected': 'confirmed', 'closed': 'none', 'no_fill': 'none'}.get(row['status'], 'unknown')
    return summary | {'traces': traces, 'events': events, 'orders': order_list, 'entry_reason': reason['reason'] if reason else None,
                      'entry_sources': json.loads(reason['sources']) if reason else [],
                      'requested_lots': terms['lots'], 'confirmed_at': confirmed_at, 'protection': protection,
                      'protection_orders': protection_orders, 'exit_reason': exit_reason}


def trades_view(root: Path, account: str, cycle: str) -> dict[str, Any] | None:
    with (read_archive(root / 'events') as db, read_archive(root / 'publications') as publications):
        if db is None or not has_table(db, 'sandbox_cycles'):
            return None if cycle else {'trades': []}
        rows = db.exec_driver_sql("""
            SELECT * FROM sandbox_cycles WHERE account_id = %s AND source IN ('sandbox', 'sandbox_mcp') AND (%s = '' OR id = %s)
            ORDER BY ordinal DESC
        """, (account, cycle, cycle)).mappings()
        results = [trade(root, db, row, publications, detail=bool(cycle)) for row in rows]
    result = (results[0] if results else None) if cycle else {'trades': results}
    return cast(dict[str, Any] | None, visible(result))
