"""Project sandbox journal records into trade details without local paths or credentials."""

from decimal import Decimal
import json
from pathlib import Path
import sqlite3
from typing import Any, cast

from news_futures.operator.news import REVISION, archive_news, event_rows, has_table, read_archive, runtime_rows, visible
from news_futures.trading.journal import journal_events


def linked_revision(root: Path, approval: str) -> str | None:
    try:
        revision = Path(approval).relative_to(root / 'decisions').parts[0]
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


def trade(root: Path, db: sqlite3.Connection, row: sqlite3.Row,
          publications: sqlite3.Connection | None, *, detail: bool) -> dict[str, Any]:
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
    approvals = [('entry', row['approval'])]
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
        elif kind == 'reviewed_close':
            approvals.append(('exit', data['approval']))
            exit_reason = 'news_exit'
        elif kind == 'reviewed_close_abandoned' and exit_reason == 'news_exit':
            exit_reason = None
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
    seen = set()
    for kind, approval in approvals:
        revision = linked_revision(root, approval)
        if revision and revision not in seen and next(
            archive_news(root, runtime_rows(publications, revision), event_rows(db, revision)), None
        ):
            traces.append({'kind': kind, 'revision': revision})
            seen.add(revision)
    reason = (db.execute('SELECT * FROM position_entry_reasons WHERE account_id = ? AND position_id = ?',
                         (row['account_id'], row['id'])).fetchone()
              if has_table(db, 'position_entry_reasons') else None)
    protection = {'protected': 'confirmed', 'closed': 'none', 'no_fill': 'none'}.get(row['status'], 'unknown')
    return summary | {'traces': traces, 'events': events, 'orders': order_list, 'entry_reason': reason['reason'] if reason else None,
                      'entry_sources': json.loads(dict(reason).get('sources', '[]')) if reason else [],
                      'requested_lots': terms['lots'], 'confirmed_at': confirmed_at, 'protection': protection,
                      'protection_orders': protection_orders, 'exit_reason': exit_reason}


def trades_view(root: Path, account: str, cycle: str) -> dict[str, Any] | None:
    with (read_archive(root / 'events.sqlite3') as db, read_archive(root / 'publications.sqlite3') as publications):
        if db is None or not has_table(db, 'sandbox_cycles'):
            return None if cycle else {'trades': []}
        rows = db.execute("""
            SELECT * FROM sandbox_cycles WHERE account_id = ? AND source = 'sandbox' AND (? = '' OR id = ?)
            ORDER BY rowid DESC
        """, (account, cycle, cycle))
        results = [trade(root, db, row, publications, detail=bool(cycle)) for row in rows]
    result = (results[0] if results else None) if cycle else {'trades': results}
    return cast(dict[str, Any] | None, visible(result))
