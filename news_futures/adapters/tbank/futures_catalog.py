"""Group available T-Invest futures by underlying asset and category."""

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from html import escape
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import httpx

from news_futures.adapters.tbank.rest import SANDBOX_REST, amount, identifier, rows
from news_futures.common.artifacts import move_artifact, write_json
from news_futures.news.publications import utc_now


CATEGORIES = {
    'financial': 'Финансы', 'oil_gas': 'Нефтегаз', 'metals_mining': 'Металлургия и добыча',
    'chemicals': 'Химия и удобрения', 'materials': 'Прочие материалы', 'it': 'Технологии',
    'telecom': 'Связь и медиа', 'consumer': 'Потребительский сектор', 'industrials': 'Промышленность и транспорт',
    'utilities': 'Электроэнергетика', 'health_care': 'Здравоохранение', 'real_estate': 'Недвижимость',
    'currency': 'Валюты', 'index': 'Индексы и фонды', 'rates': 'Ставки и облигации', 'crypto': 'Криптоактивы',
    'energy_commodity': 'Нефть, газ и топливо', 'metal_commodity': 'Металлы', 'agriculture': 'Агросырьё',
    'unclassified': 'Без категории',
}

SECTOR_CATEGORIES = {
    'financial': 'financial', 'energy': 'oil_gas', 'green_energy': 'utilities', 'utilities': 'utilities',
    'materials': 'materials', 'it': 'it', 'telecom': 'telecom', 'consumer': 'consumer',
    'industrials': 'industrials', 'health_care': 'health_care', 'real_estate': 'real_estate',
}

# More specific categories for assets whose broker sectors are broad or missing.
SHARE_OVERRIDES = {
    **dict.fromkeys(('ALRS', 'CHMF', 'ENPG', 'GMKN', 'MAGN', 'MTLR', 'NLMK', 'PLZL', 'POLY', 'RASP', 'RUAL'), 'metals_mining'),
    **dict.fromkeys(('AAOI', 'ARM', 'COHR', 'CRDO', 'CRWV', 'INOD', 'IONQ', 'SMCI', 'SNDK',
                     'ASML HOLDING NV', 'РАСЧЕТНОЕ ЗНАЧЕНИЕ АКЦИИ SK HYNIX INC.',
                     'APPLIED OPTOELECTRONICS, INC.', 'IONQ, INC.'), 'it'),
    'PHOR': 'chemicals', 'BE': 'utilities', 'OKLO': 'utilities', 'TEM': 'health_care', 'IREN': 'it',
    'RDDT': 'telecom', 'CRCL': 'financial',
}

ASSET_ALIASES = {
    'TYPE_COMMODITY': {'ALMN': 'Алюминий', 'Co': 'Медь', 'Nl': 'Никель', 'TTF': 'Газ (Европа)',
                       'Газ микро (США)': 'Газ (США)'},
    'TYPE_INDEX': {'NDX': 'Nasdaq 100', 'RTSI мини': 'RTSI', 'Индекс московской биржи': 'IMOEX'},
}

COMMODITY_GROUPS = {
    **dict.fromkeys(('Brent', 'CL', 'Нефть WTI', 'Газ (Европа)', 'Газ (США)', 'Бензин АИ-92', 'Бензин АИ-95',
                     'Дизельное топливо летнее'), 'energy_commodity'),
    **dict.fromkeys(('Алюминий', 'Золото', 'Золото в долларах', 'Золото в рублях', 'Медь', 'Никель', 'Палладий',
                     'Платина', 'Серебро', 'Цинк'), 'metal_commodity'),
    **dict.fromkeys(('Апельсиновый сок', 'Какао', 'Кофе', 'Пшеница', 'Сахар мировой', 'Сахар российский'), 'agriculture'),
}

INDEX_CATEGORIES = {
    **dict.fromkeys(('Индекс RUONIA', 'Индекс ключевой ставки', 'RGBILP', 'Индекс государственных облигаций',
                     'Индекс казначейских облигаций США'), ('rates',)),
    **dict.fromkeys(('BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'TRXUSDT', 'XRPUSDT', 'Bitcoin-фонд IBIT',
                     'iShares Ethereum Trust ETF', 'Индекс Bitcoin', 'Индекс Ethereum', 'Индекс Ripple',
                     'Индекс Solana', 'Индекс Tron', 'Индекс МосБиржи Биткоина', 'Индекс МосБиржи Рипла',
                     'Индекс МосБиржи Соланы', 'Индекс МосБиржи Трона', 'Индекс МосБиржи Эфира'), ('crypto',)),
    'Индекс нефти и газа': ('index', 'oil_gas'), 'Индекс металлов и добычи': ('index', 'metals_mining'),
    'Индекс потребительского сектора': ('index', 'consumer'), 'Индекс финансов': ('index', 'financial'),
    'Invesco PHLX Semiconductor ETF': ('index', 'it'),
}


def share_categories(symbols: set[str], sectors: set[str]) -> set[str]:
    overrides = {SHARE_OVERRIDES[symbol] for symbol in symbols if symbol in SHARE_OVERRIDES}
    return overrides or {SECTOR_CATEGORIES[sector] for sector in sectors if sector in SECTOR_CATEGORIES}


def group_futures(futures: list[dict[str, Any]], shares: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not futures or not shares:
        raise ValueError('empty_broker_catalog')
    shares_by_uid: dict[str, list[dict[str, Any]]] = defaultdict(list)
    categories_by_ticker: dict[str, set[str]] = defaultdict(set)
    for share in shares:
        ticker = identifier(share['ticker']).strip().upper()
        shares_by_uid[identifier(share['positionUid'])].append(share)
        categories_by_ticker[ticker].update(share_categories({ticker}, {share.get('sector', '')}))

    groups: dict[tuple[str, str], dict[str, Any]] = {}
    seen: set[str] = set()
    for future in futures:
        uid = identifier(future['uid'])
        available = future['apiTradeAvailableFlag']
        if uid in seen or type(available) is not bool:
            raise ValueError('invalid_future_identity_or_availability')
        seen.add(uid)
        if not available:
            continue
        asset_type = identifier(future['assetType'])
        code = identifier(future['basicAsset']).strip()
        base = ASSET_ALIASES.get(asset_type, {}).get(code, code)
        position_uid = future.get('basicAssetPositionUid') or ''
        if not isinstance(position_uid, str):
            raise ValueError('invalid_underlying_position_uid')
        # Broker currency codes remain stable across contracts with different underlying position UIDs.
        asset_id = position_uid if asset_type == 'TYPE_SECURITY' and position_uid else base
        group = groups.setdefault((asset_type, asset_id), {
            'id': f'{asset_type}:{asset_id}', 'broker_asset_type': asset_type,
            'name': base, 'broker_underlying_codes': set(), 'contracts': [],
            'underlying_position_uid': position_uid if asset_type == 'TYPE_SECURITY' else None,
        })
        group['broker_underlying_codes'].add(code)
        group['contracts'].append({
            'uid': uid, 'ticker': identifier(future['ticker']), 'class_code': identifier(future['classCode']),
            'name': identifier(future['name']), 'expiration': identifier(future['expirationDate']),
            'last_trade': identifier(future['lastTradeDate']), 'api_trade_available': available,
        })

    # Classify the actual underlying, including single-stock fixings listed by the broker as indices.
    for group in groups.values():
        asset_type, name = group['broker_asset_type'], group['name']
        codes = sorted(group['broker_underlying_codes'])
        group['broker_underlying_codes'] = codes
        categories: set[str] = set()
        if asset_type == 'TYPE_SECURITY':
            underlying = shares_by_uid.get(group['underlying_position_uid'], [])
            tickers = {identifier(share['ticker']).strip().upper() for share in underlying}
            categories = share_categories(tickers | {code.upper() for code in codes},
                                          {share.get('sector', '') for share in underlying})
            group['name'] = ', '.join(sorted(tickers)) if tickers else codes[0]
        elif asset_type == 'TYPE_INDEX':
            ticker = name.removeprefix('Фиксинг ')
            if ticker != name and ' ' not in ticker:
                categories = share_categories({ticker}, set()) or categories_by_ticker.get(ticker, set())
            else:
                categories = set(INDEX_CATEGORIES.get(name, ('index',)))
        elif asset_type == 'TYPE_COMMODITY' and name in COMMODITY_GROUPS:
            categories = {COMMODITY_GROUPS[name]}
        elif asset_type == 'TYPE_CURRENCY':
            categories = {'currency'}
        group['categories'] = sorted(categories or {'unclassified'})
        group['contracts'].sort(key=lambda item: (item['last_trade'], item['ticker'], item['uid']))
    return sorted(groups.values(), key=lambda group: (group['name'].casefold(), group['id']))


def fetch_catalog(client: httpx.Client) -> dict[str, Any]:
    listings = []
    for method in ('Futures', 'Shares'):
        response = client.post(f'{SANDBOX_REST}InstrumentsService/{method}',
                               json={'instrumentStatus': 'INSTRUMENT_STATUS_ALL'})
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError('invalid_broker_catalog')
        listings.append(rows(payload, 'instruments'))
    return {'fetched_at': utc_now().isoformat(), 'source': 't-invest-sandbox',
            'categories': CATEGORIES, 'assets': group_futures(*listings)}


def listed_futures(catalog: dict[str, Any]) -> dict[str, dict[str, Any]]:
    now = utc_now()
    return {
        item['uid']: {**item, 'asset': asset['name'], 'share_id': asset.get('underlying_position_uid')}
        for asset in catalog['assets'] for item in asset['contracts']
        if item['api_trade_available'] is True and datetime.fromisoformat(item['last_trade']) > now
    }


def active_futures(client: httpx.Client, catalog: dict[str, Any]) -> list[dict[str, Any]]:
    contracts = listed_futures(catalog)

    def read(method: str, field: str, ids: list[str]) -> list[dict[str, Any]]:
        response = client.post(f'{SANDBOX_REST}MarketDataService/{method}', json={'instrumentId': ids})
        response.raise_for_status()
        return rows(response.json(), field)

    def batch(ids: list[str]) -> list[dict[str, Any]]:
        requested = set(ids)
        active = {
            uid for status in read('GetTradingStatuses', 'tradingStatuses', ids)
            if (uid := status['instrumentUid']) in requested and status.get('apiTradeAvailableFlag') is True
            and any(status.get(flag) is True for flag in
                    ('limitOrderAvailableFlag', 'marketOrderAvailableFlag', 'bestpriceOrderAvailableFlag'))
        }
        if not active:
            return []
        try:
            quotes = read('GetLastPrices', 'lastPrices', sorted(active))
        except (httpx.HTTPError, KeyError, ValueError, TypeError):
            quotes = []
        prices = {}
        for row in quotes:
            try:
                prices[row['instrumentUid']] = {'price': str(amount(row['price'])), 'quote_at': row['time']}
            except (KeyError, ValueError, TypeError):
                continue
        return [contracts[uid] | prices.get(uid, {}) for uid in sorted(active)]

    ids = list(contracts)
    with ThreadPoolExecutor(max_workers=4) as pool:
        batches = pool.map(batch, (ids[start:start + 100] for start in range(0, len(ids), 100)))
        return sorted((item for group in batches for item in group), key=lambda item: (item['ticker'], item['uid']))


def save_catalog(directory: Path, catalog: dict[str, Any]) -> dict[str, int]:
    assets = catalog['assets']
    total = sum(len(group['contracts']) for group in assets)
    lines = ['# Каталог фьючерсов Т‑Инвестиций', '', f"Снимок от {catalog['fetched_at']} (UTC). "
             f'Через API доступны {total} контрактов на {len(assets)} базовых активов.',
             '', 'UID и даты доступных контрактов есть в [catalog.json](catalog.json). '
             'Актив может входить в несколько категорий.', '']
    for category, title in catalog['categories'].items():
        members = [group for group in assets if category in group['categories']]
        if not members:
            continue
        category_contracts = sum(len(group['contracts']) for group in members)
        lines += [f'## {title}', '', f'Базовых активов — {len(members)}, контрактов — {category_contracts}.', '',
                  '| Базовый актив | Доступные тикеры | Всего контрактов |',
                  '| --- | --- | ---: |']
        for group in members:
            name = escape(group['name']).replace('|', '&#124;')
            contracts = group['contracts']
            tickers = ', '.join(f"`{contract['ticker']}`" for contract in contracts)
            lines.append(f'| {name} | {tickers} | {len(contracts)} |')
        lines.append('')
    directory.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=directory) as temporary:
        staging = Path(temporary)
        write_json(staging / 'catalog.json', catalog)
        (staging / 'groups.md').write_text('\n'.join(lines), encoding='utf-8')
        move_artifact(staging / 'groups.md', directory / 'groups.md', replace=True)
        move_artifact(staging / 'catalog.json', directory / 'catalog.json', replace=True)
    return {'assets': len(assets), 'contracts': total, 'api_trade_available': total}
