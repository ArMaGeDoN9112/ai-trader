"""Filter the saved futures catalog for tradable, unexpired series."""

from datetime import datetime
from typing import Any

from news_futures.domain.publications import utc_now


def listed_futures(catalog: dict[str, Any]) -> dict[str, dict[str, Any]]:
    now = utc_now()
    return {
        item['uid']: {**item, 'asset': asset['name'], 'share_id': asset.get('underlying_position_uid'),
                      'categories': asset.get('categories', [])}
        for asset in catalog['assets'] for item in asset['contracts']
        if item['api_trade_available'] is True and datetime.fromisoformat(item['last_trade']) > now
    }
