"""Interpret saved broker reports as the company and contract universe."""

from datetime import datetime
import json
from typing import Annotated, Any, TypedDict

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from news_futures.dataflows.publications import utc_now


Nonempty = Annotated[str, Field(min_length=1)]


class CatalogRow(BaseModel):
    model_config = ConfigDict(strict=True, str_strip_whitespace=True, extra='ignore')
    company_id: Nonempty
    company_name: Nonempty
    ticker: Nonempty
    share_id: Nonempty | None = None


class Company(TypedDict):
    names: list[str]
    tickers: list[str]


def catalog_rows(content: bytes) -> list[CatalogRow]:
    report = json.loads(content)
    try:
        catalog = report['operations']['catalog']
        if catalog['availability'] != 'available':
            raise ValueError('catalog_unavailable')
        return TypeAdapter(list[CatalogRow]).validate_python(catalog['data'])
    except (KeyError, TypeError) as error:
        raise ValueError('invalid_catalog_report') from error


def catalog_contracts(companies: bytes, futures: bytes) -> dict[str, str]:
    owners = {row.share_id: row.company_id for row in catalog_rows(companies) if row.share_id}
    return {uid: owners[item['share_id']] for uid, item in listed_futures(json.loads(futures)).items()
            if item['share_id'] in owners}


def load_catalog(content: bytes) -> dict[str, Company]:
    companies: dict[str, dict[str, Any]] = {}
    for row in catalog_rows(content):
        company = companies.setdefault(row.company_id, {'names': set(), 'tickers': set()})
        company['names'].add(row.company_name)
        company['tickers'].add(row.ticker)
    if not companies:
        raise ValueError('catalog_empty')
    return {
        company_id: {'names': sorted(company['names']), 'tickers': sorted(company['tickers'])}
        for company_id, company in companies.items()
    }


def listed_futures(catalog: dict[str, Any]) -> dict[str, dict[str, Any]]:
    now = utc_now()
    return {
        item['uid']: {**item, 'asset': asset['name'], 'share_id': asset.get('underlying_position_uid'),
                      'categories': asset.get('categories', [])}
        for asset in catalog['assets'] for item in asset['contracts']
        if item['api_trade_available'] is True and datetime.fromisoformat(item['last_trade']) > now
    }
