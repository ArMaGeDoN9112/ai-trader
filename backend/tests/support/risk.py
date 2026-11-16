"""Sandbox approval, account and instrument fixtures."""

from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
import unittest
from unittest.mock import patch

from news_futures.agents.contracts import RoleSettings
from news_futures.agents.decisions import analyze_event
from news_futures.agents.events import EventArchive
from news_futures.common.artifacts import write_json
from news_futures.news.publications import Archive, Publication, utc_now
from tests.support.models import ScriptedEndpoint, review
from tests.support.news import company_link

class RiskFixture(unittest.TestCase):
    def setUp(self) -> None:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.events_db = self.root / 'events.sqlite3'
        self.publications_db = self.root / 'publications.sqlite3'
        self.now = utc_now()
        self.publication = Publication('rbc', '1', 'https://t.me/rbc/1',
                                       'New earnings forecast.', self.now, self.now)
        self.config = self.root / 'models.json'
        write_json(self.config, {'analyst': {'model': 'test/analyst'}, 'critic': {'model': 'test/critic'}})
        self.contract_id = 'control-future'
        self.account_id = 'sandbox-account'
        self.snapshot: dict[str, Any] = {
            'account_id': self.account_id, 'account_observed_at': self.now,
            'currency': 'RUB', 'equity': Decimal('100000'), 'available_margin': Decimal('90000'),
            'has_position': False, 'has_active_orders': False, 'execution_uncertain': False,
            'contract': {
                'contract_id': self.contract_id, 'company_id': 'sber', 'share_id': 'sber-share',
                'observed_at': self.now, 'quote_at': self.now,
                'last_trade_at': self.now + timedelta(days=1), 'last_price': Decimal('300'),
                'currency': 'RUB', 'price_unit': 'points', 'contracts_per_lot': 2,
                'tick_size': Decimal('0.5'), 'tick_value_per_contract': Decimal('2.50'),
                'margin_per_lot_buy': Decimal('1000'), 'margin_per_lot_sell': Decimal('2000'),
                'round_trip_cost_per_lot': Decimal('25'),
                'limit_buy_available': True, 'limit_sell_available': True,
            },
        }

    def approved(self, *, historical: bool = False, **terms: Any) -> Path:
        archive = Archive(self.publications_db)
        try:
            archive.save_batch('rbc', 1, [self.publication], historical=historical)
        finally:
            archive.db.close()
        publication = self.publication.record()
        directory = self.root / f'analysis-{len(list(self.root.iterdir()))}'
        snapshot_path = directory.with_suffix('.json')
        write_json(snapshot_path, {
            'account': {'id': self.account_id, 'observed_at': self.now.isoformat(),
                        'currency': 'RUB', 'equity': 100000, 'available_margin': 90000},
            'position': None, 'instruments': [
                {'company_id': 'sber', 'company_name': 'Sber', 'share_id': 'sber-share',
                 'contract_id': self.contract_id, 'allowed_for_entry': True,
                 'market': {'observed_at': self.now.isoformat(), 'last_price': 300}},
            ],
        })
        with EventArchive(self.events_db) as events:
            matcher = ScriptedEndpoint([{'match': 'different', 'event_id': None, 'change': None,
                                         'reason': 'Independent report.', 'evidence': None}])
            event = events.match(publication, [company_link(publication, 'sber')],
                                 RoleSettings(model='test/matcher'), model_factory=matcher.model)
            proposal = {
                'action': 'enter', 'publication_id': publication['id'], 'company_id': 'sber',
                'contract_id': self.contract_id, 'side': 'long', 'entry_price': 300,
                'stop_loss': 290, 'take_profit': 320, 'reason': 'New forecast.',
                'level_rationale': {'entry': 'Quote.', 'stop_loss': 'Support.', 'take_profit': 'Target.'},
                'assessments': [{'contract_id': self.contract_id, 'impact': 'verified', 'reason': 'Forecast.',
                                 'evidence': [{'revision': event['revision'], 'quote': publication['text']}]}],
            } | terms
            endpoint = ScriptedEndpoint([proposal, review()])
            with patch('news_futures.agents.decisions.chat_model', endpoint.model):
                trace = analyze_event(events, event['decision_id'], snapshot_path, self.config, directory,
                                      mode='sandbox')
            self.assertEqual(trace['status'], 'approved_for_checks')
        return directory
