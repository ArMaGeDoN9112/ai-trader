"""Sandbox approval, account and instrument fixtures."""

from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
import unittest

from news_futures.schemas.contracts import RoleSettings
from news_futures.agents.analysis import TEAM_DIRECTORY
from news_futures.agents.decisions import news_record
from news_futures.agents.schemas import EventEvidence, ProposalContext
from news_futures.common.artifact_paths import market_path
from news_futures.schemas.company import CompanyProposal, Market, MarketObservation
from news_futures.storage.events import EventArchive
from news_futures.agents.events import match_event
from news_futures.common.artifacts import write_json
from news_futures.storage.publications import Archive
from news_futures.dataflows.publications import Publication, publication_digest, utc_now
from tests.support.models import ScriptedEndpoint
from tests.support.news import company_link

class RiskFixture(unittest.TestCase):
    def setUp(self) -> None:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.events_db = self.root / 'events'
        self.publications_db = self.root / 'publications'
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
                'trading_status': 'SECURITY_TRADING_STATUS_NORMAL_TRADING',
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
        observation = MarketObservation(
            contract_id=self.contract_id, market=Market(observed_at=self.now, last_price=300),
            received_at=self.now, price_unit='points', limit_buy_available=True, limit_sell_available=True,
        )
        path = market_path(directory, observation.id)
        path.parent.mkdir(parents=True)
        write_json(path, observation.model_dump(mode='json'))
        write_json(directory / 'catalog.json', {'operations': {'catalog': {'availability': 'available', 'data': [
            {'company_id': 'sber', 'company_name': 'Sber', 'share_id': 'sber-share', 'ticker': 'SBER'},
        ]}}})
        write_json(directory / 'futures-catalog.json', {'assets': [
            {'name': 'Sber', 'underlying_position_uid': 'sber-share', 'contracts': [
                {'uid': self.contract_id, 'api_trade_available': True,
                 'last_trade': (self.now + timedelta(days=1)).isoformat()},
            ]},
        ]})
        write_json(directory / 'runtime.json', {'mode': 'sandbox', 'account_id': self.account_id})
        write_json(directory / 'instrument.json', {'at': self.now.isoformat(), 'status': 'completed', 'error': None, 'selected': {
            'company_id': 'sber', 'contract_id': self.contract_id,
        }})
        with EventArchive(self.events_db) as events:
            matcher = ScriptedEndpoint([{'match': 'different', 'event_id': None, 'change': None,
                                         'reason': 'Independent report.', 'evidence': None}])
            event = match_event(events, publication, [company_link(publication, 'sber')],
                                 RoleSettings(model='test/matcher'), model_factory=matcher.model)
            proposal = {
                'action': 'enter', 'publication_id': publication['id'], 'company_id': 'sber',
                'contract_id': self.contract_id, 'side': 'long', 'entry_price': 300,
                'stop_loss': 290, 'take_profit': 320, 'reason': 'New forecast.',
                'level_rationale': {'entry': 'Quote.', 'stop_loss': 'Support.', 'take_profit': 'Target.'},
                'assessments': [{'contract_id': self.contract_id, 'impact': 'verified', 'reason': 'Forecast.',
                                 'evidence': [{'revision': event['revision'], 'quote': publication['text']}]}],
            } | terms
        event_evidence = EventEvidence.model_validate(news_record(event))
        context = ProposalContext.model_validate({
            'publication': event_evidence.publication, 'event': event_evidence.summary(), 'affected_company_ids': ['sber'],
            'candidates': [{'company_id': 'sber', 'company_name': 'Sber', 'share_id': 'sber-share',
                            'contract_id': self.contract_id, 'allowed_for_entry': True, 'market': observation.market,
                            'market_observation_id': observation.id}],
            'account': {'observed_at': self.now, 'currency': 'RUB', 'equity': 100000, 'available_margin': 90000},
            'position': None, 'entry_reason': None, 'related_news': [],
        }).model_dump(mode='json')
        payload = CompanyProposal.model_validate(proposal).model_dump(mode='json')
        revision = event_evidence.revision
        identity = publication_digest({'revision': revision, 'account_id': self.account_id,
                                       'context': context, 'proposal': payload})
        identified = payload | {'id': identity}
        formed_at = utc_now().isoformat()
        call = {'started_at': formed_at, 'at': formed_at, 'status': 'completed'}
        directory = directory / TEAM_DIRECTORY
        directory.mkdir()
        write_json(directory / 'trace.json', {
            'revision': revision, 'strategy': 'tool_agent', 'version': 'test', 'account_id': self.account_id,
            'outcome': {'status': 'candidate', 'reason': payload['reason'], 'proposal': identified,
                        'critic_status': 'approved', 'observation_id': observation.id},
            'calls': [
                call | {'id': '1', 'kind': 'analyst', 'output': identified,
                        'input': {'context': context, 'proposal': None, 'review': None}},
                call | {'id': '2', 'kind': 'critic', 'input': {'context': context, 'proposal': identified},
                        'output': {'proposal_id': identity, 'approved': True, 'objections': [], 'reason': 'Checked.'}},
            ],
        })
        return directory
