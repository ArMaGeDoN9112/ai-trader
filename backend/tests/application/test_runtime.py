import asyncio
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from itertools import chain
import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import Any, cast
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from tests.support.risk import RiskFixture
from tests.support.routing import answer
from tests.support.models import ScriptedEndpoint, review
from tests.support.broker import ControlBroker
from tests.support.telegram import ControlledTransport

from news_futures.cli import main
from news_futures.llm_clients.groq import GroqChat
from news_futures.schemas.contracts import Review
from news_futures.storage.database import archive_exists
from news_futures.storage.locks import exclusive_operation
from news_futures.storage.events import EventArchive
from news_futures.storage.checkpoints import checkpoint_serializer
from news_futures.common.paths import DECISIONS_SETTINGS, GROQ_SETTINGS, REVIEW_SETTINGS, TELEGRAM_SOURCES
from news_futures.trading.execution import Fill
from news_futures.brokers.contracts import UnsupportedFuturesUnits
from news_futures.llm_clients.chat import chat_model
from news_futures.llm_clients.decisions import choices, request_choices
from news_futures.llm_clients.contracts import AnalysisModels
from news_futures.common.artifacts import replace_json, write_json
from news_futures.api.news import news_view, trace_view
from news_futures.api.settings import Preferences
from news_futures.storage.publications import Archive
from news_futures.dataflows.publications import Publication, utc_now
from news_futures.runtime import Runtime
from news_futures.startup import run_runtime
from news_futures.config import load_settings
from news_futures.storage.operator_state import OperatorState
from news_futures.storage.runtime import PostgresRuntimeStorage
from news_futures.workflow import Shutdown
from news_futures.brokers.futures_catalog import save_catalog
from news_futures.brokers.tbank import SANDBOX_REST, TBankReader, limit_broker_read, restrict_to_reads
from news_futures.dataflows.telegram import Difference, Source


class Interrupted(BaseException):
    pass


class RuntimeTest(unittest.TestCase):
    def test_profile_loads_packaged_defaults(self) -> None:
        packaged = {'sources': TELEGRAM_SOURCES, 'decisions': DECISIONS_SETTINGS, 'groq': GROQ_SETTINGS, 'review': REVIEW_SETTINGS}
        with TemporaryDirectory() as directory:
            profile = Path(directory) / 'runtime.json'
            write_json(profile, {
                'data_dir': '.', 'catalog': 'catalog.json', 'account_id': 'account',
            })
            settings = load_settings(profile)
        for field, path in packaged.items():
            self.assertEqual(getattr(settings, field), path)
            self.assertTrue(path.is_file())

    def test_checkpoint_deserializes_review_model(self) -> None:
        record = Review(proposal_id='proposal', approved=True, objections=[], reason='Checked.')
        data = checkpoint_serializer().dumps_typed(record)
        restored = checkpoint_serializer().loads_typed(data)
        self.assertIsInstance(restored, Review)
        self.assertEqual(restored, record)

    def test_invalid_config_is_reported_without_echoing_values(self) -> None:
        with TemporaryDirectory() as directory:
            config = Path(directory) / 'runtime.json'
            config.write_text(json.dumps({'account_id': 'account', 'token': 'private-value'}))
            with patch('sys.argv', ['news_futures', 'serve', str(config), '--once']), patch(
                'builtins.print',
            ) as output:
                self.assertEqual(main(), 1)
            self.assertIn('docs/runtime.md', str(output.call_args))
            self.assertNotIn('private-value', str(output.call_args))

    def test_observation_reader_has_no_order_endpoint(self) -> None:
        with httpx.Client(transport=httpx.MockTransport(
            lambda request: self.fail(f'Unexpected broker request: {request.url.path}'),
        ), event_hooks={'request': [restrict_to_reads]}) as client:
            reader = TBankReader(client, 'account', 'contract', 'company')
            with self.assertRaisesRegex(ValueError, 'sandbox_read_method_required'):
                reader.call('PostSandboxOrder')
            self.assertFalse(hasattr(reader, 'enter'))
            with self.assertRaisesRegex(ValueError, 'sandbox_read_method_required'):
                client.post(SANDBOX_REST + 'SandboxService/PostSandboxOrder')


class LiveControlBroker(ControlBroker):
    def read_risk(self, account_id: str, contract_id: str) -> object:
        risk = deepcopy(self.risk.snapshot)
        risk.update(has_position=bool(self.state.position_lots) or risk['has_position'],
                    has_active_orders=any(stop.status == 'active' for stop in self.state.protection))
        if contract_id != self.risk.contract_id:
            risk['contract'].update(contract_id=contract_id, company_id='vtb', share_id='vtb-share')
        return risk


class RuntimeAcceptance(RiskFixture):
    def setUp(self) -> None:
        super().setUp()
        self.started_at = utc_now()
        self.broker = LiveControlBroker(self)
        self.market = ControlledTransport()
        self.market.source = Source(123, 'markettwits')
        self.market.messages = [replace(self.market.original, source=self.market.source.id)]
        self.rbc = ControlledTransport()
        self.rbc.source = Source(456, 'rbc_news')
        self.rbc.messages = [replace(self.rbc.original, source=self.rbc.source.id)]
        client = AsyncMock(return_value=SimpleNamespace(pts=1, qts=0, seq=1, date=utc_now()))
        client.get_me.return_value = SimpleNamespace(id=1)
        self.enterContext(patch('news_futures.dataflows.telegram.open_client', return_value=client))
        self.enterContext(patch('news_futures.dataflows.telegram.TelethonTransport', return_value=self))
        self.enterContext(patch('news_futures.startup.load_token', return_value='test-key'))
        self.enterContext(patch('news_futures.llm_clients.decisions.load_token', return_value='test-key'))
        self.enterContext(patch('news_futures.llm_clients.chat.load_token', return_value='test-key'))
        self.enterContext(patch('news_futures.startup.sandbox_client', side_effect=lambda *args, **kw: httpx.Client()))
        self.futures_snapshot: dict[str, Any] = {
            'fetched_at': self.started_at.isoformat(), 'categories': {'financial': 'Финансы'},
            'assets': [{'name': company, 'categories': ['financial'], 'underlying_position_uid': f'{company}-share',
                        'contracts': [{'uid': uid, 'ticker': uid, 'api_trade_available': True,
                                       'last_trade': (self.started_at + timedelta(days=1)).isoformat()}]}
                       for company, uid in (('sber', self.contract_id), ('vtb', 'vtb-future'))],
        }
        self.fetch_futures = self.enterContext(patch('news_futures.startup.fetch_catalog',
                                                   return_value=self.futures_snapshot))
        save_catalog(self.root / 'futures-catalog', self.futures_snapshot)
        self.enterContext(patch('news_futures.startup.TBankReader', return_value=self.broker))
        self.write_broker = self.enterContext(patch('news_futures.startup.TBankSandbox', return_value=self.broker))
        self.jev_requests: list[dict[str, Any]] = []
        self.category_requests: list[dict[str, Any]] = []
        self.instrument_requests: list[dict[str, Any]] = []
        self.instrument_choice = 'contract_0'
        jev_client = self.enterContext(httpx.Client(transport=httpx.MockTransport(self.jev_reply)))
        self.enterContext(patch('news_futures.llm_clients.decisions.request_choices',
                                side_effect=lambda _client, *args, **kw: request_choices(jev_client, *args, **kw)))
        self.catalog = self.root / 'catalog.json'
        write_json(self.catalog, {'operations': {'catalog': {'availability': 'available', 'data': [
            {'company_id': company, 'company_name': company.upper(), 'ticker': company.upper(),
             'share_id': f'{company}-share'}
            for company in ('sber', 'vtb')
        ]}}})
        self.profile = self.root / 'runtime.json'
        self.profile_data = {
            'data_dir': '.', 'catalog': self.catalog.name, 'account_id': self.account_id,
            'session': 'telegram.session', 'review': self.config.name,
        }
        write_json(self.profile, self.profile_data)

    def jev_reply(self, request: httpx.Request) -> httpx.Response:
        data = json.loads(request.content)
        self.assertEqual(data['model'], 'parallel/decisions-fast:free')
        if 'instrument' in data['questions']:
            self.instrument_requests.append(data)
            return httpx.Response(200, json={'id': 'instrument-result', 'model': data['model'], 'answers': {
                'instrument': {'type': 'choice', 'choice': self.instrument_choice, 'confidence': 1.0,
                               'probabilities': {key: float(key == self.instrument_choice)
                                                 for key in data['questions']['instrument']['criteria']}},
            }})
        if all(key.startswith('category_') for key in data['questions']):
            self.category_requests.append(data)
            return httpx.Response(200, json={'id': 'category-result', 'model': data['model'], 'answers': {
                key: {'type': 'choice', 'choice': 'related' if key == 'category_financial' else 'unrelated',
                      'probabilities': {'related': float(key == 'category_financial'),
                                        'unrelated': float(key != 'category_financial')}, 'confidence': 1.0}
                for key in data['questions']
            }})
        self.jev_requests.append(data)
        text = data['state']['publication']
        return httpx.Response(200, json={
            'id': 'jev-result', 'model': data['model'],
            'answers': {
                key: answer('direct' if data['state'][key]['names'][0] in text else 'none')
                for key in data['questions']
            },
        })

    def source(self, source: Source) -> ControlledTransport:
        return self.market if source == self.market.source else self.rbc

    async def resolve(self, username: str) -> Source:
        return {'markettwits': self.market.source, 'rbc_news': self.rbc.source}[username]

    async def current_pts(self, source: Source) -> int:
        return await self.source(source).current_pts(source)

    async def history(self, source: Source, *window: Any) -> list[Publication]:
        return await self.source(source).history(source, *window)

    async def difference(self, source: Source, pts: int) -> Difference:
        return await self.source(source).difference(source, pts)

    def news(self, transport: ControlledTransport, text: str, *, media: bool = False) -> Publication:
        transport.pts += 1
        now = utc_now()
        publication = Publication(transport.source.id, str(transport.pts), f'https://t.me/source/{transport.pts}',
                                  text, now, now, unread_media=media)
        transport.differences.append(Difference(transport.pts, [publication]))
        return publication

    def proposal(self, action: str, publication: Publication) -> dict[str, Any]:
        entering = action == 'enter'
        return {
            'action': action, 'publication_id': publication.record()['id'],
            'company_id': None if action == 'skip' else 'sber',
            'contract_id': None if action == 'skip' else self.contract_id,
            'side': 'long' if entering else None, 'entry_price': 300 if entering else None,
            'stop_loss': 290 if entering else None, 'take_profit': 320 if entering else None,
            'reason': 'Исходное основание входа.' if entering else 'Сверено с первоисточником.',
            'level_rationale': {'entry': 'Quote.', 'stop_loss': 'Support.', 'take_profit': 'Target.'}
            if entering else None,
            'assessments': [{'contract_id': self.contract_id, 'impact': 'verified', 'reason': 'Forecast.',
                             'evidence': [{'revision': publication.revision_key(), 'quote': publication.text}]}],
        }

    def serve(self, *answers: Any, code: int = 0, started_at: datetime | None = None) -> ScriptedEndpoint:
        endpoint = ScriptedEndpoint(list(answers))

        def model(**kwargs: Any) -> Any:
            from news_futures.schemas.contracts import RoleSettings
            return endpoint.model(RoleSettings(model=kwargs['model']))

        with (patch('news_futures.llm_clients.chat.OpenRouterChat', side_effect=model),
              patch('news_futures.startup.utc_now',
                    side_effect=chain((started_at or self.started_at,), iter(utc_now, None))),
              patch('sys.argv', ['news_futures', 'serve', str(self.profile), '--once']),
              patch('builtins.print') as output):
            self.assertEqual(main(), code, str(output.call_args_list))
        return endpoint

    def report(self, publication: Publication | None = None) -> dict[str, Any]:
        command = ['news_futures', 'runtime-status', str(self.profile)]
        if publication is not None:
            command += ['--decision', publication.revision_key()]
        with patch('sys.argv', command), patch('builtins.print') as output:
            self.assertEqual(main(), 0, str(output.call_args_list))
        return cast(dict[str, Any], json.loads(str(output.call_args.args[0])))

    def sandbox(self) -> None:
        self.profile.write_text(json.dumps(self.profile_data | {'mode': 'sandbox'}), encoding='utf-8')

    def open_position(self) -> None:
        from news_futures.trading.executor import SandboxExecutor

        approval = self.approved()
        with EventArchive(self.events_db) as events, Archive(self.publications_db) as archive:
            result = SandboxExecutor(events, self.broker).start(archive, approval, allowed_contracts={self.contract_id})
        self.assertEqual(result['status'], 'protected')

    def delegate(self) -> dict[str, str]:
        return {'status': 'delegate', 'company_id': 'sber', 'reason': 'Проверь прогноз.'}

    def test_manual_news_runs_in_sandbox_and_is_ineligible_for_execution(self) -> None:
        from news_futures.agents.analysis import TEAM_DIRECTORY
        from news_futures.api.news import enqueue_test_news, testing_view
        from news_futures.trading.risk import reserve_entry

        self.sandbox()
        revision = enqueue_test_news(self.root, {'text': 'SBER опубликовал повышенный прогноз.'})
        archive = Archive(self.publications_db)
        self.addCleanup(archive.db.close)
        row = archive.db.exec_driver_sql('SELECT publication FROM revisions WHERE revision = %s',
                                              (revision,)).mappings().fetchone()
        assert row is not None
        saved = json.loads(row['publication'])
        at = datetime.fromisoformat(saved['published_at'])
        incoming = Publication(saved['source'], saved['external_id'], saved['url'], saved['text'], at, at)
        self.serve(self.delegate(), self.market_call(), self.analyst_answer(self.proposal('enter', incoming)),
                   review(), started_at=at + timedelta(microseconds=1))
        result = self.report(incoming)['result']
        self.assertEqual(result['status'], 'candidate')
        self.write_broker.assert_not_called()
        self.assertEqual(testing_view(self.root)['decisions'][0]['revision'], revision)
        view = trace_view(self.root, self.account_id, revision)
        self.assertTrue(view and view['testing'])
        self.assertNotIn(revision, {row['revision'] for row in news_view(
            self.root, self.profile_data, {}, {}, '',
        )['decisions']})
        directory = self.root / 'decisions' / revision / TEAM_DIRECTORY
        with EventArchive(self.events_db) as events:
            self.assertNotIn(revision, {row['revision'] for row in events.history(current_only=True)})
            admission = reserve_entry(events, archive, directory, account_id=self.account_id,
                                      allowed_contracts={self.contract_id}, read_snapshot=self.broker.read_risk)
        self.assertEqual(admission.reason, 'testing_publication')

    def tool_call(self, name: str, args: dict[str, Any]) -> dict[str, Any]:
        return {'tool_calls': [{'id': f'call-{name}', 'type': 'function',
                                'function': {'name': name, 'arguments': json.dumps(args)}}]}

    def market_call(self) -> dict[str, Any]:
        return self.tool_call('get_market_context', {'contract_id': self.contract_id})

    def analyst_answer(self, proposal: dict[str, Any]) -> dict[str, Any]:
        return self.tool_call('AnalystAnswer', {'proposal': proposal, 'missing_data': []})

    def test_analysis_delegates_and_publishes_a_reviewed_candidate_without_execution(self) -> None:
        from news_futures.trading.approval import read_approval
        from news_futures.agents.analysis import TEAM_DIRECTORY
        from news_futures.common.errors import Rejected
        from news_futures.api.news import trace_view

        self.sandbox()
        incoming = self.news(self.market, 'SBER опубликовал повышенный прогноз.')

        def follow_up(body: dict[str, Any]) -> dict[str, Any]:
            objections = json.loads(body['messages'][-1]['content'])['review']['objections']
            self.assertEqual(objections, ['Insufficient independent evidence.'])
            self.assertEqual(body['messages'][-1]['name'], 'critic')
            return self.analyst_answer(self.proposal('enter', incoming) | {
                'entry_price': 301, 'reason': 'Уточнённое основание входа.',
            })

        endpoint = self.serve(
            self.delegate(),
            self.tool_call('get_company_news', {'company_id': 'sber'}),
            self.market_call(),
            self.analyst_answer(self.proposal('enter', incoming)),
            review(False),
            follow_up,
            review(),
        )
        report = self.report(incoming)
        self.assertEqual(report['result']['mode'], 'sandbox')
        self.assertEqual(report['result']['status'], 'candidate')
        self.assertEqual(report['result']['critic_status'], 'approved')
        calls = report['agent']['calls']
        self.assertEqual([call['kind'] for call in calls], [
            'coordinator', 'analyst', 'get_company_news', 'analyst', 'get_market_context',
            'analyst', 'critic', 'analyst', 'critic',
        ])
        self.assertEqual(calls[1]['output'], {'tool': 'get_company_news'})
        self.assertEqual(calls[1]['actual_model'], 'test/analyst')
        self.assertEqual(calls[1]['provider'], 'TestProvider')
        self.assertEqual(calls[2]['arguments'], {'company_id': 'sber'})
        self.assertEqual(calls[4]['arguments'], {'contract_id': self.contract_id})
        decision_dir = self.root / 'decisions' / incoming.revision_key()
        observation_id = calls[4]['output']['observation_id']
        self.assertEqual(calls[6]['input']['context']['candidates'][0]['market_observation_id'], observation_id)
        self.assertEqual(calls[8]['input']['context']['candidates'][0]['market_observation_id'], observation_id)
        self.assertTrue((decision_dir / 'market' / f'{observation_id}.json').is_file())
        candidate_path, = (decision_dir / 'candidates').glob('*.json')
        self.assertEqual(json.loads(candidate_path.read_bytes())['observation_id'], observation_id)
        self.assertEqual([tool['function']['name'] for tool in endpoint.requests[2]['tools']],
                         ['AnalystAnswer', 'get_market_context', 'get_market_reaction',
                          'get_event_versions', 'compare_market', 'get_upcoming_events'])
        read_result = endpoint.requests[3]['messages'][-1]
        self.assertEqual(json.loads(read_result['content'])['spread_points'], 1.0)
        self.assertEqual(calls[4]['output']['instrument']['market']['order_book']['ask']['quantity_lots'], 3)
        self.assertEqual(calls[4]['output']['instrument']['market']['trading_status'],
                         'SECURITY_TRADING_STATUS_NORMAL_TRADING')
        self.assertEqual((read_result['role'], read_result['tool_call_id']),
                         ('tool', 'call-get_market_context'))
        self.assertEqual([call['handoff_to'] for call in calls if 'handoff_to' in call],
                         ['analyst', 'critic', 'analyst', 'critic'])
        proposals = [call['output'] for call in calls if call.get('handoff_to') == 'critic']
        self.assertNotEqual(proposals[0]['id'], proposals[1]['id'])
        self.assertEqual(report['result']['proposal']['id'], proposals[1]['id'])
        self.assertEqual([request['model'] for request in endpoint.requests], [
            'test/analyst', 'test/analyst', 'test/analyst', 'test/analyst', 'test/critic', 'test/analyst', 'test/critic',
        ])
        for index, request in enumerate(endpoint.requests):
            if index in (1, 2, 3, 5):
                self.assertEqual(request['tool_choice'], 'required')
            else:
                self.assertNotIn('tools', request)
                self.assertEqual(request['response_format']['type'], 'json_schema')
        self.assertTrue(endpoint.requests[0]['provider']['require_parameters'])
        view = trace_view(self.root, self.account_id, incoming.revision_key())
        assert view is not None
        self.assertEqual(view['critic_status'], 'approved')
        self.assertTrue(any(step['kind'] == 'analyst' and step['output']['handoff_to'] == 'critic'
                            for step in view['steps'] if step['output'] and 'handoff_to' in step['output']))
        self.write_broker.assert_not_called()
        directory = self.root / 'decisions' / incoming.revision_key() / TEAM_DIRECTORY
        proposal, identity, _, context = read_approval(directory)
        self.assertEqual((identity, proposal.entry_price, context['account']['id']),
                         (proposals[1]['id'], 301, self.account_id))
        quote_path = decision_dir / 'market' / f'{observation_id}.json'
        original = quote_path.read_bytes()
        altered = json.loads(original)
        altered['market']['last_price'] += 1
        quote_path.write_text(json.dumps(altered), encoding='utf-8')
        with self.assertRaisesRegex(Rejected, 'invalid_approval_record'):
            read_approval(directory)
        quote_path.write_bytes(original)
        trace_path = directory / 'trace.json'
        trace = json.loads(trace_path.read_bytes())
        trace['outcome']['proposal'] = proposals[0]
        trace_path.write_text(json.dumps(trace), encoding='utf-8')
        with self.assertRaisesRegex(Rejected, 'proposal_not_reviewed'):
            read_approval(directory)

    def test_expired_queue_item_stops_before_routing(self) -> None:
        incoming = self.news(self.market, 'SBER опубликовал прогноз.')
        late = incoming.received_at + timedelta(seconds=61)
        with patch('news_futures.runtime.utc_now', return_value=late):
            self.serve()
        result = self.report(incoming)['result']
        self.assertEqual((result['stage'], result['status'], result['reason']), ('freshness', 'skipped', 'signal_deadline'))
        self.assertGreaterEqual(result['queue_wait_ms'], 60_000)
        self.assertEqual(self.category_requests, [])
        self.write_broker.assert_not_called()

    def test_market_reaction_respects_contract_scope_and_reaches_the_critic(self) -> None:
        incoming = self.news(self.market, 'SBER опубликовал прогноз.')
        with patch.object(self.broker, 'read_candles', wraps=self.broker.read_candles) as read_candles:
            self.serve(self.delegate(), self.tool_call('get_market_reaction', {'contract_id': 'vtb-future'}),
                       self.tool_call('get_market_reaction', {'contract_id': self.contract_id}), self.market_call(),
                       self.analyst_answer(self.proposal('enter', incoming)), review())
        read_candles.assert_called_once()
        report = self.report(incoming)
        self.assertEqual(report['result']['status'], 'candidate')
        failed, completed = [call for call in report['agent']['calls'] if call['kind'] == 'get_market_reaction']
        self.assertEqual(failed['error'], 'unknown_contract')
        evidence = completed['output']
        self.assertEqual(evidence['since_receipt']['reaction_candles'], 0)
        self.assertIsNone(evidence['since_receipt']['reaction_volume_lots'])
        self.assertEqual(report['agent']['calls'][-1]['input']['context']['market_reaction'], evidence)

    def test_archive_comparison_and_calendar_evidence_reach_the_critic(self) -> None:
        incoming = self.news(self.market, 'SBER опубликовал прогноз: https://example.com/report')
        self.serve(self.delegate(), self.tool_call('get_event_versions', {}),
                   self.tool_call('compare_market', {'contract_id': self.contract_id}),
                   self.tool_call('get_upcoming_events', {}),
                   self.tool_call('get_company_news', {'company_id': 'sber'}),
                   self.tool_call('get_market_reaction', {'contract_id': self.contract_id}), self.market_call(),
                   self.analyst_answer(self.proposal('enter', incoming)), review())
        report = self.report(incoming)
        self.assertEqual(report['result']['status'], 'candidate')
        versions, comparison, calendar = report['agent']['calls'][-1]['input']['context']['extra_evidence']
        self.assertEqual(versions['earliest_archived']['cited_urls'], ['https://example.com/report'])
        self.assertEqual(comparison['instruments']['underlying'], 'uid-sber-share')
        self.assertEqual(comparison['instruments']['sector'], 'abaaa0fe-472d-4240-9679-144dd382e6ce')
        self.assertEqual(comparison['since_receipt']['change_bps'], {})
        self.assertFalse(calendar['issuer_covered'])

    def test_deadline_between_analyst_and_critic_discards_the_signal(self) -> None:
        self.sandbox()
        incoming = self.news(self.market, 'SBER опубликовал прогноз.')
        with patch('news_futures.workflow.utc_now', side_effect=utc_now) as clock:
            def slow_analyst(_: dict[str, Any]) -> dict[str, Any]:
                clock.side_effect = None
                clock.return_value = incoming.received_at + timedelta(seconds=61)
                return self.analyst_answer(self.proposal('enter', incoming))

            endpoint = self.serve(self.delegate(), self.market_call(), slow_analyst)
        result = self.report(incoming)['result']
        self.assertEqual((result['stage'], result['status'], result['reason']), ('agent', 'skipped', 'signal_deadline'))
        self.assertEqual(len(endpoint.requests), 3)
        self.assertFalse((self.root / 'decisions' / incoming.revision_key() / 'candidates').exists())
        self.write_broker.assert_not_called()

    def test_analysis_skips_without_broker_data_or_specialists(self) -> None:
        incoming = self.news(self.market, 'SBER подтвердил расписание вебинара.')
        with patch.object(self.broker, 'read_risk', side_effect=OSError):
            endpoint = self.serve({'status': 'skip', 'reason': 'Нет существенного события.', 'company_id': None})
        self.assertEqual(self.report(incoming)['result']['status'], 'skipped')
        self.assertEqual(len(endpoint.requests), 1)
        self.write_broker.assert_not_called()

    def test_analysis_stops_after_market_finds_no_supported_futures(self) -> None:
        incoming = self.news(self.market, 'SBER опубликовал прогноз.')
        with patch.object(self.broker, 'read_risk', side_effect=UnsupportedFuturesUnits) as read_risk:
            endpoint = self.serve(self.delegate(), self.market_call())
            read_risk.assert_called_once_with(self.account_id, self.contract_id)
        report = self.report(incoming)
        self.assertEqual((report['result']['status'], report['result']['reason']), ('skipped', 'no_supported_futures'))
        self.assertEqual([(call['kind'], call['status']) for call in report['agent']['calls']], [
            ('coordinator', 'completed'), ('analyst', 'completed'), ('get_market_context', 'skipped'),
        ])
        self.assertEqual(len(endpoint.requests), 2)
        self.write_broker.assert_not_called()

    def test_analysis_rejects_unknown_tool_from_analyst(self) -> None:
        incoming = self.news(self.market, 'SBER опубликовал прогноз.')
        with patch.object(self.broker, 'read_risk') as read_risk:
            self.assertEqual(len(self.serve(self.delegate(), self.tool_call('request_entry', {}), code=1).requests), 2)
        read_risk.assert_not_called()
        report = self.report(incoming)
        self.assertEqual(report['result']['status'], 'failed')
        self.assertEqual(report['agent']['calls'][-1]['error'], 'unknown_tool')
        self.assertEqual(report['agent']['calls'][-1]['requested_model'], 'test/analyst')
        self.assertNotIn('proposal', report['result'])
        self.write_broker.assert_not_called()

    def test_analysis_stops_after_second_critic_rejection(self) -> None:
        incoming = self.news(self.market, 'SBER опубликовал прогноз.')
        first = self.proposal('enter', incoming)
        endpoint = self.serve(
            self.delegate(), self.market_call(),
            self.analyst_answer(first), review(False),
            self.analyst_answer(first | {'entry_price': 301}), review(False),
        )
        result = self.report(incoming)['result']
        self.assertEqual((result['status'], result['critic_status']), ('rejected', 'rejected'))
        self.assertNotIn('proposal', result)
        self.assertEqual(len(endpoint.requests), 6)
        self.write_broker.assert_not_called()

    def test_analysis_resumes_at_critic_with_the_saved_proposal(self) -> None:
        incoming = self.news(self.market, 'SBER опубликовал прогноз.')
        with self.assertRaises(Interrupted):
            self.serve(
                self.delegate(), self.market_call(),
                self.analyst_answer(self.proposal('enter', incoming)), Interrupted(),
            )
        saved = self.report(incoming)['agent']['calls'][-1]['output']
        self.assertEqual(saved['action'], 'enter')
        endpoint = self.serve(review())
        self.assertEqual([request['model'] for request in endpoint.requests], ['test/critic'])
        self.assertEqual(len(self.instrument_requests), 1)
        self.assertEqual(self.report(incoming)['result']['proposal'], saved)
        self.write_broker.assert_not_called()

    def test_category_selection_appears_before_company_routing_in_the_runtime_trace(self) -> None:
        self.futures_snapshot['categories']['oil_gas'] = 'Нефтегаз'
        self.futures_snapshot['assets'][1]['categories'] = ['oil_gas']
        incoming = self.news(self.market, 'SBER опубликовал отчёт.')
        self.serve({'status': 'skip', 'reason': 'No signal.', 'company_id': None})
        self.assertEqual(len(self.category_requests), 1)
        self.assertEqual(list(self.jev_requests[0]['questions']), ['company_0'])
        self.assertEqual(self.jev_requests[0]['state']['company_0']['tickers'], ['SBER'])
        report = self.report(incoming)
        self.assertEqual(report['category']['selected'], ['financial'])
        self.assertEqual(report['category']['candidates'], ['sber'])
        trace = trace_view(self.root, self.account_id, incoming.revision_key())
        assert trace is not None
        self.assertEqual([step['kind'] for step in trace['steps'][:3]], ['publication', 'category', 'route'])
        self.assertEqual([step['kind'] for step in trace['steps'][3:6]], ['position', 'instrument', 'coordinator'])

    def test_decisions_selects_the_only_contract_passed_to_the_analyst(self) -> None:
        contracts = self.futures_snapshot['assets'][0]['contracts']
        contracts.insert(0, contracts[0] | {'uid': 'other-series', 'ticker': 'SBER2'})
        self.instrument_choice = 'contract_1'
        incoming = self.news(self.market, 'SBER опубликовал прогноз.')

        with patch.object(self.broker, 'read_risk', wraps=self.broker.read_risk) as read_risk:
            self.serve(self.delegate(), self.market_call(), self.analyst_answer(self.proposal('enter', incoming)), review())
        read_risk.assert_called_once_with(self.account_id, self.contract_id)
        request, = self.instrument_requests
        self.assertEqual(request['model'], self.category_requests[0]['model'])
        self.assertEqual(request['model'], self.jev_requests[0]['model'])
        self.assertEqual(list(request['questions']['instrument']['criteria']), ['contract_0', 'contract_1', 'none'])
        self.assertEqual(request['state']['contract_0']['contract_id'], 'other-series')
        report = self.report(incoming)
        self.assertEqual(report['instrument']['selected']['contract_id'], self.contract_id)
        self.assertEqual([row['contract_id'] for row in report['agent']['calls'][-1]['input']['context']['candidates']], [self.contract_id])
        self.assertEqual(report['result']['status'], 'candidate')

    def test_instrument_selection_stops_before_chat_on_none_or_invalid_choice(self) -> None:
        for choice, status, reason, code in (
            ('none', 'skipped', 'no_suitable_contract', 0), ('outside_catalog', 'failed', 'invalid_decision', 1),
        ):
            with self.subTest(choice=choice):
                self.instrument_choice = choice
                incoming = self.news(self.market, f'SBER опубликовал прогноз. {choice}')
                with patch.object(self.broker, 'read_risk') as read_risk:
                    self.assertEqual(self.serve(code=code).requests, [])
                read_risk.assert_not_called()
                report = self.report(incoming)
                result = report['result']
                self.assertEqual((result['stage'], result['status'], result['reason']), ('instrument', status, reason))
                self.assertIsNone(report['instrument']['selected'])

    def test_unrelated_category_result_skips_company_routing(self) -> None:
        self.futures_snapshot['categories'] = {'oil_gas': 'Нефтегаз'}
        for asset in self.futures_snapshot['assets']:
            asset['categories'] = ['oil_gas']
        incoming = self.news(self.market, 'Новость без привязки к компании.')
        self.assertEqual(self.serve().requests, [])
        self.assertEqual(len(self.category_requests), 1)
        self.assertEqual(self.jev_requests, [])
        report = self.report(incoming)
        self.assertEqual(report['category']['selected'], [])
        self.assertEqual(report['result']['status'], 'skipped')
        self.assertEqual(report['result']['stage'], 'category')

    def test_sandbox_analyzes_headhunter_from_catalog_without_a_profile_list(self) -> None:
        self.sandbox()
        asset = self.futures_snapshot['assets'][0]
        asset.update(name='HEAD', underlying_position_uid='head-share')
        asset['contracts'][0]['ticker'] = 'HDZ6'
        catalog = json.loads(self.catalog.read_bytes())
        catalog['operations']['catalog']['data'][0].update(
            company_id='head', company_name='HeadHunter', ticker='HEAD', share_id='head-share',
        )
        replace_json(self.catalog, catalog)
        self.snapshot['contract'].update(company_id='head', share_id='head-share', ticker='HDZ6')
        incoming = self.news(self.market, '#HEAD HeadHunter выкупил собственные акции.')
        proposal = self.proposal('enter', incoming) | {'company_id': 'head'}

        self.serve(self.delegate() | {'company_id': 'head'}, self.market_call(),
                   self.analyst_answer(proposal), review())

        self.assertEqual(self.report(incoming)['result']['status'], 'candidate')
        self.write_broker.assert_not_called()

    def test_catalog_refreshes_on_each_start_and_keeps_last_snapshot_on_failure(self) -> None:
        output = self.root / 'futures-catalog' / 'catalog.json'
        updated = self.futures_snapshot | {'fetched_at': (self.started_at + timedelta(seconds=1)).isoformat()}
        self.fetch_futures.side_effect = [self.futures_snapshot, updated, httpx.ReadTimeout('broker timeout')]
        for expected in (self.futures_snapshot, updated, updated):
            self.serve()
            self.assertEqual(json.loads(output.read_bytes()), expected)
        self.assertEqual(self.fetch_futures.call_count, 3)

    def test_catalog_refreshes_again_while_the_worker_is_running(self) -> None:
        output = self.root / 'futures-catalog' / 'catalog.json'
        updated = self.futures_snapshot | {'fetched_at': (self.started_at + timedelta(days=1)).isoformat()}
        self.fetch_futures.side_effect = chain((self.futures_snapshot,), iter(lambda: updated, None))

        async def wait_for_refresh(*args: Any, **kwargs: Any) -> int:
            self.assertEqual(json.loads(output.read_bytes()), self.futures_snapshot)
            async with asyncio.timeout(5):
                while json.loads(output.read_bytes()) != updated:
                    await asyncio.sleep(0.01)
            return 0

        write_json(self.profile.with_name('refresh.json'), self.profile_data | {'futures_refresh_seconds': 1})
        with (patch('news_futures.startup.run_telegram', side_effect=wait_for_refresh),
              patch('builtins.print')):
            self.assertEqual(asyncio.run(run_runtime(self.profile.with_name('refresh.json'), False)), 0)
        self.assertGreaterEqual(self.fetch_futures.call_count, 2)

    def test_restart_during_critic_skips_the_old_proposal_without_more_model_calls(self) -> None:
        self.sandbox()
        incoming = self.news(self.market, 'SBER raised its forecast.')

        def interrupt(body: dict[str, Any]) -> dict[str, Any]:
            raise Interrupted

        with self.assertRaises(Interrupted):
            self.serve(self.delegate(), self.market_call(), self.analyst_answer(self.proposal('enter', incoming)), interrupt)
        self.market.differences.append(Difference(self.market.pts, [incoming]))
        self.assertEqual(self.serve(started_at=utc_now()).requests, [])
        self.assertEqual(len(self.jev_requests), 1)
        self.assertEqual(self.broker.calls, [])
        self.assertEqual(self.report(incoming)['result']['reason'], 'publication_before_start')

    def test_restart_skips_saved_news_and_older_edits_and_processes_fresh_news(self) -> None:
        self.sandbox()
        incoming = self.news(self.market, 'SBER raised its forecast.')
        with patch.object(Runtime, 'process', side_effect=Interrupted):
            with self.assertRaises(Interrupted):
                self.serve()
        restarted_at = utc_now()
        edited = replace(incoming, edited_at=utc_now(), change='edit', text='SBER updated its forecast.')
        fresh = self.news(self.market, 'SBER raised its dividend.')
        self.market.differences[-1] = Difference(self.market.pts, [incoming, edited, fresh])
        self.serve(self.delegate(), self.market_call(), self.analyst_answer(self.proposal('enter', fresh)),
                   review(), started_at=restarted_at)
        self.assertEqual(len(self.jev_requests), 1)
        self.assertEqual(self.jev_requests[0]['state']['publication'], fresh.text)
        self.assertEqual(self.report(incoming)['result']['reason'], 'publication_before_start')
        self.assertEqual(self.report(edited)['result']['reason'], 'publication_before_start')
        self.assertEqual(self.report(fresh)['result']['status'], 'candidate')
        self.write_broker.assert_not_called()

    def test_panel_model_edit_applies_to_the_next_revision_and_keeps_frozen_review(self) -> None:
        controls = OperatorState(self.root)
        settings = load_settings(self.profile)
        runtime = Runtime(settings, lambda *_: self.broker, None,
                          models=AnalysisModels(chat_model, choices), storage=PostgresRuntimeStorage(settings),
                          read_limit=limit_broker_read,
                          on_controls=lambda **event: None)
        first = self.news(self.market, 'SBER raised its forecast.')
        second = self.news(self.market, 'SBER raised its dividend.')
        third = self.news(self.market, 'SBER published its results.')
        observed = []

        def analyze(settings: object, directory: Path, *args: Any, **kwargs: Any) -> dict[str, Any]:
            analyst = json.loads((directory / 'review.json').read_bytes())['analyst']
            observed.append((analyst.get('aggregator', 'openrouter'), analyst['model']))
            if len(observed) == 1:
                raise Shutdown
            return {'stage': 'agent', 'status': 'skipped'}

        archive = Archive(self.publications_db)
        with (closing(archive.db),
              patch('news_futures.runtime.analyze_news', side_effect=analyze)):
            archive.initialize_runtime()
            archive.save_batch(first.source, 1, [first, second, third])
            first_result = {'revision': first.revision_key(), 'mode': 'observation', 'inputs_saved': False}
            with self.assertRaises(Shutdown):
                runtime.process_revision(archive, first.record(), first_result)
            models = {
                'analyst': 'test/new-analyst', 'critic': 'test/critic',
            }
            controls.save_settings({'account_id': self.account_id, 'aggregator': 'groq', 'models': models})
            runtime.process_revision(archive, first.record(), first_result)
            runtime.process_revision(archive, second.record(),
                                     {'revision': second.revision_key(), 'mode': 'observation', 'inputs_saved': False})
            controls.save_settings({'account_id': self.account_id, 'aggregator': 'openrouter', 'models': models})
            runtime.process_revision(archive, third.record(),
                                     {'revision': third.revision_key(), 'mode': 'observation', 'inputs_saved': False})
        self.assertEqual(observed, [('openrouter', 'test/analyst'), ('openrouter', 'test/analyst'),
                                    ('groq', 'openai/gpt-oss-120b'), ('openrouter', 'test/new-analyst')])

    def test_panel_groq_switch_routes_chat_roles_to_groq_and_keeps_openrouter_decisions(self) -> None:
        settings = load_settings(self.profile)
        controls = OperatorState(self.root)
        preferences = Preferences(settings, ['markettwits'], controls)
        saved = preferences.saved()
        form = saved | {'base': saved, 'aggregator': 'groq'}
        with patch('news_futures.api.settings.load_token', return_value='groq-test-key') as token:
            preferences.save(form, stopped=False)
            token.assert_called_once_with('groq', 'api_key')
        stored = controls.saved()
        controls.save_settings(stored | {'models': stored['models'] | {'jev': 'typesafe/jev-1.14'}})

        first = self.news(self.market, 'SBER raised its profit forecast.')
        second = self.news(self.market, 'SBER raised its dividend.')
        endpoint = ScriptedEndpoint([
            self.delegate(), self.market_call(), self.analyst_answer(self.proposal('enter', first)), review(),
            self.delegate(), self.market_call(), self.analyst_answer(self.proposal('enter', second)), review(),
        ])

        def respond(request: httpx.Request) -> httpx.Response:
            self.assertEqual(str(request.url), 'https://api.groq.com/openai/v1/chat/completions')
            self.assertEqual(request.headers['Authorization'], 'Bearer groq-test-key')
            body = json.loads(request.content)
            self.assertNotIn('provider', body)
            if 'tools' in body:
                self.assertFalse(body['parallel_tool_calls'])
            else:
                self.assertEqual(body['response_format']['type'], 'json_schema')
                self.assertTrue(body['response_format']['json_schema']['strict'])
            return endpoint.respond(request)

        applied: list[dict[str, Any]] = []
        runtime = Runtime(settings, lambda *_: self.broker, None,
                          models=AnalysisModels(chat_model, choices), storage=PostgresRuntimeStorage(settings),
                          read_limit=limit_broker_read,
                          on_controls=lambda **event: applied.append(event),
                          started_at=self.started_at)
        archive = Archive(settings.publications)
        with closing(archive.db):
            archive.initialize_runtime()
            archive.save_batch(first.source, self.market.pts, [first, second])
        with (httpx.Client(transport=httpx.MockTransport(respond)) as client,
              patch('news_futures.llm_clients.chat.GroqChat', side_effect=lambda **kw: GroqChat(http_client=client, **kw)),
              patch('news_futures.llm_clients.chat.load_token', return_value='groq-test-key') as token,
              patch('builtins.print')):
            runtime.process()
        self.assertEqual(token.call_count, 8)
        token.assert_called_with('groq', 'api_key')
        self.assertEqual([request['model'] for request in endpoint.requests], ['openai/gpt-oss-120b'] * 8)
        self.assertEqual([request['model'] for request in self.jev_requests], ['parallel/decisions-fast:free'] * 2)
        decision = self.report(second)
        self.assertEqual(decision['result']['status'], 'candidate')
        self.assertEqual([call['provider'] for call in decision['agent']['calls'] if call.get('provider')], ['Groq'] * 4)
        self.assertEqual(applied[-1]['aggregator'], 'groq')
        self.assertEqual(applied[-1]['models']['decisions'], 'parallel/decisions-fast:free')

    def test_failed_reconciliation_keeps_the_last_confirmed_broker_state(self) -> None:
        self.sandbox()
        self.open_position()
        self.serve()
        last = self.report()['broker']['last_confirmed']
        self.assertEqual(last['position']['quantity'], 3)
        settings = load_settings(self.profile)
        runtime = Runtime(settings, lambda *_: self.broker, lambda *_: self.broker,
                          models=AnalysisModels(chat_model, choices), storage=PostgresRuntimeStorage(settings),
                          read_limit=limit_broker_read,
                          run_id='test-run')
        with (patch.object(self.broker, 'read_trade', side_effect=ConnectionError('private connection error')),
              patch('builtins.print')):
            runtime.refresh()
        status = self.report()['broker']
        self.assertFalse(status['confirmed'])
        self.assertEqual(status['run_id'], 'test-run')
        self.assertEqual(status['last_confirmed'], last)
        self.assertEqual(status['error'], 'ConnectionError')

    def test_second_runtime_cannot_start_with_the_same_data_directory(self) -> None:
        credentials = self.root / '.local' / 'credentials.toml'
        credentials.parent.mkdir()
        credentials.write_text('[openrouter]\napi_key = "test-key"\n', encoding='utf-8')
        settings = load_settings(self.profile)
        payload = json.dumps({'settings': settings.model_dump(mode='json'), 'channels': ['markettwits']})
        with exclusive_operation(self.root / 'runtime-lock') as lock:
            self.assertTrue(lock)
            result = subprocess.run(
                [sys.executable, '-m', 'news_futures.api.worker', 'test-run'],
                cwd=self.root, env=os.environ | {'PYTHONPATH': str(Path(__file__).resolve().parents[2])},
                input=payload + '\n', capture_output=True, text=True, timeout=30, check=False,
            )
            self.assertEqual(result.returncode, 1)
            self.assertEqual(json.loads(result.stdout), {'failure': 'directory_busy'})
        self.assertFalse(archive_exists(self.root / 'publications', 'revisions'))
        self.serve()
        self.assertTrue(archive_exists(self.root / 'publications', 'revisions'))

    def test_initial_history_and_old_gap_publications_cannot_open_a_position(self) -> None:
        self.sandbox()
        now = utc_now()
        old_time = now - timedelta(minutes=20)
        self.market.messages[0] = replace(self.market.messages[0], text='SBER initial history.')
        with patch('news_futures.dataflows.telegram.utc_now', return_value=old_time - timedelta(minutes=1)):
            self.serve()
        self.assertEqual(self.jev_requests, [])
        incoming = replace(self.news(self.market, 'SBER missed publication.'), published_at=old_time)
        self.market.messages.append(incoming)
        self.market.differences[-1] = Difference(self.market.pts, [], too_long=True)
        self.assertEqual(self.serve().requests, [])
        self.assertEqual(self.report(incoming)['result']['reason'], 'publication_before_start')
        self.assertEqual(self.jev_requests, [])
        self.assertEqual(self.broker.calls, [])

    def test_news_published_during_startup_is_processed_from_initial_history(self) -> None:
        fresh = self.news(self.market, 'SBER published new results during startup.')
        self.market.messages.append(fresh)
        self.market.differences.clear()

        self.serve(self.delegate(), self.market_call(), self.analyst_answer(self.proposal('enter', fresh)), review())

        self.assertEqual([request['state']['publication'] for request in self.jev_requests], [fresh.text])
        self.assertEqual(self.report(fresh)['result']['status'], 'candidate')

    def test_observation_never_writes_and_mode_switch_does_not_replay_approval(self) -> None:
        item = self.news(self.market, 'SBER raised its forecast.', media=True)
        self.serve(self.delegate(), self.market_call(), self.analyst_answer(self.proposal('enter', item)), review())
        decision = self.report(item)
        self.assertEqual(decision['result']['mode'], 'observation')
        self.assertEqual(decision['result']['status'], 'candidate')
        self.assertTrue(decision['agent']['calls'][-1]['input']['context']['publication']['unread_media'])
        self.write_broker.assert_not_called()
        self.sandbox()
        self.market.differences.append(Difference(self.market.pts, [item]))
        self.assertEqual(self.serve().requests, [])
        self.assertEqual(len(self.jev_requests), 1)
        self.assertEqual(self.broker.calls, [])
        self.assertEqual({row['source'] for row in self.report()['channels']}, {'telegram:123', 'telegram:456'})

    def test_website_adapter_uses_the_same_observation_pipeline(self) -> None:
        def website_publication(release: dict[str, str]) -> Publication:
            return Publication('web:example.invalid', release['slug'],
                               f'https://example.invalid/{release["slug"]}', release['body'],
                               datetime.fromisoformat(release['published']), utc_now())

        item = website_publication({'slug': 'release-1', 'body': 'SBER raised its forecast.',
                                    'published': utc_now().isoformat()})
        archive = Archive(load_settings(self.profile).publications)
        with closing(archive.db):
            archive.save_batch(item.source, 1, [item])

        endpoint = self.serve(self.delegate(), self.market_call(), self.analyst_answer(self.proposal('enter', item)), review())
        decision = self.report(item)
        self.assertEqual(decision['publication'], item.record())
        self.assertEqual(decision['agent']['calls'][-1]['input']['context']['publication']['url'], item.url)
        self.assertEqual(decision['result']['status'], 'candidate')
        self.assertEqual([request['model'] for request in endpoint.requests],
                         ['test/analyst', 'test/analyst', 'test/analyst', 'test/critic'])
        self.write_broker.assert_not_called()

    def test_open_position_skips_news_analysis_until_broker_confirms_exit(self) -> None:
        self.sandbox()
        self.snapshot['contract']['ticker'] = 'SRZ5'
        self.futures_snapshot['assets'][0]['contracts'][0]['ticker'] = 'SRZ5'
        self.open_position()
        self.serve()
        self.assertEqual(self.report()['broker']['protection']['status'], 'confirmed')

        update = self.news(self.rbc, 'SBER withdrew its forecast.')
        other = self.news(self.market, 'VTB cash guidance.')
        self.assertEqual(self.serve().requests, [])
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp'])
        for publication, reason in ((update, 'position_already_open'), (other, 'account_position_limit')):
            report = self.report(publication)
            self.assertEqual(report['result']['reason'], reason)
            self.assertEqual(report['position_check']['held_contract_id'], self.contract_id)
            trace = trace_view(self.root, self.account_id, publication.revision_key())
            assert trace is not None
            self.assertEqual(trace['status'], 'skipped')
            self.assertTrue(any(step['kind'] == 'position' for step in trace['steps']))
            self.assertFalse(any(step['kind'] in {'event', 'analyst', 'critic'} for step in trace['steps']))

        stop, take_profit = self.broker.state.protection
        self.broker.state = replace(self.broker.state, position_lots=0,
                                    protection=(replace(stop, status='executed'), take_profit),
                                    exits=(Fill('native-exit', 3, Decimal('290')),))
        fresh = self.news(self.market, 'SBER released a new forecast.')
        endpoint = self.serve(self.delegate(), self.market_call(), self.analyst_answer(self.proposal('skip', fresh)))
        self.assertEqual(self.report(fresh)['result']['status'], 'skipped')
        self.assertEqual([request['model'] for request in endpoint.requests], ['test/analyst'] * 3)
        self.assertIsNone(self.report()['broker']['position'])

        view = news_view(self.root, self.profile_data, {}, {}, 'sber')
        self.assertIn('enter', [order['action'] for order in view['orders']])
        self.assertTrue(all(order['contract_id'] == self.contract_id for order in view['orders']))

    def test_reprint_of_initial_history_does_not_trigger_review(self) -> None:
        self.sandbox()
        text = 'SBER previous earnings forecast.'
        self.market.messages[0] = replace(self.market.messages[0], text=text)
        repeated = self.news(self.rbc, text)
        self.assertEqual(self.serve().requests, [])
        self.assertEqual(self.report(repeated)['result']['reason'], 'historical_reprint')
        self.assertEqual(self.broker.calls, [])
