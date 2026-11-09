from contextlib import closing
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import Any, cast
import unittest
from unittest.mock import AsyncMock, patch

import httpx

import test_risk
from test_jev import answer
from test_openrouter import ScriptedEndpoint, review
from test_sandbox_execution import ControlBroker
from test_telegram import ControlledTransport

from news_futures.__main__ import main
from news_futures.jev import assess_batch
from news_futures.local import write_json
from news_futures.publications import Archive, Publication, utc_now
from news_futures.runtime import Runtime, load_settings
from news_futures.sandbox_execution import Fill, SandboxExecutor
from news_futures.telegram import Difference, Source
from news_futures.tbank_execution import SANDBOX_REST, TBankReader, restrict_to_reads


class Interrupted(BaseException):
    pass


class RuntimeTest(unittest.TestCase):
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


class RuntimeAcceptance(test_risk.RiskFixture):
    def setUp(self) -> None:
        super().setUp()
        self.broker = LiveControlBroker(self)
        self.market = ControlledTransport()
        self.market.source = Source(123, 'markettwits')
        self.market.messages = [replace(self.market.original, source=self.market.source.id)]
        self.rbc = ControlledTransport()
        self.rbc.source = Source(456, 'rbc_news')
        self.rbc.messages = [replace(self.rbc.original, source=self.rbc.source.id)]
        client = AsyncMock(return_value=SimpleNamespace(pts=1, qts=0, seq=1, date=utc_now()))
        client.get_me.return_value = SimpleNamespace(id=1)
        self.enterContext(patch('news_futures.telegram.open_client', return_value=client))
        self.enterContext(patch('news_futures.telegram.TelethonTransport', return_value=self))
        self.enterContext(patch('news_futures.runtime.load_token', return_value='test-key'))
        self.enterContext(patch('news_futures.jev.load_token', return_value='test-key'))
        self.enterContext(patch('news_futures.analysis.load_token', return_value='test-key'))
        self.enterContext(patch('news_futures.runtime.sandbox_client', side_effect=lambda *args, **kw: httpx.Client()))
        self.enterContext(patch('news_futures.runtime.TBankReader', return_value=self.broker))
        self.write_broker = self.enterContext(patch('news_futures.runtime.TBankSandbox', return_value=self.broker))
        self.jev_requests: list[dict[str, Any]] = []
        jev_client = self.enterContext(httpx.Client(transport=httpx.MockTransport(self.jev_reply)))
        self.enterContext(patch('news_futures.jev.assess_batch',
                                side_effect=lambda _client, *args: assess_batch(jev_client, *args)))
        self.matcher = self.root / 'matcher.json'
        write_json(self.matcher, {'model': 'test/matcher'})
        self.catalog = self.root / 'catalog.json'
        write_json(self.catalog, {'operations': {'catalog': {'availability': 'available', 'data': [
            {'company_id': company, 'company_name': company.upper(), 'ticker': company.upper(),
             'share_id': f'{company}-share'}
            for company in ('sber', 'vtb')
        ]}}})
        self.profile = self.root / 'runtime.json'
        self.profile_data = {
            'data_dir': '.', 'catalog': self.catalog.name, 'account_id': self.account_id,
            'contracts': {self.contract_id: 'sber', 'vtb-future': 'vtb'},
            'session': 'telegram.session', 'review': self.config.name, 'matcher': self.matcher.name,
        }
        write_json(self.profile, self.profile_data)

    def jev_reply(self, request: httpx.Request) -> httpx.Response:
        data = json.loads(request.content)
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

    def serve(self, *answers: Any, code: int = 0) -> ScriptedEndpoint:
        endpoint = ScriptedEndpoint(list(answers))

        def model(**kwargs: Any) -> Any:
            from news_futures.analysis import RoleSettings
            return endpoint.model(RoleSettings(model=kwargs['model']))

        with (patch('news_futures.analysis.OpenRouterChat', side_effect=model),
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

    def test_restart_during_critic_resumes_the_saved_proposal(self) -> None:
        self.sandbox()
        incoming = self.news(self.market, 'SBER raised its forecast.')

        def interrupt(body: dict[str, Any]) -> dict[str, Any]:
            raise Interrupted

        with self.assertRaises(Interrupted):
            self.serve(self.proposal('enter', incoming), interrupt)
        self.market.differences.append(Difference(self.market.pts, [incoming]))
        self.assertEqual(len(self.serve(review()).requests), 1)
        self.assertEqual(len(self.jev_requests), 1)
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp'])
        self.assertEqual(self.report(incoming)['result']['status'], 'protected')

    def test_restart_consumes_a_saved_publication_once_after_transport_replay(self) -> None:
        self.sandbox()
        incoming = self.news(self.market, 'SBER raised its forecast.')
        with patch.object(Runtime, 'process', side_effect=Interrupted):
            with self.assertRaises(Interrupted):
                self.serve()
        self.market.differences.append(Difference(self.market.pts, [incoming]))
        self.serve(self.proposal('enter', incoming), review())
        self.serve()
        self.assertEqual(len(self.jev_requests), 1)
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp'])

    def test_restart_reconciles_entry_with_lost_response_before_processing(self) -> None:
        self.sandbox()
        incoming = self.news(self.market, 'SBER raised its forecast.')
        enter = self.broker.enter

        def interrupt(*args: Any) -> None:
            enter(*args)
            raise Interrupted

        with patch.object(self.broker, 'enter', side_effect=interrupt), self.assertRaises(Interrupted):
            self.serve(self.proposal('enter', incoming), review())
        self.broker.state = replace(self.broker.state, settled=False)
        self.serve()
        self.assertEqual(self.broker.calls, ['enter'])
        self.assertEqual(self.report()['broker']['broker'], 'blocked')
        self.assertEqual(self.report(incoming)['result']['status'], 'processing')
        self.broker.state = replace(self.broker.state, settled=True)
        self.serve()
        self.assertEqual(self.report(incoming)['result']['status'], 'protected')
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp'])
        self.assertEqual(len(self.jev_requests), 1)

    def test_restart_between_stop_submissions_confirms_both_without_resending_the_first(self) -> None:
        self.sandbox()
        incoming = self.news(self.market, 'SBER raised its forecast.')
        protect = self.broker.protect

        def interrupt(*args: Any) -> None:
            protect(*args)
            raise Interrupted

        with patch.object(self.broker, 'protect', side_effect=interrupt), self.assertRaises(Interrupted):
            self.serve(self.proposal('enter', incoming), review())
        self.serve()
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp'])
        self.assertEqual(self.report()['broker']['protection']['status'], 'confirmed')

    def test_blocked_response_is_updated_when_restart_confirms_the_exit(self) -> None:
        self.sandbox()
        entry = self.news(self.market, 'SBER raised its forecast.')
        self.serve(self.proposal('enter', entry), review())
        original = self.report(entry)['event']['event_id']
        incoming = self.news(self.rbc, 'SBER withdrew its forecast.')
        self.broker.close_unavailable = True
        self.serve({'match': 'same', 'event_id': original, 'change': 'retraction', 'reason': 'Retracted.',
                    'evidence': None}, self.proposal('close', incoming), review())
        self.assertEqual(self.report(incoming)['result']['status'], 'blocked')
        self.broker.state = replace(self.broker.state, settled=True, position_lots=0,
                                    exits=(Fill('late-exit', 3, Decimal('303')),))
        self.serve()
        self.assertEqual(self.report(incoming)['result']['status'], 'closed')
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp', 'cancel_protection', 'close'])

    def test_restart_rejects_expired_close_before_canceling_any_protection(self) -> None:
        self.sandbox()
        entry = self.news(self.market, 'SBER raised its forecast.')
        self.serve(self.proposal('enter', entry), review())
        original = self.report(entry)['event']['event_id']
        incoming = self.news(self.rbc, 'SBER withdrew its forecast.')
        advance = SandboxExecutor._advance

        def interrupt(executor: SandboxExecutor, *args: Any, close: bool) -> dict[str, str]:
            if close:
                raise Interrupted
            return advance(executor, *args, close=close)

        with patch.object(SandboxExecutor, '_advance', new=interrupt), self.assertRaises(Interrupted):
            self.serve({'match': 'same', 'event_id': original, 'change': 'retraction', 'reason': 'Retracted.',
                        'evidence': None}, self.proposal('close', incoming), review())
        with patch('news_futures.risk.utc_now', return_value=utc_now() + timedelta(minutes=2)):
            self.serve()
        self.assertEqual(self.report(incoming)['result']['reason'], 'stale_or_future_proposal')
        self.assertEqual(self.report()['broker']['protection']['status'], 'confirmed')
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp'])

    def test_restart_reanalyzes_with_actual_position_and_keeps_the_previous_attempt(self) -> None:
        self.sandbox()
        entry = self.news(self.market, 'SBER raised its forecast.')
        self.serve(self.proposal('enter', entry), review())
        original = self.report(entry)['event']['event_id']
        incoming = self.news(self.rbc, 'SBER withdrew its forecast.')

        def interrupt(body: dict[str, Any]) -> dict[str, Any]:
            raise Interrupted

        with self.assertRaises(Interrupted):
            self.serve({'match': 'same', 'event_id': original, 'change': 'retraction', 'reason': 'Retracted.',
                        'evidence': None}, self.proposal('close', incoming), interrupt)
        stop, take_profit = self.broker.state.protection
        self.broker.state = replace(self.broker.state, position_lots=0,
                                    protection=(replace(stop, status='executed'), take_profit),
                                    exits=(Fill('native-exit', 3, Decimal('290')),))
        self.serve(self.proposal('skip', incoming), review())
        decision = self.report(incoming)
        self.assertEqual(decision['result']['status'], 'no_action')
        self.assertIsNone(decision['context']['position'])
        self.assertEqual(len(decision['previous_reviews']), 1)
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp', 'cancel_protection'])

    def test_restart_after_exit_with_lost_response_recovers_exit_and_entry_sources(self) -> None:
        self.sandbox()
        entry = self.news(self.market, 'SBER raised its forecast.')
        self.serve(self.proposal('enter', entry), review())
        original = self.report(entry)['event']['event_id']
        incoming = self.news(self.rbc, 'SBER withdrew its forecast.')
        close = self.broker.close

        def interrupt(*args: Any) -> None:
            close(*args)
            raise Interrupted

        with patch.object(self.broker, 'close', side_effect=interrupt), self.assertRaises(Interrupted):
            self.serve({'match': 'same', 'event_id': original, 'change': 'retraction', 'reason': 'Retracted.',
                        'evidence': None}, self.proposal('close', incoming), review())
        self.serve()
        decision = self.report(incoming)
        self.assertEqual(decision['result']['status'], 'closed')
        self.assertEqual(decision['context']['entry_reason']['sources'], [
            {'revision': entry.revision_key(), 'url': entry.url, 'quote': entry.text},
        ])
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp', 'cancel_protection', 'close'])

    def test_old_gap_publication_reassesses_position_without_lifting_entry_age_limit(self) -> None:
        self.sandbox()
        old_time = utc_now() - timedelta(minutes=20)
        entry = self.news(self.market, 'SBER raised its forecast.')
        with patch('news_futures.telegram.utc_now', return_value=old_time - timedelta(minutes=1)):
            self.serve(self.proposal('enter', entry), review())
        original = self.report(entry)['event']['event_id']
        incoming = replace(self.news(self.rbc, 'SBER gave updated guidance.'), published_at=old_time)
        self.rbc.messages = [incoming]
        self.rbc.differences[-1] = Difference(self.rbc.pts, [], too_long=True)
        self.serve({'match': 'same', 'event_id': original, 'change': 'new_facts', 'reason': 'Update.',
                    'evidence': None}, self.proposal('hold', incoming), review())
        self.assertEqual(self.report(incoming)['publication']['published_at'], old_time.isoformat())
        self.assertEqual(self.report(incoming)['result']['status'], 'no_action')
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp'])

    def test_initial_history_and_old_gap_publications_cannot_open_a_position(self) -> None:
        self.sandbox()
        now = utc_now()
        old_time = now - timedelta(minutes=20)
        self.market.messages[0] = replace(self.market.messages[0], text='SBER initial history.')
        with patch('news_futures.telegram.utc_now', return_value=old_time - timedelta(minutes=1)):
            self.serve()
        self.assertEqual(self.jev_requests, [])
        incoming = replace(self.news(self.market, 'SBER missed publication.'), published_at=old_time)
        self.market.messages.append(incoming)
        self.market.differences[-1] = Difference(self.market.pts, [], too_long=True)
        self.assertEqual(self.serve().requests, [])
        self.assertEqual(self.report(incoming)['result']['reason'], 'publication_context_only')
        self.assertEqual(self.broker.calls, [])

    def test_shutdown_keeps_protection_and_leaves_reviewed_close_for_restart(self) -> None:
        self.sandbox()
        entry = self.news(self.market, 'SBER raised its forecast.')
        self.serve(self.proposal('enter', entry), review())
        original = self.report(entry)['event']['event_id']
        incoming = self.news(self.rbc, 'SBER withdrew its forecast.')
        runtime = Runtime(load_settings(self.profile), lambda _: self.broker, lambda _: self.broker)

        def stop(body: dict[str, Any]) -> dict[str, Any]:
            runtime.stop.set()
            return dict(review()(body))

        with patch('news_futures.runtime.Runtime', return_value=runtime):
            self.serve({'match': 'same', 'event_id': original, 'change': 'retraction', 'reason': 'Retracted.',
                        'evidence': None}, self.proposal('close', incoming), stop)
        self.assertEqual(self.report(incoming)['result']['reason'], 'shutdown_before_execution')
        self.assertEqual(self.report()['broker']['protection']['status'], 'confirmed')
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp'])
        self.serve()
        self.assertEqual(self.report(incoming)['result']['status'], 'closed')

    def test_observation_never_writes_and_mode_switch_does_not_replay_approval(self) -> None:
        item = self.news(self.market, 'SBER raised its forecast.', media=True)
        self.serve(self.proposal('enter', item), review())
        decision = self.report(item)
        self.assertEqual(decision['result']['mode'], 'observation')
        self.assertEqual(decision['result']['status'], 'approved_for_checks')
        self.assertTrue(decision['context']['publication']['unread_media'])
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

        endpoint = self.serve(self.proposal('enter', item), review())
        decision = self.report(item)
        self.assertEqual(decision['publication'], item.record())
        self.assertEqual(decision['context']['publication']['url'], item.url)
        self.assertEqual(decision['result']['status'], 'approved_for_checks')
        self.assertEqual([request['model'] for request in endpoint.requests], ['test/analyst', 'test/critic'])
        self.write_broker.assert_not_called()

    def test_news_drives_protected_entry_hold_and_full_close(self) -> None:
        self.sandbox()
        entry = self.news(self.market, 'SBER increased profit forecast.')
        self.serve(self.proposal('enter', entry), review())
        report = self.report(entry)
        self.assertEqual(report['result']['status'], 'protected')
        self.assertEqual(self.report()['broker']['protection']['status'], 'confirmed')
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp'])
        original = report['event']['event_id']
        self.serve()
        self.assertEqual(len(self.jev_requests), 1)
        repeat = self.news(self.rbc, entry.text)
        self.serve({'match': 'same', 'event_id': original, 'change': 'none', 'reason': 'Repeat.', 'evidence': None})
        self.assertEqual(self.report(repeat)['result']['reason'], 'repeat')
        other = self.news(self.rbc, 'VTB cash guidance.')
        self.serve()
        self.assertEqual(self.report(other)['result']['reason'], 'unrelated_to_position')
        update = {'match': 'same', 'event_id': original, 'change': 'new_facts', 'reason': 'Update.', 'evidence': None}
        hold = self.news(self.rbc, 'SBER increased profit forecast with new capital data.')
        self.serve(update, self.proposal('hold', hold), review())
        self.assertEqual(self.report(hold)['result']['status'], 'no_action')
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp'])
        exit_news = self.news(self.market, 'SBER withdrew profit forecast.')
        self.serve(update | {'change': 'retraction'}, self.proposal('close', exit_news), review())
        exit_report = self.report(exit_news)
        self.assertEqual(exit_report['context']['entry_reason']['reason'], 'Исходное основание входа.')
        self.assertEqual(exit_report['result']['status'], 'closed')
        self.assertIn('reviewed_close', [row['kind'] for row in exit_report['trades'][0]['events']])
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp', 'cancel_protection', 'close'])
        self.assertIsNone(self.report()['broker']['position'])

    def test_skips_and_rereads_account_after_approval(self) -> None:
        self.sandbox()
        skip = self.news(self.market, 'SBER forecasts.')
        self.serve(self.proposal('skip', skip), review())
        self.assertEqual(self.report(skip)['result']['status'], 'no_action')
        incoming = self.news(self.rbc, 'SBER revised its forecast.')
        different = {'match': 'different', 'event_id': None, 'change': None, 'reason': 'Different.', 'evidence': None}

        def change_account(body: dict[str, Any]) -> dict[str, Any]:
            self.snapshot['has_position'] = True
            return dict(review()(body))

        self.serve(different, self.proposal('enter', incoming), change_account)
        self.assertEqual(self.report(incoming)['result']['reason'], 'account_has_exposure_or_unresolved_execution')
        self.assertEqual(self.broker.calls, [])

    def test_tbank_independent_stops_block_autonomous_entry(self) -> None:
        self.sandbox()
        self.broker.source = 'sandbox'
        incoming = self.news(self.market, 'SBER updated forecasts.')
        self.serve(self.proposal('enter', incoming), review())
        self.assertEqual(self.report(incoming)['result']['reason'], 'autonomous_protection_unavailable')
        self.assertEqual(self.broker.calls, [])

    def test_reprint_of_initial_history_does_not_trigger_review(self) -> None:
        self.sandbox()
        text = 'SBER previous earnings forecast.'
        self.market.messages[0] = replace(self.market.messages[0], text=text)
        repeated = self.news(self.rbc, text)
        self.assertEqual(self.serve().requests, [])
        self.assertEqual(self.report(repeated)['result']['reason'], 'historical_reprint')
        self.assertEqual(self.broker.calls, [])

    def test_reviewed_close_keeps_stops_when_fills_and_position_disagree(self) -> None:
        self.sandbox()
        entry = self.news(self.market, 'SBER raised forecast.')
        self.serve(self.proposal('enter', entry), review())
        event = self.report(entry)['event']['event_id']
        incoming = self.news(self.rbc, 'SBER withdrew forecast.')

        def change_position(body: dict[str, Any]) -> dict[str, Any]:
            self.broker.state = replace(self.broker.state, position_lots=4)
            return dict(review()(body))

        self.serve({'match': 'same', 'event_id': event, 'change': 'retraction',
                    'reason': 'Retraction.', 'evidence': None},
                   self.proposal('close', incoming), change_position)
        self.assertEqual(self.report(incoming)['result']['status'], 'blocked')
        self.assertEqual(self.broker.calls, ['enter', 'sl', 'tp'])
