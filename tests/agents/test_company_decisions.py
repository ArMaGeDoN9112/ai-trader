from datetime import timedelta
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
import unittest
from unittest.mock import patch

try:
    import httpx  # noqa: F401
    import langgraph  # noqa: F401
except ImportError:
    raise unittest.SkipTest('Install requirements-openrouter.txt for ticket 07 tests')

from news_futures.cli import main
from news_futures.agents.contracts import RoleSettings
from news_futures.agents.events import EventArchive
from news_futures.common.artifacts import write_json
from news_futures.news.publications import utc_now
from tests.support.news import company_link
from tests.support.models import ScriptedEndpoint, review

class CompanyDecisionTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.database = self.root / 'events.sqlite3'
        now = utc_now().isoformat().replace('+00:00', 'Z')
        self.snapshot: dict[str, Any] = {
            'account': {'id': 'sandbox-account', 'observed_at': now, 'currency': 'RUB',
                        'equity': 100000.0, 'available_margin': 90000.0},
            'position': None,
            'instruments': [
                {'company_id': company, 'company_name': company.upper(), 'share_id': f'{company}-share',
                 'contract_id': f'{company}-future', 'allowed_for_entry': allowed,
                 'market': {'observed_at': now, 'last_price': 300.0}}
                for company, allowed in [('sber', True), ('vtb', True), ('other', False)]
            ],
        }
        self.config = self.root / 'models.json'
        write_json(self.config, {'analyst': {'model': 'test/analyst'}, 'critic': {'model': 'test/critic'}})

    def archive(self, text: str, *companies: str, prior: dict[str, Any] | None = None,
                change: str = 'new_facts', inferred: bool = False) -> dict[str, Any]:
        with EventArchive(self.database) as archive:
            number = str(len(archive.history()) + 1)
            now = utc_now().isoformat()
            publication = {'id': f'rbc:{number}', 'external_id': number, 'source': 'rbc',
                           'url': f'https://t.me/rbc/{number}', 'text': text,
                           'published_at': now, 'received_at': now}
            links = [company_link(publication, company) for company in companies]
            if inferred:
                links[0].update(kind='inferred', basis='industry', requires_analyst_review=True)
            endpoint = ScriptedEndpoint([{
                'match': 'same' if prior else 'different', 'event_id': prior['event_id'] if prior else None,
                'change': change if prior else None, 'reason': 'Compared the claims.', 'evidence': None,
            }])
            return archive.match(publication, links, RoleSettings(model='test/matcher'),
                                 model_factory=endpoint.model)

    def analyze(self, event: dict[str, Any], answers: list[Any]) -> tuple[int, dict[str, Any]]:
        endpoint = ScriptedEndpoint(answers)
        directory = self.root / f'run-{len(list(self.root.iterdir()))}'
        snapshot = directory.with_suffix('.json')
        write_json(snapshot, self.snapshot)
        with patch('sys.argv', ['news_futures', 'analyze-event', str(event['decision_id']),
                               '--database', str(self.database), '--snapshot', str(snapshot),
                               '--config', str(self.config), '--directory', str(directory)]), patch(
                                   'news_futures.agents.decisions.chat_model', endpoint.model), patch('builtins.print'):
            code = main()
        trace_file = directory / 'trace.json'
        return code, json.loads(trace_file.read_bytes()) if trace_file.is_file() else {}

    def save_entry_reason(self, reason: str) -> int:
        path = self.root / f'reason-{len(list(self.root.iterdir()))}.json'
        write_json(path, {'account_id': 'sandbox-account', 'position_id': 'position-1',
                          'contract_id': 'sber-future', 'reason': reason})
        with patch('sys.argv', ['news_futures', 'entry-reason', str(path),
                               '--database', str(self.database)]), patch('builtins.print'):
            return main()

    def open_position(self) -> None:
        self.snapshot['position'] = {
            'id': 'position-1', 'contract_id': 'sber-future', 'side': 'long', 'quantity': 2,
        }
        self.snapshot['instruments'][0]['allowed_for_entry'] = False

    def proposal(self, event: dict[str, Any], action: str, company: str | None,
                 assessments: list[dict[str, Any]]) -> dict[str, Any]:
        return {'action': action, 'publication_id': event['publication']['id'],
                'company_id': company, 'contract_id': f'{company}-future' if company else None,
                'side': 'short' if action == 'enter' else None,
                'entry_price': 300.0 if action == 'enter' else None,
                'stop_loss': 305.0 if action == 'enter' else None,
                'take_profit': 290.0 if action == 'enter' else None,
                'reason': 'Sber has the stronger news impact.' if action == 'enter'
                else 'The supplied news and original thesis support this decision.',
                'level_rationale': {'entry': 'Current quote.', 'stop_loss': 'Break above resistance.',
                                    'take_profit': 'Next support.'} if action == 'enter' else None,
                'assessments': assessments}

    def assessment(self, company: str, event: dict[str, Any] | None) -> dict[str, Any]:
        return {'contract_id': f'{company}-future', 'impact': 'verified' if event else 'unverified',
                'reason': 'Checked the original claim and the business exposure.',
                'evidence': [{'revision': event['revision'], 'quote': event['publication']['text']}]
                if event else []}

    def test_compares_only_allowed_candidates_using_archive_market_and_account(self) -> None:
        prior = self.archive('Сбер и ВТБ раскрыли ожидания по капиталу.', 'sber', 'vtb')
        event = self.archive('Сбер снизил прогноз, ВТБ подтвердил прежний, Другой повысил.',
                             'sber', 'vtb', 'other', prior=prior)
        proposal = self.proposal(event, 'enter', 'sber',
                                 [self.assessment('sber', event), self.assessment('vtb', None)])
        code, trace = self.analyze(event, [proposal, review()])
        self.assertEqual(code, 0)
        self.assertEqual(trace['approved_proposal']['contract_id'], 'sber-future')
        self.assertEqual(trace['mode'], 'observation')
        for call in trace['calls']:
            context = call['input']['context']
            self.assertEqual(context['event']['revision'], event['revision'])
            self.assertEqual(context['publication']['url'], event['publication']['url'])
            self.assertEqual(context['related_news'][0]['publication']['url'], prior['publication']['url'])
            self.assertEqual(context['account'], self.snapshot['account'])
            self.assertEqual([row['contract_id'] for row in context['candidates']],
                             ['sber-future', 'vtb-future'])
            self.assertEqual(context['candidates'][1]['market']['last_price'], 300.0)

        for invalid in (
            {**proposal, 'contract_id': 'other-future', 'company_id': 'other'},
            {**proposal, 'assessments': proposal['assessments'][:1]},
            {**proposal, 'level_rationale': None},
            {**proposal, 'quantity': 5},
        ):
            code, rejected = self.analyze(event, [invalid])
            self.assertEqual(code, 1)
            self.assertIsNone(rejected['approved_proposal'])
            self.assertEqual(len(rejected['calls']), 1)

    def test_inferred_impact_needs_real_evidence_and_critic_agreement(self) -> None:
        event = self.archive('Часть банков меняет условия кредитования.', 'sber', inferred=True)
        uncertain = self.assessment('sber', None)
        skip = self.proposal(event, 'skip', None, [uncertain])
        enter = self.proposal(event, 'enter', 'sber', [uncertain])
        code, rejected = self.analyze(event, [enter])
        self.assertEqual(code, 1)
        self.assertIsNone(rejected['approved_proposal'])
        forged = self.assessment('sber', event)
        forged['evidence'][0]['quote'] = 'Сбер назван в первоисточнике'
        code, rejected = self.analyze(event, [{**enter, 'assessments': [forged]}])
        self.assertEqual(code, 1)
        self.assertIsNone(rejected['approved_proposal'])
        # A genuine quote can still fail to establish company exposure.
        enter['assessments'] = [self.assessment('sber', event)]
        code, trace = self.analyze(event, [enter, review(False), skip, review()])
        self.assertEqual(code, 0)
        self.assertEqual(trace['status'], 'no_action')
        self.assertIsNone(trace['approved_proposal'])
        self.assertEqual(len(trace['calls']), 4)
        context = trace['calls'][1]['input']['context']
        self.assertTrue(context['event']['links'][0]['requires_analyst_review'])
        self.assertEqual(trace['calls'][2]['input']['review']['approved'], False)

    def test_refutation_can_close_but_cannot_rewrite_the_original_entry_reason(self) -> None:
        thesis = 'Рост прибыли Сбера опирался на опубликованный прогноз.'
        prior = self.archive('Сбер опубликовал прогноз роста прибыли.', 'sber')
        event = self.archive('Сбер отозвал прогноз роста прибыли.', prior=prior, change='retraction')
        self.open_position()
        close = self.proposal(event, 'close', 'sber', [self.assessment('sber', event)])
        close['reason'] = 'Сбер отозвал прогноз, на котором основывался вход.'
        self.assertEqual(self.analyze(event, []), (1, {}))
        self.assertEqual(self.save_entry_reason(thesis), 0)
        self.assertEqual(self.save_entry_reason(thesis), 0)
        self.assertEqual(self.save_entry_reason('Новая причина удерживать позицию.'), 1)
        code, trace = self.analyze(event, [close, review()])
        self.assertEqual(code, 0)
        self.assertEqual(trace['approved_proposal']['action'], 'close')
        self.assertIsNone(trace['approved_proposal']['side'])
        self.assertEqual(trace['execution']['status'], 'not_requested')
        for call in trace['calls']:
            context = call['input']['context']
            self.assertEqual(context['entry_reason']['reason'], thesis)
            self.assertEqual(context['position']['quantity'], 2)
            self.assertFalse(context['candidates'][0]['allowed_for_entry'])
        for invalid in ({**close, 'quantity': 1}, {**close, 'side': 'short'}):
            self.assertEqual(self.analyze(event, [invalid])[0], 1)

    def test_critic_can_confirm_impact_on_held_company_missed_by_routing(self) -> None:
        self.open_position()
        self.assertEqual(self.save_entry_reason('Сбер повысил прогноз прибыли.'), 0)
        event = self.archive('ВТБ сохранил прогноз, Сбер отозвал свой прогноз прибыли.', 'vtb')
        close = self.proposal(event, 'close', 'sber',
                              [self.assessment('vtb', None), self.assessment('sber', event)])
        close['reason'] = 'Сбер отозвал прогноз, на котором основывался вход.'
        code, trace = self.analyze(event, [close, review()])
        self.assertEqual(code, 0)
        self.assertEqual(trace['approved_proposal']['action'], 'close')

    def test_other_instrument_can_only_lead_to_hold_and_keeps_original_reason(self) -> None:
        self.open_position()
        thesis = 'Исходная причина входа в Сбер.'
        self.assertEqual(self.save_entry_reason(thesis), 0)
        event = self.archive('ВТБ повысил прогноз прибыли.', 'vtb')
        assessments = [self.assessment('sber', None), self.assessment('vtb', event)]
        hold = self.proposal(event, 'hold', 'sber', assessments)
        code, trace = self.analyze(event, [hold, review()])
        self.assertEqual(code, 0)
        self.assertEqual(trace['status'], 'no_action')
        self.assertIsNone(trace['approved_proposal'])
        self.assertEqual(trace['calls'][0]['input']['context']['entry_reason']['reason'], thesis)
        for forbidden in (
            self.proposal(event, 'enter', 'vtb', assessments),
            self.proposal(event, 'close', 'vtb', assessments),
            self.proposal(event, 'close', 'sber', assessments),
        ):
            code, rejected = self.analyze(event, [forbidden])
            self.assertEqual(code, 1)
            self.assertIsNone(rejected['approved_proposal'])
        unsupported = self.proposal(event, 'close', 'sber', [self.assessment('sber', event), assessments[1]])
        code, trace = self.analyze(event, [unsupported, review(False), hold, review()])
        self.assertEqual(code, 0)
        self.assertEqual(trace['status'], 'no_action')
        self.assertIsNone(trace['approved_proposal'])

    def test_context_limit_leaves_the_original_in_the_archive_and_keeps_entry_reason(self) -> None:
        self.open_position()
        thesis = 'Эта исходная причина входа должна остаться видимой целиком.'
        self.assertEqual(self.save_entry_reason(thesis), 0)
        old_text = 'История раскрытия информации. ' * 2000
        old = self.archive(old_text, 'sber')
        event = self.archive('Компания подтверждает ожидания по прибыли.', 'sber', prior=old)
        hold = self.proposal(event, 'hold', 'sber', [self.assessment('sber', event)])
        code, trace = self.analyze(event, [hold, review()])
        self.assertEqual(code, 0)
        for call in trace['calls']:
            context = call['input']['context']
            self.assertEqual(context['related_news'], [])
            self.assertEqual(context['omitted_related_news_count'], 1)
            self.assertEqual(context['entry_reason']['reason'], thesis)
            self.assertEqual(context['publication']['text'], event['publication']['text'])
        with EventArchive(self.database) as archive:
            self.assertEqual(archive.report('sber')['context'][0]['publication']['text'], old_text)

    def test_repeat_and_superseded_event_do_not_start_another_review(self) -> None:
        first = self.archive('Сбер опубликовал оценку прибыли.', 'sber')
        repeat = self.archive('По сообщению Сбера опубликована оценка прибыли.',
                              'sber', prior=first, change='none')
        self.assertEqual(self.analyze(repeat, []), (1, {}))
        with EventArchive(self.database) as archive:
            archive.correct(first['revision'], 'separate', None, 'Уточнение редакции')
        self.assertEqual(self.analyze(first, []), (1, {}))

    def test_stale_account_or_market_snapshot_is_rejected_before_model_calls(self) -> None:
        event = self.archive('Сбер опубликовал оценку прибыли.', 'sber')
        stale = (utc_now() - timedelta(minutes=6)).isoformat()
        for section in (self.snapshot['account'], self.snapshot['instruments'][0]['market']):
            original = section['observed_at']
            section['observed_at'] = stale
            self.assertEqual(self.analyze(event, []), (1, {}))
            section['observed_at'] = original

    def test_correction_during_review_withholds_approved_proposal(self) -> None:
        first = self.archive('Сбер опубликовал оценку прибыли.', 'sber')
        event = self.archive('Сбер уточнил оценку прибыли.', 'sber', prior=first)
        enter = self.proposal(event, 'enter', 'sber', [self.assessment('sber', event)])

        def correct_and_approve(body: dict[str, Any]) -> dict[str, Any]:
            with EventArchive(self.database) as archive:
                archive.correct(event['revision'], 'repeat', first['event_id'], 'Перепечатка')
            approval: dict[str, Any] = review()(body)
            return approval

        code, trace = self.analyze(event, [enter, correct_and_approve])
        self.assertEqual(code, 1)
        self.assertEqual(trace['error'], 'superseded_event')
        self.assertTrue(trace['calls'][-1]['output']['approved'])
        self.assertIsNone(trace['approved_proposal'])

    def test_corrupt_database_reports_failure_when_saving_entry_reason(self) -> None:
        self.database.write_text('Invalid SQLite file', encoding='utf-8')
        self.assertEqual(self.save_entry_reason('Исходная причина входа.'), 1)
