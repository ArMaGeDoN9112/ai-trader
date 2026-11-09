import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from typing import Any
import unittest

try:
    import httpx
    from langchain_core.language_models import BaseChatModel
except ImportError:
    raise unittest.SkipTest('Install requirements-openrouter.txt for ticket 06 tests')

from news_futures.agents.contracts import RoleSettings
from news_futures.agents.events import EventArchive
from news_futures.agents.routing import route_publication
from tests.support.routing import answer
from tests.support.models import ScriptedEndpoint
from tests.support.news import company_link


class EventsTest(unittest.TestCase):
    def setUp(self) -> None:
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'events.sqlite3'
        self.settings = RoleSettings(model='test/matcher')

    def record(self, number: int, text: str, *, source: str = 'rbc',
               edited_at: str | None = None) -> dict[str, Any]:
        return {
            'id': f'{source}:{number}', 'external_id': str(number), 'source': source,
            'url': f'https://t.me/{source}/{number}', 'text': text,
            'published_at': f'2026-11-08T10:{number:02}:00+00:00',
            'received_at': '2026-11-08T11:00:00+00:00', 'edited_at': edited_at,
            'unread_media': False, 'change': 'edit' if edited_at else 'new',
        }

    def submit(self, record: dict[str, Any], *companies: str,
               endpoint: ScriptedEndpoint | None = None) -> dict[str, Any]:
        def model(settings: RoleSettings) -> BaseChatModel:
            if endpoint is None:
                self.fail('Unexpected model call')
            return endpoint.model(settings)

        links = [company_link(record, company) for company in companies]
        with EventArchive(self.path) as archive:
            return archive.match(record, links, self.settings, model_factory=model)

    def test_paraphrased_reprint_keeps_original_age_and_company_context(self) -> None:
        first = self.submit(self.record(1, 'РБК: Сбербанк увеличил чистую прибыль за октябрь до 120 млрд.'), 'sber')
        copy = self.record(2, 'По сообщению РБК октябрьский заработок Сбера достиг 120 миллиардов.',
                           source='copy') | {'extra_metadata': 'never sent to model'}
        endpoint = ScriptedEndpoint([{
            'match': 'same', 'event_id': first['event_id'], 'change': 'none',
            'reason': 'Тот же отчёт за октябрь и та же сумма со ссылкой на РБК.', 'evidence': None,
        }])
        repeat = self.submit(copy, 'sber', endpoint=endpoint)
        self.assertEqual(repeat['kind'], 'repeat')
        self.assertEqual(repeat['event_id'], first['event_id'])
        self.assertEqual(repeat['basis_at'], first['basis_at'])
        self.assertEqual(repeat['analysis_company_ids'], [])
        self.assertNotIn('never sent to model', json.dumps(endpoint.requests[0]))
        self.assertEqual(self.submit(dict(copy, received_at='2026-11-08T12:00:00+00:00'), 'sber'), repeat)
        with EventArchive(self.path) as archive:
            report = archive.report('sber')
        self.assertEqual([row['publication']['source'] for row in report['history']], ['rbc', 'copy'])
        self.assertEqual(report['history'][1]['assessment']['reason'],
                         'Тот же отчёт за октябрь и та же сумма со ссылкой на РБК.')

    def test_same_topic_can_be_a_different_event_and_search_is_bounded(self) -> None:
        first = self.submit(self.record(1, 'Сбербанк опубликовал прибыль за октябрь.'), 'sber')
        self.assertIsNone(first['call'])
        self.assertFalse(first['search']['novelty_verified'])
        for number in range(2, 11):
            endpoint = ScriptedEndpoint([{
                'match': 'different', 'event_id': None, 'change': None, 'evidence': None,
                'reason': 'Отчёт за другой период.',
            }])
            other = self.submit(self.record(number, f'Сбербанк опубликовал прибыль за период {number}.'),
                                'sber', endpoint=endpoint)
            self.assertNotEqual(other['event_id'], first['event_id'])
            self.assertEqual(other['analysis_company_ids'], ['sber'])
        self.assertEqual(other['search']['events_examined'], 9)
        self.assertEqual(len(other['search']['candidates']), 8)
        self.assertFalse(other['search']['novelty_verified'])

    def test_material_updates_keep_history_and_queue_all_affected_companies(self) -> None:
        original = self.record(1, 'РБК: Газпром и Новатэк сократили экспорт СПГ на 15%.')
        first = self.submit(original, 'gazp', 'nvtk')
        updates = [
            ('new_facts', self.record(2, 'РБК: снижение экспорта Газпрома и Новатэка затронуло Азию.'), None),
            ('correction', dict(original, text='РБК: Газпром и Новатэк сократили экспорт СПГ на 5%.',
                                edited_at='2026-11-08T11:05:00+00:00', change='edit'), None),
            ('independent_confirmation', self.record(
                3, 'По собственным замерам судов в портах, экспорт Газпрома и Новатэка упал на 5%.',
                source='port_monitor'), 'По собственным замерам судов в портах'),
            ('retraction', self.record(4, 'РБК отозвал сообщение о падении экспорта Газпрома и Новатэка.'), None),
        ]
        for change, publication, evidence in updates:
            with self.subTest(change=change):
                endpoint = ScriptedEndpoint([{
                    'match': 'same', 'event_id': first['event_id'], 'change': change,
                    'reason': f'Проверенное отличие: {change}.', 'evidence': evidence,
                }])
                update = self.submit(publication, endpoint=endpoint)
                self.assertEqual(update['kind'], 'update')
                self.assertEqual(update['analysis_company_ids'], ['gazp', 'nvtk'])
                request = endpoint.requests[0]
                self.assertTrue(request['response_format']['json_schema']['strict'])
                self.assertNotIn('tools', request)
                self.assertEqual(update['call']['cost'], 0.001)
                if change == 'correction':
                    self.assertNotEqual(update['revision'], first['revision'])
                    evidence_input = json.loads(request['messages'][-1]['content'])['candidates'][0]['evidence']
                    self.assertIn(first['revision'], [row['revision'] for row in evidence_input])
        with EventArchive(self.path) as archive:
            context = archive.report('nvtk')['context']
        self.assertEqual([row['publication'] for row in context], [original, *(item[1] for item in updates)])

    def test_uncertain_match_keeps_disputed_link_and_correction_appends_history(self) -> None:
        first = self.submit(self.record(1, 'Газпром приостановил экспорт по трубопроводу.'), 'gazp')
        endpoint = ScriptedEndpoint([{
            'match': 'uncertain', 'event_id': first['event_id'], 'change': None, 'evidence': None,
            'reason': 'В другом сообщении не назван трубопровод.',
        }])
        ambiguous = self.submit(self.record(2, 'Экспорт по трубопроводу приостановлен.'),
                                'nvtk', endpoint=endpoint)
        self.assertEqual(ambiguous['kind'], 'uncertain_match')
        self.assertNotEqual(ambiguous['event_id'], first['event_id'])
        self.assertEqual(ambiguous['assessment']['event_id'], first['event_id'])
        self.assertEqual(ambiguous['analysis_company_ids'], ['gazp', 'nvtk'])
        with EventArchive(self.path) as archive:
            self.assertEqual(archive.report('gazp')['context'], [first, ambiguous])
            self.assertEqual(archive.report('nvtk')['context'], [first, ambiguous])
            corrected = archive.correct(ambiguous['revision'], 'repeat', first['event_id'],
                                        'Первоисточник назвал тот же трубопровод.')
            self.assertEqual(corrected['correction']['supersedes'], ambiguous['decision_id'])
            self.assertEqual(corrected['analysis_company_ids'], [])
            self.assertEqual(archive.report('gazp')['context'][1], corrected)
            separate = archive.correct(ambiguous['revision'], 'separate', None, 'Найдена другая дата.')
            self.assertEqual(archive.correct(ambiguous['revision'], 'separate', None, 'Найдена другая дата.'),
                             separate)
            report = archive.report()
        self.assertNotEqual(separate['event_id'], first['event_id'])
        self.assertEqual(separate['analysis_company_ids'], ['gazp', 'nvtk'])
        self.assertEqual([row['current'] for row in report['history']], [True, False, False, True])
        self.assertEqual([row['publication'] for row in report['history'][1:]], [ambiguous['publication']] * 3)

    def test_uncertain_without_a_single_candidate_keeps_candidate_companies(self) -> None:
        self.submit(self.record(1, 'Газпром приостановил экспорт по трубопроводу.'), 'gazp')
        endpoint = ScriptedEndpoint([{
            'match': 'uncertain', 'event_id': None, 'change': None, 'evidence': None,
            'reason': 'Компания и трубопровод в сообщении не названы.',
        }])
        ambiguous = self.submit(self.record(2, 'Оператор приостановил экспорт по трубопроводу.'), endpoint=endpoint)
        self.assertEqual(ambiguous['analysis_company_ids'], ['gazp'])
        with EventArchive(self.path) as archive:
            self.assertIn(ambiguous, archive.report('gazp')['context'])
            separated = archive.correct(ambiguous['revision'], 'separate', None, 'Поступили дополнительные сведения.')
        self.assertEqual(separated['analysis_company_ids'], ['gazp'])

    def test_correction_does_not_leave_an_active_basis_from_a_detached_event(self) -> None:
        first = self.submit(self.record(1, 'Газпром приостановил экспорт по трубопроводу.'), 'gazp')
        answer = {'match': 'same', 'event_id': first['event_id'], 'change': 'none', 'evidence': None,
                  'reason': 'Перепечатка.'}
        repeated = self.submit(self.record(2, 'Канал перепечатал новость Газпрома о трубопроводе.'), 'gazp',
                               endpoint=ScriptedEndpoint([answer]))
        self.assertEqual(repeated['basis_at'], first['basis_at'])
        with EventArchive(self.path) as archive:
            archive.correct(first['revision'], 'separate', None, 'Другая остановка.')
            report = archive.report()
        self.assertIsNone(report['event_bases'][first['event_id']])
        self.assertEqual(report['history'][1]['basis_at'], first['basis_at'])
        later = self.submit(self.record(3, 'Другой канал перепечатал новость Газпрома о трубопроводе.'), 'gazp',
                            endpoint=ScriptedEndpoint([answer]))
        self.assertIsNone(later['basis_at'])
        self.assertEqual(later['analysis_company_ids'], [])

    def test_attribution_to_a_common_source_is_not_independent_evidence(self) -> None:
        first = self.submit(self.record(1, 'РБК: Сбербанк опубликовал прибыль.'), 'sber')
        endpoint = ScriptedEndpoint([{
            'match': 'same', 'event_id': first['event_id'], 'change': 'independent_confirmation',
            'evidence': 'по данным РБК', 'reason': 'Другой канал подтвердил прибыль.',
        }])
        result = self.submit(self.record(2, 'Сбербанк опубликовал прибыль по данным РБК.', source='copy'),
                             'sber', endpoint=endpoint)
        self.assertEqual(result['kind'], 'uncertain_update')
        self.assertEqual(result['call']['warning'], 'independent_origin_unverified')

    def test_invalid_model_response_cannot_merge_or_suppress_analysis(self) -> None:
        first = self.submit(self.record(1, 'РБК: Сбербанк опубликовал прибыль.'), 'sber')
        for number, answer in enumerate((
            {'match': 'same', 'event_id': 'invented', 'change': 'none', 'evidence': None},
            {'match': 'same', 'event_id': first['event_id'], 'change': 'independent_confirmation',
             'evidence': 'Наши данные подтверждают'},
            {'match': 'same', 'event_id': first['event_id'], 'change': None, 'evidence': None},
        ), start=2):
            endpoint = ScriptedEndpoint([answer | {'reason': 'Ошибка в ответе.'}])
            rejected = self.submit(self.record(number, 'Сбербанк опубликовал прибыль по данным РБК.'),
                                   'sber', endpoint=endpoint)
            self.assertEqual(rejected['kind'], 'model_error')
            self.assertNotEqual(rejected['event_id'], first['event_id'])
            self.assertEqual(rejected['analysis_company_ids'], ['sber'])
            self.assertIsNotNone(rejected['call']['error'])

    def test_concurrent_change_prevents_stale_classification(self) -> None:
        first = self.submit(self.record(1, 'Сбербанк опубликовал прибыль.'), 'sber')

        def response(body: dict[str, Any]) -> dict[str, Any]:
            self.submit(self.record(3, 'Яндекс открыл новый офис.'), 'yandex')
            return {'match': 'same', 'event_id': first['event_id'], 'change': 'none', 'evidence': None,
                    'reason': 'Та же публикация о прибыли.'}

        with self.assertRaisesRegex(ValueError, 'Архив изменился'):
            self.submit(self.record(2, 'Сбербанк сообщил прибыль.'), 'sber', endpoint=ScriptedEndpoint([response]))
        with EventArchive(self.path) as archive:
            self.assertEqual([row['publication']['id'] for row in archive.report()['history']], ['rbc:1', 'rbc:3'])

    def test_cli_matches_a_completed_route_and_displays_context(self) -> None:
        route = self.path.parent / 'route'
        publication = self.record(1, 'Сбербанк опубликовал отчёт.')
        source = self.path.parent / 'publication.json'
        source.write_text(json.dumps(publication), encoding='utf-8')
        catalog = self.path.parent / 'diagnostics.json'
        catalog.write_text(json.dumps({'operations': {'catalog': {'availability': 'available', 'data': [
            {'company_id': 'sber', 'company_name': 'Сбербанк', 'ticker': 'SBER'},
        ]}}}), encoding='utf-8')
        config = Path('news_futures/agents/jev-model.json')

        def respond(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={'id': 'jev-test', 'model': json.loads(request.content)['model'],
                                             'answers': {'company_0': answer('direct')}})

        with httpx.Client(transport=httpx.MockTransport(respond)) as client:
            trace = route_publication(config, source, catalog, route, client=client, token='test-secret')
        self.assertEqual(trace['status'], 'completed')

        def cli(*args: str) -> subprocess.CompletedProcess[str]:
            return subprocess.run([sys.executable, '-m', 'news_futures', *args, '--database', str(self.path)],
                                  capture_output=True, encoding='utf-8')

        matched = cli('event-match', str(route))
        self.assertEqual(matched.returncode, 0, matched.stderr)
        result = json.loads(matched.stdout)
        self.assertEqual(result['analysis_company_ids'], ['sber'])
        report = cli('events', '--company', 'sber')
        self.assertEqual(report.returncode, 0, report.stderr)
        self.assertEqual(json.loads(report.stdout)['context'], [result])
        corrected = cli('event-correct', result['revision'], '--kind', 'separate', '--reason', 'Проверено')
        self.assertEqual(corrected.returncode, 0, corrected.stderr)
        self.assertEqual(json.loads(corrected.stdout)['correction']['supersedes'], result['decision_id'])
        (route / 'catalog.json').write_text('{}', encoding='utf-8')
        self.assertEqual(cli('event-match', str(route)).returncode, 1)
