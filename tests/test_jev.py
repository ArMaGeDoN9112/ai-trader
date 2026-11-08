import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import httpx
from pydantic import ValidationError

from news_futures.analysis import AnalysisContext, control_context
from news_futures.jev import route_publication
from news_futures.local import publication_digest


def answer(choice: str, probability: float = 1.0) -> dict[str, object]:
    probabilities = dict.fromkeys(('direct', 'industry', 'commodity', 'regulation', 'uncertain', 'none'), 0.0)
    probabilities[choice] = probability
    probabilities['none' if choice != 'none' else 'direct'] = 1.0 - probability
    return {'type': 'choice', 'choice': choice, 'probabilities': probabilities, 'confidence': probability}


class JevTests(unittest.TestCase):
    def test_full_catalog_uses_one_question_per_company_and_freezes_the_model(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            publication, catalog, config = self.inputs(root, 81)
            requested_models: list[str] = []
            requested_tickers: list[str] = []

            def respond(request: httpx.Request) -> httpx.Response:
                body = json.loads(request.content)
                model = body['model']
                requested_models.append(model)
                for key in body['questions']:
                    requested_tickers.extend(body['state'][key]['tickers'])
                config.write_text('{"model":"typesafe/jev-1.13"}', encoding='utf-8')
                return httpx.Response(200, json={
                    'model': model, 'id': 'gen-dec-1',
                    'answers': {
                        key: answer('direct' if body['state'][key]['tickers'] == ['T80'] else 'none')
                        for key in body['questions']
                    },
                })

            with httpx.Client(transport=httpx.MockTransport(respond)) as client:
                first = route_publication(config, publication, catalog, root / 'first', client=client, token='secret')
                self.assertEqual(requested_models, ['typesafe/jev-1.14'] * 3)
                self.assertEqual(requested_tickers, [f'T{i}' for i in range(81)])
                self.assertEqual([(link['company_id'], link['kind']) for link in first['links']],
                                 [('asset-80', 'direct')])
                second = route_publication(config, publication, catalog, root / 'second', client=client, token='secret')
            self.assertEqual(requested_models[3:], ['typesafe/jev-1.13'] * 3)
            self.assertNotEqual(first['settings_version'], second['settings_version'])

    def test_empty_result_is_distinct_from_unknown_keys_malformed_answers_and_service_failure(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            publication, catalog, config = self.inputs(root, 1)
            cases = [
                ('empty', {'company_0': answer('none')}, None),
                ('uncertain', {'company_0': {
                    'type': 'choice', 'choice': 'none', 'confidence': 0.1,
                    'probabilities': {'none': 0.3, 'direct': 0.29, 'industry': 0.22,
                                      'uncertain': 0.19, 'commodity': 0, 'regulation': 0},
                }}, None),
                ('unknown', {'unknown-asset': answer('direct')}, 'invalid_decision'),
                ('malformed', {'company_0': answer('direct', 0.1)}, 'invalid_decision'),
                ('malformed-envelope', None, 'invalid_decision'),
                ('missing-request-id', {'company_0': answer('none')}, 'invalid_decision'),
                ('unavailable', None, 'service_unavailable'),
            ]
            for name, answers, expected_error in cases:
                with self.subTest(name=name):
                    def respond(request: httpx.Request) -> httpx.Response:
                        if name == 'unavailable':
                            raise httpx.ConnectError('fake secret', request=request)
                        return httpx.Response(200, json={
                            'model': 'typesafe/jev-1.14',
                            'id': None if name == 'missing-request-id' else 'fake secret',
                            'usage': {'cost': 0.01, 'input_tokens': 'invalid'}, 'answers': answers,
                        })

                    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
                        result = route_publication(config, publication, catalog, root / name,
                                                   client=client, token='fake secret')
                    self.assertEqual(result['error'], expected_error)
                    self.assertEqual(result['status'], 'failed' if expected_error else 'completed')
                    if name == 'uncertain':
                        self.assertEqual([link['kind'] for link in result['links']], ['uncertain'])
                    else:
                        self.assertEqual(result['links'], None if expected_error else [])
                    self.assertNotIn('fake secret', (root / name / 'batch-001.json').read_text(encoding='utf-8'))
                    if name != 'unavailable':
                        self.assertEqual(result['cost_usd'], 0.01)

    def test_analyst_accepts_the_routed_publication_and_rejects_a_different_revision(self) -> None:
        context = control_context()
        context['company_link']['publication_sha256'] = 'old-byte-hash'
        AnalysisContext.model_validate(context)
        context['company_link']['publication_revision_sha256'] = publication_digest(context['publication'])
        AnalysisContext.model_validate(context)
        context['publication']['text'] += ' Исправление.'
        with self.assertRaisesRegex(ValidationError, 'company_link_publication_revision_mismatch'):
            AnalysisContext.model_validate(context)

    def test_later_batch_failure_keeps_the_audit_but_withholds_all_links(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            publication, catalog, config = self.inputs(root, 41)
            requests = 0

            def respond(request: httpx.Request) -> httpx.Response:
                nonlocal requests
                requests += 1
                return httpx.Response(200, json={
                    'model': 'typesafe/jev-1.14' if requests == 1 else 'typesafe/jev-1.13',
                    'id': f'gen-dec-{requests}', 'usage': {'cost': 0.01},
                    'answers': {key: answer('direct') for key in json.loads(request.content)['questions']},
                })

            with httpx.Client(transport=httpx.MockTransport(respond)) as client:
                trace = route_publication(config, publication, catalog, root / 'run', client=client, token='secret')
            self.assertEqual(trace['status'], 'failed')
            self.assertIsNone(trace['links'])
            self.assertEqual(trace['cost_usd'], 0.02)
            self.assertEqual(len(trace['batches']), 2)
            self.assertTrue((root / 'run/batch-001.json').is_file())

    def inputs(self, root: Path, size: int) -> tuple[Path, Path, Path]:
        publication = root / 'publication.json'
        publication.write_text(json.dumps({
            'id': 'sample:1', 'url': 'https://example.invalid/1',
            'text': 'Публикация для проверки каталога.',
        }), encoding='utf-8')
        catalog = root / 'diagnostics.json'
        rows = [{'company_id': f'asset-{i}', 'company_name': f'Компания {i}', 'ticker': f'T{i}'}
                for i in range(size)]
        catalog.write_text(json.dumps({'operations': {'catalog': {
            'availability': 'available', 'data': [*rows, rows[0]],
        }}}), encoding='utf-8')
        config = root / 'config.json'
        config.write_text('{"model":"typesafe/jev-1.14"}', encoding='utf-8')
        return publication, catalog, config

    def test_independent_company_links_are_saved_for_the_analyst(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            publication = root / 'publication.json'
            publication.write_text(json.dumps({
                'id': 'sample:1', 'url': 'https://example.invalid/1',
                'text': 'Минфин предложил новый сбор для российских нефтедобытчиков.',
            }), encoding='utf-8')
            catalog = root / 'diagnostics.json'
            catalog.write_text(json.dumps({'source': 'control', 'operations': {'catalog': {
                'availability': 'available', 'data': [
                    {'company_id': ticker, 'company_name': name, 'ticker': ticker}
                    for ticker, name in [('LKOH', 'Лукойл'), ('ROSN', 'Роснефть'), ('SBER', 'Сбербанк')]
                ],
            }}}), encoding='utf-8')
            config = root / 'config.json'
            config.write_text('{"model":"typesafe/jev-1.14"}', encoding='utf-8')

            def respond(request: httpx.Request) -> httpx.Response:
                self.assertEqual(str(request.url), 'https://openrouter.ai/api/alpha/decisions')
                self.assertEqual(request.headers['Authorization'], 'Bearer test-key')
                body = json.loads(request.content)
                self.assertEqual(body['model'], 'typesafe/jev-1.14')
                self.assertEqual(len(body['questions']), 3)
                self.assertNotIn('test-key', request.content.decode())
                return httpx.Response(200, json={
                    'model': 'typesafe/jev-1.14-20261014', 'id': 'gen-dec-1', 'provider': 'TypeSafe',
                    'usage': {'cost': 0.0001},
                    'answers': dict(zip(body['questions'], [
                        answer('regulation'), answer('direct', 0.55), answer('none'),
                    ], strict=True)),
                })

            with httpx.Client(transport=httpx.MockTransport(respond)) as client:
                trace = route_publication(config, publication, catalog, root / 'run',
                                          client=client, token='test-key')
            self.assertEqual(trace['status'], 'completed')
            links = trace['links']
            self.assertEqual([(link['company_id'], link['kind']) for link in links],
                             [('LKOH', 'inferred'), ('ROSN', 'uncertain')])
            self.assertEqual(links[0]['basis'], 'regulation')
            self.assertTrue(links[0]['requires_analyst_review'])
            self.assertEqual(links[0]['publication_id'], 'sample:1')
            self.assertEqual(links[0]['publication_url'], 'https://example.invalid/1')
            self.assertEqual(trace['actual_model'], 'typesafe/jev-1.14-20261014')
            self.assertEqual(trace['cost_usd'], 0.0001)
            self.assertEqual(json.loads((root / 'run/trace.json').read_text(encoding='utf-8')), trace)
