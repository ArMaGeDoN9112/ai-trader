import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class LocalScenarioTest(unittest.TestCase):
    def command(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, '-I', '-S', str(ROOT / 'tests' / 'offline_runner.py'), *args],
            cwd=ROOT,
            env={key: value for key, value in os.environ.items()
                 if key in {'SystemRoot', 'PATH', 'TEMP', 'TMP'}},
            capture_output=True, text=True, encoding='utf-8', timeout=10,
        )

    def test_publication_passes_offline_chain_and_survives_process_exit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / 'control run'
            run = self.command('run', str(directory))
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertEqual(run.stderr, '')
            self.assertEqual(json.loads(run.stdout)['status'], 'simulated_filled')
            show = self.command('show', str(directory))
            self.assertEqual(show.returncode, 0, show.stderr)
            trace = json.loads(show.stdout)
            publication = trace['publication']
            self.assertEqual(publication, {
                'id': 'control-source:publication-001',
                'external_id': 'publication-001',
                'source': 'control-source',
                'url': 'https://example.invalid/control/publication-001',
                'text': 'ИМИТАЦИЯ: Компания «Контроль» сообщила о росте прибыли на 20%.',
                'published_at': '2026-11-06T09:00:00Z',
                'received_at': '2026-11-06T09:00:01Z',
            })
            self.assertEqual(json.loads((directory / 'publication.json').read_text(encoding='utf-8')), publication)
            catalog = trace['catalog']
            self.assertEqual(catalog['company']['id'], 'control-company')
            self.assertEqual(catalog['share']['company_id'], catalog['company']['id'])
            self.assertEqual(catalog['future']['underlying_share_id'], catalog['share']['id'])
            self.assertEqual(catalog['future']['expires_on'], '2026-12-17')
            self.assertEqual(len({catalog[key]['id'] for key in ('company', 'share', 'future')}), 3)
            self.assertEqual([(role['role'], role['mode']) for role in trace['roles']], [
                ('router', 'simulation'), ('company_analyst', 'simulation'), ('critic', 'simulation'),
            ])
            link = trace['roles'][0]['output']
            proposal = trace['roles'][1]['output']
            review = trace['roles'][2]['output']
            self.assertEqual(link['company_id'], catalog['company']['id'])
            self.assertEqual(link['publication_id'], publication['id'])
            self.assertEqual(proposal['company_id'], link['company_id'])
            self.assertEqual(proposal['publication_id'], publication['id'])
            self.assertEqual(proposal['contract_id'], catalog['future']['id'])
            self.assertEqual(review['proposal_id'], proposal['id'])
            self.assertEqual(trace['check']['accepted'], True)
            self.assertEqual(trace['check']['checker'], 'local-program')
            execution = trace['execution']
            self.assertEqual(execution['mode'], 'simulation')
            self.assertEqual(execution['status'], 'simulated_filled')
            self.assertEqual(execution['publication_id'], publication['id'])
            self.assertEqual(execution['proposal_id'], proposal['id'])
            self.assertEqual(execution['contract_id'], catalog['future']['id'])
            self.assertEqual(execution['entry_reason'], proposal['entry_reason'])
            self.assertEqual(execution['entry_reason'], 'Рост прибыли контрольной компании на 20% в контрольной публикации.')
            self.assertEqual(execution['quantity'], 1)
            self.assertEqual(execution['fill_price'], 300)
            self.assertEqual(trace['mode'], 'simulation')

            repeated = self.command('run', str(directory))
            self.assertEqual(repeated.returncode, 1)
            self.assertEqual(self.command('show', str(directory)).stdout, show.stdout)
            second = Path(temporary) / 'second'
            self.assertEqual(self.command('run', str(second)).returncode, 0)
            self.assertEqual(self.command('show', str(second)).stdout, show.stdout)

    def test_offline_guard_blocks_network_and_subprocesses(self) -> None:
        for probe in ('socket', 'http', 'subprocess', 'shell'):
            with self.subTest(probe=probe):
                result = self.command('--probe', probe)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('FORBIDDEN_EXTERNAL_CALL', result.stderr)

    def test_missing_trace_is_an_explicit_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result = self.command('show', temporary)
            self.assertEqual(result.returncode, 1)
            self.assertIn('Ошибка:', result.stderr)
            self.assertEqual(result.stdout, '')


if __name__ == '__main__':
    unittest.main()
