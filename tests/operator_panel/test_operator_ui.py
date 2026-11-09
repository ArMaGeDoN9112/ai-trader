from collections.abc import Callable
from contextlib import closing
from datetime import timedelta
from http.client import HTTPConnection
import json
from pathlib import Path
import os
import re
import subprocess
import sys
from tempfile import TemporaryDirectory
from threading import Thread
import time
from typing import Any
import unittest
from unittest.mock import patch
from urllib.request import urlopen

from websockets.sync.client import connect

from news_futures.operator.server import OperatorServer
from news_futures.operator.supervisor import Worker
from news_futures.agents.events import EventArchive
from news_futures.common.artifacts import replace_json
from news_futures.news.publications import Archive, Publication, utc_now
from news_futures.application.records import runtime_tables, save_result


CONTROLLED_WORKER = """
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sqlite3
import sys
from threading import Event, Thread
import time

directory, run_id = Path(sys.argv[1]), sys.argv[2]
profile = json.loads(sys.stdin.readline())
stop = Event()

def wait_for_stop():
    os.read(sys.stdin.fileno(), 1)
    stop.set()

Thread(target=wait_for_stop, daemon=True).start()
with (directory / 'starts').open('a') as starts:
    starts.write(run_id + '\\n')

while not (directory / 'ready').exists():
    if stop.wait(0.01):
        raise SystemExit(0)

with sqlite3.connect(directory / 'publications.sqlite3') as db:
    db.execute('CREATE TABLE IF NOT EXISTS runtime_status (id INTEGER PRIMARY KEY, data TEXT)')
    while not stop.is_set():
        control = (directory / 'control').read_text()
        if control == 'crash':
            print('token-private-value', file=sys.stderr, flush=True)
            print(json.dumps({'failure': 'token-private-value'}), flush=True)
            raise SystemExit(7)
        if control == 'empty':
            checked_at = datetime.now(timezone.utc)
            position = None
        else:
            checked_at = datetime.now(timezone.utc) - timedelta(minutes=2 if control == 'stale' else 0)
            position = {'contract_id': 'future', 'side': 'long', 'quantity': 2, 'id': 'secret-cycle-id'}
        last = {'checked_at': checked_at.isoformat(), 'position': position,
                'protection': {'status': 'confirmed' if position else 'none', 'token': 'token-private-value'}}
        status = profile | {'run_id': run_id, 'confirmed': True, 'last_confirmed': last, 'broker': 'connected'}
        if control == 'offline':
            status.update(confirmed=False, broker='blocked', error='token-private-value')
        with db:
            db.execute('INSERT OR REPLACE INTO runtime_status VALUES (1, ?)', (json.dumps(status),))
        print(json.dumps({'source': 'example', 'ok': control != 'offline', 'ready': True}), flush=True)
        stop.wait(0.02)

time.sleep(0.25)
with (directory / 'stops').open('a') as stops:
    stops.write(run_id + '\\n')
"""


class OperatorTest(unittest.TestCase):
    def setUp(self) -> None:
        directory = self.enterContext(TemporaryDirectory())
        self.root = Path(directory)
        self.profile = {'mode': 'observation', 'account_id': 'account', 'contracts': {'future': 'company'}}
        (self.root / 'control').write_text('position', encoding='utf-8')
        worker = Worker(self.root, self.profile, [sys.executable, '-c', CONTROLLED_WORKER, str(self.root)],
                        json.dumps(self.profile).encode())
        self.server = self.enterContext(OperatorServer(worker, ['example'], 0, {
            'company': {'names': ['Компания А'], 'tickers': ['ABC']},
            'other': {'names': ['Компания Б'], 'tickers': ['XYZ']},
        }))
        thread = Thread(target=self.server.serve_forever, kwargs={'poll_interval': 0.01})
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(self.server.shutdown)
        self.addCleanup(worker.close)
        self.port = self.server.server_port
        self.origin = f'http://127.0.0.1:{self.port}'
        status, page = self.request('GET', '/')
        self.assertEqual(status, 200)
        self.session = re.findall(r'<meta name="operator-session" content="([^"]+)"', page)[0]

    def request(self, method: str, path: str, **headers: str) -> tuple[int, str]:
        connection = HTTPConnection('127.0.0.1', self.port, timeout=3)
        try:
            connection.request(method, path, headers=headers)
            response = connection.getresponse()
            return response.status, response.read().decode()
        finally:
            connection.close()

    def overview(self) -> dict[str, Any]:
        code, body = self.request('GET', '/api/overview', **{'X-Operator-Session': self.session})
        self.assertEqual(code, 200)
        return dict(json.loads(body))

    def command(self, name: str, session: str | None = None) -> tuple[int, str]:
        return self.request('POST', f'/api/{name}', Origin=self.origin,
                            **{'X-Operator-Session': session or self.session})

    def wait_for(self, condition: Callable[[dict[str, Any]], object]) -> dict[str, Any]:
        deadline = time.monotonic() + 5
        while True:
            status = self.overview()
            if condition(status):
                return status
            if time.monotonic() >= deadline:
                self.fail(f'Expected state not reached: {status}')
            time.sleep(0.02)

    def control(self, value: str) -> None:
        temporary = self.root / 'control.tmp'
        temporary.write_text(value, encoding='utf-8')
        temporary.replace(self.root / 'control')

    def test_commands_require_a_local_session_and_same_origin(self) -> None:
        session = {'X-Operator-Session': self.session}
        self.assertEqual(self.request('GET', '/api/overview')[0], 403)
        self.assertEqual(self.request('POST', '/api/start', Origin=self.origin)[0], 403)
        self.assertEqual(self.request('POST', '/api/start', Origin='https://example.org', **session)[0], 403)
        self.assertEqual(self.request('POST', '/api/start', **session)[0], 403)
        self.assertEqual(self.request('GET', '/', Host='example.org')[0], 403)
        self.assertEqual(self.request('GET', '/api/start', **session)[0], 404)
        self.assertEqual(self.overview()['worker'], 'stopped')
        self.assertFalse((self.root / 'operator-commands.jsonl').exists())

    def test_two_tabs_share_one_worker_and_stop_waits_for_its_exit(self) -> None:
        self.assertEqual(self.command('start')[0], 202)
        self.assertEqual(self.overview()['worker'], 'starting')
        code, page = self.request('GET', '/')
        self.assertEqual(code, 200)
        second_session = re.findall(r'<meta name="operator-session" content="([^"]+)"', page)[0]
        self.assertEqual(self.command('start', second_session)[0], 409)
        (self.root / 'ready').touch()
        status = self.wait_for(lambda view: view['worker'] == 'running')
        self.assertEqual(status['sources'], {'example': 'connected'})
        self.assertEqual(status['account_id'], 'account')
        self.assertEqual(self.command('restart')[0], 202)
        self.assertEqual(self.overview()['worker'], 'stopping')
        self.wait_for(lambda view: view['worker'] == 'running')
        starts = (self.root / 'starts').read_text().splitlines()
        self.assertEqual(len(set(starts)), 2)
        self.assertEqual((self.root / 'stops').read_text().splitlines(), starts[:1])
        self.assertEqual(self.command('stop')[0], 202)
        self.wait_for(lambda view: view['worker'] == 'stopped')
        self.assertEqual((self.root / 'stops').read_text().splitlines(), starts)
        self.assertIsNone(self.overview()['broker']['current'])

    def test_stale_and_failed_checks_leave_history_and_crash_allows_explicit_restart(self) -> None:
        self.assertIsNone(self.overview()['broker']['last_confirmed'])
        (self.root / 'ready').touch()
        self.assertEqual(self.command('start')[0], 202)
        status = self.wait_for(lambda view: view['broker']['current'])
        self.assertEqual(status['broker']['current']['position'],
                         {'contract_id': 'future', 'side': 'long', 'quantity': 2})
        self.assertEqual(status['broker']['current']['protection'], 'confirmed')
        self.control('stale')
        status = self.wait_for(lambda view: view['broker']['current'] is None)
        self.assertEqual(status['broker']['last_confirmed']['position']['quantity'], 2)
        self.control('offline')
        status = self.wait_for(lambda view: view['broker']['state'] == 'blocked')
        self.assertIsNone(status['broker']['current'])
        self.assertEqual(status['broker']['reason'], 'broker_check_failed')
        self.assertNotIn('token-private-value', json.dumps(status))
        self.control('crash')
        status = self.wait_for(lambda view: view['worker'] == 'failed')
        self.assertEqual(status['failure'], 'worker')
        self.assertEqual(status['exit_code'], 7)
        self.assertIsNotNone(status['broker']['last_confirmed'])
        self.control('empty')
        self.assertEqual(self.command('restart')[0], 202)
        status = self.wait_for(lambda view: view['broker']['current'])
        self.assertIsNone(status['broker']['current']['position'])
        self.assertEqual(status['broker']['current']['protection'], 'none')
        journal = (self.root / 'operator-commands.jsonl').read_text()
        self.assertNotIn('token-private-value', journal)
        self.assertNotIn(self.session, journal)

    def test_stop_still_works_if_the_command_journal_cannot_be_written(self) -> None:
        (self.root / 'ready').touch()
        self.assertEqual(self.command('start')[0], 202)
        self.wait_for(lambda view: view['worker'] == 'running')
        journal = self.root / 'operator-commands.jsonl'
        journal.unlink()
        journal.mkdir()
        self.assertEqual(self.command('stop')[0], 202)
        self.assertTrue(self.wait_for(lambda view: view['worker'] == 'stopped')['journal_error'])

    def save_news(self) -> tuple[str, str, Path]:
        now = utc_now() - timedelta(minutes=1)
        source = 'telegram:42'
        linked = Publication(source, '1', 'https://t.me/source/1', '<img src=x onerror=alert(1)> ' + 'Новости. ' * 260,
                             now, now)
        earlier = now - timedelta(seconds=2)
        unlinked = Publication(source, '2', 'https://t.me/source/2', 'Без привязки', earlier, earlier)
        revision = linked.revision_key()
        links = [{'company_id': 'company', 'kind': 'direct'}, {'company_id': 'other', 'kind': 'inferred'}]
        event = {'event_id': 'event-1', 'kind': 'unmatched', 'recorded_at': now.isoformat(),
                 'analysis_company_ids': ['company', 'other'], 'call': None}
        with (closing(Archive(self.root / 'publications.sqlite3').db) as db,
              EventArchive(self.root / 'events.sqlite3') as events):
            # Real archive schemas, with saved evidence from a rejected review.
            with db, events.db:
                for publication, bindings in ((linked, links), (unlinked, [])):
                    key = publication.revision_key()
                    record = json.dumps(publication.record())
                    db.execute("INSERT INTO revisions VALUES (?, ?, 0, 'pending', NULL)", (key, record))
                    events.db.execute('INSERT INTO event_revisions VALUES (?, ?, ?)',
                                      (key, record, json.dumps(bindings)))
                    events.db.execute('INSERT INTO event_decisions (revision, details) VALUES (?, ?)',
                                      (key, json.dumps(event)))
        archive = Archive(self.root / 'publications.sqlite3')
        with closing(archive.db):
            runtime_tables(archive)
            save_result(archive, {'revision': revision, 'stage': 'review', 'status': 'rejected', 'decision_id': 1})
            save_result(archive, {'revision': unlinked.revision_key(), 'stage': 'event', 'status': 'skipped'})
        directory = self.root / 'decisions' / revision
        review = directory / 'review'
        review.mkdir(parents=True)
        context = {'candidates': [{'company_id': 'company', 'contract_id': 'old-future'},
                                  {'company_id': 'other', 'contract_id': 'other-future', 'ticker': 'XYZH6'}]}
        replace_json(directory / 'snapshot.json', {'instruments': context['candidates']})
        replace_json(directory / 'catalog.json', {'operations': {
            'future': {'availability': 'available', 'data': {
                'id': 'old-future', 'ticker': 'ABCZ5', 'underlying_share_id': 'share-a',
            }},
            'catalog': {'availability': 'available', 'data': [{'company_id': 'company', 'share_id': 'share-a'}]},
        }})
        replace_json(directory / 'runtime.json', {'session': 'private-session-path', 'api_key': 'private-key'})
        replace_json(review / 'context.json', context)
        replace_json(review / 'call-01.json', {'role': 'analyst', 'at': now.isoformat(), 'input': {'context': context},
                     'output': {'action': 'skip', 'assessments': [{'contract_id': 'other-future', 'impact': 'unverified',
                                'reason': 'Нет доказательств влияния'}]},
                     'raw_output': json.dumps({'reason': 'Полный ответ аналитика. api_hash = "private-hash-in-text"',
                                               'api_key': 'private-key-in-output', 'api_hash': 'private-hash-in-json'},
                                              ensure_ascii=False),
                     'metadata': {'session': 'private-session'}})
        replace_json(review / 'call-02.json', {'role': 'critic', 'at': (now + timedelta(seconds=1)).isoformat(),
                     'output': {'approved': False, 'objections': ['Нет подтверждения']}})
        return revision, unlinked.revision_key(), review

    def test_only_saved_bindings_reach_the_company_and_decision_views(self) -> None:
        revision, unlinked, _ = self.save_news()
        session = {'X-Operator-Session': self.session}
        self.assertEqual(self.request('GET', '/api/news')[0], 403)
        for company, kind in (('company', 'direct'), ('other', 'inferred')):
            code, body = self.request('GET', '/api/news?company=' + company, **session)
            self.assertEqual(code, 200)
            news = json.loads(body)
            self.assertEqual([row['revision'] for row in news['decisions']], [revision])
            self.assertEqual(news['publications'][0]['binding']['kind'], kind)
            self.assertEqual(news['publications'][1]['revision'], unlinked)
            self.assertFalse(news['publications'][1]['has_trace'])
            self.assertIn('old-future', news['decisions'][0]['contracts'])
            if kind == 'inferred':
                self.assertEqual(news['publications'][0]['assessments'][0]['impact'], 'unverified')
        self.assertEqual(self.request('GET', '/api/trace?revision=' + unlinked, **session)[0], 404)
        self.assertEqual(self.request('GET', '/api/trace?revision=../runtime.json', **session)[0], 404)
        code, body = self.request('GET', '/api/trace?revision=' + revision, **session)
        self.assertEqual(code, 200)
        trace = json.loads(body)
        self.assertEqual(trace['status'], 'rejected')
        self.assertIn('Нет подтверждения', body)
        self.assertIn('Полный ответ аналитика', body)
        self.assertNotIn('private-', body)

    @unittest.skipUnless(os.environ.get('OPERATOR_TEST_CHROME'), 'Set OPERATOR_TEST_CHROME to a Chromium executable')
    def test_ticker_to_rejected_trace_keeps_expansion_and_scroll_during_refresh(self) -> None:
        revision, _, review = self.save_news()
        profile = self.root / 'chromium'
        with subprocess.Popen([
            os.environ['OPERATOR_TEST_CHROME'], '--headless', '--remote-debugging-port=0',
            f'--user-data-dir={profile}', '--no-first-run', '--disable-background-networking', 'about:blank',
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) as browser:
            try:
                self.exercise_browser(profile, revision, review)
            finally:
                if browser.poll() is None:
                    browser.terminate()

    def exercise_browser(self, profile: Path, revision: str, review: Path) -> None:
        def until(check: Callable[[], object]) -> None:
            deadline = time.monotonic() + 8
            while not check():
                if time.monotonic() > deadline:
                    self.fail('Timed out: ' + repr(check))
                time.sleep(0.05)

        port_file = profile / 'DevToolsActivePort'
        until(port_file.is_file)
        port = port_file.read_text().splitlines()[0]
        with urlopen(f'http://127.0.0.1:{port}/json/list', timeout=3) as response:
            tab = next(target for target in json.load(response) if target['type'] == 'page')
        with connect(tab['webSocketDebuggerUrl'], open_timeout=3) as socket:
            sequence = 0

            def cdp(method: str, **params: object) -> dict[str, Any]:
                nonlocal sequence
                sequence += 1
                socket.send(json.dumps({'id': sequence, 'method': method, 'params': params}))
                while True:
                    response = json.loads(socket.recv(timeout=5))
                    if response.get('id') == sequence:
                        self.assertNotIn('error', response)
                        return dict(response.get('result', {}))

            def evaluate(expression: str) -> Any:
                result = cdp('Runtime.evaluate', expression=expression, returnByValue=True)
                self.assertNotIn('exceptionDetails', result)
                return result['result'].get('value')

            def expect(expression: str) -> None:
                try:
                    until(lambda: evaluate(expression))
                except AssertionError as error:
                    raise AssertionError(f'{expression}: {evaluate("document.body.innerText")!r}') from error

            def search(ticker: str) -> None:
                evaluate('location.hash = "#tickers"')
                expect('!document.querySelector("#tickers-screen").hidden')
                evaluate(f'document.querySelector("#ticker-search").value = {json.dumps(ticker)};'
                         'document.querySelector("#ticker-form").requestSubmit()')

            cdp('Emulation.setDeviceMetricsOverride', width=375, height=720, deviceScaleFactor=1, mobile=False)
            cdp('Page.navigate', url=self.origin)
            expect('document.querySelectorAll("#recent a").length === 1')
            expect('!document.querySelector("#recent").innerText.includes("Без привязки")')
            search('xyz')
            expect('document.querySelector("#company-name").textContent === "Компания Б"')
            evaluate('document.querySelector("#company-events details").open = true')
            expect('document.querySelector("#company-events").innerText.includes("Гипотеза") && '
                   'document.querySelector("#company-events").innerText.includes("Влияние не подтверждено")')
            expect('document.querySelector("#company-events").innerText.includes("Материал того же события") && '
                   'document.querySelectorAll("#company-events a[href^=\'#trace/\']").length === 1')
            search('abc')
            expect('document.querySelector("#company-name").textContent === "Компания А"')
            expect('document.querySelector("#company-events").innerText.includes("Прямая связь")')
            search('abcz5')
            expect('document.querySelector("#company-contract").value === "old-future"')
            expect('document.querySelectorAll("#company-decisions a").length === 1')
            expect('document.querySelector("#company-events").innerText.includes("Прямая связь")')
            evaluate('location.hash = "#decisions"')
            expect('!document.querySelector("#decisions-screen").hidden')
            evaluate('for (const [name, value] of [["company", "company"], ["contract", "old-future"],'
                     '["result", "rejected"]]) { const field = document.getElementById("decision-" + name);'
                     'field.value = value; field.dispatchEvent(new Event("change")); }')
            expect('document.querySelectorAll("#decisions a").length === 1')
            evaluate('const from = document.querySelector("#decision-from"); from.value = "2099-01-01";'
                     'from.dispatchEvent(new Event("change"))')
            expect('document.querySelectorAll("#decisions a").length === 0')
            evaluate('document.querySelector("#decision-from").value = "";'
                     'document.querySelector("#decision-from").dispatchEvent(new Event("change"))')
            evaluate('document.querySelector("#decisions a").click()')
            expect(f'location.hash === "#trace/{revision}" && document.querySelector("#trace-steps").innerText'
                   '.includes("Ответ критика")')
            evaluate('document.querySelector("#trace-steps details").open = true; window.scrollTo(0, 550)')
            expect('window.scrollY === 550 && !document.querySelector("#trace-steps img")')
            expect('document.querySelector("#trace-steps a[href=\'https://t.me/source/1\']") !== null')
            replace_json(review / 'call-03.json', {'role': 'analyst', 'at': utc_now().isoformat(),
                         'input': {'review': {'approved': False}}, 'output': {'action': 'skip'}})
            expect('document.querySelector("#trace-steps").innerText.includes("Пересмотр аналитика")')
            expect('window.scrollY === 550 && document.querySelector("#trace-steps details").open')
            expect('document.documentElement.scrollWidth === document.documentElement.clientWidth')

    @unittest.skipUnless(sys.platform == 'win32', 'Windows file sharing')
    def test_saved_artifact_is_replaced_after_a_concurrent_windows_read(self) -> None:
        path = self.root / 'artifact.json'
        replace_json(path, {'status': 'processing'})
        with path.open('rb') as reader, patch('news_futures.common.artifacts.sleep', side_effect=lambda _: reader.close()):
            replace_json(path, {'status': 'completed'})
        self.assertEqual(json.loads(path.read_bytes()), {'status': 'completed'})
