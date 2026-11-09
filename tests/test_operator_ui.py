from collections.abc import Callable
from http.client import HTTPConnection
import json
from pathlib import Path
import re
import sys
from tempfile import TemporaryDirectory
from threading import Thread
import time
from typing import Any
import unittest

from news_futures.operator_ui import OperatorServer, Worker


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
        self.server = self.enterContext(OperatorServer(worker, ['example'], 0))
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
