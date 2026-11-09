"""Start, stop and monitor the local application's worker process."""

from contextlib import closing
from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
from threading import Lock, Thread
from typing import Any
from uuid import uuid4

from news_futures.news.publications import utc_now
from news_futures.operator.worker import FAILURES


FRESH_FOR = timedelta(seconds=45)
SOURCE_FRESH_FOR = timedelta(seconds=60)
BLOCKS = {
    'protection_unconfirmed': 'protection_unconfirmed',
    'protection_ownership_unconfirmed': 'protection_unconfirmed',
    'protection_submission_unconfirmed': 'protection_unconfirmed',
    'broker_reconciliation_required': 'broker_reconciliation_required',
    'exit_or_cancellation_unconfirmed': 'broker_reconciliation_required',
    'fills_and_position_disagree': 'broker_position_mismatch',
    'executor_busy': 'executor_busy',
}


def broker_status(path: Path, profile: dict[str, Any], run_id: str | None) -> dict[str, Any]:
    try:
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=0.2)) as db:
            row = db.execute('SELECT data FROM runtime_status WHERE id = 1').fetchone()
        status = json.loads(row[0]) if row else {}
        if any(status.get(key) != profile[key] for key in ('mode', 'account_id', 'contracts')):
            status = {}
    except (sqlite3.Error, ValueError, AttributeError):
        status = {}

    # Legacy records can supply history if their reconciliation was successful.
    last = status.get('last_confirmed')
    if last is None and status.get('broker') == 'connected':
        last = status
    confirmed = None
    try:
        if last:
            checked_at = datetime.fromisoformat(last['checked_at']).isoformat()
            position = last['position']
            if position is not None:
                if (position['contract_id'] not in profile['contracts'] or position['side'] not in ('long', 'short')
                        or type(position['quantity']) is not int or position['quantity'] <= 0):
                    raise ValueError('Invalid position')
                position = {key: position[key] for key in ('contract_id', 'side', 'quantity')}
            protection = last['protection']['status']
            if protection not in ('none', 'confirmed', 'unconfirmed'):
                raise ValueError('Invalid protection')
            confirmed = {'checked_at': checked_at, 'position': position, 'protection': protection}
    except (KeyError, TypeError, ValueError):
        pass
    current_run = run_id is not None and status.get('run_id') == run_id
    fresh = False
    if current_run and status.get('confirmed') is True and confirmed:
        stamp = datetime.fromisoformat(confirmed['checked_at'])
        fresh = stamp.tzinfo is not None and timedelta(0) <= utc_now() - stamp <= FRESH_FOR
    blocked = current_run and status.get('broker') == 'blocked'
    error = status.get('error')
    return {
        'state': 'blocked' if blocked else 'connected' if fresh else 'unknown',
        'reason': BLOCKS.get(error, 'broker_check_failed') if blocked and isinstance(error, str) else None,
        'current': confirmed if fresh else None,
        'last_confirmed': confirmed,
    }


class Worker:
    def __init__(self, directory: Path, profile: dict[str, Any], command: list[str], settings: bytes):
        self.directory = directory.resolve()
        self.profile = profile
        self.command = command
        self.settings = settings
        self.lock = Lock()
        self.state = 'stopped'
        self.failure: str | None = None
        self.exit_code: int | None = None
        self.result: str | None = None
        self.journal_error = False
        self.closed = False
        self.restart = False
        self.run_id: str | None = None
        self.process: subprocess.Popen[bytes] | None = None
        self.thread: Thread | None = None
        self.sources: dict[str, tuple[bool, datetime]] = {}

    def record(self, action: str, result: str, *, exit_code: int | None = None) -> bool:
        entry = json.dumps({'at': utc_now().isoformat(), 'action': action, 'result': result, 'exit_code': exit_code})
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
            with (self.directory / 'operator-commands.jsonl').open('a', encoding='utf-8') as journal:
                journal.write(entry + '\n')
        except OSError:
            self.journal_error = True
            return False
        self.journal_error = False
        return True

    def _allowed(self) -> list[str]:
        if self.closed:
            return []
        return {
            'stopped': ['start', 'restart'], 'failed': ['start', 'restart'],
            'starting': ['stop'], 'running': ['stop', 'restart'], 'stopping': [],
        }[self.state]

    def request(self, action: str) -> tuple[int, str]:
        with self.lock:
            if action not in self._allowed():
                self.record(action, 'ignored')
                return 409, 'ignored'
            if not self.record(action, 'accepted') and action != 'stop':
                return 503, 'journal_error'
            self.result = action + '_accepted'
            if action == 'stop' or (action == 'restart' and self.state == 'running'):
                self.restart = action == 'restart'
                self._stop()
            else:
                self._start()
            return 202, self.result

    def _start(self) -> None:
        self.state = 'starting'
        self.failure = None
        self.exit_code = None
        self.sources.clear()
        run_id = self.run_id = uuid4().hex
        self.thread = Thread(target=self._supervise, args=(run_id,))
        self.thread.start()

    def _stop(self) -> None:
        self.state = 'stopping'
        if self.process is not None and self.process.stdin is not None:
            self.process.stdin.close()

    def _supervise(self, run_id: str) -> None:
        reason = 'worker'
        code = None
        try:
            with subprocess.Popen(
                [*self.command, run_id], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0,
                start_new_session=sys.platform != 'win32',
            ) as process:
                assert process.stdin is not None and process.stdout is not None
                try:
                    process.stdin.write(self.settings + b'\n')
                    process.stdin.flush()
                    with self.lock:
                        self.process = process
                        if self.state == 'stopping':
                            self._stop()
                    for line in process.stdout:
                        reason = self._update(line, reason)
                finally:
                    process.stdin.close()
                code = process.wait()
        except OSError:
            reason = 'worker_start'
        with self.lock:
            self.process = None
            if code == 0 and self.state == 'stopping':
                self.state = self.result = 'stopped'
                self.record('worker', 'stopped')
                if self.restart and not self.closed:
                    self.result = 'restart_accepted'
                    self._start()
            else:
                self.state = self.result = 'failed'
                self.failure = reason
                self.exit_code = code
                self.record('worker', reason, exit_code=code)
            self.restart = False

    def _update(self, line: bytes, reason: str) -> str:
        try:
            event = json.loads(line)
        except (ValueError, UnicodeError):
            return reason
        if not isinstance(event, dict):
            return reason
        with self.lock:
            if event.get('ready') is True and self.state == 'starting':
                self.state = self.result = 'running'
                self.record('worker', 'running')
            if isinstance(event.get('source'), str) and type(event.get('ok')) is bool:
                self.sources[event['source']] = (event['ok'], utc_now())
        failure = event.get('failure')
        return failure if isinstance(failure, str) and failure in FAILURES else reason

    def overview(self, channels: list[str]) -> dict[str, Any]:
        with self.lock:
            active = self.state == 'running'
            now = utc_now()
            sources = {}
            for name in channels:
                seen = self.sources.get(name)
                sources[name] = ('unknown' if not active or seen is None or now - seen[1] > SOURCE_FRESH_FOR
                                 else 'connected' if seen[0] else 'unavailable')
            broker = broker_status(self.directory / 'publications.sqlite3', self.profile,
                                   self.run_id if active else None)
            return self.profile | {
                'worker': self.state, 'failure': self.failure, 'command': self.result,
                'exit_code': self.exit_code, 'allowed': self._allowed(),
                'journal_error': self.journal_error, 'sources': sources, 'broker': broker,
            }

    def close(self) -> None:
        with self.lock:
            self.closed = True
            if self.state in ('starting', 'running'):
                self.record('stop', 'panel_shutdown')
                self._stop()
            thread = self.thread
        if thread is not None:
            thread.join()
