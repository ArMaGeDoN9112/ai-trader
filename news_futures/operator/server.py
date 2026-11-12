"""Serve the operator panel over loopback HTTP."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import re
from secrets import compare_digest, token_urlsafe
import sqlite3
import sys
from threading import Lock
from time import monotonic
from typing import Any
from urllib.parse import parse_qs

import httpx

from news_futures.adapters.tbank.futures_catalog import active_futures
from news_futures.adapters.tbank.rest import sandbox_client
from news_futures.news.publications import utc_now
from news_futures.operator.news import (
    artifact, catalog_series, enqueue_test_news, fields, news_view, testing_view, trace_view, visible,
)
from news_futures.operator.settings import Preferences
from news_futures.operator.supervisor import Worker
from news_futures.operator.trades import trades_view


WEB = Path(__file__).with_name('web') / 'dist'
ASSET = re.compile(r'/assets/([\w-]+\.(js|css))', re.ASCII)


class OperatorHandler(BaseHTTPRequestHandler):
    timeout = 5
    server: 'OperatorServer'

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def reply(self, code: int, value: object, *, mime: str = 'application/json') -> None:
        body = value if isinstance(value, bytes) else (
            json.dumps(value, ensure_ascii=False) if mime == 'application/json' else str(value)
        ).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', mime + '; charset=utf-8')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Security-Policy',
                         "default-src 'none'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; "
                         "script-src 'self'; style-src 'self'; form-action 'self'")
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Cross-Origin-Resource-Policy', 'same-origin')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def local(self) -> bool:
        return self.headers.get_all('Host') == [self.server.host]

    def authorized(self) -> bool:
        tokens = self.headers.get_all('X-Operator-Session', [])
        return len(tokens) == 1 and compare_digest(tokens[0].encode(), self.server.session.encode())

    def do_GET(self) -> None:
        path, _, query_string = self.path.partition('?')
        if not self.local():
            self.reply(403, {'result': 'forbidden'})
        elif self.path == '/':
            self.reply(200, self.server.page, mime='text/html')
        elif asset := ASSET.fullmatch(path):
            body = self.server.assets.get(asset[1])
            if body is None:
                self.reply(404, {'result': 'not_found'})
            else:
                self.reply(200, body, mime={'js': 'text/javascript', 'css': 'text/css'}[asset[2]])
        elif path in ('/api/overview', '/api/news', '/api/trace', '/api/trades', '/api/controls', '/api/tickers', '/api/testing'):
            if not self.authorized():
                self.reply(403, {'result': 'forbidden'})
                return
            worker = self.server.worker
            query = parse_qs(query_string)
            value: dict[str, Any] | None
            try:
                if path == '/api/overview':
                    value = worker.overview()
                elif path == '/api/tickers':
                    value = self.server.market_tickers()
                elif path == '/api/news':
                    value = visible(news_view(worker.directory, worker.profile, self.server.catalog,
                                              self.server.series, query.get('company', [''])[0]))
                elif path == '/api/trace':
                    value = trace_view(worker.directory, worker.profile['account_id'], query.get('revision', [''])[0])
                elif path == '/api/trades':
                    value = trades_view(worker.directory, worker.profile['account_id'], query.get('cycle', [''])[0])
                elif path == '/api/testing':
                    value = visible(testing_view(worker.directory))
                else:
                    value = worker.controls()
            except (sqlite3.Error, httpx.HTTPError, OSError, ValueError, KeyError, TypeError):
                self.reply(503, {'result': 'market_unavailable' if path == '/api/tickers' else 'archive_unavailable'})
            else:
                if value is None:
                    self.reply(404, {'result': 'not_found'})
                else:
                    self.reply(200, value)
        else:
            self.reply(404, {'result': 'not_found'})

    def do_POST(self) -> None:
        if (not self.local() or not self.authorized()
                or self.headers.get_all('Origin') != [f'http://{self.server.host}']):
            self.reply(403, {'result': 'forbidden'})
        elif self.path in ('/api/start', '/api/stop', '/api/restart'):
            code, result = self.server.worker.request(self.path.removeprefix('/api/'))
            self.reply(code, {'result': result})
        elif self.path in ('/api/pause', '/api/close', '/api/settings', '/api/testing'):
            try:
                lengths = self.headers.get_all('Content-Length', [])
                if (len(lengths) != 1 or not 0 < int(lengths[0]) <= 32768
                        or self.headers.get_all('Transfer-Encoding')
                        or self.headers.get_content_type() != 'application/json'):
                    raise ValueError
                body = json.loads(self.rfile.read(int(lengths[0])))
                if not isinstance(body, dict):
                    raise ValueError
                if self.path == '/api/testing':
                    revision = enqueue_test_news(self.server.worker.directory, body)
                    self.reply(202, {'result': 'test_news_queued', 'revision': revision})
                    return
                code, result = self.server.worker.configure(self.path.removeprefix('/api/'), body)
            except (ValueError, TimeoutError):
                code, result = 400, 'invalid_test_news' if self.path == '/api/testing' else 'invalid_request'
            except (OSError, sqlite3.Error):
                code, result = 503, 'archive_unavailable' if self.path == '/api/testing' else 'settings_unavailable'
            self.reply(code, {'result': result})
        else:
            self.reply(404, {'result': 'not_found'})


class OperatorServer(ThreadingHTTPServer):
    def __init__(self, worker: Worker, channels: list[str], port: int, catalog: dict[str, Any] | None = None,
                 series: dict[str, Any] | None = None, *, web: Path = WEB,
                 share_companies: dict[str, str] | None = None, ca_file: Path | None = None):
        self.worker = worker
        self.worker.channels = channels
        self.catalog = catalog or {}
        self.series = series or {}
        self.share_companies = share_companies or {}
        self.ca_file = ca_file
        self.market_lock = Lock()
        self.market_cache: dict[str, Any] = {}
        self.market_until = 0.0
        self.session = token_urlsafe(32)
        self.page = (web / 'index.html').read_text(encoding='utf-8').replace('__SESSION__', self.session)
        self.assets = {file.name: file.read_bytes() for file in (web / 'assets').iterdir()
                       if ASSET.fullmatch('/assets/' + file.name)}
        super().__init__(('127.0.0.1', port), OperatorHandler)
        self.host = f'127.0.0.1:{self.server_port}'

    def market_tickers(self) -> dict[str, Any]:
        with self.market_lock:
            if monotonic() < self.market_until:
                return self.market_cache
            catalog = artifact(self.worker.directory / 'futures-catalog' / 'catalog.json')
            if not catalog:
                return {'checked_at': None, 'tickers': []}
            with sandbox_client(self.ca_file, read_only=True) as client:
                tickers = active_futures(client, catalog)
            owners = self.worker.profile['contracts']
            self.market_cache = {'checked_at': utc_now().isoformat(), 'tickers': [
                fields(item, 'uid', 'ticker', 'asset', 'expiration', 'price', 'quote_at') | {
                    'company_id': self.share_companies.get(item['share_id']) or owners.get(item['uid']),
                } for item in tickers
            ]}
            self.market_until = monotonic() + 25
            return self.market_cache

    def handle_error(self, request: Any, client_address: Any) -> None:
        pass


def serve_operator(config: Path, port: int) -> int:
    from news_futures.agents.routing import catalog_rows, load_catalog
    from news_futures.application.config import load_settings
    from news_futures.application.operator_state import OperatorState
    from news_futures.adapters.telegram import configured_channels

    try:
        settings = load_settings(config)
        channels = configured_channels(settings.sources)
        preferences = Preferences(settings, channels, OperatorState(settings.data_dir))
        profile, channels, payload = preferences.prepare(check_cycles=False)
    except (OSError, ValueError, TypeError, KeyError, sqlite3.Error):
        print('Проверьте общую конфигурацию и список источников. См. docs/runtime.md.', file=sys.stderr)
        return 1
    worker = Worker(settings.data_dir, profile, [sys.executable, '-m', 'news_futures.operator.worker'],
                    payload, preferences)
    catalog: dict[str, Any] = {}
    series: dict[str, Any] = {}
    share_companies: dict[str, str] = {}
    try:
        content = settings.catalog.read_bytes()
        catalog = dict(load_catalog(content))
        series = catalog_series(json.loads(content))
        share_companies = {row.share_id: row.company_id for row in catalog_rows(content) if row.share_id}
    except (OSError, ValueError, KeyError, TypeError):
        pass
    try:
        with OperatorServer(worker, channels, port, catalog, series, share_companies=share_companies,
                            ca_file=settings.ca_file) as server:
            print(f'Панель оператора: http://{server.host}/', flush=True)
            server.serve_forever()
    except KeyboardInterrupt:
        return 0
    except FileNotFoundError:
        print('Соберите панель командой npm --prefix news_futures/operator/web run build.', file=sys.stderr)
        return 1
    except OSError:
        print('Не удалось запустить панель. Проверьте локальный порт и файлы приложения.', file=sys.stderr)
        return 1
    finally:
        worker.close()
    return 0
