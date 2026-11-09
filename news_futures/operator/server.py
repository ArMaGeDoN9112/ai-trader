"""Serve the operator panel over loopback HTTP."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from secrets import compare_digest, token_urlsafe
import sqlite3
import sys
from typing import Any
from urllib.parse import parse_qs

from news_futures.operator.news import catalog_series, news_view, trace_view, visible
from news_futures.operator.supervisor import Worker


PAGE = Path(__file__).with_name('page.html')


class OperatorHandler(BaseHTTPRequestHandler):
    timeout = 5
    server: 'OperatorServer'

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def reply(self, code: int, value: object, *, html: bool = False) -> None:
        body = (str(value) if html else json.dumps(value, ensure_ascii=False)).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', ('text/html' if html else 'application/json') + '; charset=utf-8')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Security-Policy',
                         f"default-src 'none'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; "
                         f"script-src 'nonce-{self.server.nonce}'; style-src 'unsafe-inline'")
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
            self.reply(200, self.server.page, html=True)
        elif path in ('/api/overview', '/api/news', '/api/trace'):
            if not self.authorized():
                self.reply(403, {'result': 'forbidden'})
                return
            worker = self.server.worker
            query = parse_qs(query_string)
            value: dict[str, Any] | None
            try:
                if path == '/api/overview':
                    value = worker.overview(self.server.channels)
                elif path == '/api/news':
                    value = visible(news_view(worker.directory, worker.profile, self.server.catalog,
                                              self.server.series, query.get('company', [''])[0]))
                else:
                    value = trace_view(worker.directory, worker.profile['account_id'], query.get('revision', [''])[0])
            except (sqlite3.Error, OSError, ValueError, KeyError, TypeError):
                self.reply(503, {'result': 'archive_unavailable'})
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
        else:
            self.reply(404, {'result': 'not_found'})


class OperatorServer(ThreadingHTTPServer):
    def __init__(self, worker: Worker, channels: list[str], port: int, catalog: dict[str, Any] | None = None,
                 series: dict[str, Any] | None = None):
        self.worker = worker
        self.channels = channels
        self.catalog = catalog or {}
        self.series = series or {}
        self.session = token_urlsafe(32)
        self.nonce = token_urlsafe(24)
        self.page = PAGE.read_text(encoding='utf-8').replace('__SESSION__', self.session).replace('__NONCE__', self.nonce)
        super().__init__(('127.0.0.1', port), OperatorHandler)
        self.host = f'127.0.0.1:{self.server_port}'

    def handle_error(self, request: Any, client_address: Any) -> None:
        pass


def serve_operator(config: Path, port: int) -> int:
    from news_futures.agents.routing import load_catalog
    from news_futures.application.config import load_settings
    from news_futures.trading.executor import AUTONOMOUS_PROTECTION_UNAVAILABLE
    from news_futures.adapters.telegram import configured_channels

    try:
        settings = load_settings(config)
        channels = configured_channels(settings.sources)
    except (OSError, ValueError, TypeError, KeyError):
        print('Проверьте общую конфигурацию и список источников. См. docs/runtime.md.', file=sys.stderr)
        return 1
    profile = settings.model_dump(include={'mode', 'account_id', 'contracts'})
    profile['entry_block'] = AUTONOMOUS_PROTECTION_UNAVAILABLE if settings.mode == 'sandbox' else None
    payload = json.dumps({'settings': settings.model_dump(mode='json'), 'channels': channels}).encode()
    worker = Worker(settings.data_dir, profile, [sys.executable, '-m', 'news_futures.operator.worker'], payload)
    catalog: dict[str, Any] = {}
    series: dict[str, Any] = {}
    try:
        content = settings.catalog.read_bytes()
        catalog = dict(load_catalog(content))
        series = catalog_series(json.loads(content))
    except (OSError, ValueError, KeyError, TypeError):
        pass
    try:
        with OperatorServer(worker, channels, port, catalog, series) as server:
            print(f'Панель оператора: http://{server.host}/', flush=True)
            server.serve_forever()
    except KeyboardInterrupt:
        return 0
    except OSError:
        print('Не удалось запустить панель. Проверьте локальный порт и файлы приложения.', file=sys.stderr)
        return 1
    finally:
        worker.close()
    return 0
