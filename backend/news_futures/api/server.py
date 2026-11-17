"""Serve the local operator API."""

from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import asynccontextmanager, contextmanager
import json
from pathlib import Path
import re
from secrets import compare_digest, token_urlsafe
from socket import create_server
import sys
from threading import Lock
from time import monotonic
from typing import Any
from urllib.parse import parse_qs

from anyio import fail_after
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
import httpx
from sqlalchemy.exc import SQLAlchemyError
from starlette.exceptions import HTTPException as StarletteHTTPException
from uvicorn import Config, Server

from news_futures.api.news import (
    artifact, catalog_series, enqueue_test_news, fields, news_view, testing_view, trace_view, visible,
)
from news_futures.api.settings import Preferences
from news_futures.api.supervisor import Worker
from news_futures.api.trades import trades_view
from news_futures.brokers.futures_catalog import active_futures
from news_futures.brokers.tbank import TBankReader, limit_broker_read, sandbox_client
from news_futures.common.errors import RuntimeErrorInfo
from news_futures.dataflows.publications import utc_now


class Operator:
    def __init__(self, worker: Worker, channels: list[str], catalog: dict[str, Any] | None = None,
                 series: dict[str, Any] | None = None, *,
                 share_companies: dict[str, str] | None = None, ca_file: Path | None = None,
                 frontend: Path | None = None):
        self.worker = worker
        self.worker.channels = channels
        self.catalog = catalog or {}
        self.series = series or {}
        self.share_companies = share_companies or {}
        self.ca_file = ca_file
        self.frontend = frontend.resolve() if frontend is not None else None
        self.market_lock = Lock()
        self.market_cache: dict[str, Any] = {}
        self.market_until = 0.0
        self.session = token_urlsafe(32)

    def account_balance(self) -> dict[str, Any]:
        with sandbox_client(self.ca_file, read_only=True) as client, limit_broker_read(4):
            equity = TBankReader(client, self.worker.profile['account_id'], '', None).account_equity()
        return {'equity': str(equity), 'checked_at': utc_now().isoformat()}

    def market_tickers(self) -> dict[str, Any]:
        with self.market_lock:
            if monotonic() < self.market_until:
                return self.market_cache
            catalog = artifact(self.worker.directory / 'futures-catalog' / 'catalog.json')
            if not catalog:
                return {'checked_at': None, 'tickers': []}
            with sandbox_client(self.ca_file, read_only=True) as client:
                tickers = active_futures(client, catalog)
            self.market_cache = {'checked_at': utc_now().isoformat(), 'tickers': [
                fields(item, 'uid', 'ticker', 'asset', 'expiration', 'price', 'quote_at') | {
                    'company_id': self.share_companies.get(item['share_id']),
                } for item in tickers
            ]}
            self.market_until = monotonic() + 25
            return self.market_cache


def create_app(operator: Operator, host: str) -> FastAPI:
    worker = operator.worker

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await run_in_threadpool(worker.close)

    app = FastAPI(lifespan=lifespan, openapi_url=None, redirect_slashes=False)

    def authorized(request: Request) -> bool:
        tokens = request.headers.getlist('X-Operator-Session')
        return len(tokens) == 1 and compare_digest(tokens[0].encode(), operator.session.encode())

    @app.middleware('http')
    async def protect(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        response: Response
        if (request.headers.getlist('Host') != [host]
                or (request.method == 'POST' and (
                    not authorized(request) or request.headers.getlist('Origin') != [f'http://{host}']
                ))):
            response = JSONResponse({'result': 'forbidden'}, status_code=403)
        else:
            response = await call_next(request)
        response.headers['Cache-Control'] = 'no-store'
        response.headers['Content-Security-Policy'] = (
            "default-src 'none'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; "
            "script-src 'self'; style-src 'self'; form-action 'self'"
        )
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Cross-Origin-Resource-Policy'] = 'same-origin'
        return response

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request: Request, error: StarletteHTTPException) -> JSONResponse:
        if error.status_code in (404, 405):
            return JSONResponse({'result': 'not_found'}, status_code=404)
        return JSONResponse({'result': error.detail}, status_code=error.status_code)

    def require_no_query(request: Request) -> None:
        if request.url.query:
            raise HTTPException(404)

    @app.get('/api/session', dependencies=[Depends(require_no_query)])
    def session() -> dict[str, str]:
        return {'session': operator.session}

    def require_session(request: Request) -> None:
        if not authorized(request):
            raise HTTPException(403, 'forbidden')

    reads = APIRouter(prefix='/api', dependencies=[Depends(require_session)])

    @contextmanager
    def read_errors(unavailable: str = 'archive_unavailable') -> Iterator[None]:
        try:
            yield
        except (SQLAlchemyError, httpx.HTTPError, OSError, ValueError, KeyError, TypeError) as error:
            raise HTTPException(503, unavailable) from error

    def query_value(request: Request, key: str) -> str:
        return parse_qs(request.url.query).get(key, [''])[0]

    @reads.get('/overview')
    def overview() -> JSONResponse:
        with read_errors():
            return JSONResponse(worker.overview())

    @reads.get('/account-balance')
    def account_balance() -> JSONResponse:
        with read_errors('balance_unavailable'):
            return JSONResponse(operator.account_balance())

    @reads.get('/tickers')
    def tickers() -> JSONResponse:
        with read_errors('market_unavailable'):
            return JSONResponse(operator.market_tickers())

    @reads.get('/news')
    def news(request: Request) -> JSONResponse:
        with read_errors():
            value = news_view(worker.directory, worker.profile, operator.catalog, operator.series,
                              query_value(request, 'company'))
            return JSONResponse(visible(value))

    @reads.get('/trace')
    def trace(request: Request) -> JSONResponse:
        with read_errors():
            value = trace_view(worker.directory, worker.profile['account_id'], query_value(request, 'revision'))
        if value is None:
            raise HTTPException(404)
        return JSONResponse(value)

    @reads.get('/trades')
    def trades(request: Request) -> JSONResponse:
        with read_errors():
            value = trades_view(worker.directory, worker.profile['account_id'], query_value(request, 'cycle'))
        if value is None:
            raise HTTPException(404)
        return JSONResponse(value)

    @reads.get('/testing')
    def testing() -> JSONResponse:
        with read_errors():
            return JSONResponse(visible(testing_view(worker.directory)))

    @reads.get('/controls')
    def controls() -> JSONResponse:
        with read_errors():
            return JSONResponse(worker.controls())

    app.include_router(reads)

    commands = APIRouter(prefix='/api', dependencies=[Depends(require_no_query)])

    def command_result(code: int, result: str) -> JSONResponse:
        return JSONResponse({'result': result}, status_code=code)

    @commands.post('/start')
    def start_worker() -> JSONResponse:
        return command_result(*worker.request('start'))

    @commands.post('/stop')
    def stop_worker() -> JSONResponse:
        return command_result(*worker.request('stop'))

    @commands.post('/restart')
    def restart_worker() -> JSONResponse:
        return command_result(*worker.request('restart'))

    async def read_json(request: Request) -> dict[str, Any]:
        lengths = request.headers.getlist('Content-Length')
        mime = request.headers.get('Content-Type', '').partition(';')[0].strip().lower()
        if (len(lengths) != 1 or not 0 < int(lengths[0]) <= 32768
                or 'Transfer-Encoding' in request.headers or mime != 'application/json'):
            raise ValueError
        with fail_after(5):
            body = json.loads(await request.body())
        if not isinstance(body, dict):
            raise ValueError
        return body

    async def configure_worker(action: str, request: Request) -> JSONResponse:
        try:
            body = await read_json(request)
            code, result = await run_in_threadpool(worker.configure, action, body)
        except (ValueError, TimeoutError) as error:
            raise HTTPException(400, 'invalid_request') from error
        except (OSError, SQLAlchemyError) as error:
            raise HTTPException(503, 'settings_unavailable') from error
        return command_result(code, result)

    @commands.post('/pause')
    async def pause(request: Request) -> JSONResponse:
        return await configure_worker('pause', request)

    @commands.post('/close')
    async def close_position(request: Request) -> JSONResponse:
        return await configure_worker('close', request)

    @commands.post('/settings')
    async def save_settings(request: Request) -> JSONResponse:
        return await configure_worker('settings', request)

    @commands.post('/testing')
    async def submit_test_news(request: Request) -> JSONResponse:
        try:
            body = await read_json(request)
            revision = await run_in_threadpool(enqueue_test_news, worker.directory, body)
        except (ValueError, TimeoutError) as error:
            raise HTTPException(400, 'invalid_test_news') from error
        except (OSError, SQLAlchemyError) as error:
            raise HTTPException(503, 'archive_unavailable') from error
        return JSONResponse({'result': 'test_news_queued', 'revision': revision}, status_code=202)

    app.include_router(commands)

    def frontend_file(path: str, mime: str) -> Response:
        root = operator.frontend
        if root is None:
            raise HTTPException(404)
        file = root / path
        if not file.resolve().is_relative_to(root):
            raise HTTPException(404)
        try:
            return Response(file.read_bytes(), media_type=mime)
        except FileNotFoundError as error:
            raise HTTPException(404) from error

    @app.get('/')
    def home() -> Response:
        return frontend_file('index.html', 'text/html')

    @app.get('/index.html')
    def index_html() -> Response:
        return frontend_file('index.html', 'text/html')

    @app.get('/assets/{name}')
    def asset_file(name: str) -> Response:
        if not re.fullmatch(r'[a-zA-Z0-9_.-]+\.(js|css)', name):
            raise HTTPException(404)
        mime = 'text/css' if name.endswith('.css') else 'text/javascript'
        return frontend_file(f'assets/{name}', mime)

    return app


def serve_operator(config: Path, port: int, bind: str, *, frontend: Path | None = None) -> int:
    from news_futures.dataflows.catalog import catalog_rows, load_catalog
    from news_futures.config import load_settings
    from news_futures.storage.migrate import require_migration
    from news_futures.storage.operator_state import OperatorState
    from news_futures.dataflows.telegram import configured_channels

    try:
        if frontend is not None and not (frontend / 'index.html').is_file():
            raise RuntimeErrorInfo('Frontend build not found. Run npm run build in frontend.')
        settings = load_settings(config)
        require_migration(settings.data_dir)
        channels = configured_channels(settings.sources)
        preferences = Preferences(settings, channels, OperatorState(settings.data_dir))
        profile, channels, payload = preferences.prepare()
    except RuntimeErrorInfo as error:
        print(error, file=sys.stderr)
        return 1
    except (OSError, ValueError, TypeError, KeyError, SQLAlchemyError):
        print('Проверьте общую конфигурацию и список источников. См. docs/runtime.md.', file=sys.stderr)
        return 1
    worker = Worker(settings.data_dir, profile, [sys.executable, '-m', 'news_futures.api.worker'],
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

    operator = Operator(worker, channels, catalog, series, share_companies=share_companies,
                        ca_file=settings.ca_file, frontend=frontend)
    try:
        with create_server((bind, port)) as listener:
            port = listener.getsockname()[1]
            app = create_app(operator, f'127.0.0.1:{port}')
            server = Server(Config(app, host=bind, port=port, access_log=False, proxy_headers=False, ws='none'))
            print(f'API оператора: {bind}:{port}', flush=True)
            server.run(sockets=[listener])
        return 0 if server.started else 1
    except KeyboardInterrupt:
        return 0
    except OSError:
        print('Не удалось запустить панель. Проверьте локальный порт и файлы приложения.', file=sys.stderr)
        return 1
