import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import importlib.util
from io import StringIO
import json
import logging
from pathlib import Path
import tempfile
from typing import Any
import unittest
from unittest.mock import patch

if importlib.util.find_spec('mcp') is None:
    raise unittest.SkipTest('Install requirements-sandbox.txt for ticket 04 tests')

import httpx
from mcp import types

from news_futures.__main__ import main
from news_futures.sandbox import (
    Binding, CAPABILITIES, READS, SANDBOX_URL, Settings, diagnose, run_sandbox,
)


NOW = datetime(2026, 11, 6, 10, tzinfo=timezone.utc)
TOKEN = 'local-test-token-never-save'
SCHEMA: dict[str, Any] = {'type': 'object', 'properties': {}, 'additionalProperties': False}


def control_data() -> dict[str, Any]:
    return {
        'accounts': [{'id': 'sandbox-account', 'status': 'open'}],
        'account': {'id': 'sandbox-account', 'equity': '100000', 'currency': 'rub'},
        'positions': [], 'orders': [], 'stop_orders': [],
        'catalog': [{'company_id': 'issuer', 'company_name': 'Control company',
                     'share_id': 'share', 'ticker': 'TEST'}],
        'future': {'id': 'future', 'figi': 'control-figi', 'ticker': 'TESTZ6',
                   'underlying_share_id': 'share', 'expires_at': '2026-12-17T00:00:00Z',
                   'lot': 1, 'basic_asset_size': '100', 'currency': 'rub',
                   'price_unit': 'points', 'quantity_unit': 'lots'},
        'margin': {'contract_id': 'future', 'currency': 'rub', 'tick_size': '1',
                   'tick_value': '2', 'initial_margin_buy': '5000', 'initial_margin_sell': '5500'},
        'quote': {'contract_id': 'future', 'price': '300', 'price_unit': 'points',
                  'timestamp': NOW.isoformat()},
        'trading_status': {'contract_id': 'future', 'api_trade_available': True,
                           'buy_available': True, 'sell_available': True},
    }


class ControlSession:
    def __init__(self) -> None:
        self.data = control_data()
        self.tools = [types.Tool(
            name=f'control_{operation}', inputSchema=deepcopy(SCHEMA),
            description=f'CONTROL ONLY: {operation}',
            annotations=types.ToolAnnotations(readOnlyHint=operation in READS),
        ) for operation in (*READS, *CAPABILITIES)]
        self.calls: list[str] = []
        self.error: str | None = None
        self.text = False
        self.text_prefix = ''

    def settings(self) -> Settings:
        return Settings(environment='sandbox', endpoint=SANDBOX_URL, bindings={
            operation: Binding(tool=tool.name, input_schema=deepcopy(tool.inputSchema),
                               evidence={'/description': tool.description})
            for operation, tool in zip((*READS, *CAPABILITIES), self.tools)
        })

    async def list_tools(self, cursor: str | None = None) -> types.ListToolsResult:
        return types.ListToolsResult(tools=self.tools[:5] if cursor is None else self.tools[5:],
                                    nextCursor='second' if cursor is None else None)

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> types.CallToolResult:
        self.calls.append(name)
        if self.error:
            raise RuntimeError(self.error)
        operation = name.removeprefix('control_')
        if operation not in READS:
            raise AssertionError('WRITE WAS CALLED')
        data = self.data[operation]
        if self.text:
            return types.CallToolResult(content=[types.TextContent(
                type='text', text=self.text_prefix + json.dumps(data),
            )])
        # MCP structuredContent must be an object, so wrap list results.
        return types.CallToolResult(content=[], structuredContent={'data': data})


class SandboxTest(unittest.TestCase):
    def report(self, session: ControlSession, settings: Settings | None = None) -> dict[str, Any]:
        settings = settings or session.settings()
        if not session.text:
            for operation, binding in settings.bindings.items():
                if operation in READS:
                    binding.result_path = '/data'
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'diagnostic'
            report = asyncio.run(diagnose(session, settings, path, token=TOKEN, source='control', now=NOW))
            self.assertEqual(json.loads((path / 'diagnostics.json').read_text(encoding='utf-8')), report)
            self.assertEqual(len(json.loads((path / 'tools.json').read_text(encoding='utf-8'))), len(session.tools))
            for artifact in path.iterdir():
                self.assertNotIn(TOKEN, artifact.read_text(encoding='utf-8'))
            return report

    def test_control_confirms_separate_identities_and_units_without_writes(self) -> None:
        session = ControlSession()
        report = self.report(session)
        self.assertEqual(report['status'], 'data_confirmed')
        self.assertEqual(report['source'], 'control')
        self.assertEqual(report['contract'], {
            'confirmed': True, 'company_id': 'issuer', 'share_id': 'share', 'contract_id': 'future',
            'order_quantity_unit': 'lots', 'contracts_per_lot': 1,
            'price_unit': 'points', 'point_value': '2',
        })
        self.assertFalse(report['execution_enabled'])
        self.assertEqual(session.calls, [f'control_{operation}' for operation in READS])
        self.assertEqual(report['operations']['positions']['data'], [])
        self.assertTrue(all(item['availability'] == 'schema_reviewed_not_exercised'
                            for item in report['capabilities'].values()))
        for feature in ('oco', 'reduce_only', 'atomic_entry'):
            self.assertEqual(report[feature], 'not_verified')

    def test_no_bindings_reports_missing_capabilities_without_guessing(self) -> None:
        session = ControlSession()
        report = self.report(session, Settings(environment='sandbox', endpoint=SANDBOX_URL))
        self.assertEqual(report['status'], 'limited')
        self.assertFalse(report['contract']['confirmed'])
        self.assertEqual(session.calls, [])
        self.assertTrue(all(item['availability'] == 'unavailable'
                            for item in report['operations'].values()))

    def test_schema_drift_bad_arguments_or_unreviewed_reads_are_never_called(self) -> None:
        for problem in ('schema', 'arguments', 'evidence', 'write', 'missing'):
            with self.subTest(problem=problem):
                session = ControlSession()
                settings = session.settings()
                binding = settings.bindings['account']
                if problem == 'schema':
                    binding.input_schema = {'type': 'string'}
                elif problem == 'arguments':
                    binding.arguments = {'unexpected': TOKEN}
                elif problem == 'evidence':
                    binding.evidence = {}
                elif problem == 'write':
                    binding.tool = 'control_create_order'
                    binding.evidence = {'/description': 'CONTROL ONLY: create_order'}
                else:
                    binding.tool = 'nonexistent'
                report = self.report(session, settings)
                self.assertEqual(report['operations']['account']['availability'], 'unavailable')
                self.assertNotIn(binding.tool, session.calls)

    def test_invalid_units_unavailable_or_incomplete_contract_is_not_confirmed(self) -> None:
        changes: list[tuple[str, str, Any]] = [
            ('future', 'lot', 2), ('future', 'lot', True),
            ('future', 'quantity_unit', 'contracts'), ('future', 'price_unit', 'rub'),
            ('future', 'underlying_share_id', 'unknown'),
            ('future', 'expires_at', '2026-01-01T00:00:00Z'),
            ('margin', 'tick_size', '0'), ('margin', 'tick_value', 'NaN'),
            ('margin', 'initial_margin_buy', '-1'), ('margin', 'currency', 'usd'),
            ('quote', 'price', '300.5'), ('quote', 'contract_id', 'another-future'),
            ('quote', 'timestamp', '2026-11-06T09:58:59Z'),
            ('quote', 'timestamp', '2026-11-06T10:00:01Z'),
            ('quote', 'timestamp', '2026-11-06T10:00:00'),
            ('trading_status', 'api_trade_available', False),
            ('trading_status', 'sell_available', False),
        ]
        for operation, key, value in changes:
            with self.subTest(operation=operation, key=key, value=value):
                session = ControlSession()
                session.data[operation][key] = value
                report = self.report(session)
                self.assertEqual(report['status'], 'limited')
                self.assertFalse(report['contract']['confirmed'])
        session = ControlSession()
        del session.data['future']['figi']
        self.assertFalse(self.report(session)['contract']['confirmed'])

    def test_errors_and_credentials_are_not_archived(self) -> None:
        session = ControlSession()
        session.error = f'Authorization: Bearer {TOKEN}'
        report = self.report(session)
        self.assertEqual(report['status'], 'limited')
        session = ControlSession()
        session.data['account']['authorization'] = 'another-secret'
        session.data['account']['notice'] = f'Bearer {TOKEN}'
        session.tools[0].description = f'server echoed {TOKEN}'
        report = self.report(session)
        self.assertNotIn('another-secret', json.dumps(report))
        session.text = True
        session.text_prefix = ' \n\t'
        report = self.report(session)
        self.assertNotIn('another-secret', json.dumps(report))

    def test_json_text_results_and_explicit_field_mapping(self) -> None:
        session = ControlSession()
        session.text = True
        settings = session.settings()
        session.data['future']['raw_uid'] = session.data['future'].pop('id')
        settings.bindings['future'].fields = {
            **{key: f'/{key}' for key in session.data['future'] if key != 'raw_uid'}, 'id': '/raw_uid',
        }
        self.assertTrue(self.report(session, settings)['contract']['confirmed'])

    def test_broker_quotations_preserve_decimal_units_and_currency(self) -> None:
        session = ControlSession()
        session.data['quote']['price'] = {'units': '300', 'nano': 0}
        session.data['margin']['tick_size'] = {'units': '0', 'nano': 500000000}
        session.data['margin']['tick_value'] = {'units': '1', 'nano': 0, 'currency': 'rub'}
        session.data['margin']['initial_margin_buy'] = {'units': '5000', 'nano': 0, 'currency': 'rub'}
        self.assertEqual(self.report(session)['contract']['point_value'], '2')
        for amount in ({'units': '1', 'nano': -1, 'currency': 'rub'},
                       {'units': '1', 'nano': 1000000000, 'currency': 'rub'},
                       {'units': '1', 'nano': 0, 'currency': 'usd'}):
            session.data['margin']['tick_value'] = amount
            self.assertFalse(self.report(session)['contract']['confirmed'])

    def test_quote_received_during_reads_uses_completion_time(self) -> None:
        session = ControlSession()
        settings = session.settings()
        for operation in READS:
            settings.bindings[operation].result_path = '/data'
        session.data['quote']['timestamp'] = (NOW + timedelta(seconds=1)).isoformat()
        with tempfile.TemporaryDirectory() as temporary, patch(
            'news_futures.sandbox.datetime', wraps=datetime,
        ) as clock:
            clock.now.side_effect = [NOW, NOW + timedelta(seconds=2)]
            report = asyncio.run(diagnose(session, settings, Path(temporary) / 'report',
                                         token=TOKEN, source='control'))
        self.assertTrue(report['contract']['confirmed'])
        self.assertEqual(report['checked_at'], (NOW + timedelta(seconds=2)).isoformat())

    def test_existing_directory_is_not_overwritten(self) -> None:
        session = ControlSession()
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(FileExistsError):
                asyncio.run(diagnose(session, session.settings(), Path(temporary),
                                     token=TOKEN, source='control', now=NOW))
            self.assertEqual(session.calls, [])

    def test_sdk_transport_discovery_authentication_and_endpoint_guard(self) -> None:
        requests: list[httpx.Request] = []
        session = ControlSession()
        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            self.assertEqual(str(request.url), SANDBOX_URL)
            self.assertEqual(request.headers['Authorization'], f'Bearer {TOKEN}')
            if request.method == 'DELETE':
                return httpx.Response(200)
            message = json.loads(request.content)
            if 'id' not in message:
                return httpx.Response(202)
            if message['method'] == 'initialize':
                result: Any = {'protocolVersion': message['params']['protocolVersion'],
                               'capabilities': {'tools': {}},
                               'serverInfo': {'name': 'control', 'version': '1'}}
            elif message['method'] == 'tools/list':
                result = {'tools': [tool.model_dump(mode='json') for tool in session.tools]}
            else:
                raise AssertionError('Unbound tool was called')
            return httpx.Response(200, json={'jsonrpc': '2.0', 'id': message['id'], 'result': result})
        original_client = httpx.AsyncClient
        def client(**kwargs: Any) -> httpx.AsyncClient:
            self.assertFalse(kwargs['follow_redirects'])
            self.assertFalse(kwargs['trust_env'])
            return original_client(transport=httpx.MockTransport(handler), **kwargs)
        with tempfile.TemporaryDirectory() as temporary:
            config = Path(temporary) / 'config.json'
            config.write_text(Settings(environment='sandbox', endpoint=SANDBOX_URL).model_dump_json(), encoding='utf-8')
            credentials = Path(temporary) / 'credentials.toml'
            credentials.write_text(f'[sandbox]\ntoken = "{TOKEN}"\n', encoding='utf-8')
            with patch('news_futures.credentials.CREDENTIALS_PATH', credentials), patch(
                'news_futures.sandbox.httpx.AsyncClient', side_effect=client,
            ):
                report = asyncio.run(run_sandbox(config, Path(temporary) / 'report'))
            self.assertEqual(report['source'], 'sandbox_mcp')
            self.assertEqual(report['status'], 'limited')
            self.assertGreaterEqual(len(requests), 3)
            with patch('news_futures.sandbox.httpx.AsyncClient') as factory:
                for field, value in (('endpoint', 'https://invest-public-api.tbank.ru/mcp'),
                                     ('endpoint', SANDBOX_URL + '?token=bad'),
                                     ('environment', 'production')):
                    invalid = {'environment': 'sandbox', 'endpoint': SANDBOX_URL}
                    invalid[field] = value
                    config.write_text(json.dumps(invalid), encoding='utf-8')
                    with self.assertRaises(ValueError):
                        asyncio.run(run_sandbox(config, Path(temporary) / 'bad'))
                factory.assert_not_called()

    def test_cli_does_not_echo_configuration_or_authorization_errors(self) -> None:
        previous_logging = logging.root.manager.disable
        async def fail(*args: Any, **kwargs: Any) -> dict[str, Any]:
            logging.warning(TOKEN)
            logging.getLogger('mcp.client.streamable_http').error(TOKEN)
            raise RuntimeError(TOKEN)
        with patch('news_futures.sandbox.run_sandbox', side_effect=fail), patch(
            'sys.argv', ['news_futures', 'sandbox-diagnostics'],
        ), patch('sys.stderr', new_callable=StringIO) as error, patch(
            'sys.stdout', new_callable=StringIO,
        ) as output:
            self.assertEqual(main(), 1)
            self.assertNotIn(TOKEN, error.getvalue())
            self.assertEqual(output.getvalue(), '')
        self.assertEqual(logging.root.manager.disable, previous_logging)


if __name__ == '__main__':
    unittest.main()
