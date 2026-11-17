import ast
from importlib.util import resolve_name
from pathlib import Path
from unittest import TestCase


PACKAGE = Path(__file__).resolve().parents[1] / 'news_futures'


class ArchitectureTests(TestCase):
    def test_agents_cannot_import_order_clients(self) -> None:
        forbidden = {'news_futures.brokers.tbank', 'news_futures.brokers.mcp_orders',
                     'news_futures.trading.executor', 'news_futures.trading.sandbox'}
        for path in (PACKAGE / 'agents').glob('*.py'):
            for node in ast.walk(ast.parse(path.read_text(encoding='utf-8'))):
                modules = []
                if isinstance(node, ast.Import):
                    modules = [name.name for name in node.names]
                elif isinstance(node, ast.ImportFrom):
                    module = resolve_name('.' * node.level + (node.module or ''), 'news_futures.agents')
                    modules = [module, *(f'{module}.{name.name}' for name in node.names)]
                else:
                    continue
                with self.subTest(file=path.name, line=node.lineno):
                    self.assertFalse(forbidden.intersection(modules))

    def test_old_layers_are_removed(self) -> None:
        for name in ('application', 'domain', 'adapters', 'bootstrap'):
            self.assertFalse((PACKAGE / name).exists())
