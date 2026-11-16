"""Keep the inner layers independent of delivery and infrastructure adapters."""

import ast
from importlib.util import resolve_name
from pathlib import Path
from sys import stdlib_module_names
from unittest import TestCase


PACKAGE = Path(__file__).resolve().parents[1] / 'news_futures'


class ArchitectureTests(TestCase):
    def test_inner_layers_only_depend_inward(self) -> None:
        for layer in ('common', 'domain', 'application'):
            for path in (PACKAGE / layer).rglob('*.py'):
                package = '.'.join(('news_futures', *path.parent.relative_to(PACKAGE).parts))
                for node in ast.walk(ast.parse(path.read_text(encoding='utf-8'))):
                    imports: list[str] = []
                    if isinstance(node, ast.Import):
                        imports = [name.name for name in node.names]
                    elif isinstance(node, ast.ImportFrom):
                        module = resolve_name('.' * node.level + (node.module or ''), package)
                        imports = [f'{module}.{name.name}' for name in node.names]
                    for module in imports:
                        with self.subTest(file=str(path.relative_to(PACKAGE)), module=module):
                            self.assert_allowed(layer, module)

    def assert_allowed(self, layer: str, module: str) -> None:
        root, _, rest = module.partition('.')
        if root == 'news_futures':
            inward = {'common': {'common'}, 'domain': {'common', 'domain'},
                      'application': {'common', 'domain', 'application'}}
            self.assertIn(rest.partition('.')[0], inward[layer])
        elif layer != 'application':
            self.assertIn(root, stdlib_module_names | {'pydantic', '__future__'})
            self.assertNotIn(root, {'sqlite3', 'http', 'urllib', 'socket'})
        else:
            self.assertNotIn(root, {'sqlite3', 'httpx', 'telethon', 'openai', 'mcp'})
            self.assertFalse(module.startswith('langgraph.checkpoint.sqlite'))
