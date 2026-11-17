from hashlib import sha256
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from news_futures.api.trades import linked_revision
from news_futures.storage.database import schema_name
from news_futures.common.paths import local_path, persistent_path


class LocalModeTest(TestCase):
    def test_windows_workspace_and_compose_share_storage_paths(self) -> None:
        workspace = Path(__file__).resolve().parents[2]
        data = workspace / '.local' / 'runtime'
        revision = 'a' * 64
        approval = data / 'decisions' / revision / 'tool-agent'
        root = '/app/backend/.local/runtime'
        with patch.dict('os.environ', {'TRADER_CONTAINER_LOCAL': '/app/backend/.local'}):
            stored = persistent_path(approval)
            self.assertEqual(stored, f'{root}/decisions/{revision}/tool-agent')
            self.assertEqual(local_path(stored), approval)
            self.assertEqual(linked_revision(data, stored), revision)
            expected = sha256(f'{root}/publications'.encode()).hexdigest()[:32]
            self.assertEqual(schema_name(data / 'publications'), 'trader_' + expected)
