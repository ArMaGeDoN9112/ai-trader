from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from news_futures.credentials import CredentialsError, load_token


class CredentialsTest(unittest.TestCase):
    def test_invalid_credentials_report_only_safe_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'credentials.toml'
            with patch('news_futures.credentials.CREDENTIALS_PATH', path):
                with self.assertRaisesRegex(CredentialsError, 'credentials.example.toml'):
                    load_token('openrouter', 'api_key')
                for content in ('[openrouter]\napi_key = "secret-marker',
                                '[openrouter]\napi_key = "secret-marker invalid"'):
                    with self.subTest(content=content):
                        path.write_text(content, encoding='utf-8')
                        with self.assertRaises(CredentialsError) as failure:
                            load_token('openrouter', 'api_key')
                        self.assertNotIn('secret-marker', str(failure.exception))
