import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import httpx
from langchain_openrouter import ChatOpenRouter
from openrouter import OpenRouter
from pydantic import SecretStr

from news_futures.llm_clients.chat import chat_model
from news_futures.llm_clients.openrouter import OpenRouterChat
from news_futures.schemas.contracts import RoleSettings
from tests.support.models import ScriptedEndpoint


PROPOSAL = {
    'action': 'enter', 'publication_id': 'control-source:publication-001',
    'company_id': 'control-company', 'contract_id': 'control-future-2026-12',
    'side': 'long', 'entry_price': 300.0, 'stop_loss': 290.0,
    'take_profit': 320.0, 'reason': 'Profit growth supports the control company.',
}


class OpenRouterTest(unittest.TestCase):
    def test_production_client_uses_only_openrouter_key_and_strict_provider(self) -> None:
        credentials = Path(self.enterContext(tempfile.TemporaryDirectory())) / 'credentials.toml'
        credentials.write_text('[openrouter]\napi_key = "local-test-secret"\n', encoding='utf-8')
        with patch('news_futures.credentials.CREDENTIALS_PATH', credentials), patch.dict(os.environ, {
                                     'OPENROUTER_API_KEY': 'wrong-openrouter-key',
                                     'OPENAI_API_KEY': 'wrong-key',
                                     'OPENAI_BASE_URL': 'https://example.invalid'}):
            model = chat_model(RoleSettings(model='test/analyst'))
        self.assertEqual(str(model.openai_api_base), 'https://openrouter.ai/api/v1')
        assert isinstance(model.openai_api_key, SecretStr)
        self.assertEqual(model.openai_api_key.get_secret_value(), 'local-test-secret')
        self.assertIsNotNone(model.extra_body)
        self.assertTrue(model.extra_body and model.extra_body['provider']['require_parameters'])
        self.assertEqual(model.max_retries, 0)

    def test_upstream_openrouter_sdk_drops_provider_metadata(self) -> None:
        endpoint = ScriptedEndpoint([PROPOSAL])
        sdk = OpenRouter(api_key='local-test-secret',
                         client=httpx.Client(transport=httpx.MockTransport(endpoint.respond)),
                         async_client=httpx.AsyncClient())
        model = ChatOpenRouter(model='test/analyst', api_key=SecretStr('local-test-secret'), client=sdk)
        response = model.invoke('control')
        self.assertNotIn('provider', response.response_metadata)


if __name__ == '__main__':
    unittest.main()
