"""Compatibility integration: retain metadata dropped by ChatOpenRouter's SDK.

See docs/agents/openrouter-compatibility.md for the pinned upstream limitation.
"""

from typing import Any

from langchain_core.outputs import ChatResult
from langchain_openai import ChatOpenAI
from openai import BaseModel


class OpenRouterChat(ChatOpenAI):
    def _create_chat_result(
        self, response: dict[str, Any] | BaseModel,
        generation_info: dict[str, Any] | None = None,
    ) -> ChatResult:
        result = super()._create_chat_result(response, generation_info)
        data = response if isinstance(response, dict) else response.model_dump(warnings=False)
        for generation in result.generations:
            generation.message.response_metadata.update({
                'provider': data.get('provider'),
                'model_provider': 'openrouter',
                'model_name': data.get('model'),
                'cost': (data.get('usage') or {}).get('cost'),
                'cost_details': (data.get('usage') or {}).get('cost_details'),
                'openrouter_metadata': data.get('openrouter_metadata'),
            })
        if result.llm_output is not None:
            result.llm_output.update(model_provider='openrouter', model_name=data.get('model'))
        return result
