"""Groq Chat Completions through the OpenAI-compatible LangChain client."""

from typing import Any

from langchain_core.outputs import ChatResult
from langchain_openai import ChatOpenAI
from openai import BaseModel


class GroqChat(ChatOpenAI):
    def _create_chat_result(
        self, response: dict[str, Any] | BaseModel,
        generation_info: dict[str, Any] | None = None,
    ) -> ChatResult:
        result = super()._create_chat_result(response, generation_info)
        if result.llm_output is not None:
            data = response if isinstance(response, dict) else response.model_dump(warnings=False)
            result.llm_output.update(model_provider='groq', provider='Groq', model_name=data.get('model'))
        return result
