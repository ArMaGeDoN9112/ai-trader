# OpenRouter integration evidence — ticket 03

Verified locally on 2026-11-06 with Python 3.11. This documents the integration
exception allowed by `agent-stack.md`; it does not change the workflow or authority
boundary in ADR 0001/0002.

## Reproduced gap

The documented default is `ChatOpenRouter`, `langchain-openrouter==0.2.9` with
`openrouter==0.11.46`. Its structured invocation supports
`method='json_schema', strict=True, include_raw=True` and provider routing with
`require_parameters=True`.

However, the OpenRouter SDK's `ChatResult` schema omits the top-level `provider`
field. Its generated serializer drops that field from an otherwise valid HTTP
response. The integration then receives no provider to copy into AIMessage.
`test_upstream_openrouter_sdk_drops_provider_metadata` reproduces this with the
real SDK and an httpx MockTransport response containing `provider: TestProvider`.
This fails ticket 03's requirement to retain available provider information.

## Fixed compatibility path

Ticket 03 therefore uses `langchain-openai==1.7.4`, `openai==3.46.0` against the
fixed OpenRouter Chat Completions URL. `OpenRouterChat` extends only response
metadata extraction: reported model, provider, cost, cost details and router
metadata. Chat invocation, messages, structured output, HTTP transport, timeout
and parsing stay within LangChain and its SDK. This is not a separate model runtime.

The same controlled HTTP test verifies strict JSON Schema, required provider
parameter support, independently configured model IDs and retained provider/cost.
The compatibility path is fixed at development time: there is no runtime switch
to another model, endpoint, function calling or free text on failure.

Model IDs must be exact reported OpenRouter IDs. Missing or different reported
models are rejected, including an alias resolving to a different ID. Change the
configured ID explicitly rather than treating an alias as verified identity.
Only `OPENROUTER_API_KEY` supplies credentials. The base URL is explicit;
OpenAI key and endpoint environment variables do not redirect the client.

LangChain's generic JSON parser can repair incomplete text. Admission instead
validates the complete original content with Pydantic after checking the finish
reason and refusal flag. Invalid outputs are journalled and rejected.

## Sources

- [ChatOpenRouter integration](https://docs.langchain.com/oss/python/integrations/chat/openrouter)
- [OpenRouter structured outputs](https://openrouter.ai/docs/guides/features/structured-outputs)
- [ChatOpenAI API scope and configuration](https://docs.langchain.com/oss/python/integrations/chat/openai)
- [LangGraph StateGraph](https://docs.langchain.com/oss/python/langgraph/graph-api)

The HTTP tests are offline integration evidence, not live provider verification.
The real-model acceptance check remains pending until local access is configured.
