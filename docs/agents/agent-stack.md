# Agent stack

This is the implementation policy for the news-futures application. See the
[specification](../../.scratch/news-futures/spec.md) for the operator workflow,
application boundaries and trading rules.

## Application assembly

Keep one installable Python package and one runtime that the CLI configures.
Compose sources, persistence, models, market and broker access at startup.
Keep source-specific payloads, SDK types and CLI rendering at their boundaries.
Pass typed application data into the graph and keep execution behind a separate
interface. Reuse the same pipeline for observation and sandbox, with broker
write access available only to the sandbox executor in the supported deployment.
Preserve the existing diagnostics as independent ways to inspect integrations.

## Orchestration and contracts

| Technology | Responsibility |
| --- | --- |
| LangGraph StateGraph | Typed cycle state, routing, analyst/critic transitions and bounded revision. |
| LangChain Core | Messages, prompts, chat-model interfaces and structured model invocation. |
| Pydantic | Validation of model-facing proposals, reviews and external structured results. |
| langgraph-checkpoint-sqlite | Persistent graph checkpoints for durable recovery. |

Express the approved workflow as explicit nodes and conditional edges. Use the
approved three roles; a new node or model call does not require a new agent role.
Keep numerical risk checks and the execution lifecycle as ordinary Python code
with independent tests. A validated schema does not establish factual correctness
or permission to trade.

## Models and integrations

- For OpenRouter chat roles, use ChatOpenRouter from `langchain-openrouter`.
  Model IDs and supported options belong in per-role configuration. If the
  integration lacks a required capability, document the gap and test a compatible
  LangChain integration. The current application uses the tested
  [ticket 03 compatibility path](openrouter-compatibility.md) to retain
  OpenRouter provider metadata.
- Request the required structured schema explicitly, including the appropriate
  `json_schema` method and provider parameter support. Validate the result locally;
  errors or incompatible models produce no executable proposal. A free-text
  fallback is not an approved trading proposal.
- Jev uses OpenRouter's System One interface. Keep its typed decision adapter
  separate from chat models; invoke it from a graph node without pretending it
  implements a conversational model. Versions can change within that contract.
- Use Telethon for the approved MTProto source adapter. Additional channels are
  configuration; a website adapter emits the same publication contract.
- Use the official MCP Python SDK for MCP transport. Broker operations are invoked
  by the deterministic executor through reviewed tool bindings. A LangChain MCP
  adapter may expose necessary allowlisted read tools; broker write tools are not
  bound to a model's tool-calling loop.

Freeze model configuration for the complete analysis/review cycle and record
requested and actual models, available provider metadata and validated outputs.
Use the current official integration documentation when adding dependencies and
lock compatible versions at that time; upstream version floors are not our lock.

## Persistence and external effects

Graph checkpoints track a cycle; the publication archive, company context, reason
for entry and execution journal remain durable application records. A checkpointer
does not replace these records or the source's recovery state.

Use a persistent SQLite checkpointer for restart recovery, not an in-memory saver.
Assign a stable cycle identity. Resuming a graph can run a node again: persist
execution intent, use stable request identifiers, and reconcile the broker before
retrying an uncertain operation. Checkpoints do not make a broker request atomic
or guarantee exactly-once execution.

Preserve the authority boundary in
[ADR 0001](../adr/0001-separate-proposals-from-execution.md). Graph replay, retries
and model output cannot bypass risk checks, create a second position or turn an
unconfirmed stop into confirmed protection.

## Tests, observability and scope

Preserve ticket 01's offline standard-library command and tests. Introduce the
framework-backed path in the relevant later ticket, with fake models and broker
responses for repeatable tests. Test conditional transitions, bounded revision,
invalid output and resumed execution without duplicate broker effects.

Keep the required audit trail locally and make it inspectable from the CLI.
LangSmith is optional, explicitly enabled observability.
External traces must exclude secrets and sessions. Add vector databases, managed
agent hosting or additional agent runtimes only for a concrete requirement.

Upstream data vendors and CLI dependencies are examples for their own use cases;
our sources and broker come from the spec. Add dependencies when a ticket uses
them, rather than installing the full TradingAgents stack.

## Decision and evidence

- [ADR 0002](../adr/0002-langgraph-langchain-agent-workflows.md).
- [TradingAgents technology audit](../../.scratch/news-futures/tradingagents-stack.md).
- [LangGraph graph API](https://docs.langchain.com/oss/python/langgraph/graph-api).
- [LangGraph persistence](https://docs.langchain.com/oss/python/langgraph/persistence).
- [ChatOpenRouter](https://docs.langchain.com/oss/python/integrations/chat/openrouter).
