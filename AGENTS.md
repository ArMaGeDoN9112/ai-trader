## Stack

Use Python for application code and tests. The user chose Python because it is
easier for them to read and maintain. Prefer the standard library outside the
documented agent framework and integration responsibilities.

Use LangGraph for real agent workflows, LangChain for model integration, and
Pydantic for model-facing schemas. Before implementing or changing agent
orchestration, model calls, persistence, or source/broker integrations, read
`docs/agents/agent-stack.md` for the required stack and responsibility boundaries.

Keep the completed ticket 01 control scenario runnable with the standard library
alone. Introduce framework dependencies in the tickets that need them.

## Commits

Use Conventional Commits: `<type>[optional scope][!]: <description>`
(for example, `feat: add position tracking` or `fix(wallet): handle expired sessions`).

## Agent skills

### Issue tracker

Track issues and specs in `.scratch/<feature>/`. See `docs/agents/issue-tracker.md`.

### Triage labels

Use the five standard triage roles. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout. Before exploring the codebase, read `docs/agents/domain.md`.
