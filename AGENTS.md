## Commits

Use Conventional Commits: `<type>[optional scope][!]: <description>`
(for example, `feat: add position tracking` or `fix(wallet): handle expired sessions`).

## Pre-production compatibility

Use the current schema and interfaces. When replacing a type, path, or format,
update its callers and delete obsolete readers, aliases, migrations, tests, and
documentation. Add backward compatibility only when explicitly requested.

## Agent skills

### AI agent design

For any task that touches AI agents, read and apply the `ai-agent-design` skill
at `~/.agents/skills/ai-agent-design/SKILL.md` before proceeding.

### Issue tracker

Track issues and specs in `.scratch/<feature>/`. See `docs/agents/issue-tracker.md`.

### Triage labels

Use the five standard triage roles. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout. Before exploring the codebase, read `docs/agents/domain.md`.
