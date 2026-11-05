# Issue tracker: Local Markdown

Issues and specs live as Markdown files in `.scratch/`.

## Conventions

- One feature per directory: `.scratch/<feature-slug>/`.
- Spec: `.scratch/<feature-slug>/spec.md`.
- One ticket per file: `.scratch/<feature-slug>/issues/<NN>-<slug>.md`,
  numbered from `01`.
- Record triage state in a `Status:` line near the top.
  Use the roles in `triage-labels.md`.
- Append conversation history under `## Comments`.

## Publish to the issue tracker

Create the appropriate spec or ticket file using the conventions above.
Create its directory if needed.

## Fetch the relevant ticket

Read the file at the referenced path. Resolve a ticket number within
the relevant feature directory.

## Wayfinding operations

- Map: `.scratch/<effort>/map.md`, containing Notes, Decisions-so-far,
  and Fog sections.
- Child ticket: `.scratch/<effort>/issues/NN-<slug>.md`.
  Put the question in the body and record `Type:` as
  `research`, `prototype`, `grilling`, or `task`.
- Record blocking dependencies as `Blocked by: NN, NN`.
  A ticket is unblocked when all listed tickets are resolved.
- Frontier: select the lowest-numbered open, unblocked, unclaimed ticket.
- Claim: set `Status: claimed` and save before starting work.
- Resolve: append the answer under `## Answer`, set `Status: resolved`,
  and append a gist and link to the map's Decisions-so-far section.

Wayfinding uses `claimed` and `resolved` as workflow statuses;
triage uses the roles in `triage-labels.md`.
