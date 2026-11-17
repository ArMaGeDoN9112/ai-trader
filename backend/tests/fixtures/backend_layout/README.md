Fixtures captured before the layout migration with code from commit
`3902b81aec6fd6d72b95ebfda3fd558341f67a62`, the installed PostgreSQL saver and
`tests.application.test_runtime.RuntimeAcceptance` against an isolated test database.
All model and broker responses are scripted; no external orders were submitted.

`fixed.json` stops at the second critic after one rejection and a revised proposal.
It contains serialized `news_futures.domain.company.CompanyContext`,
`CompanyProposal` and `news_futures.domain.contracts.Review`, including pending writes.
`tool_agent.json` stops at the first critic after market evidence and an analyst proposal.
This graph stores its typed agent state as JSON inside msgpack strings.

Each fixture contains the original PostgreSQL rows (`checkpoints`,
`checkpoint_blobs`, `checkpoint_writes`) and the frozen JSON artifacts for that
publication. Binary columns use base64. IDs, namespaces, versions, parent links,
task paths and write indices are kept as captured. Tests freeze the clock when
resuming the tool agent to preserve its original market freshness and deadline.
