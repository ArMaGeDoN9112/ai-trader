"""Assemble the manual trading command for the selected T-Bank sandbox account."""

from contextlib import closing
from pathlib import Path

from news_futures.adapters.tbank.rest import TBankSandbox, sandbox_client
from news_futures.agents.events import EventArchive
from news_futures.news.publications import Archive
from news_futures.trading.executor import SandboxExecutor


def run_sandbox_trade(
    *, account_id: str, contract_id: str, company_id: str | None, database: Path,
    ca_file: Path | None, approval: Path | None, publications: Path, close: bool,
    allow_independent_stops: bool,
) -> dict[str, str]:
    with sandbox_client(ca_file) as client:
        broker = TBankSandbox(client, account_id, contract_id, company_id)
        with EventArchive(database) as events:
            executor = SandboxExecutor(events, broker)
            if approval is None:
                result = executor.reconcile(close=close, contract_id=contract_id)
                if result['status'] == 'idle':
                    broker.require_open_account()
                return result
            archive = Archive(publications)
            with closing(archive.db):
                return executor.start(archive, approval, allowed_contracts={contract_id},
                                      allow_independent_stops=allow_independent_stops)
