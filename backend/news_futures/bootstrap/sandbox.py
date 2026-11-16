"""Assemble the manual trading command for the selected T-Bank sandbox account."""

from contextlib import closing
from pathlib import Path
from typing import Literal

from news_futures.adapters.sqlite.events import EventArchive
from news_futures.adapters.sqlite.publications import Archive
from news_futures.adapters.tbank.rest import TBankSandbox, sandbox_client
from news_futures.application.trading.executor import SandboxExecutor


def run_sandbox_trade(
    *, account_id: str, contract_id: str, company_id: str | None, database: Path,
    ca_file: Path | None, approval: Path | None, publications: Path, close: bool,
    allow_independent_stops: bool,
    transport: Literal['rest', 'mcp'] = 'rest',
) -> dict[str, str]:
    with sandbox_client(ca_file, read_only=transport == 'mcp') as client:
        broker: TBankSandbox | TBankMCPSandbox
        if transport == 'mcp':
            from news_futures.adapters.tbank.mcp_orders import TBankMCPSandbox
            broker = TBankMCPSandbox(client, account_id, contract_id, company_id, ca_file)
            if approval is not None:
                broker.prepare()
        else:
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
