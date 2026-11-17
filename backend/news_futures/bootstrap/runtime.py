"""Compose the runtime with PostgreSQL, Telegram, model clients and T-Bank."""

from collections.abc import Awaitable, Callable
from contextlib import ExitStack
from pathlib import Path
import json

import httpx

from news_futures.adapters.credentials import load_token
from news_futures.adapters.models.chat import chat_model
from news_futures.adapters.models.decisions import choices
from news_futures.adapters.postgres.locks import exclusive_operation
from news_futures.adapters.postgres.migrate import require_migration
from news_futures.adapters.postgres.runtime import PostgresRuntimeStorage
from news_futures.adapters.tbank.futures_catalog import fetch_catalog, save_catalog
from news_futures.adapters.tbank.rest import TBankReader, TBankSandbox, limit_broker_read, sandbox_client
from news_futures.adapters.telegram import configured_channels, default_session, run_telegram, session_path
from news_futures.application.config import RuntimeSettings, cycle_inputs, groq_models, load_settings
from news_futures.application.ports.broker import BrokerReader, SandboxBroker
from news_futures.application.ports.models import AnalysisModels
from news_futures.application.runtime import Runtime, run
from news_futures.common.errors import RuntimeErrorInfo, error_code
from news_futures.domain.publications import utc_now


async def run_runtime(
    config: Path | RuntimeSettings, once: bool, *,
    run_id: str | None = None, channels: list[str] | None = None,
    on_phase: Callable[[str], None] | None = None,
    on_source: Callable[[str, bool, int], None] | None = None,
    on_controls: Callable[..., None] | None = None,
) -> int:
    started_at = utc_now()
    settings = load_settings(config) if isinstance(config, Path) else config
    cycle_inputs(settings)
    groq_models(settings)
    try:
        source_channels = configured_channels(settings.sources) if channels is None else channels
        session = session_path(settings.session or default_session())
    except (OSError, ValueError, TypeError, KeyError) as error:
        raise RuntimeErrorInfo('Проверьте sources и внешний путь к session.') from error
    load_token('openrouter', 'api_key')
    if on_phase is not None:
        on_phase('directory')
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    use_mcp = settings.order_transport == 'mcp'
    if use_mcp:
        from news_futures.adapters.tbank.mcp_orders import TBankMCPReader, TBankMCPSandbox
    with ExitStack() as stack:
        if not stack.enter_context(exclusive_operation(settings.data_dir / 'runtime-lock')):
            if on_phase is not None:
                on_phase('directory_busy')
            raise RuntimeErrorInfo('serve уже работает с этим каталогом данных.')
        require_migration(settings.data_dir)
        if on_phase is not None:
            on_phase('broker')
        client = stack.enter_context(sandbox_client(settings.ca_file, read_only=use_mcp or settings.mode == 'observation'))

        def reader(contract: str, company: str | None) -> BrokerReader:
            adapter = TBankMCPReader if use_mcp else TBankReader
            return adapter(client, settings.account_id, contract, company)

        def broker(contract: str, company: str | None) -> SandboxBroker:
            if use_mcp:
                return TBankMCPSandbox(client, settings.account_id, contract, company, settings.ca_file)
            return TBankSandbox(client, settings.account_id, contract, company)

        runtime = Runtime(settings, reader, broker if settings.mode == 'sandbox' else None,
                          models=AnalysisModels(chat_model, choices), storage=PostgresRuntimeStorage(settings),
                          read_limit=limit_broker_read,
                          run_id=run_id, on_controls=on_controls, started_at=started_at)

        async def consume(process: Callable[[], Awaitable[None]]) -> int:
            return await run_telegram(
                source_channels, settings.publications, session, once, process=process,
                on_source=on_source, live_since=started_at,
            )

        def refresh_futures_catalog() -> None:
            try:
                save_catalog(settings.futures_catalog.parent, fetch_catalog(client, settings.futures_categories))
            except (httpx.HTTPError, OSError, ValueError, KeyError, TypeError) as error:
                print(json.dumps({'futures_catalog_error': error_code(error)}), flush=True)

        return await run(runtime, source_channels, once, consume=consume,
                         refresh_futures_catalog=refresh_futures_catalog, on_phase=on_phase)
