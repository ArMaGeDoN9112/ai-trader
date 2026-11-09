"""Run the application in the child process owned by the operator panel."""

import asyncio
from contextlib import redirect_stderr, redirect_stdout
import json
import os
import sys
from threading import Thread


FAILURES = {'configuration', 'directory', 'directory_busy', 'broker', 'sources', 'worker'}


def worker_main(run_id: str) -> int:
    output = sys.stdout
    phase = 'configuration'

    def emit(**event: object) -> None:
        print(json.dumps(event), file=output, flush=True)

    def on_phase(value: str) -> None:
        nonlocal phase
        phase = value
        if value == 'running':
            emit(ready=True)

    async def run() -> int:
        from news_futures.application.runtime import run_runtime
        from news_futures.application.config import RuntimeSettings

        payload = json.loads(sys.stdin.buffer.readline())
        settings = RuntimeSettings.model_validate_json(json.dumps(payload['settings']))
        task = asyncio.create_task(run_runtime(
            settings, False, run_id=run_id, channels=payload['channels'], on_phase=on_phase,
            on_source=lambda name, ok: emit(source=name, ok=ok),
        ))
        loop = asyncio.get_running_loop()

        def wait_for_stop() -> None:
            # The control pipe closes on stop and if the panel process disappears.
            os.read(sys.stdin.fileno(), 1)
            try:
                loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:
                pass

        Thread(target=wait_for_stop, daemon=True).start()
        try:
            return await task
        except asyncio.CancelledError:
            return 0

    # Only the fixed event protocol reaches the panel. SDK diagnostics can contain secrets.
    with open(os.devnull, 'w', encoding='utf-8') as sink, redirect_stdout(sink), redirect_stderr(sink):
        try:
            code = asyncio.run(run())
        except Exception:
            code = 1
        if code or phase != 'running':
            emit(failure=phase if phase in FAILURES else 'worker')
        return code


if __name__ == '__main__':
    raise SystemExit(worker_main(sys.argv[1]))
