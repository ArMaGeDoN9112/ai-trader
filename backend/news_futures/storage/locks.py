"""Session locks held while a runtime or sandbox executor is active."""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from news_futures.storage.database import connect, lock_key


@contextmanager
def exclusive_operation(path: Path, operation: str = 'runtime') -> Iterator[bool]:
    with connect(path) as db:
        yield bool(db.exec_driver_sql('SELECT pg_try_advisory_lock(%s::bigint)',
                                     (lock_key(path, operation),)).scalar_one())
