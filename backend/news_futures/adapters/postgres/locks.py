"""Session locks held while a runtime or sandbox executor is active."""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from news_futures.adapters.postgres.database import connect, lock_key


@contextmanager
def exclusive_operation(path: Path, operation: str = 'runtime') -> Iterator[bool]:
    with connect(path) as db:
        row = db.execute('SELECT pg_try_advisory_lock(%s::bigint) AS acquired',
                         (lock_key(path, operation),)).fetchone()
        assert row is not None
        yield bool(row['acquired'])
