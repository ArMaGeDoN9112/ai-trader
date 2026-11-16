"""Hold an exclusive operation lock without locking the journal connection."""

from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path
import sqlite3


@contextmanager
def exclusive_operation(path: Path | str) -> Iterator[bool]:
    with closing(sqlite3.connect(path, timeout=0)) as lock:
        try:
            lock.execute('BEGIN IMMEDIATE')
        except sqlite3.OperationalError as error:
            if error.sqlite_errorcode != sqlite3.SQLITE_BUSY:
                raise
            yield False
        else:
            yield True
