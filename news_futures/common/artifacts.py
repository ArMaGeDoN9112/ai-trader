"""Stable publication digests and atomic writes for local JSON artifacts."""

from hashlib import sha256
import json
from pathlib import Path
import sys
from time import monotonic, sleep
from typing import Any


def publication_digest(publication: dict[str, Any]) -> str:
    return sha256(json.dumps(publication, sort_keys=True, allow_nan=False).encode()).hexdigest()


def write_json(path: Path, value: object) -> None:
    with path.open('x', encoding='utf-8') as output:
        json.dump(value, output, ensure_ascii=False, indent=2, allow_nan=False)
        output.write('\n')


def replace_json(path: Path, value: object) -> None:
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + '\n', encoding='utf-8')
    move_artifact(temporary, path, replace=True)


def move_artifact(source: Path, target: Path, *, replace: bool = False) -> None:
    move = source.replace if replace else source.rename
    deadline = monotonic() + 0.5
    while True:
        try:
            move(target)
            return
        except PermissionError as error:
            # A Windows reader can briefly deny renaming an open artifact or its directory.
            if sys.platform != 'win32' or error.winerror not in (5, 32, 33) or monotonic() >= deadline:
                raise
            sleep(0.01)
