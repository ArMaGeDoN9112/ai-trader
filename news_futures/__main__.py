import argparse
from io import TextIOWrapper
import json
from pathlib import Path
import sys
from typing import cast
from uuid import uuid4

from news_futures.local import run


def main() -> int:
    parser = argparse.ArgumentParser(description='Локальная ИМИТАЦИЯ news-futures/01, без API и брокера.')
    commands = parser.add_subparsers(dest='command', required=True)
    run_command = commands.add_parser('run', help='Сохранить новый контрольный запуск')
    run_command.add_argument('directory', nargs='?', type=Path)
    show_command = commands.add_parser('show', help='Прочитать сохранённую трассу')
    show_command.add_argument('directory', type=Path)
    args = parser.parse_args()
    try:
        if args.command == 'run':
            directory = args.directory or Path('.local/news-futures') / str(uuid4())
            trace = run(directory)
            result = {
                'mode': 'simulation', 'status': trace['execution']['status'],
                'directory': str(directory.resolve()),
                'trace_file': str((directory / 'trace.json').resolve()),
            }
        else:
            result = json.loads((args.directory / 'trace.json').read_text(encoding='utf-8'))
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (OSError, ValueError) as error:
        print(f'Ошибка: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    cast(TextIOWrapper, sys.stdout).reconfigure(encoding='utf-8')
    cast(TextIOWrapper, sys.stderr).reconfigure(encoding='utf-8')
    raise SystemExit(main())
