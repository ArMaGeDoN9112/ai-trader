import argparse
from io import TextIOWrapper
import json
from pathlib import Path
import sys
from typing import cast
from uuid import uuid4

from news_futures.local import run


def main() -> int:
    parser = argparse.ArgumentParser(description='news-futures: контрольные сценарии.')
    commands = parser.add_subparsers(dest='command', required=True)
    sandbox_command = commands.add_parser('sandbox-diagnostics', help='Диагностика sandbox MCP без заявок')
    sandbox_command.add_argument('directory', nargs='?', type=Path)
    sandbox_command.add_argument('--config', type=Path, default=Path(__file__).with_name('sandbox-settings.json'))
    run_command = commands.add_parser('run', help='Сохранить новый контрольный запуск')
    run_command.add_argument('directory', nargs='?', type=Path)
    show_command = commands.add_parser('show', help='Прочитать сохранённую трассу')
    show_command.add_argument('directory', type=Path)
    analyze_command = commands.add_parser('analyze', help='Аналитик и критик через OpenRouter, без брокера')
    analyze_command.add_argument('directory', nargs='?', type=Path)
    analyze_command.add_argument('--config', type=Path, default=Path(__file__).with_name('openrouter-models.json'))
    analyze_command.add_argument('--context', type=Path)
    login_command = commands.add_parser('telegram-login', help='Локальная пользовательская авторизация')
    login_command.add_argument('--session', type=Path)
    telegram_command = commands.add_parser('telegram', help='Непрерывный приём Telegram, обработка — имитация')
    telegram_command.add_argument('--session', type=Path)
    telegram_command.add_argument('--config', type=Path, default=Path(__file__).with_name('telegram-sources.json'))
    telegram_command.add_argument('--database', type=Path, default=Path('.local/telegram/archive.sqlite3'))
    telegram_command.add_argument('--once', action='store_true', help='История и все доступные изменения, затем выход')
    archive_command = commands.add_parser('publications', help='Прочитать архив, редакции и статусы')
    archive_command.add_argument('--database', type=Path, default=Path('.local/telegram/archive.sqlite3'))
    args = parser.parse_args()
    try:
        if args.command == 'sandbox-diagnostics':
            try:
                import asyncio
                from news_futures.sandbox import run_sandbox
                directory = args.directory or Path('.local/news-futures-04') / str(uuid4())
                report = asyncio.run(run_sandbox(args.config, directory))
                print(json.dumps(report, ensure_ascii=False, indent=2))
                return 0 if report['status'] == 'data_confirmed' else 1
            except Exception:
                print('Sandbox: подключение или настройки не подтверждены. См. docs/sandbox.md.', file=sys.stderr)
                return 1
        if args.command == 'analyze':
            try:
                from news_futures.analysis import run_cycle
                directory = args.directory or Path('.local/news-futures-03') / str(uuid4())
                trace = run_cycle(args.config, directory, context_path=args.context)
                print(json.dumps({
                    'mode': trace['mode'], 'status': trace['status'],
                    'directory': str(directory.resolve()),
                    'trace_file': str((directory / 'trace.json').resolve()),
                }, ensure_ascii=False, indent=2))
                return 1 if trace['status'] == 'rejected' else 0
            except Exception as error:
                print(f'OpenRouter: {type(error).__name__}. См. локальную инструкцию README.', file=sys.stderr)
                return 1
        if args.command in {'telegram', 'telegram-login'}:
            import asyncio
            from news_futures.telegram import default_session, login_telegram, run_telegram
            try:
                session = args.session or default_session()
                if args.command == 'telegram-login':
                    return asyncio.run(login_telegram(session))
                return asyncio.run(run_telegram(args.config, args.database, session, args.once))
            except KeyboardInterrupt:
                return 130
            except Exception as error:
                print(f'Telegram: {type(error).__name__}. См. локальную инструкцию README.', file=sys.stderr)
                return 1
        if args.command == 'publications':
            from news_futures.publications import Archive
            if not args.database.is_file():
                raise ValueError('Архив не найден')
            archive = Archive(args.database)
            try:
                print(json.dumps(archive.report(), ensure_ascii=False, indent=2))
            finally:
                archive.db.close()
            return 0
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
