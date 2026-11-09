import argparse
from io import TextIOWrapper
import json
import logging
from pathlib import Path
import sqlite3
import sys
from typing import cast
from uuid import uuid4

from news_futures.credentials import CredentialsError
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
    route_command = commands.add_parser('route', help='Привязки публикации к компаниям через Jev')
    route_command.add_argument('directory', nargs='?', type=Path)
    route_command.add_argument('--publication', type=Path, required=True)
    route_command.add_argument('--catalog', type=Path, required=True)
    route_command.add_argument('--config', type=Path, default=Path(__file__).with_name('jev-model.json'))
    login_command = commands.add_parser('telegram-login', help='Локальная пользовательская авторизация')
    login_command.add_argument('--session', type=Path)
    telegram_command = commands.add_parser('telegram', help='Непрерывный приём Telegram, обработка — имитация')
    telegram_command.add_argument('--session', type=Path)
    telegram_command.add_argument('--config', type=Path, default=Path(__file__).with_name('telegram-sources.json'))
    telegram_command.add_argument('--database', type=Path, default=Path('.local/telegram/archive.sqlite3'))
    telegram_command.add_argument('--once', action='store_true', help='История и все доступные изменения, затем выход')
    archive_command = commands.add_parser('publications', help='Прочитать архив, редакции и статусы')
    archive_command.add_argument('--database', type=Path, default=Path('.local/telegram/archive.sqlite3'))
    match_command = commands.add_parser('event-match', help='Сопоставить редакцию из трассы route с событием')
    match_command.add_argument('directory', type=Path)
    match_command.add_argument('--config', type=Path, default=Path(__file__).with_name('event-model.json'))
    events_command = commands.add_parser('events', help='Показать историю событий и контекст компаний')
    events_command.add_argument('--company')
    correct_command = commands.add_parser('event-correct', help='Записать исправление сопоставления')
    correct_command.add_argument('revision')
    correct_command.add_argument('--kind', choices=('separate', 'repeat', 'update'), required=True)
    correct_command.add_argument('--event')
    correct_command.add_argument('--reason', required=True)
    decision_command = commands.add_parser('analyze-event', help='Проанализировать запрос из архива событий')
    decision_command.add_argument('decision_id', type=int)
    decision_command.add_argument('--snapshot', type=Path, required=True)
    decision_command.add_argument('--directory', type=Path)
    decision_command.add_argument('--config', type=Path, default=Path(__file__).with_name('openrouter-models.json'))
    reason_command = commands.add_parser('entry-reason', help='Сохранить исходную причину входа в позицию')
    reason_command.add_argument('record', type=Path)
    trades_command = commands.add_parser('sandbox-trades', help='Показать журнал сделок в песочнице')
    serve_command = commands.add_parser('serve', help='Непрерывный цикл от новости до решения')
    serve_command.add_argument('config', type=Path)
    serve_command.add_argument('--once', action='store_true', help='Получить доступные новости и обработать очередь')
    status_command = commands.add_parser('runtime-status', help='Состояние рабочего запуска и решения')
    status_command.add_argument('config', type=Path)
    status_command.add_argument('--decision')
    trade_command = commands.add_parser('sandbox-trade', help='Вход и сверка выбранного счёта Т-Инвестиций в песочнице')
    trade_command.add_argument('action', choices=('start', 'reconcile', 'close'))
    trade_command.add_argument('--account', required=True)
    trade_command.add_argument('--contract', required=True)
    trade_command.add_argument('--company')
    trade_command.add_argument('--approval', type=Path)
    trade_command.add_argument('--publications', type=Path, default=Path('.local/telegram/archive.sqlite3'))
    trade_command.add_argument('--ca-file', type=Path)
    trade_command.add_argument('--allow-independent-stops', action='store_true')
    for command in (match_command, events_command, correct_command, decision_command, reason_command,
                    trades_command, trade_command):
        command.add_argument('--database', type=Path, default=Path('.local/events.sqlite3'))
    args = parser.parse_args()
    try:
        if args.command in {'serve', 'runtime-status'}:
            try:
                from news_futures.runtime import RuntimeErrorInfo, run_runtime, runtime_report
            except ImportError:
                print('Установите зависимости по docs/runtime.md.', file=sys.stderr)
                return 1
            try:
                if args.command == 'runtime-status':
                    print(json.dumps(runtime_report(args.config, args.decision), ensure_ascii=False, indent=2))
                    return 0
                import asyncio
                return asyncio.run(run_runtime(args.config, args.once))
            except KeyboardInterrupt:
                return 130
            except (RuntimeErrorInfo, CredentialsError) as error:
                print(f'{error} См. docs/runtime.md.', file=sys.stderr)
                return 1
            except Exception as error:
                print(f'Рабочий цикл: {type(error).__name__}. Проверьте подключение и docs/runtime.md.',
                      file=sys.stderr)
                return 1
        if args.command == 'sandbox-diagnostics':
            previous_logging = logging.root.manager.disable
            # MCP may log raw server payloads, including echoed credentials.
            logging.disable(logging.CRITICAL)
            try:
                import asyncio
                from news_futures.sandbox import run_sandbox
                directory = args.directory or Path('.local/news-futures-04') / str(uuid4())
                report = asyncio.run(run_sandbox(args.config, directory))
                print(json.dumps(report, ensure_ascii=False, indent=2))
                return 0 if report['status'] == 'data_confirmed' else 1
            except CredentialsError:
                raise
            except Exception:
                print('Sandbox: подключение или настройки не подтверждены. См. docs/sandbox.md.', file=sys.stderr)
                return 1
            finally:
                logging.disable(previous_logging)
        if args.command == 'route':
            try:
                from news_futures.jev import route_publication
                directory = args.directory or Path('.local/news-futures-05') / str(uuid4())
                trace = route_publication(args.config, args.publication, args.catalog, directory)
                print(json.dumps({
                    'status': trace['status'], 'error': trace['error'],
                    'trace_file': str((directory / 'trace.json').resolve()),
                }, ensure_ascii=False, indent=2))
                return 0 if trace['status'] == 'completed' else 1
            except Exception as error:
                print(f'Jev: {type(error).__name__}. См. docs/jev.md.', file=sys.stderr)
                return 1
        if args.command in {'analyze', 'analyze-event'}:
            try:
                if args.command == 'analyze-event':
                    from news_futures.decisions import analyze_event
                    from news_futures.events import EventArchive
                    if not args.database.is_file():
                        raise ValueError('Архив событий не найден')
                    directory = args.directory or Path('.local/news-futures-07') / str(uuid4())
                    with EventArchive(args.database) as events:
                        trace = analyze_event(events, args.decision_id, args.snapshot, args.config, directory)
                else:
                    from news_futures.analysis import run_cycle
                    directory = args.directory or Path('.local/news-futures-03') / str(uuid4())
                    trace = run_cycle(args.config, directory, context_path=args.context)
                if trace['error'] == 'CredentialsError':
                    print('Заполните openrouter.api_key в .local/credentials.toml.', file=sys.stderr)
                print(json.dumps({
                    'mode': trace['mode'], 'status': trace['status'],
                    'directory': str(directory.resolve()),
                    'trace_file': str((directory / 'trace.json').resolve()),
                }, ensure_ascii=False, indent=2))
                return 1 if trace['status'] == 'rejected' else 0
            except Exception as error:
                if args.command == 'analyze-event':
                    detail = str(error) if type(error) is ValueError else type(error).__name__
                    print(f'Анализ события: {detail}. См. docs/company-decisions.md.', file=sys.stderr)
                    return 1
                print(f'OpenRouter: {type(error).__name__}. См. локальную инструкцию README.', file=sys.stderr)
                return 1
        if args.command == 'entry-reason':
            from news_futures.decisions import EntryReason, record_entry_reason
            from news_futures.events import EventArchive
            try:
                entry = EntryReason.model_validate_json(args.record.read_bytes())
            except ValueError as error:
                raise ValueError('Проверьте формат причины входа в docs/company-decisions.md') from error
            try:
                with EventArchive(args.database) as events:
                    record_entry_reason(events, entry)
            except sqlite3.DatabaseError as error:
                raise ValueError('Не удалось сохранить причину входа в базе событий') from error
            print('Причина входа сохранена.')
            return 0
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
            except CredentialsError:
                raise
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
        if args.command == 'sandbox-trades':
            from news_futures.events import EventArchive
            from news_futures.sandbox_execution import trade_journal
            if not args.database.is_file():
                raise ValueError('Архив событий не найден')
            with EventArchive(args.database) as events:
                print(json.dumps(trade_journal(events), ensure_ascii=False, indent=2))
            return 0
        if args.command == 'sandbox-trade':
            from news_futures.tbank_execution import run_sandbox_trade
            if not args.database.is_file():
                raise ValueError('Архив событий не найден')
            if args.action == 'start':
                if not args.allow_independent_stops:
                    raise ValueError('Для входа требуется --allow-independent-stops')
                if args.approval is None or not args.company or not args.publications.is_file():
                    raise ValueError('Укажите --approval, --company и существующий архив --publications')
            elif args.approval is not None or args.company is not None:
                raise ValueError('--approval и --company используются только для start')
            try:
                result = run_sandbox_trade(
                    account_id=args.account, contract_id=args.contract, company_id=args.company,
                    database=args.database, ca_file=args.ca_file, approval=args.approval,
                    publications=args.publications, close=args.action == 'close',
                    allow_independent_stops=args.allow_independent_stops,
                )
            except CredentialsError:
                raise
            except Exception:
                print('Sandbox: подключение или настройки не подтверждены. См. docs/sandbox-execution.md.',
                      file=sys.stderr)
                return 1
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 1 if result['status'] in ('blocked', 'rejected') else 0
        if args.command in {'event-match', 'events', 'event-correct'}:
            from news_futures.events import EventArchive, match_route
            if args.command != 'event-match' and not args.database.is_file():
                raise ValueError('Архив событий не найден')
            with EventArchive(args.database) as events:
                if args.command == 'event-match':
                    event_result = match_route(events, args.directory, args.config)
                elif args.command == 'events':
                    event_result = events.report(args.company)
                else:
                    event_result = events.correct(args.revision, args.kind, args.event, args.reason)
            print(json.dumps(event_result, ensure_ascii=False, indent=2))
            return 1 if event_result.get('kind') == 'model_error' else 0
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
