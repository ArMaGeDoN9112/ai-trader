import asyncio
from dataclasses import replace
from datetime import datetime, timedelta
import importlib.util
from io import StringIO
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
from typing import Any
import unittest
from unittest.mock import AsyncMock, patch

from news_futures.publications import Archive, Publication, utc_now
from news_futures.telegram import (
    Difference, Source, TelethonTransport, collect, configured_channels, session_path, watch_channel,
    run_telegram,
    open_client,
)


class ControlledTransport:
    def __init__(self) -> None:
        self.source = Source(123, 'example')
        now = utc_now()
        self.original = Publication(self.source.id, '1', 'https://t.me/example/1',
                                    'Историческая публикация', now - timedelta(days=1), now)
        self.messages = [self.original]
        self.differences: list[Difference] = []
        self.requests: list[int] = []
        self.windows: list[tuple[datetime, datetime]] = []
        self.pts = 10

    async def resolve(self, username: str) -> Source:
        if username == 'broken':
            raise OSError('sensitive phone api_hash session material')
        return self.source

    async def current_pts(self, source: Source) -> int:
        return self.pts

    async def history(self, source: Source, since: datetime, until: datetime) -> list[Publication]:
        self.windows.append((since, until))
        return self.messages

    async def difference(self, source: Source, pts: int) -> Difference:
        self.requests.append(pts)
        return self.differences.pop(0) if self.differences else Difference(pts, [])


class CollectionTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.archive = Archive(Path(self.temporary.name) / 'archive.sqlite3')
        self.addCleanup(self.archive.db.close)
        self.transport = ControlledTransport()

    async def test_seven_days_history_new_edit_repeated_delivery_and_restart(self) -> None:
        transport = self.transport
        await collect(transport, self.archive, transport.source)
        self.assertEqual(transport.windows[0][1] - transport.windows[0][0], timedelta(days=7))
        self.assertEqual(self.archive.report()['revisions'][0]['status'], 'context_only')
        now = utc_now()
        live = replace(transport.original, external_id='2', text='Новость', published_at=now)
        edit = replace(live, text='Исправление', edited_at=now, change='edit')
        transport.differences = [Difference(11, [live], final=False), Difference(12, [edit])]
        await collect(transport, self.archive, transport.source)
        self.archive.process_pending()
        transport.differences = [Difference(12, [replace(edit, received_at=utc_now())])]
        await collect(transport, self.archive, transport.source)
        self.assertEqual(len(transport.windows), 1)
        self.assertEqual(transport.requests, [10, 10, 11, 12])
        self.assertEqual(len(self.archive.report()['revisions']), 3)
        self.assertEqual([row['status'] for row in self.archive.report()['revisions']],
                         ['context_only', 'simulated', 'simulated'])

    async def test_failed_save_does_not_ack_and_other_source_continues(self) -> None:
        transport = self.transport
        await collect(transport, self.archive, transport.source)
        live = replace(transport.original, external_id='2', published_at=utc_now())
        transport.differences = [Difference(11, [live])]
        self.archive.db.executescript('''
            CREATE TRIGGER fail_one BEFORE INSERT ON channels WHEN NEW.source='telegram:123'
            BEGIN SELECT RAISE(ABORT, 'save failed'); END;
        ''')
        other = ControlledTransport()
        other.source = Source(456, 'second')
        other.messages = []
        with patch('sys.stderr', new_callable=StringIO) as errors, patch('sys.stdout', new_callable=StringIO):
            results = await asyncio.gather(
                watch_channel(transport, self.archive, 'example', True),
                watch_channel(other, self.archive, 'second', True),
            )
        self.assertEqual(results, [False, True])
        self.assertIn('IntegrityError', errors.getvalue())
        self.assertEqual(self.archive.cursor(transport.source.id)['pts'], 10)  # type: ignore[index]
        self.assertIsNotNone(self.archive.cursor(other.source.id))
        self.archive.db.execute('DROP TRIGGER fail_one')
        transport.differences = [Difference(11, [live])]
        await collect(transport, self.archive, transport.source)
        self.assertEqual(transport.requests[-1], 10)
        self.assertEqual(len(self.archive.report()['revisions']), 2)

    async def test_unavailable_history_is_error_not_success(self) -> None:
        with patch.object(self.transport, 'history', side_effect=OSError('unavailable')):
            with self.assertRaises(OSError):
                await collect(self.transport, self.archive, self.transport.source)
        self.assertIsNone(self.archive.cursor(self.transport.source.id))

    async def test_too_long_recovers_available_history_and_marks_lost_editions(self) -> None:
        transport = self.transport
        await collect(transport, self.archive, transport.source)
        transport.differences = [Difference(20, [], too_long=True)]
        transport.pts = 20
        live = replace(transport.original, external_id='2', published_at=utc_now())
        transport.messages = [live]
        await collect(transport, self.archive, transport.source)
        report = self.archive.report()
        self.assertIn('intermediate_editions_and_deleted_posts_unrecoverable',
                      [row['code'] for row in report['notices']])
        self.assertEqual(report['channels'][0]['pts'], 20)
        self.assertEqual(len(report['revisions']), 2)
        self.assertEqual(report['revisions'][-1]['historical'], 0)
        self.assertEqual(report['revisions'][-1]['status'], 'pending')
        self.assertEqual(transport.requests[-1], 20)
        self.assertEqual(transport.windows[-1][0], transport.windows[0][0])

    async def test_nonfinal_page_does_not_advance_history_recovery_time(self) -> None:
        transport = self.transport
        await collect(transport, self.archive, transport.source)
        before = dict(self.archive.cursor(transport.source.id))  # type: ignore[arg-type]
        transport.differences = [Difference(11, [], final=False)]
        original_difference = transport.difference

        async def fail_second(source: Source, pts: int) -> Difference:
            if pts == 11:
                raise OSError('disconnect')
            return await original_difference(source, pts)

        with patch.object(transport, 'difference', side_effect=fail_second):
            with self.assertRaises(OSError):
                await collect(transport, self.archive, transport.source)
        after = dict(self.archive.cursor(transport.source.id))  # type: ignore[arg-type]
        self.assertEqual(after['pts'], 11)
        self.assertEqual(after['checked_at'], before['checked_at'])

    async def test_gap_recovery_includes_posts_in_the_second_of_the_previous_poll(self) -> None:
        transport = self.transport
        poll = utc_now().replace(microsecond=500_000)
        with patch('news_futures.telegram.utc_now', return_value=poll):
            await collect(transport, self.archive, transport.source)
        incoming = replace(transport.original, external_id='2', published_at=poll.replace(microsecond=0))
        transport.messages.append(incoming)
        transport.pts = 20
        transport.differences = [Difference(20, [], too_long=True)]
        await collect(transport, self.archive, transport.source)
        self.assertEqual([row['historical'] for row in self.archive.report()['revisions']], [1, 0])

    async def test_error_messages_do_not_leak_exception_contents(self) -> None:
        with patch('sys.stderr', new_callable=StringIO) as errors:
            result = await watch_channel(self.transport, self.archive, 'broken', True)
        self.assertFalse(result)
        self.assertNotIn('sensitive', errors.getvalue() + json.dumps(self.archive.report()))
        self.assertIn('OSError', errors.getvalue())

    def test_config_only_adds_sources_and_rejects_invalid_names(self) -> None:
        path = Path(self.temporary.name) / 'sources.json'
        defaults = configured_channels(Path('news_futures/telegram-sources.json'))
        self.assertEqual(defaults, ['markettwits', 'rbc_news'])
        path.write_text(json.dumps({'channels': defaults + ['another_channel']}))
        self.assertEqual(len(configured_channels(path)), 3)
        for names in [[], ['rbc_news', 'RBC_NEWS'], ['https://t.me/rbc_news'], ['valid', 7]]:
            path.write_text(json.dumps({'channels': names}))
            with self.assertRaises(ValueError):
                configured_channels(path)

    def test_session_cannot_be_saved_in_repository(self) -> None:
        with self.assertRaises(ValueError):
            session_path(Path('.local/secret.session'))


@unittest.skipUnless(importlib.util.find_spec('telethon'), 'Install requirements-telegram.txt for SDK tests')
class TelethonAdapterTest(unittest.IsolatedAsyncioTestCase):
    async def test_login_requires_user_session_and_safe_local_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            credentials = Path(temporary) / 'credentials.toml'
            credentials.write_text(f'[telegram]\napi_id = 123\napi_hash = "{"0" * 32}"\n', encoding='utf-8')
            for authorized, bot, login in [(True, True, False), (False, False, False), (False, False, True)]:
                with self.subTest(authorized=authorized, bot=bot, login=login):
                    client = AsyncMock()
                    client.is_user_authorized.return_value = authorized
                    client.get_me.return_value = SimpleNamespace(bot=bot)
                    with (patch('telethon.TelegramClient', return_value=client) as factory,
                          patch('news_futures.credentials.CREDENTIALS_PATH', credentials),
                          patch('sys.stdin.isatty', return_value=False),
                          patch('getpass.getpass') as prompt):
                        with self.assertRaises(ValueError):
                            await open_client(Path(temporary) / 'user.session', login)
                        self.assertEqual(factory.call_args.args[1:], (123, '0' * 32))
                        client.disconnect.assert_awaited_once()
                        client.send_code_request.assert_not_awaited()
                        prompt.assert_not_called()

    async def test_once_command_wires_session_archive_and_resumes(self) -> None:
        transport = ControlledTransport()
        state = SimpleNamespace(pts=1, qts=2, seq=3, date=utc_now())
        client = AsyncMock(return_value=state)
        client.get_me.return_value = SimpleNamespace(id=42)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = root / 'sources.json'
            config.write_text(json.dumps({'channels': ['example']}))
            database = root / 'archive.sqlite3'
            with (patch('news_futures.telegram.open_client', return_value=client),
                  patch('news_futures.telegram.TelethonTransport', return_value=transport),
                  patch('sys.stdout', new_callable=StringIO)):
                self.assertEqual(await run_telegram(config, database, root / 'user.session', True), 0)
                self.assertEqual(await run_telegram(config, database, root / 'user.session', True), 0)
            archive = Archive(database)
            try:
                self.assertEqual(len(archive.report()['revisions']), 1)
                self.assertEqual(archive.report()['revisions'][0]['status'], 'context_only')
                saved = archive.db.execute('SELECT state FROM sessions').fetchone()
                self.assertEqual(json.loads(saved['state'])['qts'], 2)
                self.assertEqual(client.disconnect.await_count, 2)
                self.assertEqual(len(transport.windows), 1)
            finally:
                archive.db.close()

    async def test_stop_waits_for_the_queue_consumer_to_finish_draining(self) -> None:
        client = AsyncMock(return_value=SimpleNamespace(pts=1, qts=2, seq=3, date=utc_now()))
        client.get_me.return_value = SimpleNamespace(id=42)
        started, draining, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def process() -> None:
            started.set()
            try:
                await asyncio.Future[None]()
            except asyncio.CancelledError:
                draining.set()
                await release.wait()
                raise

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with (patch('news_futures.telegram.open_client', return_value=client),
                  patch('news_futures.telegram.TelethonTransport', return_value=ControlledTransport()),
                  patch('sys.stdout', new_callable=StringIO)):
                task = asyncio.create_task(run_telegram(
                    ['example'], root / 'archive.sqlite3', root / 'user.session', False, process=process,
                ))
                try:
                    await asyncio.wait_for(started.wait(), 2)
                    task.cancel()
                    await asyncio.wait_for(draining.wait(), 2)
                    done, _ = await asyncio.wait({task}, timeout=0.05)
                    self.assertFalse(done)
                    client.disconnect.assert_not_awaited()
                finally:
                    release.set()
                    if not task.cancelling():
                        task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await asyncio.wait_for(task, 2)
                client.disconnect.assert_awaited_once()

    async def test_sdk_requests_only_configured_channels_and_normalizes_edits(self) -> None:
        from telethon import functions, types
        now = utc_now()
        message = types.Message(1, types.PeerChannel(123), date=now,
                                message='Подпись', media=types.MessageMediaUnsupported())
        edited = types.Message(1, types.PeerChannel(123), date=now,
                               message='Исправленная подпись', edit_date=now)
        requests: list[Any] = []

        class Client:
            async def get_entity(self, username: str) -> Any:
                self.username = username
                return types.Channel(123, 'Example', types.ChatPhotoEmpty(), now, broadcast=True)

            async def __call__(self, request: Any) -> Any:
                requests.append(request)
                return types.updates.ChannelDifference(
                    12, [message], [types.UpdateEditChannelMessage(edited, 12, 1)], [], [], final=True,
                )

        transport = TelethonTransport(Client())
        source = await transport.resolve('example')
        difference = await transport.difference(source, 10)
        self.assertIsInstance(requests[0], functions.updates.GetChannelDifferenceRequest)
        self.assertEqual(requests[0].pts, 10)
        self.assertEqual(requests[0].channel.id, 123)
        self.assertEqual(difference.pts, 12)
        self.assertEqual([item.text for item in difference.publications], ['Подпись', 'Исправленная подпись'])
        self.assertTrue(difference.publications[0].unread_media)
        with self.assertRaises(ValueError):
            transport.publication(source, types.Message(1, types.PeerChannel(999), date=now, message='foreign'))

    async def test_history_window_pagination_and_no_media_download(self) -> None:
        from telethon import types
        now = utc_now()
        messages = [types.Message(i, types.PeerChannel(123), date=now - timedelta(days=days),
                                  message=f'post {i}') for i, days in [(4, -1), (3, 0), (2, 6), (1, 8)]]
        requests: list[dict[str, Any]] = []

        class Client:
            async def iter_messages(self, entity: Any, **kwargs: Any) -> Any:
                requests.append(kwargs)
                for message in messages:
                    yield message

        transport = TelethonTransport(Client())
        transport.entities[123] = 'configured-entity'
        history = await transport.history(Source(123, 'example'), now - timedelta(days=7), now)
        self.assertEqual([publication.external_id for publication in history], ['3', '2'])
        self.assertEqual(requests, [{'offset_date': now + timedelta(seconds=1), 'limit': None}])


if __name__ == '__main__':
    unittest.main()
