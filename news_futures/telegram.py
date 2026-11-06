"""Pull channel differences from application-owned pts, not SDK event acknowledgements."""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
import getpass
import json
import logging
import os
from pathlib import Path
import re
import sys
from typing import Any, Protocol
from uuid import uuid4

from news_futures.publications import Archive, Publication, utc_now


@dataclass(frozen=True)
class Source:
    channel_id: int
    username: str

    @property
    def id(self) -> str:
        return f'telegram:{self.channel_id}'


@dataclass(frozen=True)
class Difference:
    pts: int
    publications: list[Publication]
    final: bool = True
    too_long: bool = False
    timeout: int = 5


class Transport(Protocol):
    async def resolve(self, username: str) -> Source: ...
    async def current_pts(self, source: Source) -> int: ...
    async def history(self, source: Source, since: datetime, until: datetime) -> list[Publication]: ...
    async def difference(self, source: Source, pts: int) -> Difference: ...


def configured_channels(path: Path) -> list[str]:
    channels = json.loads(path.read_text(encoding='utf-8'))['channels']
    if (not isinstance(channels, list) or not channels
            or any(not isinstance(name, str) or not re.fullmatch(r'[a-zA-Z][a-zA-Z0-9_]{4,31}', name)
                   for name in channels)):
        raise ValueError('Config must contain a nonempty channels list of Telegram usernames')
    names = [name.lower() for name in channels]
    if len(set(names)) != len(names):
        raise ValueError('Duplicate Telegram source')
    return names


async def collect(transport: Transport, archive: Archive, source: Source) -> int:
    state = archive.cursor(source.id)
    if state is None:
        pts = await transport.current_pts(source)
        until = utc_now()
        since = until - timedelta(days=7)
        history = await transport.history(source, since, until)
        archive.save_batch(source.id, pts, history, historical=True, checked_at=until,
                           history_since=since, notices=(
            'initial_history_available_only_7_days',
            'prior_and_intermediate_editions_unavailable',
        ))
        state = archive.cursor(source.id)
        assert state is not None
    while True:
        requested_at = utc_now()
        difference = await transport.difference(source, state['pts'])
        notices: tuple[str, ...] = ()
        publications = difference.publications
        if difference.too_long:
            # Snapshot before scanning, so updates during recovery remain replayable.
            pts = await transport.current_pts(source)
            history = await transport.history(
                source, datetime.fromisoformat(state['history_since']), utc_now(),
            )
            publications = history + publications
            notices = ('channel_difference_too_long', 'history_available_only',
                       'intermediate_editions_and_deleted_posts_unrecoverable')
        else:
            pts = difference.pts
        checkpoint_time = (requested_at if difference.final and not difference.too_long
                           else datetime.fromisoformat(state['checked_at']))
        archive.save_batch(source.id, pts, publications, notices=notices, checked_at=checkpoint_time)
        state = archive.cursor(source.id)
        assert state is not None
        if difference.final and not difference.too_long:
            return max(1, difference.timeout)


async def watch_channel(transport: Transport, archive: Archive, username: str, once: bool) -> bool:
    source: Source | None = None
    while True:
        delay = 5
        ok = False
        try:
            if source is None:
                source = await transport.resolve(username)
            delay = await collect(transport, archive, source)
            ok = True
            print(json.dumps({'source': source.id, 'status': 'received'}, ensure_ascii=False), flush=True)
        except Exception as error:
            # Exception messages can contain phone numbers, requests or session material.
            code = type(error).__name__
            identity = source.id if source else f'telegram:@{username}'
            try:
                archive.notice(identity, code)
            except Exception:
                pass  # Storage can be unavailable too; stderr remains an independent signal.
            print(json.dumps({'source': identity, 'error': code}), file=sys.stderr, flush=True)
            delay = max(5, int(getattr(error, 'seconds', 5)))
        if once:
            return ok
        await asyncio.sleep(delay)


async def process_queue(archive: Archive) -> None:
    while True:
        try:
            archive.process_pending()
        except Exception as error:
            print(json.dumps({'worker': 'control_simulation', 'error': type(error).__name__}),
                  file=sys.stderr, flush=True)
        await asyncio.sleep(1)


def default_session() -> Path:
    base = Path(os.environ.get('LOCALAPPDATA') or os.environ.get('XDG_STATE_HOME')
                or str(Path.home() / '.local' / 'state'))
    return base / 'news-futures' / 'telegram' / 'user.session'


def session_path(path: Path) -> Path:
    path = path.resolve()
    repository = Path(__file__).resolve().parents[1]
    if path.is_relative_to(repository):
        raise ValueError('MTProto session must be outside the repository')
    return path if path.suffix == '.session' else Path(str(path) + '.session')


class TelethonTransport:
    def __init__(self, client: Any):
        self.client = client
        self.entities: dict[int, Any] = {}

    async def resolve(self, username: str) -> Source:
        from telethon import types
        entity = await self.client.get_entity(username)
        if not isinstance(entity, types.Channel) or not entity.broadcast:
            raise ValueError('Configured source must be a broadcast channel')
        if entity.left:
            raise ValueError('Join the configured channel in Telegram before collecting')
        self.entities[entity.id] = entity
        return Source(entity.id, username)

    async def current_pts(self, source: Source) -> int:
        from telethon import functions
        full = await self.client(functions.channels.GetFullChannelRequest(self.entities[source.channel_id]))
        return int(full.full_chat.pts)

    def publication(self, source: Source, message: Any, change: str = 'new') -> Publication | None:
        from telethon import types
        if not isinstance(message, types.Message) or message.date is None:
            return None
        if not isinstance(message.peer_id, types.PeerChannel) or message.peer_id.channel_id != source.channel_id:
            raise ValueError('Message from an unconfigured channel')
        return Publication(
            source=source.id, external_id=str(message.id),
            url=f'https://t.me/{source.username}/{message.id}',
            text=message.message or '', published_at=message.date, received_at=utc_now(),
            edited_at=message.edit_date,
            unread_media=message.media is not None and not isinstance(message.media, types.MessageMediaEmpty),
            change=change if change == 'delete' else ('edit' if message.edit_date else change),
        )

    async def history(self, source: Source, since: datetime, until: datetime) -> list[Publication]:
        publications = []
        async for message in self.client.iter_messages(
            self.entities[source.channel_id], offset_date=until + timedelta(seconds=1), limit=None,
        ):
            if message.date < since:
                break
            if message.date > until:
                continue
            publication = self.publication(source, message)
            if publication:
                publications.append(publication)
        return publications

    async def difference(self, source: Source, pts: int) -> Difference:
        from telethon import functions, types
        result = await self.client(functions.updates.GetChannelDifferenceRequest(
            channel=self.entities[source.channel_id], filter=types.ChannelMessagesFilterEmpty(),
            pts=pts, limit=100, force=True,
        ))
        publications = []
        too_long = isinstance(result, types.updates.ChannelDifferenceTooLong)
        messages = result.messages if too_long else getattr(result, 'new_messages', [])
        for message in messages:
            publication = self.publication(source, message)
            if publication:
                publications.append(publication)
        for update in getattr(result, 'other_updates', []):
            if isinstance(update, types.UpdateEditChannelMessage):
                publication = self.publication(source, update.message, 'edit')
                if publication:
                    publications.append(publication)
            elif isinstance(update, types.UpdateDeleteChannelMessages):
                if update.channel_id != source.channel_id:
                    raise ValueError('Deletion from an unconfigured channel')
                for message_id in update.messages:
                    now = utc_now()
                    publications.append(Publication(
                        source.id, str(message_id), f'https://t.me/{source.username}/{message_id}',
                        '', now, now, change='delete',
                    ))
        next_pts = result.dialog.pts if too_long else result.pts
        return Difference(int(next_pts), publications, bool(result.final), too_long,
                          getattr(result, 'timeout', None) or 5)


async def open_client(path: Path, login: bool) -> Any:
    from telethon import TelegramClient, errors
    api_id = int(os.environ['TELEGRAM_API_ID'])
    api_hash = os.environ['TELEGRAM_API_HASH']
    if api_id <= 0 or not re.fullmatch(r'[a-fA-F0-9]{32}', api_hash):
        raise ValueError('Invalid Telegram API credentials')
    path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger('news_futures.telethon')
    logger.handlers = [logging.NullHandler()]
    logger.propagate = False
    client = TelegramClient(str(path), api_id, api_hash, receive_updates=False,
                            flood_sleep_threshold=0, base_logger=logger)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            if not login:
                raise ValueError('Run telegram-login locally first')
            if not sys.stdin.isatty():
                raise ValueError('Authorization requires a local interactive terminal')
            phone = getpass.getpass('Telegram phone (hidden): ')
            await client.send_code_request(phone)
            try:
                await client.sign_in(phone, getpass.getpass('Telegram code (hidden): '))
            except errors.SessionPasswordNeededError:
                await client.sign_in(password=getpass.getpass('Telegram 2FA password: '))
        me = await client.get_me()
        if me.bot:
            raise ValueError('A Telegram user session is required')
        return client
    except BaseException:
        await client.disconnect()
        raise


async def run_telegram(config: Path, database: Path, path: Path, once: bool) -> int:
    channels = configured_channels(config)
    path = session_path(path)
    client = await open_client(path, login=False)
    archive: Archive | None = None
    worker: asyncio.Task[None] | None = None
    try:
        from telethon import functions
        identity_path = path.with_suffix('.identity')
        if not identity_path.exists():
            with identity_path.open('x', encoding='utf-8') as identity:
                identity.write(str(uuid4()))
        archive = Archive(database)
        state = await client(functions.updates.GetStateRequest())
        archive.bind_session(identity_path.read_text(encoding='utf-8'), (await client.get_me()).id, {
            'pts': state.pts, 'qts': state.qts, 'seq': state.seq, 'date': state.date.isoformat(),
        })
        transport = TelethonTransport(client)
        if not once:
            worker = asyncio.create_task(process_queue(archive))
        results = await asyncio.gather(*(watch_channel(transport, archive, name, once) for name in channels))
        archive.process_pending()
        return 0 if all(results) else 1
    finally:
        if worker:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        if archive:
            archive.db.close()
        await client.disconnect()


async def login_telegram(path: Path) -> int:
    client = await open_client(session_path(path), login=True)
    await client.disconnect()
    print('Telegram user session authorized locally.')
    return 0
