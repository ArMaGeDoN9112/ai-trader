from contextlib import closing
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest

from langgraph.checkpoint.base import empty_checkpoint
from langgraph.checkpoint.serde.jsonplus import EXT_PYDANTIC_V2
import ormsgpack
from psycopg.errors import ForeignKeyViolation

from news_futures.storage.checkpoints import checkpoint_serializer, checkpoints
from news_futures.storage.database import connect, has_table
from news_futures.storage.events import EventArchive
from news_futures.storage.migrate import migrate, require_migration
from news_futures.schemas.contracts import Review


class MigrationTest(unittest.TestCase):
    def test_runtime_import_preserves_ids_checkpoint_history_and_writes(self) -> None:
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with closing(sqlite3.connect(directory / 'events.sqlite3')) as source, source:
                source.executescript("""
                    CREATE TABLE event_revisions (revision TEXT PRIMARY KEY, publication TEXT, links TEXT);
                    CREATE TABLE event_decisions (id INTEGER PRIMARY KEY, revision TEXT, details TEXT);
                    INSERT INTO event_revisions VALUES ('original', '{}', '[]');
                    INSERT INTO event_decisions VALUES (7, 'original', '{}');
                """)

            serde = checkpoint_serializer()
            parent = empty_checkpoint()
            child = empty_checkpoint()
            child['channel_values']['test'] = {'value': 3}
            child['channel_versions']['test'] = 1
            with closing(sqlite3.connect(directory / 'checkpoints.sqlite3')) as source, source:
                source.executescript("""
                    CREATE TABLE checkpoints (
                        thread_id TEXT, checkpoint_ns TEXT, checkpoint_id TEXT, parent_checkpoint_id TEXT,
                        type TEXT, checkpoint BLOB, metadata BLOB
                    );
                    CREATE TABLE writes (
                        thread_id TEXT, checkpoint_ns TEXT, checkpoint_id TEXT, task_id TEXT,
                        task_path TEXT, idx INTEGER, channel TEXT, type TEXT, value BLOB
                    );
                """)
                for state, previous in ((parent, None), (child, parent['id'])):
                    kind, blob = serde.dumps_typed(state)
                    source.execute("INSERT INTO checkpoints VALUES ('thread', '', ?, ?, ?, ?, ?)",
                                   (state['id'], previous, kind, blob, json.dumps({'step': 1})))
                kind, blob = serde.dumps_typed('pending')
                source.execute("INSERT INTO writes VALUES ('thread', '', ?, 'task', 'path', 3, 'test', ?, ?)",
                               (child['id'], kind, blob))
                review = Review(proposal_id='proposal', approved=True, objections=[], reason='Checked.')
                legacy = ormsgpack.Ext(EXT_PYDANTIC_V2, ormsgpack.packb(
                    ['news_futures.agents.contracts', 'Review', review.model_dump(), 'model_validate_json'],
                ))
                source.execute("INSERT INTO writes VALUES ('thread', '', ?, 'task', 'path', 4, 'review', 'msgpack', ?)",
                               (child['id'], ormsgpack.packb(legacy)))

            with self.assertRaisesRegex(ValueError, 'migrate-sqlite'):
                require_migration(directory)
            report = migrate(directory)
            self.assertEqual(report['events']['event_decisions'], 1)
            self.assertEqual(report['checkpoints'], {'checkpoints': 2, 'checkpoint_writes': 2})
            require_migration(directory)
            with self.assertRaisesRegex(ValueError, 'already imported'):
                migrate(directory)

            with EventArchive(directory / 'events') as events, events.transaction():
                self.assertEqual(events.history()[0]['decision_id'], 7)
                self.assertEqual(events.append_revision('next', {}, {}, [])['decision_id'], 8)

            with checkpoints(directory / 'checkpoints') as saver:
                result = saver.get_tuple({'configurable': {'thread_id': 'thread', 'checkpoint_ns': '',
                                                            'checkpoint_id': child['id']}})
                assert result is not None and result.parent_config is not None
                self.assertEqual(result.parent_config['configurable']['checkpoint_id'], parent['id'])
                self.assertEqual(result.checkpoint['channel_values']['test'], {'value': 3})
                self.assertEqual(result.pending_writes, [('task', 'test', 'pending'), ('task', 'review', review)])

    def test_failed_import_rolls_back_data_in_all_archives(self) -> None:
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with closing(sqlite3.connect(directory / 'publications.sqlite3')) as source, source:
                source.executescript("""
                    CREATE TABLE notices (id INTEGER PRIMARY KEY, source TEXT, observed_at TEXT, code TEXT);
                    INSERT INTO notices VALUES (1, 'telegram:123', '2026-01-01', 'source_checked');
                """)
            with closing(sqlite3.connect(directory / 'events.sqlite3')) as source, source:
                source.executescript("""
                    CREATE TABLE event_decisions (id INTEGER PRIMARY KEY, revision TEXT, details TEXT);
                    INSERT INTO event_decisions VALUES (1, 'missing', '{}');
                """)

            with self.assertRaises(ForeignKeyViolation):
                migrate(directory)
            with connect(directory / 'publications') as db:
                self.assertIsNone(db.exec_driver_sql('SELECT 1 FROM notices').mappings().fetchone())
                self.assertFalse(has_table(db, 'sqlite_import'))
