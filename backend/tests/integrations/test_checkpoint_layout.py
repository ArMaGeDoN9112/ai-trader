import base64
from datetime import datetime, timedelta
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest import TestCase
from unittest.mock import Mock, patch

from langgraph.checkpoint.serde.jsonplus import EXT_PYDANTIC_V2
import ormsgpack
from psycopg import sql
from psycopg.types.json import Jsonb

from news_futures.agents.decisions import news_record
from news_futures.agents.schemas import EventEvidence
from news_futures.agents.analysis import AgentLimits, run_analysis
from news_futures.schemas.contracts import Review, RoleSettings
from news_futures.storage.checkpoints import checkpoint_serializer, checkpoints, migrate_model_paths
from news_futures.storage.database import connect, transaction
from news_futures.storage.events import EventArchive
from news_futures.storage.locks import exclusive_operation
from news_futures.storage.migrate import migrate_checkpoints
from news_futures.trading.approval import read_approval
from tests.support.models import ScriptedEndpoint, review


FIXTURES = Path(__file__).resolve().parents[1] / 'fixtures/backend_layout'


class CheckpointLayoutTest(TestCase):
    def setUp(self) -> None:
        self.directory = Path(self.enterContext(TemporaryDirectory()))

    def restore(self) -> dict[str, Any]:
        fixture: dict[str, Any] = json.loads((FIXTURES / 'analysis.json').read_bytes())
        with checkpoints(self.directory / 'checkpoints'):
            pass
        with connect(self.directory / 'checkpoints') as db, transaction(db):
            for table, rows in fixture['tables'].items():
                for row in rows:
                    values = [base64.b64decode(value['base64']) if isinstance(value, dict) and 'base64' in value
                              else Jsonb(value) if isinstance(value, dict) else value for value in row.values()]
                    query = sql.SQL('INSERT INTO {} ({}) VALUES ({})').format(
                        sql.Identifier(table), sql.SQL(', ').join(map(sql.Identifier, row)),
                        sql.SQL(', ').join(sql.Placeholder() for _ in values),
                    )
                    db.exec_driver_sql(query.as_string(), tuple(values))
        for name, content in fixture['files'].items():
            path = self.directory / 'decision' / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding='utf-8')
        return fixture

    def rows(self) -> dict[str, list[dict[str, Any]]]:
        with connect(self.directory / 'checkpoints') as db:
            return {table: [dict(row) for row in db.exec_driver_sql(sql.SQL('SELECT * FROM {} ORDER BY {}').format(
                sql.Identifier(table), sql.SQL(', ').join(map(sql.Identifier, keys)),
            ).as_string()).mappings()] for table, keys in (
                ('checkpoints', ('thread_id', 'checkpoint_ns', 'checkpoint_id')),
                ('checkpoint_blobs', ('thread_id', 'checkpoint_ns', 'channel', 'version')),
                ('checkpoint_writes', ('thread_id', 'checkpoint_ns', 'checkpoint_id', 'task_id', 'idx')),
            )}

    def test_unknown_nested_type_aborts_the_whole_migration(self) -> None:
        self.restore()
        unknown = ormsgpack.Ext(EXT_PYDANTIC_V2, ormsgpack.packb(
            ['news_futures.domain.company', 'UnknownModel', {}, 'model_validate_json'],
        ))
        blob = ormsgpack.packb({'nested': [unknown]})
        with connect(self.directory / 'checkpoints') as db:
            db.exec_driver_sql("INSERT INTO checkpoint_blobs VALUES ('unknown', '', 'test', '1', 'msgpack', %s)", (blob,))
        before = self.rows()
        with self.assertRaisesRegex(ValueError, 'Unsupported checkpoint model'):
            migrate_checkpoints(self.directory)
        self.assertEqual(self.rows(), before)
        with self.assertRaisesRegex(ValueError, 'Unsupported checkpoint model'):
            checkpoint_serializer().loads_typed(('msgpack', blob))

    def test_nested_legacy_models_migrate_without_changing_plain_text(self) -> None:
        record = Review(proposal_id='proposal', approved=True, objections=[], reason='Checked.')
        legacy = ormsgpack.Ext(EXT_PYDANTIC_V2, ormsgpack.packb(
            ['news_futures.domain.contracts', 'Review', record.model_dump(), 'model_validate_json'],
        ))
        blob = ormsgpack.packb({'nested': [legacy], 'text': 'news_futures.domain.contracts'})
        migrated = migrate_model_paths('msgpack', blob)
        self.assertEqual(checkpoint_serializer().loads_typed(('msgpack', migrated)),
                         {'nested': [record], 'text': 'news_futures.domain.contracts'})

    def test_migration_requires_stopped_runtime_and_sandbox(self) -> None:
        self.restore()
        before = self.rows()
        for path, operation in ((self.directory / 'runtime-lock', 'runtime'),
                                (self.directory / 'events', 'sandbox'),
                                (self.directory / 'checkpoints', 'checkpoint-setup')):
            with exclusive_operation(path, operation) as acquired:
                self.assertTrue(acquired)
                with self.assertRaisesRegex(ValueError, 'Stop the runtime'):
                    migrate_checkpoints(self.directory)
        self.assertEqual(self.rows(), before)

    def test_analysis_resumes_old_pending_writes_without_repeating_reads_or_execution(self) -> None:
        fixture = self.restore()
        self.assertEqual(migrate_checkpoints(self.directory), {'checkpoint_blobs': 0, 'checkpoint_writes': 0})
        directory = self.directory / 'decision'
        event = EventEvidence.model_validate(news_record(json.loads((directory / 'event.json').read_bytes())))
        endpoint = ScriptedEndpoint([review()])
        broker = Mock()
        executor = self.enterContext(patch('news_futures.trading.executor.SandboxExecutor.start'))
        now = datetime.fromisoformat(fixture['publication']['received_at']) + timedelta(seconds=3)
        with (checkpoints(self.directory / 'checkpoints') as saver,
              EventArchive(self.directory / 'events') as events,
              patch('news_futures.agents.analysis.utc_now', return_value=now)):
            for _ in range(2):
                outcome = run_analysis(
                    directory, fixture['revision'], event, 'sandbox-account', {'control-future': 'sber'},
                    events, broker.read_market, lambda: None, lambda: None, saver,
                    RoleSettings(model='test/coordinator'), AgentLimits(), endpoint.model,
                    deadline=now + timedelta(seconds=30), read_candles=broker.read_candles,
                    read_comparison=broker.read_comparison, received_at=now - timedelta(seconds=3),
                )
                self.assertEqual(outcome.status, 'candidate')
        proposal, identity, _, _ = read_approval(directory / 'tool-agent')
        self.assertEqual(identity, outcome.proposal.id if outcome.proposal else None)
        self.assertEqual(proposal.entry_price, 300)
        self.assertEqual([request['model'] for request in endpoint.requests], ['test/critic'])
        self.assertEqual(broker.mock_calls, [])
        executor.assert_not_called()
