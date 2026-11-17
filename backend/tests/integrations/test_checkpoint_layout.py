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

from news_futures.agents.decisions import SYSTEM, news_record, validate_company_proposal
from news_futures.agents.review import review_cycle
from news_futures.agents.schemas import EventEvidence
from news_futures.agents.tool_agent import AgentLimits, run_tool_agent
from news_futures.cli import main
from news_futures.common.artifacts import replace_json
from news_futures.schemas.company import CompanyContext, CompanyProposal
from news_futures.schemas.contracts import Review, RoleSettings, Settings
from news_futures.storage.checkpoints import checkpoint_serializer, checkpoints, migrate_model_paths
from news_futures.storage.database import connect
from news_futures.storage.events import EventArchive
from news_futures.storage.locks import exclusive_operation
from news_futures.storage.migrate import migrate_checkpoints
from news_futures.trading.approval import read_approval
from tests.support.models import ScriptedEndpoint, review


FIXTURES = Path(__file__).resolve().parents[1] / 'fixtures/backend_layout'


class CheckpointLayoutTest(TestCase):
    def setUp(self) -> None:
        self.directory = Path(self.enterContext(TemporaryDirectory()))

    def restore(self, strategy: str) -> dict[str, Any]:
        fixture: dict[str, Any] = json.loads((FIXTURES / f'{strategy}.json').read_bytes())
        with checkpoints(self.directory / 'checkpoints'):
            pass
        with connect(self.directory / 'checkpoints') as db, db.transaction():
            for table, rows in fixture['tables'].items():
                for row in rows:
                    values = [base64.b64decode(value['base64']) if isinstance(value, dict) and 'base64' in value
                              else Jsonb(value) if isinstance(value, dict) else value for value in row.values()]
                    db.execute(sql.SQL('INSERT INTO {} ({}) VALUES ({})').format(
                        sql.Identifier(table), sql.SQL(', ').join(map(sql.Identifier, row)),
                        sql.SQL(', ').join(sql.Placeholder() for _ in values),
                    ), values)
        for name, content in fixture['files'].items():
            path = self.directory / 'decision' / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding='utf-8')
        return fixture

    def rows(self) -> dict[str, list[dict[str, Any]]]:
        with connect(self.directory / 'checkpoints') as db:
            return {table: db.execute(sql.SQL('SELECT * FROM {} ORDER BY {}').format(
                sql.Identifier(table), sql.SQL(', ').join(map(sql.Identifier, keys)),
            )).fetchall() for table, keys in (
                ('checkpoints', ('thread_id', 'checkpoint_ns', 'checkpoint_id')),
                ('checkpoint_blobs', ('thread_id', 'checkpoint_ns', 'channel', 'version')),
                ('checkpoint_writes', ('thread_id', 'checkpoint_ns', 'checkpoint_id', 'task_id', 'idx')),
            )}

    def test_migration_preserves_history_versions_pending_writes_and_nested_models(self) -> None:
        self.restore('fixed')
        before = self.rows()
        with checkpoints(self.directory / 'checkpoints') as saver:
            with self.assertRaisesRegex(ValueError, 'migrate-checkpoints'):
                list(saver.list(None))
        report = migrate_checkpoints(self.directory)
        self.assertGreater(report['checkpoint_blobs'], 0)
        self.assertGreater(report['checkpoint_writes'], 0)
        models = set()
        for table, rows in self.rows().items():
            for original, migrated in zip(before[table], rows, strict=True):
                expected = dict(original)
                if original.get('blob') is not None:
                    expected['blob'] = migrate_model_paths(original['type'], original['blob'])
                    value = checkpoint_serializer().loads_typed((migrated['type'], migrated['blob']))
                    models.add(type(value))
                    if isinstance(value, CompanyContext):
                        self.assertEqual(value.account.id, 'sandbox-account')
                self.assertEqual(expected, migrated)
        self.assertTrue({CompanyContext, CompanyProposal, Review}.issubset(models))
        profile = self.directory / 'runtime.json'
        replace_json(profile, {'data_dir': '.', 'catalog': 'catalog.json', 'account_id': 'sandbox-account'})
        with patch('sys.argv', ['news_futures', 'migrate-checkpoints', str(profile)]), patch('builtins.print') as output:
            self.assertEqual(main(), 0)
        self.assertEqual(json.loads(output.call_args.args[0]), {'checkpoint_blobs': 0, 'checkpoint_writes': 0})
        with checkpoints(self.directory / 'checkpoints') as saver:
            self.assertEqual(len(list(saver.list(None))), len(before['checkpoints']))

    def test_unknown_nested_type_aborts_the_whole_migration(self) -> None:
        self.restore('fixed')
        unknown = ormsgpack.Ext(EXT_PYDANTIC_V2, ormsgpack.packb(
            ['news_futures.domain.company', 'UnknownModel', {}, 'model_validate_json'],
        ))
        blob = ormsgpack.packb({'nested': [unknown]})
        with connect(self.directory / 'checkpoints') as db:
            db.execute("INSERT INTO checkpoint_blobs VALUES ('unknown', '', 'test', '1', 'msgpack', %s)", (blob,))
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
        self.restore('fixed')
        before = self.rows()
        for path, operation in ((self.directory / 'runtime-lock', 'runtime'),
                                (self.directory / 'events', 'sandbox'),
                                (self.directory / 'checkpoints', 'checkpoint-setup')):
            with exclusive_operation(path, operation) as acquired:
                self.assertTrue(acquired)
                with self.assertRaisesRegex(ValueError, 'Stop the runtime'):
                    migrate_checkpoints(self.directory)
        self.assertEqual(self.rows(), before)

    def test_fixed_resumes_old_checkpoint_at_critic_once(self) -> None:
        fixture = self.restore('fixed')
        migrate_checkpoints(self.directory)
        thread = next(row['thread_id'] for row in fixture['tables']['checkpoints'] if ':review:' in row['thread_id'])
        directory = self.directory / 'decision'
        attempt = directory / 'review' / thread.rsplit(':', 1)[1]
        endpoint = ScriptedEndpoint([review()])
        executor = self.enterContext(patch('news_futures.trading.executor.SandboxExecutor.start'))
        with checkpoints(self.directory / 'checkpoints') as saver:
            saved = saver.get({'configurable': {'thread_id': thread}})
            assert saved is not None
            context = saved['channel_values']['context']
            for _ in range(2):
                trace = review_cycle(Settings.model_validate_json((directory / 'review.json').read_bytes()),
                                     attempt, context, model_factory=endpoint.model, mode='observation',
                                     system=SYSTEM, proposal_schema=CompanyProposal, validate=validate_company_proposal,
                                     checkpointer=saver, thread_id=thread)
                self.assertEqual(trace['status'], 'approved_for_checks')
                self.assertEqual(trace['approved_proposal']['entry_price'], 301)
        replace_json(attempt / 'trace.json', trace)
        self.assertEqual(trace['calls'][:3], [json.loads(fixture['files'][
            f'review/{attempt.name}/call-{index:02}.json']) for index in (1, 2, 3)])
        with self.assertRaisesRegex(ValueError, 'sandbox_approval_required'):
            read_approval(attempt)
        self.assertEqual([request['model'] for request in endpoint.requests], ['test/critic'])
        executor.assert_not_called()

    def test_tool_agent_resumes_old_pending_writes_without_repeating_reads_or_execution(self) -> None:
        fixture = self.restore('tool_agent')
        self.assertEqual(migrate_checkpoints(self.directory), {'checkpoint_blobs': 0, 'checkpoint_writes': 0})
        directory = self.directory / 'decision'
        event = EventEvidence.model_validate(news_record(json.loads((directory / 'event.json').read_bytes())))
        endpoint = ScriptedEndpoint([review()])
        broker = Mock()
        executor = self.enterContext(patch('news_futures.trading.executor.SandboxExecutor.start'))
        now = datetime.fromisoformat(fixture['publication']['received_at']) + timedelta(seconds=3)
        with (checkpoints(self.directory / 'checkpoints') as saver,
              EventArchive(self.directory / 'events') as events,
              patch('news_futures.agents.tool_agent.utc_now', return_value=now)):
            for _ in range(2):
                outcome = run_tool_agent(
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
