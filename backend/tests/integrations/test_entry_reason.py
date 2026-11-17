from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from psycopg import sql

from news_futures.cli import main
from news_futures.common.artifacts import write_json
from news_futures.storage.database import connect, schema_name


class EntryReasonTest(TestCase):
    def test_incompatible_schema_reports_failure_when_saving_entry_reason(self) -> None:
        root = Path(self.enterContext(TemporaryDirectory()))
        database = root / 'events'
        with connect(database) as db:
            db.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema_name(database))))
            db.execute('CREATE TABLE position_entry_reasons (invalid TEXT)')
        path = root / 'reason.json'
        write_json(path, {'account_id': 'sandbox-account', 'position_id': 'position-1',
                          'contract_id': 'sber-future', 'reason': 'Исходная причина входа.'})
        with patch.object(sys, 'argv', ['news_futures', 'entry-reason', str(path),
                                       '--database', str(database)]), patch('builtins.print'):
            self.assertEqual(main(), 1)
