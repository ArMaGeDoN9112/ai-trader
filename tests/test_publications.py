from dataclasses import replace
from datetime import timedelta
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from news_futures.publications import Archive, Publication, utc_now


class ArchiveTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / 'archive.sqlite3'
        self.archive = Archive(self.path)
        self.addCleanup(lambda: self.archive.db.close())
        now = utc_now()
        self.publication = Publication('telegram:123', '7', 'https://t.me/example/7',
                                       'Исходная подпись', now, now, unread_media=True)

    def test_history_editions_duplicate_and_restart(self) -> None:
        original = self.publication
        self.archive.save_batch(original.source, 10, [original], historical=True)
        self.archive.process_pending()
        self.assertEqual(self.archive.report()['revisions'][0]['status'], 'context_only')
        self.assertIsNone(self.archive.report()['revisions'][0]['trace'])
        edited = replace(original, text='Исправленная подпись', edited_at=utc_now(), change='edit')
        self.archive.save_batch(original.source, 11, [edited])
        self.archive.process_pending()
        redelivery = replace(edited, received_at=utc_now(), url='https://t.me/renamed/7', change='new')
        self.archive.save_batch(original.source, 11, [redelivery])
        self.archive.db.close()
        self.archive = Archive(self.path)
        report = self.archive.report()
        self.assertEqual(len(report['revisions']), 2)
        self.assertEqual(report['revisions'][0]['publication']['text'], original.text)
        revision = report['revisions'][1]
        self.assertEqual(revision['status'], 'simulated')
        self.assertTrue(revision['publication']['unread_media'])
        self.assertEqual(revision['trace']['publication']['text'], edited.text)
        self.assertEqual(revision['trace']['execution']['publication_id'], 'telegram:123:7')
        self.assertTrue(all(role['mode'] == 'simulation' for role in revision['trace']['roles']))
        self.assertEqual([role['role'] for role in revision['trace']['roles']],
                         ['router', 'company_analyst', 'critic'])
        self.assertEqual(self.archive.cursor(original.source)['pts'], 11)  # type: ignore[index]

    def test_storage_failure_rolls_back_revision_job_and_cursor(self) -> None:
        original = self.publication
        self.archive.save_batch(original.source, 10, [])
        self.archive.db.executescript('''
            CREATE TRIGGER fail_cursor BEFORE INSERT ON channels
            BEGIN SELECT RAISE(ABORT, 'disk failure'); END;
        ''')
        with self.assertRaises(sqlite3.IntegrityError):
            self.archive.save_batch(original.source, 11, [original])
        self.assertEqual(self.archive.report()['revisions'], [])
        self.assertEqual(self.archive.cursor(original.source)['pts'], 10)  # type: ignore[index]
        self.archive.db.execute('DROP TRIGGER fail_cursor')
        self.archive.save_batch(original.source, 11, [original])
        self.assertEqual(self.archive.report()['revisions'][0]['status'], 'pending')

    def test_failed_processing_remains_pending_and_can_resume(self) -> None:
        self.archive.save_batch(self.publication.source, 1, [self.publication])
        with patch('news_futures.publications.process_publication', side_effect=ValueError('failed')):
            with self.assertRaises(ValueError):
                self.archive.process_pending()
        self.assertEqual(self.archive.report()['revisions'][0]['status'], 'pending')
        self.archive.db.close()
        self.archive = Archive(self.path)
        self.archive.process_pending()
        self.assertEqual(self.archive.report()['revisions'][0]['status'], 'simulated')

    def test_operator_reads_persisted_archive_in_offline_process(self) -> None:
        self.archive.save_batch(self.publication.source, 1, [self.publication])
        self.archive.process_pending()
        root = Path(__file__).resolve().parents[1]
        result = subprocess.run(
            [sys.executable, '-I', '-S', str(root / 'tests/offline_runner.py'),
             'publications', '--database', str(self.path)],
            capture_output=True, text=True, encoding='utf-8', cwd=root, timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report['revisions'][0]['publication']['text'], self.publication.text)
        self.assertEqual(report['revisions'][0]['status'], 'simulated')

    def test_media_only_and_old_publications_do_not_execute(self) -> None:
        media = replace(self.publication, text='')
        old = replace(self.publication, external_id='8', published_at=utc_now() - timedelta(hours=1))
        self.archive.save_batch(media.source, 1, [media, old])
        self.archive.process_pending()
        revisions = self.archive.report()['revisions']
        self.assertEqual([row['status'] for row in revisions], ['no_text', 'context_only'])
        self.assertTrue(all(row['trace'] is None for row in revisions))

    def test_pending_publication_expires_before_processing(self) -> None:
        self.archive.save_batch(self.publication.source, 1, [self.publication])
        with patch('news_futures.publications.utc_now', return_value=utc_now() + timedelta(minutes=16)):
            self.archive.process_pending()
        self.assertEqual(self.archive.report()['revisions'][0]['status'], 'context_only')

    def test_session_identity_is_checked_but_not_exposed_to_processing(self) -> None:
        state = {'pts': 1, 'qts': 2, 'seq': 3, 'date': utc_now().isoformat()}
        self.archive.bind_session('local-identity', 1234, state)
        self.archive.bind_session('local-identity', 1234, state)
        for session, account in [('other-session', 1234), ('local-identity', 5678)]:
            with self.assertRaises(ValueError):
                self.archive.bind_session(session, account, state)
        self.archive.save_batch(self.publication.source, 1, [self.publication])
        self.archive.process_pending()
        self.assertNotIn('local-identity', json.dumps(self.archive.report()))

    def test_delete_delivery_is_idempotent_and_preserves_original(self) -> None:
        original = self.publication
        deleted = replace(original, text='', change='delete', published_at=utc_now())
        self.archive.save_batch(original.source, 1, [original])
        self.archive.save_batch(original.source, 2, [deleted])
        self.archive.save_batch(original.source, 2, [replace(deleted, published_at=utc_now())])
        self.assertEqual(len(self.archive.report()['revisions']), 2)
        self.assertEqual(self.archive.report()['revisions'][0]['publication']['text'], original.text)

    def test_invalid_batch_and_naive_timestamp_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            replace(self.publication, published_at=utc_now().replace(tzinfo=None))
        with self.assertRaises(ValueError):
            self.archive.save_batch('unconfigured', 1, [self.publication])
        self.archive.save_batch(self.publication.source, 10, [])
        with self.assertRaises(ValueError):
            self.archive.save_batch(self.publication.source, 9, [self.publication])


if __name__ == '__main__':
    unittest.main()
