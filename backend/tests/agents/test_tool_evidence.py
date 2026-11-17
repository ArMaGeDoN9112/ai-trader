from datetime import datetime, timedelta, timezone
from unittest import TestCase

from news_futures.application.analysis.calendar import CALENDAR, EventCalendar
from news_futures.application.analysis.team_dto import EventEvidence, PublicationEvidence
from news_futures.application.analysis.tool_evidence import compare_candles, event_versions
from news_futures.domain.company import MinuteCandle


class ToolEvidenceTest(TestCase):
    def test_comparison_requires_the_same_completed_minute_for_every_instrument(self) -> None:
        anchor = datetime(2026, 11, 17, 10, tzinfo=timezone.utc)

        def candle(minutes: int, price: float) -> MinuteCandle:
            return MinuteCandle(started_at=anchor + timedelta(minutes=minutes), close=price, volume_lots=8)

        comparison = compare_candles({
            'future': ('f', [candle(-1, 100), candle(0, 102), candle(1, 109)]),
            'underlying': ('s', [candle(-1, 200), candle(0, 202), candle(1, 205)]),
            'IMOEX': ('i', [candle(-1, 300), candle(0, 300)]),
            'sector': ('sector', []),
        }, company='a', key='read', published_at=anchor, received_at=anchor + timedelta(seconds=20),
           end=anchor + timedelta(minutes=2))
        self.assertEqual(comparison.since_publication.latest_close_at, anchor + timedelta(minutes=1))
        self.assertEqual(comparison.since_publication.future_minus_reference_bps, {'underlying': 100, 'IMOEX': 200})
        self.assertEqual(comparison.since_receipt.change_bps, {})
        self.assertEqual(comparison.missing_instruments, ['sector'])

    def test_versions_include_edit_and_cited_link_without_future_archive_decision(self) -> None:
        publication = PublicationEvidence(id='source:1', source='source', url='https://t.me/source/1',
                                          text='Revenue estimate is 20.', published_at='2026-11-17T10:00:00Z',
                                          edited_at='2026-11-17T10:01:00Z', unread_media=False, change='edit')
        event = EventEvidence(publication=publication, decision_id=2, revision='new', event_id='new-event',
                              kind='saved', basis_at=None, assessment=None, correction=None, links=[])
        current = publication.model_dump() | {'cited_urls': ['https://issuer.example/report']}
        result = event_versions([
            {'decision_id': 1, 'revision': 'old', 'kind': 'saved', 'event_id': 'old-event',
             'publication': current | {'text': 'Revenue estimate is 15.', 'edited_at': None}},
            {'decision_id': 2, 'revision': 'new', 'kind': 'saved', 'event_id': 'new-event', 'publication': current},
            {'decision_id': 3, 'revision': 'future', 'kind': 'update', 'event_id': 'new-event', 'publication': current},
        ], event, 'a', 'read')
        self.assertEqual(result.added_text, ['20.'])
        self.assertEqual(result.removed_text, ['15.'])
        self.assertEqual([version.revision for version in result.versions], ['old', 'new'])
        self.assertEqual(result.versions[-1].cited_urls, ['https://issuer.example/report'])

    def test_calendar_keeps_date_only_disclosure_on_the_day_and_scopes_by_company(self) -> None:
        calendar = EventCalendar.model_validate_json(CALENDAR.read_bytes())
        now = datetime(2026, 11, 26, 15, tzinfo=timezone.utc)
        moex = calendar.upcoming('d68f33b3-76d9-423b-a656-b2580cb0c9f2', now)
        self.assertEqual([item.kind for item in moex], ['earnings', 'corporate', 'macro'])
        self.assertEqual([item.kind for item in calendar.upcoming('other', now)], ['macro'])
