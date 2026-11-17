from datetime import datetime, timedelta, timezone
from io import StringIO
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from news_futures.cli import main
from news_futures.replay import replay
from news_futures.common.artifacts import write_json
from news_futures.schemas.company import CompanyProposal, Market, MarketObservation, SignalCandidate


class ReplayTest(TestCase):
    def test_reports_the_first_timely_outcome_with_a_fresh_market_timestamp(self) -> None:
        now = datetime(2026, 11, 17, 10, tzinfo=timezone.utc)

        def quote(price: float, observed_seconds: int, received_seconds: int, uid: str = 'future') -> MarketObservation:
            return MarketObservation(contract_id=uid, price_unit='points', limit_buy_available=True,
                                     limit_sell_available=True, received_at=now + timedelta(seconds=received_seconds),
                                     market=Market(last_price=price, observed_at=now + timedelta(seconds=observed_seconds)))

        baseline = quote(100, -6, -5)
        proposal = CompanyProposal(action='enter', company_id='company', contract_id=baseline.contract_id, side='short',
                                   publication_id='channel:1', entry_price=100, stop_loss=105, take_profit=97,
                                   reason='Signal', assessments=[], level_rationale=None)
        signal = SignalCandidate(revision='a' * 64, event_decision_id=3, strategy='tool_agent',
                                 proposal_id='review-1', proposal=proposal, observation_id=baseline.id,
                                 received_at=now - timedelta(seconds=10), decided_at=now,
                                 deadline_at=now + timedelta(seconds=5))
        timely = quote(98, 61, 63)
        quotes = [quote(99, 62, 64), quote(70, 61, 62, 'other'), quote(102, 59, 61), timely,
                  quote(97, 304, 306), quote(90, 903, 905)]
        with TemporaryDirectory() as temp:
            decisions = Path(temp)
            directory = decisions / signal.revision
            (directory / 'candidates').mkdir(parents=True)
            (directory / 'market').mkdir()
            write_json(directory / 'candidates' / 'review-1.json', signal.model_dump(mode='json'))
            quote_path = directory / 'market' / f'{baseline.id}.json'
            write_json(quote_path, baseline.model_dump(mode='json'))
            history = decisions / 'history.jsonl'
            history.write_text('\n'.join(item.model_dump_json() for item in quotes), encoding='utf-8')
            with patch('sys.argv', ['news_futures', 'replay', str(decisions), '--quotes', str(history),
                                   '--max-outcome-lag-seconds', '5']), patch('sys.stdout', new_callable=StringIO) as output:
                self.assertEqual(main(), 0)
            report = json.loads(output.getvalue())
            row, = report['signals']
            first, middle, last = row['outcomes']
            self.assertEqual(first['observation_id'], timely.id)
            self.assertAlmostEqual(first['markout_bps'], 200.0)
            self.assertEqual(middle['status'], 'missing_quote')
            self.assertEqual(last['status'], 'observed')
            self.assertEqual([item['observed'] for item in report['summary']], [1, 0, 1])

            quote_path.write_text(quote(101, -6, -5).model_dump_json(), encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'invalid_decision_observation'):
                replay(decisions, quotes, max_outcome_lag=timedelta(seconds=5))
