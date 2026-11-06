from copy import deepcopy
import json
from pathlib import Path
from typing import Any
import unittest

from news_futures.execution import simulate_execution


class IndependentAdmissionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.context = json.loads(
            (Path(__file__).resolve().parents[1] / 'news_futures' / 'control.json').read_text(encoding='utf-8')
        )

    def test_critic_approval_cannot_bypass_program_checks(self) -> None:
        mutations: list[tuple[dict[str, Any], str]] = [
            ({'contract_id': 'unknown-future'}, 'contract_not_allowed'),
            ({'company_id': 'unknown-company'}, 'unknown_company_link'),
            ({'publication_id': 'unknown-publication'}, 'unknown_publication'),
            ({'id': 'another-proposal'}, 'critic_not_approved'),
            ({'quantity': 0}, 'invalid_quantity'),
            ({'quantity': 2}, 'invalid_quantity'),
            ({'quantity': True}, 'invalid_quantity'),
            ({'side': 'short'}, 'unsupported_side'),
            ({'entry_price': '300'}, 'invalid_prices'),
            ({'entry_price': float('nan')}, 'invalid_prices'),
            ({'stop_loss': -1}, 'invalid_prices'),
            ({'stop_loss': 300}, 'invalid_protection_levels'),
            ({'take_profit': 290}, 'invalid_protection_levels'),
            ({'entry_reason': ' '}, 'missing_entry_reason'),
        ]
        for changes, error in mutations:
            with self.subTest(changes=changes):
                proposal = {**self.context['analyst_response'], **changes}
                result = simulate_execution(proposal, self.context['critic_response'], self.context)
                self.assertFalse(result['check']['accepted'])
                self.assertIn(error, result['check']['errors'])
                self.assertEqual(result['execution'], {'mode': 'simulation', 'status': 'rejected'})

    def test_missing_or_negative_critic_review_never_fills(self) -> None:
        for review in (None, {}, {'approved': False, 'proposal_id': 'control-proposal-001'}):
            with self.subTest(review=review):
                result = simulate_execution(self.context['analyst_response'], review, self.context)
                self.assertFalse(result['check']['accepted'])
                self.assertEqual(result['execution']['status'], 'rejected')

    def test_malformed_proposal_never_fills(self) -> None:
        proposals: list[object] = [None, [], {}, {'id': None}]
        for proposal in proposals:
            with self.subTest(proposal=proposal):
                result = simulate_execution(proposal, self.context['critic_response'], self.context)
                self.assertFalse(result['check']['accepted'])
                self.assertEqual(result['execution']['status'], 'rejected')

    def test_broken_catalog_relationship_never_fills(self) -> None:
        context = deepcopy(self.context)
        context['catalog']['future']['underlying_share_id'] = 'unknown-share'
        result = simulate_execution(context['analyst_response'], context['critic_response'], context)
        self.assertIn('unknown_company_link', result['check']['errors'])
        self.assertEqual(result['execution']['status'], 'rejected')


if __name__ == '__main__':
    unittest.main()
