from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
from threading import Barrier
from unittest.mock import patch

from news_futures.agents.contracts import RoleSettings
from news_futures.agents.events import EventArchive
from news_futures.news.publications import Archive
from news_futures.trading.risk import Admission, cancel_unsubmitted, reserve_entry
from news_futures.trading.terms import ContractSnapshot, RiskSnapshot
from tests.support.models import ScriptedEndpoint
from tests.support.news import company_link
from tests.support.risk import RiskFixture


class RiskAdmissionTest(RiskFixture):
    def reserve(
        self, directory: Path, *,
        reader: Callable[[str, str], object] | None = None, allowed: set[str] | None = None,
    ) -> Admission:
        with EventArchive(self.events_db) as events:
            archive = Archive(self.publications_db)
            try:
                return reserve_entry(events, archive, directory, account_id=self.account_id,
                                     allowed_contracts={self.contract_id} if allowed is None else allowed,
                                     read_snapshot=reader or (lambda account, contract: self.snapshot))
            finally:
                archive.db.close()

    def test_sizes_reviewed_proposal_in_whole_lots_with_costs(self) -> None:
        result = self.reserve(self.approved())
        self.assertIsNone(result.reason)
        assert result.reservation is not None
        self.assertEqual(result.reservation.lots, 4)
        self.assertEqual(result.reservation.contracts, 8)
        self.assertEqual(result.reservation.planned_risk, Decimal('500'))
        self.assertEqual(result.reservation.initial_margin, Decimal('4000'))
        self.assertEqual(result.reservation.notional, Decimal('12000'))

    def test_risk_exposure_and_margin_can_each_reject_the_minimum_lot(self) -> None:
        directory = self.approved()
        cases = [
            ({}, {'round_trip_cost_per_lot': Decimal('400.000000001')}, 'risk_budget_exceeded'),
            ({'equity': Decimal('1499.999999999')}, {}, 'leverage_limit_exceeded'),
            ({'available_margin': Decimal('999.999999999')}, {}, 'insufficient_margin'),
        ]
        original = deepcopy(self.snapshot)
        for account, contract, reason in cases:
            with self.subTest(reason=reason):
                self.snapshot = deepcopy(original)
                self.snapshot.update(account)
                self.snapshot['contract'].update(contract)
                self.assertEqual(self.reserve(directory), Admission(reason=reason))

    def test_short_uses_sell_margin_and_rounds_down(self) -> None:
        directory = self.approved(side='short', stop_loss=310, take_profit=280)
        self.snapshot['available_margin'] = Decimal('5999.99')
        result = self.reserve(directory)
        assert result.reservation is not None
        self.assertEqual(result.reservation.side, 'short')
        self.assertEqual(result.reservation.lots, 2)
        self.assertEqual(result.reservation.contracts, 4)
        self.assertEqual(result.reservation.initial_margin, Decimal('4000'))

    def test_short_leverage_accounts_for_price_improvement_up_to_the_stop(self) -> None:
        self.snapshot['equity'] = Decimal('1500')
        approval = self.approved(side='short', stop_loss=310, take_profit=280)
        self.assertEqual(self.reserve(approval), Admission(reason='leverage_limit_exceeded'))

    def test_leverage_uses_fresh_quote_and_equity_at_boundary(self) -> None:
        directory = self.approved()
        self.snapshot['equity'] = Decimal('3004.999999999')
        self.snapshot['contract']['last_price'] = Decimal('300.5')
        result = self.reserve(directory)
        assert result.reservation is not None
        self.assertEqual(result.reservation.lots, 1)
        self.assertEqual(result.reservation.notional, Decimal('3005'))

    def test_rejects_incomplete_invalid_or_unavailable_contract_and_account(self) -> None:
        directory = self.approved()
        for field, value, reason in (
            ('tick_size', Decimal(0), 'risk_data_unavailable_or_invalid'),
            ('tick_value_per_contract', Decimal('NaN'), 'risk_data_unavailable_or_invalid'),
            ('contracts_per_lot', True, 'risk_data_unavailable_or_invalid'),
            ('price_unit', 'rubles', 'risk_data_unavailable_or_invalid'),
            ('margin_per_lot_buy', Decimal(-1), 'risk_data_unavailable_or_invalid'),
            ('round_trip_cost_per_lot', None, 'risk_data_unavailable_or_invalid'),
            ('last_trade_at', self.now, 'contract_unavailable'),
            ('limit_sell_available', False, 'contract_unavailable'),
            ('share_id', 'other-share', 'contract_specification_mismatch'),
            ('last_price', Decimal('289'), 'market_outside_protection_levels'),
            ('last_price', Decimal('300.25'), 'price_off_tick'),
            ('quote_at', self.now - timedelta(seconds=61), 'stale_or_future_risk_data'),
            ('observed_at', self.now + timedelta(minutes=1), 'stale_or_future_risk_data'),
        ):
            with self.subTest(field=field, value=value):
                snapshot = deepcopy(self.snapshot)
                snapshot['contract'][field] = value
                self.assertEqual(self.reserve(directory, reader=lambda a, c: snapshot), Admission(reason=reason))
        for field, value, reason in (
            ('account_id', 'different-account', 'account_mismatch'),
            ('equity', Decimal(0), 'risk_data_unavailable_or_invalid'),
            ('account_observed_at', self.now - timedelta(seconds=61), 'stale_or_future_risk_data'),
            ('has_position', True, 'account_has_exposure_or_unresolved_execution'),
            ('has_active_orders', True, 'account_has_exposure_or_unresolved_execution'),
            ('execution_uncertain', True, 'account_has_exposure_or_unresolved_execution'),
        ):
            with self.subTest(field=field):
                snapshot = self.snapshot | {field: value}
                self.assertEqual(self.reserve(directory, reader=lambda a, c: snapshot), Admission(reason=reason))
        self.assertEqual(self.reserve(directory, allowed=set()), Admission(reason='contract_not_allowed'))
        self.assertEqual(self.reserve(self.approved(stop_loss=290.25)), Admission(reason='price_off_tick'))

    def test_same_proposal_id_does_not_authorize_a_changed_stop(self) -> None:
        directory = self.approved()
        path = directory / 'trace.json'
        trace = json.loads(path.read_bytes())
        trace['approved_proposal']['stop_loss'] = 299.5
        trace['calls'][-2]['output']['stop_loss'] = 299.5
        trace['calls'][-1]['input']['proposal']['stop_loss'] = 299.5
        path.write_text(json.dumps(trace), encoding='utf-8')
        self.assertEqual(self.reserve(directory), Admission(reason='proposal_not_reviewed'))

    def test_already_constructed_snapshot_is_revalidated_at_the_boundary(self) -> None:
        directory = self.approved()
        snapshot = RiskSnapshot.model_validate(self.snapshot)
        contract = ContractSnapshot.model_validate(self.snapshot['contract'])
        invalid = snapshot.model_copy(update={
            'contract': contract.model_copy(update={'contracts_per_lot': -2}),
        })
        self.assertEqual(self.reserve(directory, reader=lambda a, c: invalid),
                         Admission(reason='risk_data_unavailable_or_invalid'))

    def test_observation_approval_cannot_be_reused_in_sandbox(self) -> None:
        directory = self.approved()
        path = directory / 'trace.json'
        trace = json.loads(path.read_bytes())
        trace['mode'] = 'observation'
        path.write_text(json.dumps(trace), encoding='utf-8')
        self.assertEqual(self.reserve(directory), Admission(reason='sandbox_approval_required'))

    def test_historical_publication_is_context_only_even_when_fresh(self) -> None:
        self.assertEqual(self.reserve(self.approved(historical=True)), Admission(reason='publication_context_only'))

    def test_rechecks_latest_source_after_reading_broker_data(self) -> None:
        directory = self.approved()

        def read_snapshot(account: str, contract: str) -> object:
            self.assertEqual((account, contract), (self.account_id, self.contract_id))
            archive = Archive(self.publications_db)
            try:
                archive.save_batch('rbc', 2, [replace(self.publication, text='', change='delete')])
            finally:
                archive.db.close()
            return self.snapshot

        self.assertEqual(self.reserve(directory, reader=read_snapshot),
                         Admission(reason='publication_revision_unconfirmed'))

    def test_edit_with_empty_text_revokes_the_previous_revision(self) -> None:
        directory = self.approved()
        archive = Archive(self.publications_db)
        try:
            archive.save_batch('rbc', 2, [replace(self.publication, text='', change='edit')])
        finally:
            archive.db.close()
        self.assertEqual(self.reserve(directory), Admission(reason='publication_revision_unconfirmed'))

    def test_delayed_edit_cannot_supersede_a_known_deletion(self) -> None:
        directory = self.approved()
        archive = Archive(self.publications_db)
        try:
            archive.save_batch('rbc', 2, [replace(self.publication, text='', change='delete')])
            archive.save_batch('rbc', 3, [replace(self.publication, text='Delayed edit.', change='edit',
                                                  edited_at=self.now)])
            latest = archive.latest_revision(self.publication.record()['id'])
            assert latest is not None
            self.assertEqual(latest['publication']['change'], 'delete')
        finally:
            archive.db.close()
        self.assertEqual(self.reserve(directory), Admission(reason='publication_revision_unconfirmed'))

    def test_new_retraction_requires_a_new_analysis(self) -> None:
        directory = self.approved()
        with EventArchive(self.events_db) as events:
            original = events.history()[0]
            publication = replace(self.publication, external_id='2', url='https://t.me/rbc/2',
                                  text='The new earnings forecast was withdrawn.').record()
            endpoint = ScriptedEndpoint([{
                'match': 'same', 'event_id': original['event_id'], 'change': 'retraction',
                'reason': 'The company retracted the forecast.', 'evidence': None,
            }])
            update = events.match(publication, [company_link(publication, 'sber')],
                                  RoleSettings(model='test/matcher'), model_factory=endpoint.model)
            self.assertEqual(update['kind'], 'update')
        self.assertEqual(self.reserve(directory), Admission(reason='event_context_superseded'))

    def test_proposal_expires_during_the_fresh_account_read(self) -> None:
        directory = self.approved()
        trace = json.loads((directory / 'trace.json').read_bytes())
        formed = datetime.fromisoformat(trace['calls'][0]['formed_at'])
        for age, reason in (
            (timedelta(microseconds=-1), 'stale_or_future_proposal'),
            (timedelta(seconds=60, microseconds=1), 'stale_or_future_proposal'),
            (timedelta(seconds=60), None),
        ):
            with self.subTest(age=age):
                checked = formed + age
                def read_snapshot(account: str, contract: str) -> object:
                    clock.return_value = checked
                    self.snapshot['account_observed_at'] = checked
                    self.snapshot['contract'].update(observed_at=checked, quote_at=checked)
                    return self.snapshot

                with patch('news_futures.trading.risk.utc_now', return_value=formed) as clock:
                    result = self.reserve(directory, reader=read_snapshot)
                self.assertEqual(result.reason, reason)

    def test_stale_and_future_publication_cannot_enter(self) -> None:
        for age in (timedelta(minutes=15, seconds=1), -timedelta(minutes=1)):
            with self.subTest(age=age):
                identifier = str(len(list(self.root.iterdir())))
                self.publication = replace(self.publication, external_id=identifier,
                                           url=f'https://t.me/rbc/{identifier}',
                                           published_at=self.now - age)
                self.assertEqual(self.reserve(self.approved()),
                                 Admission(reason='stale_or_unverified_publication_time'))

    def test_concurrent_runs_share_one_durable_reservation(self) -> None:
        directory = self.approved()
        other_directory = self.approved()
        start = Barrier(2)

        def try_entry(path: Path) -> Admission:
            start.wait(timeout=5)
            return self.reserve(path)

        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(try_entry, [directory, other_directory]))
        self.assertCountEqual([result.reason for result in results], [None, 'entry_already_reserved'])
        self.assertEqual(self.reserve(directory), Admission(reason='entry_already_reserved'))
        winner = next(result.reservation for result in results if result.reservation is not None)
        with EventArchive(self.events_db) as events:
            self.assertFalse(cancel_unsubmitted(events, winner.model_copy(update={'id': 'wrong'})))
            self.assertTrue(cancel_unsubmitted(events, winner))
        self.assertIsNotNone(self.reserve(other_directory).reservation)

    def test_same_day_losses_do_not_block_a_new_entry(self) -> None:
        # Control equity after twelve realized losses of 500 RUB on the same day.
        self.snapshot['equity'] = Decimal('94000')
        result = self.reserve(self.approved())
        assert result.reservation is not None
        self.assertEqual(result.reservation.planned_risk, Decimal('500'))
