from bisect import bisect_left, bisect_right
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean
from typing import Any

from news_futures.common.artifact_paths import read_observation
from news_futures.schemas.company import MarketObservation, SignalCandidate


HORIZONS_MINUTES = (1, 5, 15)


def replay(decisions: Path, quotes: Iterable[MarketObservation], *, max_outcome_lag: timedelta) -> dict[str, Any]:
    if max_outcome_lag <= timedelta():
        raise ValueError('max_outcome_lag must be positive')
    by_contract: dict[str, list[MarketObservation]] = defaultdict(list)
    for observation in quotes:
        by_contract[observation.contract_id].append(observation)
    received_times: dict[str, list[datetime]] = {}
    for contract_id, series in by_contract.items():
        series.sort(key=lambda quote: (quote.received_at, quote.market.observed_at, quote.id))
        received_times[contract_id] = [quote.received_at for quote in series]

    candidates = []
    for path in decisions.glob('*/candidates/*.json'):
        candidate = SignalCandidate.model_validate_json(path.read_bytes())
        if path.parent.parent.name != candidate.revision:
            raise ValueError(f'candidate_revision_mismatch: {path}')
        candidates.append(candidate)
    candidates.sort(key=lambda signal: (signal.decided_at, signal.revision, signal.proposal_id))

    moves: dict[tuple[str, int], list[float]] = defaultdict(list)
    totals: dict[str, int] = defaultdict(int)
    rows = []
    for signal in candidates:
        uid = signal.proposal.contract_id
        assert uid is not None
        baseline = read_observation(decisions / signal.revision, signal.observation_id)
        if baseline.contract_id != uid or not signal.received_at <= baseline.received_at <= signal.decided_at:
            raise ValueError(f'invalid_decision_observation: {signal.observation_id}')
        totals[signal.strategy] += 1
        series = by_contract.get(uid, [])
        times = received_times.get(uid, [])
        outcomes = []
        for minutes in HORIZONS_MINUTES:
            target = signal.decided_at + timedelta(minutes=minutes)
            start = bisect_left(times, target)
            end = bisect_right(times, target + max_outcome_lag, lo=start)
            quote = next((series[index] for index in range(start, end)
                          if series[index].market.observed_at >= target), None)
            outcome: dict[str, Any] = {'horizon_minutes': minutes, 'target_at': target.isoformat()}
            if quote is None:
                outcome['status'] = 'missing_quote'
            else:
                direction = 1 if signal.proposal.side == 'long' else -1
                markout = direction * (quote.market.last_price / baseline.market.last_price - 1) * 10_000
                moves[signal.strategy, minutes].append(markout)
                outcome.update(status='observed', observation_id=quote.id, markout_bps=markout,
                               observation=quote.model_dump(mode='json'))
            outcomes.append(outcome)
        rows.append({'candidate': signal.model_dump(mode='json'),
                     'decision_observation': baseline.model_dump(mode='json'), 'outcomes': outcomes})

    summary = []
    for strategy, total in sorted(totals.items()):
        for minutes in HORIZONS_MINUTES:
            values = moves[strategy, minutes]
            summary.append({'strategy': strategy, 'horizon_minutes': minutes, 'candidates': total,
                            'observed': len(values), 'missing': total - len(values),
                            'mean_markout_bps': mean(values) if values else None})
    return {'metric': 'last_price_markout_bps', 'max_outcome_lag_seconds': max_outcome_lag.total_seconds(),
            'summary': summary, 'signals': rows}
