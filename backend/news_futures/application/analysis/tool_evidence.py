from datetime import datetime, timedelta
from difflib import SequenceMatcher
import re
from typing import Any

from news_futures.application.analysis.team_dto import (
    ComparisonObservation, ComparisonWindow, EventEvidence, EventVersion, EventVersionsObservation,
)
from news_futures.domain.company import MinuteCandle


MINUTE = timedelta(minutes=1)
REACTION_BASELINE = 5 * MINUTE


def event_versions(history: list[dict[str, Any]], event: EventEvidence, company: str,
                   key: str) -> EventVersionsObservation:
    rows = sorted((row for row in history if row['decision_id'] <= event.decision_id
                   and (row['event_id'] == event.event_id or row['publication']['id'] == event.publication.id)),
                  key=lambda row: row['decision_id'])
    previous = [row for row in rows if row['revision'] != event.revision]
    prior = next((row for row in reversed(previous) if row['publication']['id'] == event.publication.id),
                 previous[-1] if previous else None)

    def version(row: dict[str, Any]) -> EventVersion:
        publication = row['publication']
        raw_urls = re.findall(r'https?://[^\s<>"\)]+', publication['text'])
        urls = dict.fromkeys(url.rstrip('.,;:!?]') for url in (*publication.get('cited_urls', ()), *raw_urls)
                             if len(url) <= 300)
        return EventVersion(decision_id=row['decision_id'], revision=row['revision'], event_id=row['event_id'],
                            publication_url=publication['url'], published_at=publication['published_at'],
                            edited_at=publication['edited_at'], kind=row['kind'], cited_urls=list(urls)[:3])

    added: list[str] = []
    removed: list[str] = []
    truncated = False
    if prior:
        before = prior['publication']['text'].split()
        after = event.publication.text.split()
        truncated = len(before) > 1000 or len(after) > 1000
        for tag, left, right, start, end in SequenceMatcher(None, before[:1000], after[:1000]).get_opcodes():
            if tag == 'equal':
                continue
            for excerpts, words in ((added, after[start:end]), (removed, before[left:right])):
                if words:
                    fragment = ' '.join(words)
                    truncated |= len(fragment) > 200 or len(excerpts) >= 3
                    if len(excerpts) < 3:
                        excerpts.append(fragment[:200])
    return EventVersionsObservation(company_id=company, observation_id=key, earliest_archived=version(rows[0]),
                                    versions=[version(row) for row in rows[-3:]],
                                    compared_to=version(prior) if prior else None, added_text=added,
                                    removed_text=removed, excerpts_truncated=truncated or len(rows) > 3)


def compare_candles(series: dict[str, tuple[str, list[MinuteCandle]]], *, company: str, key: str,
                    published_at: datetime, received_at: datetime, end: datetime) -> ComparisonObservation:
    closes = {name: {bar.started_at + MINUTE: bar.close for bar in bars} for name, (_, bars) in series.items()}
    missing = [name for name, bars in closes.items() if not bars]
    available = {name: bars for name, bars in closes.items() if bars}
    common = sorted(set.intersection(*(set(bars) for bars in available.values()))) if 'future' in available else []

    def window(anchor: datetime) -> ComparisonWindow:
        baseline = max((at for at in common if anchor - REACTION_BASELINE + MINUTE <= at <= anchor), default=None)
        latest = max((at for at in common if anchor + MINUTE <= at <= end), default=None)
        change = {name: round((bars[latest] / bars[baseline] - 1) * 10_000, 2) for name, bars in available.items()
                  if baseline is not None and latest is not None}
        return ComparisonWindow(anchor_at=anchor, baseline_close_at=baseline, latest_close_at=latest, change_bps=change,
                                future_minus_reference_bps={name: round(change['future'] - value, 2)
                                                            for name, value in change.items() if name != 'future'})

    return ComparisonObservation(company_id=company, observation_id=key, as_of=end,
                                 instruments={name: uid for name, (uid, _) in series.items()},
                                 missing_instruments=missing,
                                 since_publication=window(published_at), since_receipt=window(received_at))
