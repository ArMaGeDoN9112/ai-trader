from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, Field, HttpUrl, model_validator

from news_futures.domain.contracts import Contract


CALENDAR = Path(__file__).with_name('events-calendar.json')
MOSCOW = timezone(timedelta(hours=3))


class ScheduledEvent(Contract):
    starts_at: AwareDatetime | date
    kind: Literal['macro', 'earnings', 'corporate']
    company_id: str | None
    title: str = Field(min_length=1, max_length=180)
    source_url: HttpUrl

    @model_validator(mode='after')
    def company_scope(self) -> 'ScheduledEvent':
        if (self.company_id is None) != (self.kind == 'macro') or self.company_id == '':
            raise ValueError('invalid_calendar_company_scope')
        return self


class EventCalendar(Contract):
    checked_on: date
    events: list[ScheduledEvent]

    def upcoming(self, company_id: str, now: datetime) -> list[ScheduledEvent]:
        midnight = datetime.combine(now.astimezone(MOSCOW).date(), time.min, MOSCOW)
        through = now + timedelta(days=45)
        eligible = []
        for item in self.events:
            if item.company_id not in (None, company_id):
                continue
            exact = isinstance(item.starts_at, datetime)
            start = item.starts_at if isinstance(item.starts_at, datetime) else datetime.combine(item.starts_at, time.min, MOSCOW)
            if (now if exact else midnight) <= start <= through:
                eligible.append((start, item))
        return [item for _, item in sorted(eligible, key=lambda entry: entry[0])[:8]]
