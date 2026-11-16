"""Telegram source with deterministic history and differences."""

from datetime import datetime, timedelta

from news_futures.adapters.telegram import Difference, Source
from news_futures.news.publications import Publication, utc_now

class ControlledTransport:
    def __init__(self) -> None:
        self.source = Source(123, 'example')
        now = utc_now()
        self.original = Publication(self.source.id, '1', 'https://t.me/example/1',
                                    'Историческая публикация', now - timedelta(days=1), now)
        self.messages = [self.original]
        self.differences: list[Difference] = []
        self.requests: list[int] = []
        self.windows: list[tuple[datetime, datetime]] = []
        self.pts = 10

    async def resolve(self, username: str) -> Source:
        if username == 'broken':
            raise OSError('sensitive phone api_hash session material')
        return self.source

    async def current_pts(self, source: Source) -> int:
        return self.pts

    async def history(self, source: Source, since: datetime, until: datetime) -> list[Publication]:
        self.windows.append((since, until))
        return self.messages

    async def difference(self, source: Source, pts: int) -> Difference:
        self.requests.append(pts)
        return self.differences.pop(0) if self.differences else Difference(pts, [])
