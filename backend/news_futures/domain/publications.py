"""Source-neutral publication identity, revision and timestamps."""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
from typing import Any


TEST_SOURCE = 'manual:test'


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Publication:
    source: str
    external_id: str
    url: str
    text: str
    published_at: datetime
    received_at: datetime
    edited_at: datetime | None = None
    unread_media: bool = False
    change: str = 'new'
    cited_urls: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.source or not self.external_id or self.change not in {'new', 'edit', 'delete'}:
            raise ValueError('Invalid publication identity or change')
        for value in (self.published_at, self.received_at, self.edited_at):
            if value is not None and (value.tzinfo is None or value.utcoffset() is None):
                raise ValueError('Publication times must have a timezone')
        if (len(self.cited_urls) > 12 or any(not isinstance(url, str) or len(url) > 300
                                           or not url.startswith(('https://', 'http://')) for url in self.cited_urls)):
            raise ValueError('Invalid publication links')

    def record(self) -> dict[str, Any]:
        result = asdict(self)
        if self.cited_urls:
            result['cited_urls'] = list(self.cited_urls)
        else:
            del result['cited_urls']
        result['id'] = f'{self.source}:{self.external_id}'
        for field in ('published_at', 'received_at', 'edited_at'):
            value = result[field]
            result[field] = value.astimezone(timezone.utc).isoformat() if value else None
        return result

    def revision_key(self) -> str:
        record = self.record()
        if self.change == 'delete':
            return hashlib.sha256(f'delete:{record["id"]}'.encode()).hexdigest()
        # Delivery time, username URLs and new/edit envelopes are not a new revision.
        content = [record[key] for key in (
            'source', 'external_id', 'published_at', 'edited_at', 'text', 'unread_media',
        )]
        if self.cited_urls:
            content.append(list(self.cited_urls))
        return hashlib.sha256(json.dumps(content, ensure_ascii=False).encode()).hexdigest()


def publication_digest(publication: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(publication, sort_keys=True, allow_nan=False).encode()).hexdigest()
