"""Archived publication evidence for event tests."""

from typing import Any

from news_futures.common.artifacts import publication_digest

def company_link(record: dict[str, Any], company: str) -> dict[str, Any]:
    digest = publication_digest(record)
    return {
        'company_id': company, 'publication_id': record['id'], 'publication_url': record['url'],
        'publication_revision_sha256': digest, 'publication_sha256': digest, 'catalog_sha256': 'a' * 64,
        'kind': 'direct', 'basis': 'direct', 'score': 0.99, 'confidence': 0.9,
        'requires_analyst_review': False,
    }
