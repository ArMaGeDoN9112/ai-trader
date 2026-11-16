"""Select supporting evidence for the original reason for entry."""

from typing import Any

from news_futures.domain.company import CompanyProposal, EntrySource


def entry_sources(proposal: CompanyProposal, context: dict[str, Any]) -> list[EntrySource]:
    publications = {row['revision']: row['publication'] for row in context['related_news']}
    publications[context['event']['revision']] = context['publication']
    return [
        EntrySource(revision=ref.revision, url=source['url'], quote=ref.quote)
        for assessment in proposal.assessments if assessment.contract_id == proposal.contract_id
        for ref in assessment.evidence
        if (source := publications.get(ref.revision)) and ref.quote in source['text']
    ]
