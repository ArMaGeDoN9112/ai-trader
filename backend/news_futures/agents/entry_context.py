"""Persist the original reason for entry and its supporting evidence."""

import json
from typing import Any

from news_futures.agents.company import CompanyProposal, EntryReason, EntrySource
from news_futures.agents.events import EventArchive


def entry_sources(proposal: CompanyProposal, context: dict[str, Any]) -> list[EntrySource]:
    publications = {row['revision']: row['publication'] for row in context['related_news']}
    publications[context['event']['revision']] = context['publication']
    return [
        EntrySource(revision=ref.revision, url=source['url'], quote=ref.quote)
        for assessment in proposal.assessments if assessment.contract_id == proposal.contract_id
        for ref in assessment.evidence
        if (source := publications.get(ref.revision)) and ref.quote in source['text']
    ]


def record_entry_reason(archive: EventArchive, record: EntryReason) -> None:
    """Register the original reason once per account and position lifecycle."""
    with archive.db:
        archive.db.execute('BEGIN IMMEDIATE')
        save_entry_reason(archive, record)


def save_entry_reason(archive: EventArchive, record: EntryReason) -> None:
    archive.db.execute('''
        CREATE TABLE IF NOT EXISTS position_entry_reasons (
            account_id TEXT NOT NULL, position_id TEXT NOT NULL,
            contract_id TEXT NOT NULL, reason TEXT NOT NULL, sources TEXT NOT NULL DEFAULT '[]',
            PRIMARY KEY (account_id, position_id)
        )
    ''')
    existing = entry_reason(archive, record.account_id, record.position_id)
    if existing is None:
        archive.db.execute('INSERT INTO position_entry_reasons VALUES (?, ?, ?, ?, ?)', (
            record.account_id, record.position_id, record.contract_id, record.reason,
            json.dumps(record.model_dump(mode='json')['sources'], ensure_ascii=False),
        ))
    elif existing != record:
        raise ValueError('Причина входа уже зафиксирована для этой позиции')


def entry_reason(archive: EventArchive, account_id: str, position_id: str) -> EntryReason | None:
    if not archive.db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'position_entry_reasons'"
    ).fetchone():
        return None
    row = archive.db.execute(
        'SELECT * FROM position_entry_reasons WHERE account_id = ? AND position_id = ?',
        (account_id, position_id),
    ).fetchone()
    if row is None:
        return None
    data = dict(row)
    data['sources'] = json.loads(data['sources'])
    return EntryReason.model_validate(data)
