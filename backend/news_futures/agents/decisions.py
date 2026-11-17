"""Validate company proposals against archived evidence."""

from typing import Any

from news_futures.agents.events import model_publication
from news_futures.agents.schemas import ProposalEvidence
from news_futures.common.errors import RuntimeErrorInfo
from news_futures.schemas.company import CompanyProposal
from news_futures.schemas.contracts import check_levels


CONTEXT_RULES = """Never change the configured instruments, settings or risk
limits. Do not invent facts or claim authorization.

Use the incoming publication, event assessment and company links, related
original news, timestamped market and account snapshots, position and original
entry reason. Links, event matches and model assessments are hypotheses. Check
direct, inferred, uncertain and missing links against the original texts. State
the concrete company exposure, mechanism and remaining uncertainty. Classify
impact as unverified when evidence is insufficient, including when the context
was limited. Never treat omitted material as evidence of absence. Distinguish
original claims from corrections and repetitions."""

ANALYST_RULES = """Analyst: assess each supplied candidate contract exactly once. In each assessment
give a comparative reason based on the news and market, and cite exact quotes
with the source revision. Use event.revision for the incoming publication and
related_news[].revision for earlier sources. Copy a continuous excerpt verbatim,
including punctuation. For a skip with unverified impact, use an empty evidence
list when you have no exact supporting quote. Mark impact verified only when the supplied evidence
substantiates the impact of the incoming news on that company. Without a position,
compare all candidates and choose at most one allowed long or short entry, or
skip with null company_id and contract_id. Give a comparative reason for the
choice. For entry justify each of entry, stop-loss and take-profit in
level_rationale; otherwise set level_rationale to null. Prices must
be positive and ordered SL < entry < TP for long and TP < entry < SL for short.
Do not specify quantity. With a position, propose hold or full close of that
position only, never rotation, increase, partial close or reversal. Compare new
evidence with the saved entry reason. A close needs new evidence concerning the
held company that justifies closing. Unrelated news is insufficient. For every
action except enter, side and all price fields must be null."""

CRITIC_RULES = """Critic: independently verify the quotes, company exposure, uncertainty,
contradictions, comparison, choice and rationale for the entry and exit levels,
including whether close is supported by
the news about the held company and the saved entry reason. Approve only the
exact proposal ID and only without material objections. Otherwise refuse with
nonempty objections. On revision, the analyst must address every objection or
skip/hold. Return only the required JSON schema.
"""

def news_record(row: dict[str, Any]) -> dict[str, Any]:
    return {key: row[key] for key in ('decision_id', 'revision', 'event_id', 'kind', 'basis_at',
                                     'assessment', 'links')} | {
        'publication': model_publication(row['publication']), 'correction': row.get('correction'),
    }


def check_company_proposal(proposal: CompanyProposal, context: ProposalEvidence) -> CompanyProposal:
    action = proposal.action
    candidates = {candidate.contract_id: candidate for candidate in context.candidates}
    assessments = {row.contract_id: row for row in proposal.assessments}
    if (proposal.publication_id != context.publication.id
            or assessments.keys() != candidates.keys()
            or len(assessments) != len(proposal.assessments)):
        raise ValueError('invalid_candidate_comparison')
    if action not in {'enter', 'skip'}:
        raise ValueError('action_incompatible_with_position')
    check_levels(action, proposal.side, proposal.entry_price, proposal.stop_loss, proposal.take_profit)
    if (proposal.level_rationale is not None) != (action == 'enter'):
        raise ValueError('invalid_level_rationale')

    trigger = context.event.revision
    texts = {row.revision: row.publication.text for row in context.related_news}
    texts[trigger] = context.publication.text
    checked = []
    for assessment in proposal.assessments:
        evidence = [ref for ref in assessment.evidence if ref.quote in texts.get(ref.revision, '')]
        if action == 'skip' and assessment.impact == 'unverified':
            assessment = assessment.model_copy(update={'evidence': evidence})
        elif len(evidence) != len(assessment.evidence):
            raise RuntimeErrorInfo('unverifiable_source_quote')
        if assessment.impact == 'verified' and not any(ref.revision == trigger for ref in assessment.evidence):
            raise ValueError('verified_impact_requires_incoming_evidence')
        checked.append(assessment)

    contract_id = proposal.contract_id
    if action == 'skip':
        if contract_id is not None or proposal.company_id is not None:
            raise ValueError('skip_must_not_select_contract')
        return proposal.model_copy(update={'assessments': checked})
    if contract_id is None:
        raise ValueError('unknown_proposal_reference')
    candidate = candidates.get(contract_id)
    if candidate is None or proposal.company_id != candidate.company_id:
        raise ValueError('unknown_proposal_reference')
    if assessments[contract_id].impact != 'verified' or not candidate.allowed_for_entry:
        raise ValueError('action_requires_verified_company_impact')
    return proposal
