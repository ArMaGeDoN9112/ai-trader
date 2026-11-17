"""Verify that saved sandbox approval refers to the exact reviewed proposal."""

from datetime import datetime
import json
from pathlib import Path
from typing import Any

from news_futures.application.catalog import catalog_contracts
from news_futures.application.analysis.team_dto import AgentTrace, AnalystInput, CriticTask
from news_futures.application.artifact_paths import read_observation
from news_futures.common.errors import Rejected
from news_futures.domain.company import CompanyProposal, Instrument
from news_futures.domain.contracts import Review
from news_futures.domain.publications import publication_digest


def check_observation(directory: Path, instrument: Instrument, formed_at: datetime) -> None:
    if observation_id := instrument.market_observation_id:
        quote = read_observation(directory, observation_id)
        if (quote.contract_id != instrument.contract_id or quote.market != instrument.market
                or quote.received_at > formed_at):
            raise Rejected('proposal_not_reviewed')


def read_agent_approval(directory: Path, trace: AgentTrace) -> tuple[CompanyProposal, str, datetime, dict[str, Any]]:
    from news_futures.application.analysis.decisions import check_company_proposal

    outcome = trace.outcome
    if outcome is None or outcome.status != 'candidate' or outcome.critic_status != 'approved' or outcome.proposal is None:
        raise Rejected('proposal_not_reviewed')
    payload = outcome.proposal
    proposal_id = payload.id
    proposal = CompanyProposal.model_validate(payload.model_dump(exclude={'id'}))
    if proposal.action != 'enter':
        raise Rejected('entry_proposal_required')
    analyst = next((call for call in reversed(trace.calls) if call.kind == 'analyst'
                    and call.status == 'completed' and call.output == payload), None)
    critic = next((call for call in reversed(trace.calls) if call.kind == 'critic'
                   and call.status == 'completed' and isinstance(call.input, CriticTask)
                   and call.input.proposal.id == proposal_id), None)
    if (analyst is None or not isinstance(analyst.input, AnalystInput) or analyst.at is None
            or critic is None or not isinstance(critic.input, CriticTask) or not isinstance(critic.output, Review)):
        raise Rejected('proposal_not_reviewed')
    review = critic.output
    evidence = analyst.input.context
    context = evidence.model_dump(mode='json')
    if (critic.input.proposal != payload or critic.input.context != evidence or evidence.account is None
            or not review.approved or review.objections or review.proposal_id != proposal_id
            or evidence.event.revision != trace.revision
            or publication_digest({'revision': trace.revision, 'account_id': trace.account_id, 'context': context,
                                   'proposal': proposal.model_dump(mode='json')}) != proposal_id):
        raise Rejected('proposal_not_reviewed')
    check_company_proposal(proposal, evidence)
    inputs = directory.parent
    instrument = next(item for item in evidence.candidates if item.contract_id == proposal.contract_id)
    formed_at = datetime.fromisoformat(analyst.at)
    if instrument.market_observation_id != outcome.observation_id:
        raise Rejected('proposal_not_reviewed')
    check_observation(inputs, instrument, formed_at)
    runtime = json.loads((inputs / 'runtime.json').read_bytes())
    account = runtime['account_id']
    if (runtime['mode'] not in ('observation', 'sandbox') or runtime['analysis_strategy'] != 'tool_agent'
            or not isinstance(account, str) or not account or account != trace.account_id):
        raise Rejected('invalid_approval_record')
    contracts = catalog_contracts((inputs / 'catalog.json').read_bytes(), (inputs / 'futures-catalog.json').read_bytes())
    selection = json.loads((inputs / 'instrument.json').read_bytes())
    if (proposal.contract_id is None or contracts.get(proposal.contract_id) != proposal.company_id
            or selection['status'] != 'completed' or selection['error'] is not None
            or selection['selected']['contract_id'] != proposal.contract_id
            or selection['selected']['company_id'] != proposal.company_id):
        raise Rejected('invalid_approval_record')
    return proposal, proposal_id, formed_at, context | {
        'account': context['account'] | {'id': account},
    }


def read_approval(directory: Path) -> tuple[CompanyProposal, str, datetime, dict[str, Any]]:
    try:
        content = (directory / 'trace.json').read_bytes()
        trace = json.loads(content)
        if not isinstance(trace, dict):
            raise Rejected('invalid_approval_record')
        if trace.get('strategy') == 'tool_agent':
            return read_agent_approval(directory, AgentTrace.model_validate_json(content))
        if trace['mode'] != 'sandbox' or trace['status'] != 'approved_for_checks' or trace['error'] is not None:
            raise Rejected('sandbox_approval_required')
        payload = trace['approved_proposal']
        count = len(trace['calls'])
        analyst, critic = [
            json.loads((directory / f'call-{index:02}.json').read_bytes())
            for index in (count - 1, count)
        ]
        proposal_id = payload['id']
        if (not isinstance(proposal_id, str) or not proposal_id.strip()
                or analyst['role'] != 'analyst' or critic['role'] != 'critic'
                or analyst['mode'] != 'sandbox' or critic['mode'] != 'sandbox'
                or analyst['error'] is not None or critic['error'] is not None
                or publication_digest(payload) != publication_digest(analyst['output'])
                or publication_digest(payload) != publication_digest(critic['input']['proposal'])
                or analyst['input']['context'] != critic['input']['context']):
            raise Rejected('proposal_not_reviewed')
        review = Review.model_validate(critic['output'])
        if not review.approved or review.objections or review.proposal_id != proposal_id:
            raise Rejected('proposal_not_reviewed')
        proposal = CompanyProposal.model_validate({key: value for key, value in payload.items() if key != 'id'})
        formed_at = datetime.fromisoformat(analyst['formed_at'])
        for candidate in critic['input']['context']['candidates']:
            if candidate['contract_id'] == proposal.contract_id and 'market_observation_id' in candidate:
                check_observation(directory.parent.parent, Instrument.model_validate_json(json.dumps(candidate)), formed_at)
    except Rejected:
        raise
    except (OSError, ValueError, KeyError, TypeError, IndexError, OverflowError) as error:
        raise Rejected('invalid_approval_record') from error
    return proposal, proposal_id, formed_at, critic['input']['context']
