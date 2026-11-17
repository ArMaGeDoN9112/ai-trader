"""Persist the source before processing fixed, explicitly simulated responses."""

import json
from pathlib import Path
from typing import Any

from news_futures.common.artifacts import write_json
from news_futures.common.paths import CONTROL_CASE


def run(directory: Path) -> dict[str, Any]:
    control = json.loads(CONTROL_CASE.read_text(encoding='utf-8'))
    directory.mkdir(parents=True, exist_ok=False)
    write_json(directory / 'publication.json', control['publication'])
    trace = control_trace(control)
    write_json(directory / 'trace.json', trace)
    return trace


def process_publication(publication: dict[str, Any]) -> dict[str, Any]:
    """Use the fixed 01 responses, never infer a real company or place an order."""
    control = json.loads(CONTROL_CASE.read_text(encoding='utf-8'))
    control['publication'] = publication
    control['clock'] = publication['received_at']
    for response in ('router_response', 'analyst_response'):
        control[response]['publication_id'] = publication['id']
    control['analyst_response']['id'] = f"simulation:{publication['id']}"
    control['critic_response']['proposal_id'] = control['analyst_response']['id']
    trace = control_trace(control)
    trace['notice'] = 'Fixed control responses; company links and trades are simulated, not news analysis.'
    return trace


def control_trace(control: dict[str, Any]) -> dict[str, Any]:

    roles = []
    link = control['router_response']
    roles.append({
        'role': 'router', 'mode': 'simulation',
        'input': {'publication_id': control['publication']['id'], 'catalog': control['catalog']},
        'output': link,
    })
    proposal = control['analyst_response']
    roles.append({
        'role': 'company_analyst', 'mode': 'simulation',
        'input': {'publication_id': control['publication']['id'], 'company_link': link},
        'output': proposal,
    })
    review = control['critic_response']
    roles.append({
        'role': 'critic', 'mode': 'simulation',
        'input': {'publication_id': control['publication']['id'], 'proposal': proposal},
        'output': review,
    })
    trace = {
        'mode': 'simulation', 'clock': control['clock'],
        'publication': control['publication'], 'catalog': control['catalog'],
        'roles': roles, **simulate_execution(proposal, review, control),
    }
    return trace


def check_proposal(
    proposal: object, review: object, context: dict[str, Any],
) -> dict[str, Any]:
    errors: list[str] = []
    if not isinstance(proposal, dict) or not isinstance(review, dict):
        return {'checker': 'local-program', 'accepted': False, 'errors': ['invalid_response']}

    catalog = context['catalog']
    link = context['router_response']
    if (proposal.get('publication_id') != context['publication']['id']
            or link.get('publication_id') != context['publication']['id']):
        errors.append('unknown_publication')
    if (proposal.get('company_id') != catalog['company']['id']
            or link.get('company_id') != catalog['company']['id']
            or catalog['share']['company_id'] != catalog['company']['id']
            or catalog['future']['underlying_share_id'] != catalog['share']['id']):
        errors.append('unknown_company_link')
    if proposal.get('contract_id') != catalog['future']['id']:
        errors.append('contract_not_allowed')
    if not isinstance(proposal.get('id'), str) or not proposal['id'].strip():
        errors.append('missing_proposal_id')
    if review.get('approved') is not True or review.get('proposal_id') != proposal.get('id'):
        errors.append('critic_not_approved')
    if proposal.get('side') != 'long':
        errors.append('unsupported_side')
    # shortcut: one control contract only; sizing and risk admission belong to ticket 08.
    if type(proposal.get('quantity')) is not int or proposal['quantity'] != 1:
        errors.append('invalid_quantity')
    stop = proposal.get('stop_loss')
    entry = proposal.get('entry_price')
    take = proposal.get('take_profit')
    if type(stop) is not int or type(entry) is not int or type(take) is not int:
        errors.append('invalid_prices')
    elif min(stop, entry, take) <= 0:
        errors.append('invalid_prices')
    elif not stop < entry < take:
        errors.append('invalid_protection_levels')
    if not isinstance(proposal.get('entry_reason'), str) or not proposal['entry_reason'].strip():
        errors.append('missing_entry_reason')
    return {'checker': 'local-program', 'accepted': not errors, 'errors': errors}


def simulate_execution(
    proposal: object, review: object, context: dict[str, Any],
) -> dict[str, Any]:
    check = check_proposal(proposal, review, context)
    execution: dict[str, Any] = {'mode': 'simulation', 'status': 'rejected'}
    if check['accepted'] and isinstance(proposal, dict):
        execution.update({
            'status': 'simulated_filled',
            'proposal_id': proposal['id'],
            'publication_id': proposal['publication_id'],
            'company_id': proposal['company_id'],
            'contract_id': proposal['contract_id'],
            'side': proposal['side'],
            'quantity': proposal['quantity'],
            'fill_price': proposal['entry_price'],
            'entry_reason': proposal['entry_reason'],
        })
    return {'check': check, 'execution': execution}
