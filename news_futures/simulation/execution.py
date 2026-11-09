"""Independent program admission and an exclusively simulated executor."""

from typing import Any


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
