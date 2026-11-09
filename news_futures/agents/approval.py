"""Verify that saved sandbox approval refers to the exact reviewed proposal."""

from datetime import datetime
import json
from pathlib import Path
from typing import Any

from news_futures.agents.company import CompanyProposal
from news_futures.agents.contracts import Review
from news_futures.common.artifacts import publication_digest
from news_futures.common.errors import Rejected


def read_approval(directory: Path) -> tuple[CompanyProposal, str, datetime, dict[str, Any]]:
    try:
        trace = json.loads((directory / 'trace.json').read_bytes())
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
    except Rejected:
        raise
    except (OSError, ValueError, KeyError, TypeError, IndexError, OverflowError) as error:
        raise Rejected('invalid_approval_record') from error
    return proposal, proposal_id, formed_at, critic['input']['context']
