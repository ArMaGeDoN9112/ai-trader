"""Persist the source before processing fixed, explicitly simulated responses."""

import json
from pathlib import Path
from typing import Any

from news_futures.execution import simulate_execution


def write_json(path: Path, value: object) -> None:
    with path.open('x', encoding='utf-8') as output:
        json.dump(value, output, ensure_ascii=False, indent=2, allow_nan=False)
        output.write('\n')


def run(directory: Path) -> dict[str, Any]:
    control = json.loads(Path(__file__).with_name('control.json').read_text(encoding='utf-8'))
    directory.mkdir(parents=True, exist_ok=False)
    write_json(directory / 'publication.json', control['publication'])
    trace = control_trace(control)
    write_json(directory / 'trace.json', trace)
    return trace


def process_publication(publication: dict[str, Any]) -> dict[str, Any]:
    """Use the fixed 01 responses, never infer a real company or place an order."""
    control = json.loads(Path(__file__).with_name('control.json').read_text(encoding='utf-8'))
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
