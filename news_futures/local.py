"""Persist the source before processing fixed, explicitly simulated responses."""

from hashlib import sha256
import json
from pathlib import Path
import sys
from time import monotonic, sleep
from typing import Any

from news_futures.execution import simulate_execution


def publication_digest(publication: dict[str, Any]) -> str:
    return sha256(json.dumps(publication, sort_keys=True, allow_nan=False).encode()).hexdigest()


def write_json(path: Path, value: object) -> None:
    with path.open('x', encoding='utf-8') as output:
        json.dump(value, output, ensure_ascii=False, indent=2, allow_nan=False)
        output.write('\n')


def replace_json(path: Path, value: object) -> None:
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + '\n', encoding='utf-8')
    move_artifact(temporary, path, replace=True)


def move_artifact(source: Path, target: Path, *, replace: bool = False) -> None:
    move = source.replace if replace else source.rename
    deadline = monotonic() + 0.5
    while True:
        try:
            move(target)
            return
        except PermissionError as error:
            # A Windows reader can briefly deny renaming an open artifact or its directory.
            if sys.platform != 'win32' or error.winerror not in (5, 32, 33) or monotonic() >= deadline:
                raise
            sleep(0.01)


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
