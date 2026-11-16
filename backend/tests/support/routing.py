"""Jev fixtures shared by routing and workflow tests."""

def answer(choice: str, probability: float = 1.0) -> dict[str, object]:
    probabilities = dict.fromkeys(('direct', 'industry', 'commodity', 'regulation', 'uncertain', 'none'), 0.0)
    probabilities[choice] = probability
    probabilities['none' if choice != 'none' else 'direct'] = 1.0 - probability
    return {'type': 'choice', 'choice': choice, 'probabilities': probabilities, 'confidence': probability}
