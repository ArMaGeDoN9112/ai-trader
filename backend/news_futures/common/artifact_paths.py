"""Paths within one decision trace."""

from pathlib import Path

from news_futures.schemas.company import MarketObservation


def review_path(directory: Path, attempt: str) -> Path:
    return directory / 'review' / attempt


def market_path(directory: Path, observation_id: str) -> Path:
    return directory / 'market' / f'{observation_id}.json'


def read_observation(directory: Path, observation_id: str) -> MarketObservation:
    path = market_path(directory, observation_id)
    observation = MarketObservation.model_validate_json(path.read_bytes())
    if observation.id != observation_id:
        raise ValueError(f'invalid_decision_observation: {path}')
    return observation
