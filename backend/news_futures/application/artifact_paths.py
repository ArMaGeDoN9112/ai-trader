"""Paths within one decision trace."""

from pathlib import Path


def review_path(directory: Path, attempt: str) -> Path:
    return directory / 'review' / attempt
