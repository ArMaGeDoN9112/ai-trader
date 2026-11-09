"""Read local credentials without echoing the file in diagnostics."""

from pathlib import Path
import tomllib
from typing import Any


CREDENTIALS_PATH = Path('.local/credentials.toml')


class CredentialsError(ValueError):
    pass


def load_credentials(section: str) -> dict[str, Any]:
    try:
        with CREDENTIALS_PATH.open('rb') as source:
            settings = tomllib.load(source)
    except FileNotFoundError:
        raise CredentialsError('Создайте .local/credentials.toml из credentials.example.toml.') from None
    except (OSError, ValueError):
        raise CredentialsError('Не удалось прочитать .local/credentials.toml. Проверьте формат TOML.') from None
    values = settings.get(section)
    if not isinstance(values, dict):
        raise CredentialsError(f'Заполните секцию [{section}] в .local/credentials.toml.')
    return values


def load_token(section: str, key: str) -> str:
    value = load_credentials(section).get(key)
    if not isinstance(value, str) or not value.strip() or any(char.isspace() for char in value.strip()):
        raise CredentialsError(f'Заполните {section}.{key} в .local/credentials.toml.')
    return value.strip()
