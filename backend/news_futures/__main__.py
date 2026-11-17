"""Entry point for python -m news_futures."""

from io import TextIOWrapper
import sys
from typing import cast

from news_futures.cli import main


if __name__ == '__main__':
    cast(TextIOWrapper, sys.stdout).reconfigure(encoding='utf-8')
    cast(TextIOWrapper, sys.stderr).reconfigure(encoding='utf-8')
    raise SystemExit(main())
