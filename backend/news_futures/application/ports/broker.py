"""Broker interfaces owned by the trading use cases."""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol

from news_futures.common.errors import RuntimeErrorInfo
from news_futures.domain.company import MarketObservation, MinuteCandle, OrderBook, Snapshot
from news_futures.domain.terms import EntryReservation


class SubmissionRejected(ValueError):
    """The broker definitively refused a submission before creating an order."""


class UnsupportedFuturesUnits(ValueError):
    """The contract uses units the executor cannot price safely."""


class NoSupportedFutures(RuntimeErrorInfo):
    """The account snapshot has no supported, unexpired contracts."""


@dataclass(frozen=True)
class MarketReading:
    snapshot: Snapshot
    observation: MarketObservation


class ReadMarket(Protocol):
    def __call__(self, contract_id: str, timeout: float, /) -> MarketReading: ...


class ReadCandles(Protocol):
    def __call__(self, contract_id: str, start: datetime, end: datetime, timeout: float, /) -> list[MinuteCandle]: ...


class ReadComparison(Protocol):
    def __call__(self, contract_id: str, start: datetime, end: datetime, timeout: float, /) -> dict[str, tuple[str, list[MinuteCandle]]]: ...


class BrokerReader(Protocol):
    def account_activity(self) -> tuple[bool, bool]: ...

    def read_risk(self, account_id: str, contract_id: str) -> object: ...
    def read_order_book(self) -> OrderBook: ...
    def read_candles(self, start: datetime, end: datetime) -> list[MinuteCandle]: ...
    def read_reference_candles(self, instrument: str, start: datetime, end: datetime) -> list[MinuteCandle]: ...
    def resolve_share(self, position_uid: str) -> str: ...
    def read_trade(
        self, entry: EntryReservation, operations: dict[str, str], receipts: dict[str, str | None],
    ) -> object: ...


class SandboxBroker(BrokerReader, Protocol):
    @property
    def source(self) -> Literal['control', 'sandbox', 'sandbox_mcp']: ...

    @property
    def account_id(self) -> str: ...

    def enter(self, entry: EntryReservation, operation_id: str) -> str | None:
        """Submit a limit order in points, immediate fill-or-cancel in lots."""
        ...

    def protect(
        self, entry: EntryReservation, operation_id: str, lots: int, kind: Literal['sl', 'tp'],
    ) -> str | None:
        """Submit one native stop and return its broker ID."""
        ...

    def cancel_protection(self, entry: EntryReservation, operation_id: str, ids: tuple[str, ...]) -> None: ...
    def cancel_orders(self, entry: EntryReservation, operation_id: str, ids: tuple[str, ...]) -> None: ...
    def close(self, entry: EntryReservation, operation_id: str, lots: int) -> str | None: ...
