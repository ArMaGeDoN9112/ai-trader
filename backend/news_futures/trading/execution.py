"""Normalized broker observations and reconciliation invariants."""

from decimal import Decimal
from typing import Annotated, Literal

from pydantic import ConfigDict, Field, TypeAdapter
from pydantic.dataclasses import dataclass

from news_futures.trading.terms import EntryReservation


BROKER_DATA = ConfigDict(strict=True, revalidate_instances='always', extra='forbid')
Identifier = Annotated[str, Field(min_length=1)]
Lots = Annotated[int, Field(gt=0)]
Points = Annotated[Decimal, Field(gt=0, allow_inf_nan=False, max_digits=18, decimal_places=9)]


@dataclass(frozen=True, config=BROKER_DATA)
class Fill:
    order_id: Identifier
    lots: Lots
    price_points: Points


@dataclass(frozen=True, config=BROKER_DATA)
class Protection:
    id: Identifier
    kind: Literal['sl', 'tp']
    lots: Lots
    trigger_points: Points
    side: Literal['buy', 'sell']
    status: Literal['active', 'canceled', 'executed', 'expired'] = 'active'
    terms_confirmed: bool = True


@dataclass(frozen=True, config=BROKER_DATA)
class TradeState:
    """Normalized account-wide reconciliation of the cycle and its broker effects.

    Settled means every submitted request is accounted for, including rejected
    requests and activated stop children. An absent result alone is insufficient.
    Position is signed in lots. Fills are per exchange order, with average prices.
    Foreign positions, orders and stops are reported as other exposure.
    Cancellable orders are active exchange IDs confirmed to belong to this cycle.
    """

    settled: bool
    position_lots: int
    entry: Fill | None
    exits: tuple[Fill, ...]
    protection: tuple[Protection, ...]
    active_orders: bool
    other_exposure: bool
    cancellable_orders: tuple[Identifier, ...] = ()


TRADE_STATE = TypeAdapter(TradeState)


def reconciled_fills(entry: EntryReservation, state: TradeState) -> tuple[int, dict[str, list[int | str]]] | None:
    entry_lots = state.entry.lots if state.entry else 0
    remaining = entry_lots - sum(fill.lots for fill in state.exits)
    fills = ((state.entry,) if state.entry else ()) + state.exits
    sign = 1 if entry.side == 'long' else -1
    if (not 0 <= remaining <= entry_lots <= entry.lots or state.position_lots != sign * remaining
            or len({fill.order_id for fill in fills}) != len(fills)):
        return None
    return remaining, {fill.order_id: [fill.lots, str(fill.price_points.normalize())] for fill in fills}


def verified_protection(entry: EntryReservation, lots: int, active: tuple[Protection, ...]) -> set[tuple[str, Decimal]]:
    expected_side = 'sell' if entry.side == 'long' else 'buy'
    return {(stop.kind, stop.trigger_points) for stop in active
            if stop.lots == lots and stop.side == expected_side and stop.terms_confirmed}


def close_requests(operations: dict[str, str]) -> list[tuple[str, str]]:
    return [(action, key) for action, key in operations.items() if action.partition(':')[0] == 'close']
