"""Declarative strategy adapters that compile to the canonical signals table."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Optional, Union

import pyarrow as pa

__all__ = ["from_signals", "target_positions"]


def _values(value: Any, length: int, name: str) -> list[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        return [value] * length
    out = list(value)
    if len(out) != length:
        raise ValueError(f"{name} has {len(out)} values; expected {length}")
    return out


def from_signals(
    timestamps: Sequence[Any],
    *,
    instrument_id: Union[str, Sequence[str]],
    entries: Sequence[bool],
    exits: Sequence[bool],
    size: Union[float, Sequence[float]] = 1.0,
    outcome: Union[int, Sequence[int]] = 0,
    entry_side: str = "buy",
    kind: str = "market",
    limit_price: Optional[Union[float, Sequence[Optional[float]]]] = None,
    time_in_force: Optional[str] = None,
    tag: Optional[str] = None,
    reduce_only_exit: bool = True,
) -> pa.Table:
    """Compile aligned entry/exit arrays into deterministic order intent.

    Timestamps must already represent feature availability. This function
    never shifts a signal or assumes bar-close execution.
    """
    from .backtest import signal_table

    times = list(timestamps)
    n = len(times)
    entry_flags = [bool(value) for value in _values(entries, n, "entries")]
    exit_flags = [bool(value) for value in _values(exits, n, "exits")]
    instruments = _values(instrument_id, n, "instrument_id")
    sizes = _values(size, n, "size")
    outcomes = _values(outcome, n, "outcome")
    prices = _values(limit_price, n, "limit_price")
    if entry_side not in ("buy", "sell"):
        raise ValueError("entry_side must be 'buy' or 'sell'")
    if kind not in ("market", "limit"):
        raise ValueError("kind must be 'market' or 'limit'")
    rows = []
    exit_side = "sell" if entry_side == "buy" else "buy"
    for index, timestamp in enumerate(times):
        if entry_flags[index] and exit_flags[index]:
            raise ValueError(f"entries and exits are both true at row {index}")
        if not entry_flags[index] and not exit_flags[index]:
            continue
        quantity = float(sizes[index])
        if quantity <= 0:
            raise ValueError(f"size must be positive at row {index}")
        is_exit = exit_flags[index]
        rows.append(
            {
                "ts": timestamp,
                "instrument_id": str(instruments[index]),
                "outcome": int(outcomes[index]),
                "side": exit_side if is_exit else entry_side,
                "quantity": quantity,
                "kind": kind,
                "limit_price": prices[index],
                "time_in_force": time_in_force,
                "tag": (
                    f"{tag}-exit"
                    if is_exit and tag
                    else f"{tag}-entry"
                    if tag
                    else None
                ),
                "reduce_only": bool(is_exit and reduce_only_exit),
            }
        )
    return signal_table(rows)


def target_positions(
    timestamps: Sequence[Any],
    targets: Sequence[float],
    *,
    instrument_id: Union[str, Sequence[str]],
    outcome: Union[int, Sequence[int]] = 0,
    initial_position: float = 0.0,
    tag: Optional[str] = "target-position",
) -> pa.Table:
    """Convert desired positions into the minimum sequence of market orders."""
    from .backtest import signal_table

    times = list(timestamps)
    desired = [float(value) for value in _values(targets, len(times), "targets")]
    instruments = _values(instrument_id, len(times), "instrument_id")
    outcomes = _values(outcome, len(times), "outcome")
    positions: dict[tuple[str, int], float] = {}
    rows = []
    for index, timestamp in enumerate(times):
        key = (str(instruments[index]), int(outcomes[index]))
        current = positions.get(key, float(initial_position))
        delta = desired[index] - current
        if delta == 0:
            continue
        rows.append(
            {
                "ts": timestamp,
                "instrument_id": key[0],
                "outcome": key[1],
                "side": "buy" if delta > 0 else "sell",
                "quantity": abs(delta),
                "kind": "market",
                "tag": tag,
                "reduce_only": False,
            }
        )
        positions[key] = desired[index]
    return signal_table(rows)
