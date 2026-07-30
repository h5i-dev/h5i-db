"""Replay a public account's filled trades against the historical book.

The strictest realism question a backtester can be asked: given the trades an
account actually took, does the engine reproduce the same portfolio? The answer
is usually no, and that is the point. A ledger is a record of fills that already
happened, so replaying it as *intent* asks the historical book to accept each
order on its own merits. A simulator that forced the fills would reproduce the
ledger by construction and test nothing.

So the loader compiles a ledger into commands, not into fills:

- each row becomes a limit order at the price the account got, so the book can
  refuse it if the liquidity was not there;
- `immediate_or_cancel`, because a ledger row is a fill that happened at an
  instant, not a resting order with unknown lifetime;
- sells are `reduce_only`, so a replay cannot invent short exposure the ledger
  never showed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional, Sequence

import pyarrow as pa

from ._markets import MarketSpec, token_index

__all__ = ["LedgerRow", "commands_from_ledger", "compare_to_ledger", "ledger_table"]


@dataclass(frozen=True)
class LedgerRow:
    """One filled trade as a public ledger reports it."""

    ts_ns: int
    instrument_id: str
    outcome: int
    side: str
    quantity: float
    price: float
    trade_id: Optional[str] = None

    def __post_init__(self) -> None:
        if self.side not in ("buy", "sell"):
            raise ValueError(f"side must be buy or sell, got {self.side!r}")
        if self.quantity <= 0:
            raise ValueError("a ledger row with non-positive quantity is not a fill")
        if not 0.0 < self.price < 1.0:
            raise ValueError(
                f"{self.instrument_id}: price {self.price} is outside (0, 1); "
                "event-contract prices are probabilities"
            )


def _coerce_rows(
    rows: Iterable[Mapping[str, Any]],
    markets: Sequence[MarketSpec],
    *,
    token_field: str = "asset_id",
    instrument_field: str = "instrument_id",
    outcome_field: str = "outcome",
    time_field: str = "timestamp",
    side_field: str = "side",
    size_field: str = "size",
    price_field: str = "price",
    trade_id_field: str = "transaction_hash",
) -> list[LedgerRow]:
    """Vendor ledger dicts into typed rows, resolved against the market specs.

    A row identifies its outcome either by vendor token or by an explicit
    instrument/outcome pair. Anything else is refused: guessing which side of a
    binary market a fill belonged to would silently invert the position.
    """
    from ._markets import _as_nanos

    tokens = token_index(markets)
    by_id = {spec.instrument_id: spec for spec in markets}
    out: list[LedgerRow] = []
    for index, row in enumerate(rows):
        token = row.get(token_field)
        if token is not None and str(token) in tokens:
            spec, outcome = tokens[str(token)]
            instrument_id = spec.instrument_id
        else:
            instrument_id = str(row.get(instrument_field) or "")
            if instrument_id not in by_id:
                raise KeyError(
                    f"ledger row {index}: cannot resolve to a market; it carries "
                    f"neither a known {token_field} nor a known {instrument_field}"
                )
            spec = by_id[instrument_id]
            raw_outcome = row.get(outcome_field)
            if raw_outcome is None:
                raise KeyError(
                    f"ledger row {index}: {instrument_id} has "
                    f"{spec.outcome_count} outcomes and the row names none"
                )
            if isinstance(raw_outcome, str) and raw_outcome in spec.outcome_labels:
                outcome = spec.outcome_labels.index(raw_outcome)
            else:
                outcome = int(raw_outcome)
            if not 0 <= outcome < spec.outcome_count:
                raise ValueError(
                    f"ledger row {index}: outcome {outcome} is not one of "
                    f"0..{spec.outcome_count - 1}"
                )
        stamp = _as_nanos(row.get(time_field))
        if stamp is None:
            raise KeyError(f"ledger row {index}: no {time_field}")
        side = str(row.get(side_field, "")).strip().lower()
        out.append(
            LedgerRow(
                ts_ns=stamp,
                instrument_id=instrument_id,
                outcome=outcome,
                side=side,
                quantity=float(row[size_field]),
                price=float(row[price_field]),
                trade_id=str(row[trade_id_field]) if row.get(trade_id_field) else None,
            )
        )
    return sorted(out, key=lambda item: (item.ts_ns, item.instrument_id, item.outcome))


def ledger_table(rows: Sequence[LedgerRow]) -> pa.Table:
    """The ledger itself as a table, for comparing against what replayed."""
    return pa.table(
        {
            "ts_ns": pa.array([row.ts_ns for row in rows], pa.int64()),
            "instrument_id": pa.array([row.instrument_id for row in rows], pa.string()),
            "outcome": pa.array([row.outcome for row in rows], pa.uint16()),
            "side": pa.array([row.side for row in rows], pa.string()),
            "quantity": pa.array([row.quantity for row in rows], pa.float64()),
            "price": pa.array([row.price for row in rows], pa.float64()),
            "trade_id": pa.array([row.trade_id for row in rows], pa.string()),
        }
    )


def commands_from_ledger(
    rows: Iterable[Mapping[str, Any]] | Sequence[LedgerRow],
    markets: Sequence[MarketSpec],
    *,
    submit_delay_ns: int = 1_000,
    **field_names: str,
) -> pa.Table:
    """Compile a ledger into a commands table the engine can replay.

    `submit_delay_ns` shifts each order past the instant it was decided from,
    which is the same rule any signal follows: an order sharing a timestamp with
    a book event may match the previous snapshot, so submitting just after the
    fill's own instant is both deterministic and the honest reading. It defaults
    to one microsecond.
    """
    from ..backtest import command_table

    typed = (
        list(rows)
        if rows and isinstance(next(iter(rows)), LedgerRow)
        else _coerce_rows(rows, markets, **field_names)  # type: ignore[arg-type]
    )
    if submit_delay_ns < 0:
        raise ValueError("submit_delay_ns must not be negative")
    commands = [
        {
            "ts": row.ts_ns + submit_delay_ns,
            "action": "submit",
            "instrument_id": row.instrument_id,
            "outcome": row.outcome,
            "side": row.side,
            "quantity": row.quantity,
            "kind": "limit",
            "limit_price": row.price,
            "time_in_force": "ioc",
            "reduce_only": row.side == "sell",
            "client_order_id": row.trade_id or f"ledger-{row.ts_ns}-{row.outcome}",
            "tag": "ledger-replay",
        }
        for row in typed
    ]
    return command_table(commands)


def compare_to_ledger(
    result: Any,
    rows: Sequence[LedgerRow],
    *,
    quantity_tolerance: float = 1e-9,
) -> dict[str, Any]:
    """Compare a replay's fills against the ledger it was compiled from.

    Reports per-market reconciliation rather than a single pass/fail, because
    the interesting output is *where* the book refused. A ledger row with no
    matching fill means the historical book would not have accepted that order
    at that price, which is information about the ledger and about the data.
    """
    fills = result.fills.to_pandas()
    wanted: dict[tuple[str, int], dict[str, float]] = {}
    for row in rows:
        key = (row.instrument_id, row.outcome)
        entry = wanted.setdefault(key, {"buy": 0.0, "sell": 0.0, "notional": 0.0})
        entry[row.side] += row.quantity
        entry["notional"] += row.quantity * row.price

    got: dict[tuple[str, int], dict[str, float]] = {}
    for record in fills.itertuples():
        key = (record.instrument_id, int(record.outcome))
        entry = got.setdefault(key, {"buy": 0.0, "sell": 0.0, "notional": 0.0})
        entry[str(record.side)] += float(record.quantity)
        entry["notional"] += float(record.quantity) * float(record.price)

    markets = []
    for key in sorted(set(wanted) | set(got)):
        expected = wanted.get(key, {"buy": 0.0, "sell": 0.0, "notional": 0.0})
        actual = got.get(key, {"buy": 0.0, "sell": 0.0, "notional": 0.0})
        markets.append(
            {
                "instrument_id": key[0],
                "outcome": key[1],
                "ledger_buy": expected["buy"],
                "ledger_sell": expected["sell"],
                "replay_buy": actual["buy"],
                "replay_sell": actual["sell"],
                "ledger_notional": expected["notional"],
                "replay_notional": actual["notional"],
                "reproduced": (
                    abs(expected["buy"] - actual["buy"]) <= quantity_tolerance
                    and abs(expected["sell"] - actual["sell"]) <= quantity_tolerance
                ),
            }
        )
    ledger_quantity = sum(row.quantity for row in rows)
    replay_quantity = float(fills.quantity.sum()) if len(fills) else 0.0
    return {
        "ledger_rows": len(rows),
        "replay_fills": int(len(fills)),
        "ledger_quantity": ledger_quantity,
        "replay_quantity": replay_quantity,
        "fill_ratio": (replay_quantity / ledger_quantity) if ledger_quantity else None,
        "markets_reproduced": sum(1 for row in markets if row["reproduced"]),
        "markets": markets,
        "orders_without_fills": result.explain().get("orders_without_fills"),
    }
