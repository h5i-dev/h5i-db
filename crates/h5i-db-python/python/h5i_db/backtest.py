"""Event-driven backtesting on versioned data (ROADMAP_QUANT.md Part B).

A run reads pinned market data, replays it deterministically, and writes its
results into a fork of the database. Both ends are tables, so nothing here
needs an API of its own:

    import h5i_db
    from h5i_db import backtest, quant

    db = h5i_db.Database("market.db")
    report = backtest.run(db, "momentum-001", starting_cash=10_000,
                          signals="signals", snapshot="2024-q1")

    fork = db.fork(report["fork"])
    quant.tearsheet(quant.from_levels(fork, "bt_equity"), path="run.html")

The strategy is the ``signals`` table: timestamped order intent, executed
through the full matching, fee, latency and queue path. Whatever produced
that table -- a factor pipeline, a notebook, an agent -- the kernel sees only
intent.
"""

from __future__ import annotations

import datetime as _dt
import json
from typing import Any, Optional, Sequence, Union

import pyarrow as pa

__all__ = [
    "run",
    "SIGNAL_SCHEMA",
    "signal_table",
    "create_signal_table",
    "MARKET_DATA_TABLES",
    "RESULT_TABLES",
]

#: Mirrors `crates/h5i-db-backtest/src/schema.rs::signals()`.
SIGNAL_SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("ns"), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("outcome", pa.uint16(), nullable=False),
        pa.field("side", pa.string(), nullable=False),
        pa.field("quantity", pa.float64(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("limit_price", pa.float64()),
        pa.field("time_in_force", pa.string()),
        pa.field("tag", pa.string()),
        pa.field("reduce_only", pa.bool_()),
    ]
)

#: Tables a run reads.
MARKET_DATA_TABLES = (
    "book_deltas",
    "trades",
    "bars",
    "instruments",
    "resolutions",
    "funding",
)

#: Tables a run writes, into its own fork.
RESULT_TABLES = ("bt_run", "bt_orders", "bt_fills", "bt_positions", "bt_equity")


def _to_nanos(value: Union[int, str, _dt.datetime]) -> int:
    if isinstance(value, bool):
        raise TypeError("a timestamp must not be a bool")
    if isinstance(value, int):
        return value
    if isinstance(value, _dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=_dt.timezone.utc)
        return int(value.timestamp() * 1_000_000_000)
    if isinstance(value, str):
        return _to_nanos(_dt.datetime.fromisoformat(value.replace("Z", "+00:00")))
    raise TypeError(f"cannot read {value!r} as a timestamp")


def signal_table(rows: Sequence[dict]) -> pa.Table:
    """Build a signals table from plain dicts.

    Each row needs ``ts``, ``instrument_id``, ``side`` and ``quantity``;
    ``kind`` defaults to ``market`` and ``outcome`` to 0. A ``limit`` kind
    requires ``limit_price`` -- it is not quietly downgraded to a market
    order, because guessing which the author meant is how a backtest trades
    at a price nobody asked for.
    """
    columns: dict = {name: [] for name in SIGNAL_SCHEMA.names}
    for index, row in enumerate(rows):
        missing = {"ts", "instrument_id", "side", "quantity"} - set(row)
        if missing:
            raise ValueError(f"signal {index} is missing {sorted(missing)}")
        kind = row.get("kind", "market")
        if kind not in ("market", "limit"):
            raise ValueError(f"signal {index} has unknown kind {kind!r}")
        if kind == "limit" and row.get("limit_price") is None:
            raise ValueError(f"signal {index} is a limit order with no limit_price")
        columns["ts"].append(_to_nanos(row["ts"]))
        columns["instrument_id"].append(str(row["instrument_id"]))
        columns["outcome"].append(int(row.get("outcome", 0)))
        columns["side"].append(str(row["side"]))
        columns["quantity"].append(float(row["quantity"]))
        columns["kind"].append(kind)
        limit = row.get("limit_price")
        columns["limit_price"].append(None if limit is None else float(limit))
        columns["time_in_force"].append(row.get("time_in_force"))
        columns["tag"].append(row.get("tag"))
        columns["reduce_only"].append(bool(row.get("reduce_only", False)))

    columns["ts"] = pa.array(columns["ts"], type=pa.timestamp("ns"))
    return pa.table(columns, schema=SIGNAL_SCHEMA)


def create_signal_table(db: Any, name: str = "signals") -> None:
    """Create the signals table if it does not exist."""
    if name not in db.tables():
        db.create_table(name, SIGNAL_SCHEMA, time_column="ts")


def run(
    db: Any,
    run_id: str,
    *,
    starting_cash: float,
    signals: str = "signals",
    fee_kind: Optional[str] = None,
    fee_rate: Optional[float] = None,
    maker_rebate: Optional[float] = None,
    maker_fee_rate: Optional[float] = None,
    queue_position: bool = False,
    optimistic_queue: bool = False,
    latency_nanos: Optional[int] = None,
    slippage_ticks: Optional[int] = None,
    window: Optional[tuple] = None,
    version: Optional[int] = None,
    as_of: Optional[str] = None,
    snapshot: Optional[str] = None,
    equity_interval_nanos: Optional[int] = None,
    minimum_coverage: Optional[float] = None,
) -> dict:
    """Replay ``signals`` against stored market data and record the run.

    Results land on a fork named ``bt-<run_id>``; the returned dict names it
    along with the run's digest, cash, fills and any warnings. A run is a
    pure function of (pin, signals, config), so two runs with the same
    arguments produce the same digest and the same numbers.

    ``queue_position`` puts resting orders behind the size already displayed
    at their price, which is the honest reading of an L2 feed.
    ``fee_kind="kalshi"`` applies Kalshi's centicent and per-order cash
    rounding. Set ``fee_rate`` from the applicable series fee schedule and,
    when the series charges makers, set ``maker_fee_rate`` as well.
    ``minimum_coverage`` refuses to run at all when the data covers less of
    the requested window than that.
    """
    if window is not None:
        if len(window) != 2:
            raise ValueError("window must be (start, end)")
        window = (_to_nanos(window[0]), _to_nanos(window[1]))
    native_args = (
        run_id,
        float(starting_cash),
        signals,
        fee_kind,
        fee_rate,
        maker_rebate,
        queue_position,
        optimistic_queue,
        latency_nanos,
        slippage_ticks,
        window,
        version,
        as_of,
        snapshot,
        equity_interval_nanos,
        minimum_coverage,
    )
    # `maker_fee_rate` was appended to the native ABI. Omitting it preserves
    # compatibility with an older installed extension for non-Kalshi runs.
    if maker_fee_rate is None:
        payload = db._native.run_backtest(*native_args)
    else:
        payload = db._native.run_backtest(*native_args, maker_fee_rate)
    return json.loads(payload)
