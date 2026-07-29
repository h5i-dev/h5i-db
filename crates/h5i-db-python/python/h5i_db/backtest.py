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

import argparse
import datetime as _dt
import json
import sys
from pathlib import Path
from typing import Any, Optional, Sequence, Union

import pyarrow as pa

from .backtest_config import (
    BacktestConfig,
    DataConfig,
    ExecutionConfig,
    OutputConfig,
    PortfolioConfig,
    PreflightInspection,
    ReplayFidelity,
    RiskConfig,
    inspect,
)
from .backtest_result import BacktestResult, _persist_config, list_runs, open_result
from .backtest_strategy import from_signals, target_positions
from .backtest_study import (
    BacktestStudy,
    StudyResult,
    ValidationWindows,
    study,
)

__all__ = [
    "BacktestConfig",
    "BacktestResult",
    "BacktestStudy",
    "DataConfig",
    "ExecutionConfig",
    "OutputConfig",
    "PortfolioConfig",
    "PreflightInspection",
    "ReplayFidelity",
    "RiskConfig",
    "StudyResult",
    "ValidationWindows",
    "run",
    "execute",
    "from_signals",
    "inspect",
    "list_runs",
    "main",
    "open_result",
    "SIGNAL_SCHEMA",
    "COMMAND_SCHEMA",
    "signal_table",
    "command_table",
    "study",
    "target_positions",
    "create_signal_table",
    "create_command_table",
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

#: Mirrors `crates/h5i-db-backtest/src/schema.rs::commands()`.
COMMAND_SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("ns"), nullable=False),
        pa.field("action", pa.string(), nullable=False),
        pa.field("client_order_id", pa.string(), nullable=False),
        pa.field("instrument_id", pa.string()),
        pa.field("outcome", pa.uint16()),
        pa.field("side", pa.string()),
        pa.field("quantity", pa.float64()),
        pa.field("kind", pa.string()),
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


def command_table(rows: Sequence[dict]) -> pa.Table:
    """Build a validated declarative submit/amend/cancel command table."""
    columns: dict = {name: [] for name in COMMAND_SCHEMA.names}
    for index, row in enumerate(rows):
        missing = {"ts", "action", "client_order_id"} - set(row)
        if missing:
            raise ValueError(f"command {index} is missing {sorted(missing)}")
        action = str(row["action"]).lower()
        if action not in ("submit", "amend", "cancel"):
            raise ValueError(f"command {index} has unknown action {action!r}")
        client_order_id = str(row["client_order_id"])
        if not client_order_id:
            raise ValueError(f"command {index} has an empty client_order_id")

        if action == "submit":
            required = {"instrument_id", "side", "quantity"} - set(row)
            if required:
                raise ValueError(
                    f"submit command {index} is missing {sorted(required)}"
                )
            kind = str(row.get("kind", "market")).lower()
            if kind not in ("market", "limit"):
                raise ValueError(f"command {index} has unknown kind {kind!r}")
            if kind == "limit" and row.get("limit_price") is None:
                raise ValueError(f"limit submit command {index} has no limit_price")
        else:
            kind = None
            if action == "amend" and all(
                row.get(field) is None for field in ("quantity", "limit_price")
            ):
                raise ValueError(
                    f"amend command {index} changes neither quantity nor limit_price"
                )

        columns["ts"].append(_to_nanos(row["ts"]))
        columns["action"].append(action)
        columns["client_order_id"].append(client_order_id)
        columns["instrument_id"].append(
            str(row["instrument_id"])
            if action == "submit" and row.get("instrument_id") is not None
            else None
        )
        columns["outcome"].append(
            int(row.get("outcome", 0)) if action == "submit" else None
        )
        columns["side"].append(str(row["side"]).lower() if action == "submit" else None)
        quantity = row.get("quantity")
        columns["quantity"].append(None if quantity is None else float(quantity))
        columns["kind"].append(kind)
        limit = row.get("limit_price")
        columns["limit_price"].append(None if limit is None else float(limit))
        columns["time_in_force"].append(
            row.get("time_in_force") if action == "submit" else None
        )
        columns["tag"].append(row.get("tag") if action == "submit" else None)
        columns["reduce_only"].append(
            bool(row.get("reduce_only", False)) if action == "submit" else None
        )

    columns["ts"] = pa.array(columns["ts"], type=pa.timestamp("ns"))
    return pa.table(columns, schema=COMMAND_SCHEMA)


def create_command_table(db: Any, name: str = "commands") -> None:
    """Create a lifecycle command table if it does not exist."""
    if name not in db.tables():
        db.create_table(name, COMMAND_SCHEMA, time_column="ts")


def run(
    db: Any,
    run_id: str,
    *,
    starting_cash: float,
    signals: Optional[str] = "signals",
    commands: Optional[str] = None,
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
    max_order_quantity: Optional[float] = None,
    max_abs_position: Optional[float] = None,
    max_open_orders: Optional[int] = None,
) -> BacktestResult:
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
    data_config = DataConfig(
        signals=signals,
        commands=commands,
        snapshot=snapshot,
        version=version,
        as_of=as_of,
        window=window,
        minimum_coverage=minimum_coverage,
    )
    native_args = (
        run_id,
        float(starting_cash),
        data_config.strategy_table,
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
    risk_args = (max_order_quantity, max_abs_position, max_open_orders)
    if (
        maker_fee_rate is None
        and all(value is None for value in risk_args)
        and commands is None
    ):
        payload = db._native.run_backtest(*native_args)
    elif all(value is None for value in risk_args) and commands is None:
        payload = db._native.run_backtest(*native_args, maker_fee_rate)
    else:
        payload = db._native.run_backtest(
            *native_args, maker_fee_rate, *risk_args, commands
        )
    config = BacktestConfig(
        run_id=run_id,
        portfolio=PortfolioConfig(starting_cash=starting_cash),
        data=data_config,
        execution=ExecutionConfig(
            fee_kind=fee_kind,
            fee_rate=fee_rate,
            maker_rebate=maker_rebate,
            maker_fee_rate=maker_fee_rate,
            queue_position=queue_position,
            optimistic_queue=optimistic_queue,
            latency_nanos=latency_nanos,
            slippage_ticks=slippage_ticks,
        ),
        risk=RiskConfig(
            max_order_quantity=max_order_quantity,
            max_abs_position=max_abs_position,
            max_open_orders=max_open_orders,
        ),
        output=OutputConfig(equity_interval_nanos=equity_interval_nanos),
    )
    inspection = inspect(db, config)
    result = BacktestResult(
        json.loads(payload),
        db=db,
        config=config,
        inspection=inspection,
    )
    _persist_config(db, result.fork_name, config, inspection)
    return result


def execute(
    db: Any,
    config: BacktestConfig,
    *,
    preflight: bool = True,
) -> BacktestResult:
    """Execute a complete typed configuration through the native kernel."""
    if not isinstance(config, BacktestConfig):
        raise TypeError("config must be a BacktestConfig")
    inspection = inspect(db, config)
    if preflight:
        inspection.raise_for_errors()
    data = config.data
    execution = config.execution
    output = config.output
    result = run(
        db,
        config.run_id,
        starting_cash=config.portfolio.starting_cash,
        signals=data.signals,
        commands=data.commands,
        fee_kind=execution.fee_kind,
        fee_rate=execution.fee_rate,
        maker_rebate=execution.maker_rebate,
        maker_fee_rate=execution.maker_fee_rate,
        queue_position=execution.queue_position,
        optimistic_queue=execution.optimistic_queue,
        latency_nanos=execution.latency_nanos,
        slippage_ticks=execution.slippage_ticks,
        window=data.window,
        version=data.version,
        as_of=data.as_of,
        snapshot=data.snapshot,
        equity_interval_nanos=output.equity_interval_nanos,
        minimum_coverage=data.minimum_coverage,
        max_order_quantity=config.risk.max_order_quantity,
        max_abs_position=config.risk.max_abs_position,
        max_open_orders=config.risk.max_open_orders,
    )
    result.inspection = inspection
    return result


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Command-line entry point for reproducible backtest operations."""
    parser = argparse.ArgumentParser(prog="python -m h5i_db.backtest")
    subparsers = parser.add_subparsers(dest="command", required=True)

    inspect_parser = subparsers.add_parser("inspect", help="validate a config")
    inspect_parser.add_argument("database")
    inspect_parser.add_argument("config")

    run_parser = subparsers.add_parser("run", help="execute a config")
    run_parser.add_argument("database")
    run_parser.add_argument("config")
    run_parser.add_argument(
        "--allow-preflight-errors",
        action="store_true",
        help="record preflight findings but do not refuse the run",
    )

    list_parser = subparsers.add_parser("list", help="list backtest runs")
    list_parser.add_argument("database")

    report_parser = subparsers.add_parser("report", help="render a run report")
    report_parser.add_argument("database")
    report_parser.add_argument("run_id")
    report_parser.add_argument("--output", required=True)
    report_parser.add_argument(
        "--execution-only",
        action="store_true",
        help="render the execution manifest instead of an equity tearsheet",
    )

    verify_parser = subparsers.add_parser("verify", help="re-execute a run")
    verify_parser.add_argument("database")
    verify_parser.add_argument("run_id")

    args = parser.parse_args(argv)
    from . import Database

    db = Database(args.database)
    try:
        if args.command == "inspect":
            config = BacktestConfig.from_json(Path(args.config))
            payload = inspect(db, config).to_dict()
            print(json.dumps(payload, indent=2, default=str))
            return 0 if payload["ok"] else 2
        if args.command == "run":
            config = BacktestConfig.from_json(Path(args.config))
            result = execute(
                db,
                config,
                preflight=not args.allow_preflight_errors,
            )
            print(json.dumps(result.summary(), indent=2, default=str))
            return 0
        if args.command == "list":
            print(json.dumps(list_runs(db), indent=2, default=str))
            return 0
        if args.command == "report":
            result = open_result(db, args.run_id)
            if args.execution_only or result.equity.num_rows == 0:
                result.html_summary(args.output)
            else:
                result.tearsheet(args.output)
            print(str(Path(args.output)))
            return 0
        if args.command == "verify":
            payload = open_result(db, args.run_id).verify()
            print(json.dumps(payload, indent=2, default=str))
            return 0 if payload["verified"] else 3
        parser.error(f"unknown command {args.command!r}")
        return 2
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
