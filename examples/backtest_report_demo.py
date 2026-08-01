#!/usr/bin/env python
"""One backtest, and the page it produces.

What this shows, in a few seconds:

    a small prediction market is built and pinned, an EMA-crossover rule is
    turned into a signals table, the run executes into its own fork, and
    `result.report(path)` writes the whole thing as a single HTML file with no
    dependencies and no network access at view time.

The report is not a tearsheet. A tearsheet answers "how did the equity curve
behave"; this answers "should I believe these numbers, and what actually
happened":

- replay fidelity and the pin come first, because periodic snapshots cannot
  support the same execution claims a tick book can;
- coverage, the run digest and the config digest sit beside them, so the page
  says which data version produced it;
- the order lifecycle carries rejection reasons in the engine's own words, and
  every fill is plotted and tabled;
- the configuration is carried verbatim at the end, so the page is enough to
  re-run the run it describes.

The demo deliberately submits one oversized sell, so the report has a real
rejection to explain rather than a synthetic one.

Run it:

    python examples/backtest_report_demo.py                 # run, write, open
    python examples/backtest_report_demo.py --no-browser    # just write it
    python examples/backtest_report_demo.py --verify        # also reproduce it
    python examples/backtest_report_demo.py --keep          # keep the database

Needs the `h5i_db` package importable. Nothing here reaches the network.
"""

from __future__ import annotations

import argparse
import datetime as dt
import math
import random
import shutil
import sys
import webbrowser
from pathlib import Path
from typing import Any, Optional, Sequence

import pyarrow as pa

import h5i_db
from h5i_db import backtest

REPO = Path(__file__).resolve().parent.parent
SECOND_NS = 1_000_000_000
TICK = 0.01

BOOK_SCHEMA = pa.schema(
    [
        pa.field("ts_init", pa.timestamp("ns"), nullable=False),
        pa.field("ts_event", pa.timestamp("ns"), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("outcome", pa.uint16(), nullable=False),
        pa.field("action", pa.string(), nullable=False),
        pa.field("side", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("size", pa.float64()),
        pa.field("event_index", pa.int64(), nullable=False),
        pa.field("is_last", pa.bool_(), nullable=False),
        pa.field("source_vendor", pa.string()),
    ]
)
INSTRUMENT_SCHEMA = pa.schema(
    [
        pa.field("ts_init", pa.timestamp("ns"), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("venue", pa.string(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("outcome", pa.uint16(), nullable=False),
        pa.field("outcome_label", pa.string(), nullable=False),
        pa.field("tick_size", pa.float64(), nullable=False),
        pa.field("lot_size", pa.float64(), nullable=False),
        pa.field("expiration_ns", pa.int64()),
        pa.field("settlement_observable_ns", pa.int64()),
    ]
)


def _snap(value: float) -> float:
    """Prices live on the venue's tick grid; the engine refuses limits off it."""
    return round(round(value / TICK) * TICK, 4)


def build_market(markets: int, steps: int, seed: int) -> dict[str, pa.Table]:
    """A handful of binary event contracts quoting every five minutes.

    A mean-reverting walk around a latent probability, with spreads that widen
    at the tails the way real event books do. Nothing here is tuned to make the
    strategy look good, and the report is more interesting when it does not.
    """
    rng = random.Random(seed)
    base = dt.datetime(2026, 3, 2, 13, 30, 0)
    step = dt.timedelta(minutes=5)
    expiry = base + step * (steps - 1)

    def nanos(moment: dt.datetime) -> int:
        return int(moment.replace(tzinfo=dt.timezone.utc).timestamp() * SECOND_NS)

    questions = [
        "Fed holds rates at the March meeting",
        "CPI prints below 3.0% year over year",
        "Nonfarm payrolls beat consensus",
        "Brent closes above $95 this month",
        "ECB cuts before the Fed",
        "Ten-year yield ends the week above 4%",
    ]
    book: dict[str, list] = {name: [] for name in BOOK_SCHEMA.names}
    instruments: dict[str, list] = {name: [] for name in INSTRUMENT_SCHEMA.names}
    event = 0

    plan = []
    for index in range(markets):
        p_true = 0.20 + 0.60 * (index + 0.5) / markets
        plan.append(
            {
                "id": f"EVT-{index:03d}",
                "question": questions[index % len(questions)],
                "p_true": p_true,
                "mid": p_true,
                "phase": rng.random() * math.tau,
            }
        )

    for market in plan:
        for outcome, label in ((0, "YES"), (1, "NO")):
            instruments["ts_init"].append(base)
            instruments["instrument_id"].append(market["id"])
            instruments["venue"].append("demo-exchange")
            instruments["kind"].append("prediction_market")
            instruments["outcome"].append(outcome)
            instruments["outcome_label"].append(label)
            instruments["tick_size"].append(TICK)
            instruments["lot_size"].append(1.0)
            instruments["expiration_ns"].append(nanos(expiry))
            # The demo stops at expiry, so nothing settles. The report says so
            # in its own warnings rather than the run pretending otherwise.
            instruments["settlement_observable_ns"].append(None)

    def emit(at, instrument, outcome, bid, ask, bid_size, ask_size):
        nonlocal event
        event += 1
        for position, (side, price, size) in enumerate(
            (("buy", bid, bid_size), ("sell", ask, ask_size))
        ):
            book["ts_init"].append(at)
            book["ts_event"].append(at)
            book["instrument_id"].append(instrument)
            book["outcome"].append(outcome)
            book["action"].append("snapshot")
            book["side"].append(side)
            book["price"].append(_snap(price))
            book["size"].append(size)
            book["event_index"].append(event)
            book["is_last"].append(position == 1)
            book["source_vendor"].append("demo")

    for index in range(steps):
        at = base + step * index
        for market in plan:
            drift = 0.10 * (market["p_true"] - market["mid"])
            shock = (rng.random() - 0.5) * 0.06
            market["mid"] = min(
                0.95,
                max(
                    0.05,
                    market["mid"]
                    + drift
                    + shock * math.sqrt(market["mid"] * (1 - market["mid"])),
                ),
            )
            mid = market["mid"]
            half = 0.005 + 0.015 * abs(mid - 0.5)
            depth = 150.0 + 70.0 * math.sin(market["phase"] + index / 4)
            ask_depth = 150.0 + 70.0 * math.cos(market["phase"] + index / 6)
            emit(at, market["id"], 0, mid - half, mid + half, depth, ask_depth)
            emit(at, market["id"], 1, 1 - mid - half, 1 - mid + half, ask_depth, depth)

    return {
        "instruments": pa.table(
            {
                k: pa.array(v, type=INSTRUMENT_SCHEMA.field(k).type)
                for k, v in instruments.items()
            },
            schema=INSTRUMENT_SCHEMA,
        ).sort_by(
            [
                ("ts_init", "ascending"),
                ("instrument_id", "ascending"),
                ("outcome", "ascending"),
            ]
        ),
        "book_deltas": pa.table(
            {k: pa.array(v, type=BOOK_SCHEMA.field(k).type) for k, v in book.items()},
            schema=BOOK_SCHEMA,
        ),
        "_questions": {market["id"]: market["question"] for market in plan},
    }


def build_signals(db: Any) -> Any:
    """An EMA crossover, plus one sell the engine must refuse.

    The refusal is the point: a prediction-market contract cannot be borrowed,
    so selling more than you hold is not a fill at a bad price, it is an order
    that never existed. A report that only showed fills would leave that out.
    """
    panel = backtest.quote_panel(db, snapshot="research-pin")
    plan = backtest.strategies.ema_crossover(panel, fast=4, slow=12, quantity=150.0)
    if plan.num_signals == 0:
        raise SystemExit("the generated market produced no crossovers; try --seed")

    first = plan.signals.to_pylist()[0]
    oversized = backtest.signal_table(
        [
            {
                # A moment after the first signal, so the signals table stays
                # sorted, which the engine requires.
                "ts": first["ts"] + dt.timedelta(seconds=1),
                "instrument_id": first["instrument_id"],
                "side": "sell",
                "quantity": 5_000.0,
                "tag": "deliberately-oversized",
            }
        ]
    )
    signals = pa.concat_tables([plan.signals, oversized]).sort_by(
        [("ts", "ascending"), ("instrument_id", "ascending")]
    )
    backtest.create_signal_table(db)
    db.append("signals", signals, note=str(plan.parameters))
    return plan


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--markets", type=int, default=6)
    parser.add_argument("--steps", type=int, default=120)
    parser.add_argument("--seed", type=int, default=11)
    parser.add_argument("--out", default=None, help="where to write the report")
    parser.add_argument("--db", default=None, help="where to build the database")
    parser.add_argument("--keep", action="store_true", help="do not delete on exit")
    parser.add_argument("--no-browser", action="store_true")
    parser.add_argument(
        "--verify",
        action="store_true",
        help="re-execute the stored config and compare, before reporting",
    )
    args = parser.parse_args(argv)

    db_path = Path(args.db or (REPO / "target" / "backtest-report-demo.db"))
    out = Path(args.out or (REPO / "target" / "backtest-report-demo.html"))
    if db_path.exists():
        shutil.rmtree(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    out.parent.mkdir(parents=True, exist_ok=True)

    print("1. One pinned market")
    tables = build_market(args.markets, args.steps, args.seed)
    questions = tables.pop("_questions")
    db = h5i_db.Database(str(db_path), create=True)
    try:
        for name, table in tables.items():
            db.create_table(name, table.schema, time_column="ts_init")
            db.append(name, table, note="demo market data")
        db.snapshot("research-pin", tables=list(tables), note="frozen research input")
        rows = db.sql("SELECT count(*) AS n FROM book_deltas").to_pandas()["n"][0]
        print(f"   {args.markets} markets, {rows:,} book rows, pinned as 'research-pin'")
        print(f"   e.g. {list(questions.values())[0]!r}")

        print("\n2. A rule, as a table of signals")
        plan = build_signals(db)
        print(f"   {plan.strategy} {dict(plan.parameters)}")
        print(f"   {plan.num_signals} signals, plus one oversized sell to be refused")

        config = backtest.BacktestConfig(
            run_id="report-demo",
            data=backtest.DataConfig(signals="signals", snapshot="research-pin"),
            # Sized so the run is visibly invested: the peak position is about a
            # tenth of equity. A demo that risks 0.4% of its cash draws an
            # exposure line flat against the axis and teaches nothing.
            portfolio=backtest.PortfolioConfig(starting_cash=5_000.0),
            execution=backtest.ExecutionConfig(fee_kind="kalshi", fee_rate=0.07),
            output=backtest.OutputConfig(equity_interval_nanos=5 * 60 * SECOND_NS),
            metadata={"study": "report-demo", "candidate": plan.strategy},
        )

        print("\n3. Preflight: what the data can and cannot support")
        inspection = backtest.inspect(db, config)
        print(f"   fidelity: {inspection.fidelity.value}")
        for issue in inspection.issues:
            print(f"   {issue.severity}/{issue.code}: {issue.message}")
        inspection.raise_for_errors()

        print("\n4. The run")
        result = backtest.execute(db, config)
        summary = result.summary()
        print(
            f"   fork {result.fork_name}: {summary['records_processed']:,} records, "
            f"{summary['orders']} orders, {summary['fills']} fills"
        )
        print(
            f"   realized {summary['realized_pnl']:,.2f} "
            f"after {summary['commissions']:,.2f} of fees"
        )
        for reason, count in result.explain()["rejection_reasons"].items():
            print(f"   refused x{count}: {reason}")

        if args.verify:
            print("\n   verifying: re-executing the stored config")
            verification = result.verify()
            print(
                "   reproduced"
                if verification["verified"]
                else "   DID NOT REPRODUCE -- see result.verify()"
            )

        print("\n5. The report")
        result.report(out)
        print(f"   {out}  ({out.stat().st_size / 1024:.0f} KB, self-contained)")
        print("   in the page, top to bottom:")
        print("     - replay fidelity and the preflight findings above everything")
        print("     - the pin, both digests, coverage, the execution model")
        print("     - equity, drawdown, how invested it was, every fill")
        print("     - order lifecycle, the refusal in the engine's own words")
        print("     - the configuration verbatim: enough to re-run this run")

        if not args.no_browser:
            webbrowser.open(out.resolve().as_uri())
    finally:
        db.close()
        if not args.keep and db_path.exists():
            shutil.rmtree(db_path)
            print(f"\n   removed {db_path} (--keep to keep it)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
