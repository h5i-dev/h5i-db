"""What a Python strategy callback costs h5i-db, per event.

The cross-engine benchmark measures h5i through its *declarative* path: the
strategy is a `signals` table, so the replay never calls Python. Nautilus is
measured through a Python callback per quote. That is a real difference in
what the two systems were asked to do, and quoting the ratio without saying
so invites the reading that the kernel is 11.7x faster at the same job.

h5i has the other path too -- `backtest.EventStrategy`, whose adapter
reacquires the GIL for every callback on purpose. This measures it.

The three arms run the same 200k events over the same database, so
everything outside the strategy boundary (scan, decode, fork creation, the
result write) is identical and cancels in the differences:

    signals    declarative; no Python during replay        the fast default
    noop       EventStrategy.on_event returns None         pure boundary cost
    trading    EventStrategy submits the same 200 orders   boundary + work

    per-event boundary cost = (noop - signals) / events

Arms alternate within one process rather than running in fresh ones: the
quantity wanted is a difference between arms, and alternating cancels the
drift this machine shows between minutes.

    python3 benchmarks/backtest_compare/h5i_callback_boundary.py --rounds 5
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import statistics
import tempfile
import time

import pyarrow as pa

import h5i_db
from h5i_db import backtest

SECOND = 1_000_000_000
TICK_NANOS = 1_000_000
MARKET = "BENCH-PERP-0000"
VENUE = "bench"
EPOCH = dt.datetime(2024, 1, 1)

BOOK_DELTAS = pa.schema(
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
INSTRUMENTS = pa.schema(
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
SIGNALS = pa.schema(
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
        pa.field("post_only", pa.bool_()),
    ]
)


def mid_at(step: int) -> float:
    """The generator the Rust bench uses, so the books match event for event."""
    return 100.0 + ((step % 400) - 200) * 0.01


def signal_steps(events: int, signals: int) -> list[int]:
    """Order timestamps, spread evenly, one instrument in from the start."""
    spacing = max(events // signals, 1)
    return [1 + index * spacing for index in range(signals)]


def seed(path: str, events: int, signals: int) -> h5i_db.Database:
    db = h5i_db.Database(path, create=True)
    db.create_table("instruments", INSTRUMENTS, time_column="ts_init")
    db.create_table("book_deltas", BOOK_DELTAS, time_column="ts_init")
    db.create_table("signals", SIGNALS, time_column="ts")

    db.append(
        "instruments",
        pa.table(
            {
                "ts_init": [EPOCH],
                "instrument_id": [MARKET],
                "venue": [VENUE],
                "kind": ["perpetual"],
                "outcome": [0],
                "outcome_label": [""],
                "tick_size": [0.01],
                "lot_size": [1.0],
                "expiration_ns": [None],
                "settlement_observable_ns": [None],
            },
            schema=INSTRUMENTS,
        ),
    )

    # Two rows per event, a bid and an ask sharing one event_index, `is_last`
    # on the second. That is one top-of-book snapshot per event, which is
    # what `--common-quotes` generates on the Rust side.
    stamps, prices, sides, indices, lasts = [], [], [], [], []
    for step in range(events):
        at = EPOCH + dt.timedelta(microseconds=step * TICK_NANOS / 1000)
        mid = mid_at(step)
        for side, price, last in (("buy", mid - 0.01, False), ("sell", mid + 0.01, True)):
            stamps.append(at)
            prices.append(round(price, 2))
            sides.append(side)
            indices.append(step)
            lasts.append(last)
    rows = len(stamps)
    db.append(
        "book_deltas",
        pa.table(
            {
                "ts_init": stamps,
                "ts_event": stamps,
                "instrument_id": [MARKET] * rows,
                "outcome": [0] * rows,
                "action": ["snapshot"] * rows,
                "side": sides,
                "price": prices,
                "size": [500.0] * rows,
                "event_index": indices,
                "is_last": lasts,
                "source_vendor": [None] * rows,
            },
            schema=BOOK_DELTAS,
        ),
    )

    steps = signal_steps(events, signals)
    db.append(
        "signals",
        pa.table(
            {
                "ts": [
                    EPOCH + dt.timedelta(microseconds=s * TICK_NANOS / 1000) for s in steps
                ],
                "instrument_id": [MARKET] * len(steps),
                "outcome": [0] * len(steps),
                "side": ["buy" if i % 2 == 0 else "sell" for i in range(len(steps))],
                "quantity": [1.0] * len(steps),
                "kind": ["market"] * len(steps),
                "limit_price": [None] * len(steps),
                "time_in_force": [None] * len(steps),
                "tag": [None] * len(steps),
                "reduce_only": [None] * len(steps),
                "post_only": [None] * len(steps),
            },
            schema=SIGNALS,
        ),
    )
    db.snapshot("seed")
    return db


class NoopStrategy(backtest.EventStrategy):
    """Costs exactly one crossing of the boundary per event and nothing else."""

    def __init__(self) -> None:
        self.seen = 0

    def on_event(self, context, event):
        self.seen += 1
        return None


class TradingStrategy(backtest.EventStrategy):
    """The same orders the signals table holds, decided in Python instead.

    The signals table fires at steps ``1 + index * spacing``. Steps are
    zero-based and ``on_event`` counts from one, so step ``s`` is the
    ``s + 1``-th callback: the first order is due on the second event.
    """

    def __init__(self, spacing: int, total: int) -> None:
        self.spacing = spacing
        self.total = total
        self.seen = 0
        self.sent = 0

    def on_event(self, context, event):
        self.seen += 1
        due = self.seen - 2
        if due < 0 or due % self.spacing != 0 or self.sent >= self.total:
            return None
        side = "buy" if self.sent % 2 == 0 else "sell"
        self.sent += 1
        return {
            "action": "submit",
            "client_order_id": f"o{self.sent}",
            "instrument_id": MARKET,
            "side": side,
            "quantity": 1.0,
            "kind": "market",
        }


def field(report, name: str, default=0):
    """Reports are mapping-like wrappers, not plain dicts."""
    try:
        return report[name]
    except (KeyError, TypeError):
        return getattr(report, name, default)


def run_arm(db, arm: str, run_id: str, events: int, signals: int):
    spacing = max(events // signals, 1)
    start = time.perf_counter()
    if arm == "signals":
        report = backtest.run(
            db, run_id, starting_cash=1_000_000.0, signals="signals", snapshot="seed"
        )
    else:
        strategy = NoopStrategy() if arm == "noop" else TradingStrategy(spacing, signals)
        report = backtest.run_strategy(
            db,
            run_id,
            strategy,
            strategy_id=f"bench.{arm}:v1",
            starting_cash=1_000_000.0,
            data=backtest.DataConfig(snapshot="seed"),
        )
    elapsed = (time.perf_counter() - start) * 1000.0
    return elapsed, report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=200_000)
    parser.add_argument("--signals", type=int, default=200)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    arms = ("signals", "noop", "trading")
    with tempfile.TemporaryDirectory() as tmp:
        print(f"seeding {args.events} events ...", flush=True)
        db = seed(f"{tmp}/boundary.db", args.events, args.signals)

        samples: dict[str, list[float]] = {arm: [] for arm in arms}
        records: dict[str, int] = {}
        # Round 0 is a warm-up and is not recorded.
        for round_index in range(args.rounds + 1):
            for arm in arms:
                ms, report = run_arm(
                    db, arm, f"{arm}-{round_index}", args.events, args.signals
                )
                if round_index == 0:
                    records[arm] = field(report, "records_processed")
                    print(
                        f"  warm-up {arm:8} {ms:8.1f} ms  "
                        f"records={records[arm]} fills={field(report, 'fills')}",
                        flush=True,
                    )
                    continue
                samples[arm].append(ms)
        db.close()

    medians = {arm: statistics.median(samples[arm]) for arm in arms}
    boundary_us = (medians["noop"] - medians["signals"]) / args.events * 1000.0

    print()
    print(f"{'arm':10} {'median ms':>10} {'min':>9} {'max':>9}")
    for arm in arms:
        print(
            f"{arm:10} {medians[arm]:10.1f} {min(samples[arm]):9.1f} "
            f"{max(samples[arm]):9.1f}"
        )
    print()
    print(f"boundary cost: {boundary_us:.3f} us/event over {args.events} events")
    print(f"noop vs signals: {medians['noop'] / medians['signals']:.2f}x")

    if args.output:
        with open(args.output, "w") as handle:
            json.dump(
                {
                    "events": args.events,
                    "signals": args.signals,
                    "rounds": args.rounds,
                    "records_processed": records,
                    "samples_ms": samples,
                    "median_ms": medians,
                    "boundary_us_per_event": boundary_us,
                },
                handle,
                indent=1,
            )


if __name__ == "__main__":
    main()
