"""Driving a backtest from Python (ROADMAP_QUANT.md Part B).

Both ends of a run are tables: the signals table is the strategy, and the
``bt_*`` tables on the run's fork are the result. These tests exercise that
whole path from Python, and end where it should end -- at a tearsheet.
"""

from __future__ import annotations

import datetime as dt
import tempfile

import numpy as np
import pyarrow as pa
import pytest

import h5i_db
from h5i_db import backtest, quant

SECOND = 1_000_000_000
MARKET = "will-x-happen"

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


def _seeded(tmp) -> h5i_db.Database:
    db = h5i_db.Database(f"{tmp}/bt.db", create=True)
    db.create_table("instruments", INSTRUMENTS, time_column="ts_init")
    db.create_table("book_deltas", BOOK_DELTAS, time_column="ts_init")
    db.append(
        "instruments",
        pa.table(
            {
                "ts_init": [dt.datetime(2024, 1, 1)] * 2,
                "instrument_id": [MARKET] * 2,
                "venue": ["polymarket"] * 2,
                "kind": ["prediction_market"] * 2,
                "outcome": [0, 1],
                "outcome_label": ["YES", "NO"],
                "tick_size": [0.0001] * 2,
                "lot_size": [1.0] * 2,
                "expiration_ns": [None, None],
                "settlement_observable_ns": [None, None],
            },
            schema=INSTRUMENTS,
        ),
    )

    # Ten one-second snapshots, two rows each (one bid, one ask).
    rows: dict = {name: [] for name in BOOK_DELTAS.names}
    base = dt.datetime(2024, 1, 1)
    for step in range(1, 11):
        at = base + dt.timedelta(seconds=step)
        for index, (side, price) in enumerate(
            [("buy", 0.40 + step * 0.01), ("sell", 0.42 + step * 0.01)]
        ):
            rows["ts_init"].append(at)
            rows["ts_event"].append(at)
            rows["instrument_id"].append(MARKET)
            rows["outcome"].append(0)
            rows["action"].append("snapshot")
            rows["side"].append(side)
            rows["price"].append(round(price, 4))
            rows["size"].append(500.0)
            rows["event_index"].append(step)
            rows["is_last"].append(index == 1)
            rows["source_vendor"].append("test")
    db.append("book_deltas", pa.table(rows, schema=BOOK_DELTAS))
    db.snapshot("seed")
    return db


def _signals(db, rows):
    backtest.create_signal_table(db)
    db.append("signals", backtest.signal_table(rows))


def test_a_run_from_python_produces_fills_and_a_fork():
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 100.0,
                    "tag": "entry",
                }
            ],
        )
        report = backtest.run(
            db, "py-001", starting_cash=1_000.0, snapshot="seed"
        )
        assert report["fork"] == "bt-py-001"
        assert report["fills"] == 1
        assert report["records_processed"] > 0
        assert len(report["digest"]) == 64

        fork = db.fork("bt-py-001")
        fills = fork.read("bt_fills").to_pylist()
        assert len(fills) == 1
        assert fills[0]["side"] == "buy"
        assert fills[0]["tag"] == "entry"
        db.close()


def test_the_run_is_reproducible_from_its_arguments():
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 50.0,
                }
            ],
        )
        first = backtest.run(db, "rep-a", starting_cash=500.0, snapshot="seed")
        second = backtest.run(db, "rep-b", starting_cash=500.0, snapshot="seed")
        for key in ("final_cash", "realized_pnl", "fills", "records_processed"):
            assert first[key] == second[key], key
        db.close()


def test_a_run_feeds_the_tearsheet():
    """The whole point: simulation to report with no adapter in between."""
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 2),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 100.0,
                }
            ],
        )
        report = backtest.run(
            db,
            "tear-001",
            starting_cash=1_000.0,
            snapshot="seed",
            equity_interval_nanos=SECOND,
        )
        assert report["equity_points"] >= 2

        fork = db.fork("bt-tear-001")
        series = quant.from_levels(fork, "bt_equity")
        stats = series.stats()
        assert stats["n_periods"] >= 1
        html = quant.tearsheet(series, title="Backtest run")
        assert "Backtest run" in html
        db.close()


def test_fees_reduce_the_final_cash():
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 100.0,
                }
            ],
        )
        free = backtest.run(db, "free", starting_cash=1_000.0, snapshot="seed")
        charged = backtest.run(
            db,
            "charged",
            starting_cash=1_000.0,
            snapshot="seed",
            fee_rate=0.07,
        )
        assert charged["commissions"] > 0
        assert free["commissions"] == 0
        assert charged["final_cash"] < free["final_cash"]
        db.close()


def test_a_limit_signal_without_a_price_is_refused_before_it_runs():
    with pytest.raises(ValueError, match="limit_price"):
        backtest.signal_table(
            [
                {
                    "ts": 0,
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 1.0,
                    "kind": "limit",
                }
            ]
        )


def test_signal_rows_must_be_complete():
    with pytest.raises(ValueError, match="missing"):
        backtest.signal_table([{"ts": 0, "instrument_id": MARKET}])
    with pytest.raises(ValueError, match="unknown kind"):
        backtest.signal_table(
            [
                {
                    "ts": 0,
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 1.0,
                    "kind": "iceberg",
                }
            ]
        )


def test_coverage_floor_refuses_a_thin_window():
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(db, [])
        start = dt.datetime(2024, 1, 1)
        with pytest.raises(h5i_db.InvalidInputError, match="coverage"):
            backtest.run(
                db,
                "thin",
                starting_cash=100.0,
                snapshot="seed",
                window=(start, start + dt.timedelta(seconds=200)),
                minimum_coverage=0.9,
            )
        db.close()


def test_an_unsorted_signal_table_is_refused():
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        # Written out of order on purpose; the reader sorts, so this must
        # succeed rather than fail -- a table has no inherent order.
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 10.0,
                },
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 5),
                    "instrument_id": MARKET,
                    "side": "sell",
                    "quantity": 10.0,
                },
            ],
        )
        report = backtest.run(db, "sorted", starting_cash=500.0, snapshot="seed")
        assert report["fills"] == 2
        db.close()


def test_queue_position_changes_nothing_for_a_taker():
    """A marketable order does not depend on queue position."""
    with tempfile.TemporaryDirectory() as tmp:
        db = _seeded(tmp)
        _signals(
            db,
            [
                {
                    "ts": dt.datetime(2024, 1, 1, 0, 0, 3),
                    "instrument_id": MARKET,
                    "side": "buy",
                    "quantity": 10.0,
                }
            ],
        )
        plain = backtest.run(db, "plain", starting_cash=500.0, snapshot="seed")
        queued = backtest.run(
            db, "queued", starting_cash=500.0, snapshot="seed", queue_position=True
        )
        assert plain["fills"] == queued["fills"] == 1
        assert plain["final_cash"] == queued["final_cash"]
        db.close()
