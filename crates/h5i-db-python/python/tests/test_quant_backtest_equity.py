"""Turning a backtest run's equity curve into a tearsheet.

A run writes ``bt_equity`` into its own fork (see `crates/h5i-db-backtest`).
These tests pin the Python half of that contract: the exact schema the Rust
writer produces is reconstructed here, and everything downstream -- returns,
stats, tearsheet -- must work on it untouched.

The schema is duplicated deliberately rather than imported: if the Rust
writer changes a column name or type, this fails, which is the point.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import tempfile

import numpy as np
import pyarrow as pa
import pytest

import h5i_db
from h5i_db import quant

empyrical = pytest.importorskip("empyrical", reason="golden oracle not installed")
import pandas as pd  # noqa: E402

# Mirrors crates/h5i-db-backtest/src/schema.rs::equity().
BT_EQUITY = pa.schema(
    [
        pa.field("ts", pa.timestamp("ns"), nullable=False),
        pa.field("cash", pa.float64(), nullable=False),
        pa.field("position_value", pa.float64(), nullable=False),
        pa.field("equity", pa.float64(), nullable=False),
        pa.field("realized_pnl", pa.float64(), nullable=False),
        pa.field("unrealized_pnl", pa.float64(), nullable=False),
    ]
)


def _curve(n=250, seed=13, start_equity=100_000.0):
    rng = np.random.default_rng(seed)
    steps = rng.normal(0.0005, 0.012, size=n)
    equity = start_equity * np.exp(np.cumsum(steps))
    base = dt.datetime(2024, 1, 1)
    ts = [base + dt.timedelta(days=i) for i in range(n)]
    return ts, equity


@contextlib.contextmanager
def equity_db(**kwargs):
    ts, equity = _curve(**kwargs)
    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(f"{tmp}/run.db", create=True)
        db.create_table("bt_equity", BT_EQUITY, time_column="ts")
        db.append(
            "bt_equity",
            pa.table(
                {
                    "ts": ts,
                    "cash": equity * 0.4,
                    "position_value": equity * 0.6,
                    "equity": equity,
                    "realized_pnl": equity - equity[0],
                    "unrealized_pnl": np.zeros(len(equity)),
                },
                schema=BT_EQUITY,
            ),
        )
        db.snapshot("run")
        try:
            yield db, pd.Series(equity, index=pd.DatetimeIndex(ts))
        finally:
            db.close()


def test_from_levels_matches_a_pandas_pct_change():
    with equity_db() as (db, equity):
        series = quant.from_levels(db, "bt_equity", snapshot="run")
        got = series.frame.collect().to_pandas().set_index("ts")["ret"]
    expected = equity.pct_change().dropna()
    assert len(got) == len(expected), "the first bar has no prior level"
    np.testing.assert_allclose(got.to_numpy(), expected.to_numpy(), rtol=1e-12)


def test_stats_on_a_run_match_empyrical():
    with equity_db() as (db, equity):
        stats = quant.from_levels(db, "bt_equity", snapshot="run").stats()
    reference = equity.pct_change().dropna()
    for key, expected in [
        ("sharpe_ratio", empyrical.sharpe_ratio(reference)),
        ("max_drawdown", empyrical.max_drawdown(reference)),
        ("annual_volatility", empyrical.annual_volatility(reference)),
        ("cumulative_return", empyrical.cum_returns_final(reference)),
    ]:
        np.testing.assert_allclose(stats[key], expected, rtol=1e-9, atol=1e-12)


def test_a_run_renders_a_tearsheet(tmp_path):
    with equity_db() as (db, _equity):
        series = quant.from_levels(db, "bt_equity", snapshot="run")
        out = tmp_path / "run.html"
        html = quant.tearsheet(series, path=str(out), title="Backtest run")
    assert out.exists()
    assert "Backtest run" in html
    assert "Provenance" in html
    assert series.provenance.pin.is_pinned


def test_the_level_column_is_configurable():
    """Cash-only or position-only curves are the same machinery."""
    with equity_db() as (db, _equity):
        cash = quant.from_levels(db, "bt_equity", level="cash", snapshot="run")
        equity_series = quant.from_levels(db, "bt_equity", snapshot="run")
        # cash is a fixed fraction of equity here, so the returns coincide.
        np.testing.assert_allclose(
            cash.stats()["sharpe_ratio"],
            equity_series.stats()["sharpe_ratio"],
            rtol=1e-9,
        )
    assert cash.provenance.parameters["level_column"] == "cash"


def test_a_flat_level_series_has_no_return_and_no_drawdown():
    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(f"{tmp}/flat.db", create=True)
        db.create_table("bt_equity", BT_EQUITY, time_column="ts")
        n = 20
        db.append(
            "bt_equity",
            pa.table(
                {
                    "ts": [dt.datetime(2024, 1, 1) + dt.timedelta(days=i) for i in range(n)],
                    "cash": np.full(n, 1000.0),
                    "position_value": np.zeros(n),
                    "equity": np.full(n, 1000.0),
                    "realized_pnl": np.zeros(n),
                    "unrealized_pnl": np.zeros(n),
                },
                schema=BT_EQUITY,
            ),
        )
        stats = quant.from_levels(db, "bt_equity").stats()
        db.close()
    assert stats["cumulative_return"] == pytest.approx(0.0, abs=1e-15)
    assert stats["max_drawdown"] == pytest.approx(0.0, abs=1e-15)


def test_a_run_on_a_fork_reads_like_any_other_table():
    """Runs live on forks, so the tearsheet path must work through one."""
    ts, equity = _curve(n=60)
    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(f"{tmp}/base.db", create=True)
        # The base holds market data; the run's tables exist only in the fork.
        db.create_table(
            "prices",
            pa.schema([pa.field("ts", pa.timestamp("ns"), nullable=False)]),
            time_column="ts",
        )
        db.create_fork("bt-demo")
        fork = db.fork("bt-demo")
        fork.create_table("bt_equity", BT_EQUITY, time_column="ts")
        fork.append(
            "bt_equity",
            pa.table(
                {
                    "ts": ts,
                    "cash": equity,
                    "position_value": np.zeros(len(equity)),
                    "equity": equity,
                    "realized_pnl": equity - equity[0],
                    "unrealized_pnl": np.zeros(len(equity)),
                },
                schema=BT_EQUITY,
            ),
        )
        stats = quant.from_levels(fork, "bt_equity").stats()
        assert "bt_equity" not in db.tables(), "the base must stay clean"
        db.close()
    assert stats["n_periods"] == len(equity) - 1
