"""Performance statistics tests (ROADMAP_QUANT.md M3, acceptance criteria 5.2).

Every headline statistic is checked against ``empyrical-reloaded`` executing
on the same returns series. empyrical is the definition of record here:
pyfolio's ratio functions are deprecated wrappers around it.

The edge fixtures are deliberate -- an all-negative series, a single
drawdown spanning the whole sample, a sub-year history -- because that is
where ratio implementations usually diverge.
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
from scipy import stats as scipy_stats  # noqa: E402

RETURN_SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("us"), nullable=False),
        pa.field("ret", pa.float64()),
    ]
)
ANNUALIZATION = 252
TOL = dict(rtol=1e-9, atol=1e-12)


def _series(values, start=dt.datetime(2023, 1, 2)):
    dates = [start + dt.timedelta(days=i) for i in range(len(values))]
    table = pa.table(
        {"ts": dates, "ret": [float(v) for v in values]}, schema=RETURN_SCHEMA
    )
    pandas_series = pd.Series(
        np.asarray(values, dtype=float), index=pd.DatetimeIndex(dates)
    )
    return table, pandas_series


@contextlib.contextmanager
def open_returns(values, benchmark_values=None, annualization=ANNUALIZATION):
    table, series = _series(values)
    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(f"{tmp}/perf.db", create=True)
        db.create_table("rets", RETURN_SCHEMA, time_column="ts")
        db.append("rets", table)
        bench = None
        if benchmark_values is not None:
            btable, bseries = _series(benchmark_values)
            db.create_table("bench", RETURN_SCHEMA, time_column="ts")
            db.append("bench", btable)
            bench = quant.returns(db, "bench", annualization=annualization)
        else:
            bseries = None
        try:
            yield (
                quant.returns(db, "rets", annualization=annualization),
                series,
                bench,
                bseries,
            )
        finally:
            db.close()


def _random_returns(n=500, seed=5, mu=0.0004, sigma=0.011):
    rng = np.random.default_rng(seed)
    return rng.normal(mu, sigma, size=n)


# -- headline statistics ----------------------------------------------------


@pytest.mark.parametrize(
    "name,values",
    [
        ("normal", _random_returns()),
        ("all_negative", -np.abs(_random_returns(n=300, seed=9))),
        ("short_history", _random_returns(n=40, seed=3)),
        ("one_long_drawdown", np.concatenate([[0.4], -np.full(200, 0.002)])),
        ("flat_then_move", np.concatenate([np.zeros(50), _random_returns(n=100)])),
    ],
)
def test_stats_match_empyrical(name, values):
    with open_returns(values) as (series, ref, _bench, _bref):
        got = series.stats()

    assert got["n_periods"] == len(ref)
    checks = {
        "cumulative_return": empyrical.cum_returns_final(ref),
        "annual_return": empyrical.annual_return(ref, annualization=ANNUALIZATION),
        "annual_volatility": empyrical.annual_volatility(
            ref, annualization=ANNUALIZATION
        ),
        "sharpe_ratio": empyrical.sharpe_ratio(ref, annualization=ANNUALIZATION),
        "sortino_ratio": empyrical.sortino_ratio(ref, annualization=ANNUALIZATION),
        "downside_risk": empyrical.downside_risk(ref, annualization=ANNUALIZATION),
        "max_drawdown": empyrical.max_drawdown(ref),
        "omega_ratio": empyrical.omega_ratio(ref, annualization=ANNUALIZATION),
        "stability": empyrical.stability_of_timeseries(ref),
        "tail_ratio": empyrical.tail_ratio(ref),
    }
    for key, expected in checks.items():
        if expected is None or (isinstance(expected, float) and np.isnan(expected)):
            continue
        np.testing.assert_allclose(
            got[key], expected, err_msg=f"{name}: {key} diverges", **TOL
        )


def test_calmar_matches_empyrical():
    values = _random_returns(n=400, seed=17)
    with open_returns(values) as (series, ref, _b, _br):
        got = series.stats()
    np.testing.assert_allclose(
        got["calmar_ratio"],
        empyrical.calmar_ratio(ref, annualization=ANNUALIZATION),
        **TOL,
    )


def test_skew_kurtosis_and_var_match_pyfolio_conventions():
    """pyfolio takes skew/kurtosis from scipy (biased) and VaR from itself."""
    values = _random_returns(n=600, seed=23)
    with open_returns(values) as (series, ref, _b, _br):
        got = series.stats()
    np.testing.assert_allclose(got["skew"], scipy_stats.skew(ref), **TOL)
    np.testing.assert_allclose(got["kurtosis"], scipy_stats.kurtosis(ref), **TOL)
    np.testing.assert_allclose(
        got["daily_value_at_risk"], ref.mean() - 2.0 * ref.std(), **TOL
    )


def test_alpha_and_beta_match_empyrical():
    values = _random_returns(n=500, seed=31)
    bench = _random_returns(n=500, seed=32)
    # Correlate them so beta is not noise.
    values = 0.6 * bench + 0.4 * values
    with open_returns(values, benchmark_values=bench) as (series, ref, bseries, bref):
        got = series.stats(benchmark=bseries)
    np.testing.assert_allclose(
        got["beta"], empyrical.beta(ref, bref), rtol=1e-8, atol=1e-12
    )
    np.testing.assert_allclose(
        got["alpha"],
        empyrical.alpha(ref, bref, annualization=ANNUALIZATION),
        rtol=1e-8,
        atol=1e-12,
    )


def test_annualization_is_configurable():
    """A crypto series is hourly, not daily; the factor is an argument."""
    values = _random_returns(n=800, seed=41)
    hourly = 24 * 365
    with open_returns(values, annualization=hourly) as (series, ref, _b, _br):
        got = series.stats()
    np.testing.assert_allclose(
        got["sharpe_ratio"], empyrical.sharpe_ratio(ref, annualization=hourly), **TOL
    )
    np.testing.assert_allclose(
        got["annual_volatility"],
        empyrical.annual_volatility(ref, annualization=hourly),
        **TOL,
    )


# -- curves and drawdowns ---------------------------------------------------


def test_equity_curve_matches_empyrical_cum_returns():
    values = _random_returns(n=300, seed=51)
    with open_returns(values) as (series, ref, _b, _br):
        curve = series.equity_curve().to_pandas().set_index("ts")
    expected = empyrical.cum_returns(ref)
    np.testing.assert_allclose(
        curve["cumulative_return"].to_numpy(), expected.to_numpy(), **TOL
    )


def test_underwater_matches_empyrical_drawdown_series():
    values = _random_returns(n=300, seed=53)
    with open_returns(values) as (series, ref, _b, _br):
        under = series.underwater().to_pandas().set_index("ts")
    cumulative = empyrical.cum_returns(ref, starting_value=100)
    running_peak = np.maximum.accumulate(np.concatenate([[100.0], cumulative.values]))[
        1:
    ]
    expected = (cumulative.values - running_peak) / running_peak
    np.testing.assert_allclose(under["drawdown"].to_numpy(), expected, **TOL)


def test_drawdown_table_finds_the_worst_episode():
    """The deepest episode's depth and valley must agree with empyrical."""
    values = _random_returns(n=400, seed=61)
    with open_returns(values) as (series, ref, _b, _br):
        table = series.drawdown_table(top=5)
        under = series.underwater().to_pandas()
    assert table, "a random walk of 400 bars always has a drawdown"
    worst = table[0]
    np.testing.assert_allclose(
        -worst["net_drawdown"], empyrical.max_drawdown(ref), **TOL
    )
    valley_row = under.loc[under["drawdown"].idxmin()]
    assert worst["valley_date"] == valley_row["ts"]
    # Episodes must not overlap, and must be ordered worst first.
    depths = [row["net_drawdown"] for row in table]
    assert depths == sorted(depths, reverse=True)
    spans = [
        (row["peak_date"], row["recovery_date"] or row["valley_date"])
        for row in table
    ]
    for i in range(len(spans)):
        for j in range(i + 1, len(spans)):
            a, b = spans[i], spans[j]
            assert a[1] <= b[0] or b[1] <= a[0], "drawdown episodes overlap"


def test_drawdown_table_on_a_monotone_series_is_empty():
    with open_returns(np.full(50, 0.001)) as (series, _ref, _b, _br):
        assert series.drawdown_table() == []


# -- rolling statistics -----------------------------------------------------


def test_rolling_sharpe_matches_pandas_reference():
    values = _random_returns(n=300, seed=71)
    window = 63
    with open_returns(values) as (series, ref, _b, _br):
        got = series.rolling_sharpe(window).to_pandas().set_index("ts")
    expected = (
        ref.rolling(window).mean() / ref.rolling(window).std()
    ) * np.sqrt(ANNUALIZATION)
    # SQL frames produce a value from the first row; pandas needs a full
    # window. Compare where both are defined.
    valid = expected.notna()
    np.testing.assert_allclose(
        got["rolling_sharpe"].to_numpy()[valid.to_numpy()],
        expected[valid].to_numpy(),
        rtol=1e-9,
        atol=1e-12,
    )


def test_rolling_volatility_matches_pandas_reference():
    values = _random_returns(n=300, seed=73)
    window = 42
    with open_returns(values) as (series, ref, _b, _br):
        got = series.rolling_volatility(window).to_pandas().set_index("ts")
    expected = ref.rolling(window).std() * np.sqrt(ANNUALIZATION)
    valid = expected.notna()
    np.testing.assert_allclose(
        got["rolling_volatility"].to_numpy()[valid.to_numpy()],
        expected[valid].to_numpy(),
        rtol=1e-9,
        atol=1e-12,
    )


def test_rolling_beta_matches_pandas_reference():
    bench = _random_returns(n=300, seed=81)
    values = 0.5 * bench + 0.5 * _random_returns(n=300, seed=82)
    window = 60
    with open_returns(values, benchmark_values=bench) as (series, ref, bs, bref):
        got = series.rolling_beta(bs, window).to_pandas().set_index("ts")
    cov = ref.rolling(window).cov(bref)
    var = bref.rolling(window).var()
    expected = cov / var
    valid = expected.notna()
    np.testing.assert_allclose(
        got["rolling_beta"].to_numpy()[valid.to_numpy()],
        expected[valid].to_numpy(),
        rtol=1e-8,
        atol=1e-10,
    )


# -- the guarantees ---------------------------------------------------------


def test_stats_are_reproducible_at_a_pin():
    values = _random_returns(n=400, seed=91)
    table, _ = _series(values)
    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(f"{tmp}/perf.db", create=True)
        db.create_table("rets", RETURN_SCHEMA, time_column="ts")
        db.append("rets", table)
        db.snapshot("v1")
        later = _series(
            _random_returns(n=10, seed=92),
            start=dt.datetime(2023, 1, 2) + dt.timedelta(days=len(values)),
        )[0]
        db.append("rets", later)
        pinned = quant.returns(db, "rets", snapshot="v1")
        first = pinned.stats()
        for _ in range(4):
            assert pinned.stats() == first, "a pinned stat must not move"
        assert pinned.provenance.pin.is_pinned
        latest = quant.returns(db, "rets")
        assert latest.stats()["n_periods"] > first["n_periods"]
        db.close()


def test_factor_returns_feed_the_tearsheet_stats():
    """A factor panel's returns are a returns series: the layers compose."""
    pytest.importorskip("alphalens")
    from test_quant_factor import PERIODS, QUANTILES, _synthetic, open_db

    data = _synthetic(seed=101)
    with open_db(data) as db:
        panel = quant.build_panel(
            db,
            "signals",
            "prices",
            periods=PERIODS,
            quantiles=QUANTILES,
            filter_zscore=None,
            max_loss=1.0,
        )
        frame = panel.returns().to_pandas()[["ts", "ret_1"]]
        db.create_table(
            "factor_returns",
            RETURN_SCHEMA,
            time_column="ts",
        )
        db.append(
            "factor_returns",
            pa.table(
                {
                    "ts": frame["ts"].tolist(),
                    "ret": frame["ret_1"].astype(float).tolist(),
                },
                schema=RETURN_SCHEMA,
            ),
        )
        series = quant.returns(db, "factor_returns")
        got = series.stats()
    reference = pd.Series(
        frame["ret_1"].to_numpy(), index=pd.DatetimeIndex(frame["ts"])
    )
    np.testing.assert_allclose(
        got["sharpe_ratio"],
        empyrical.sharpe_ratio(reference, annualization=ANNUALIZATION),
        **TOL,
    )
