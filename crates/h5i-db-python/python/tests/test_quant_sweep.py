"""Fork sweeps, verification and restatement impact (ROADMAP_QUANT.md M4).

These cover the claims that are properties of the store rather than of the
statistics: trials are isolated in forks and comparable in one query, a
pinned computation can be re-verified, and a data revision's effect on an
answer can be measured rather than guessed.
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

PRICE_SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("us"), nullable=False),
        pa.field("asset", pa.string()),
        pa.field("price", pa.float64()),
    ]
)
FACTOR_SCHEMA = pa.schema(
    [
        pa.field("ts", pa.timestamp("us"), nullable=False),
        pa.field("asset", pa.string()),
        pa.field("factor", pa.float64()),
    ]
)


def _panel_data(seed=17, n_dates=40, n_assets=20, flip=False):
    rng = np.random.default_rng(seed)
    assets = [f"A{i:02d}" for i in range(n_assets)]
    dates = [dt.datetime(2024, 1, 1) + dt.timedelta(days=d) for d in range(n_dates)]
    steps = rng.normal(0.0003, 0.012, size=(n_dates, n_assets))
    levels = 100.0 * np.exp(np.cumsum(steps, axis=0))
    factors = rng.normal(size=(n_dates, n_assets))
    if flip:
        factors = -factors
    ts = [d for d in dates for _ in assets]
    asset = [a for _ in dates for a in assets]
    return (
        pa.table(
            {"ts": ts, "asset": asset, "price": levels.reshape(-1).tolist()},
            schema=PRICE_SCHEMA,
        ),
        pa.table(
            {"ts": ts, "asset": asset, "factor": factors.reshape(-1).tolist()},
            schema=FACTOR_SCHEMA,
        ),
        dates,
    )


@contextlib.contextmanager
def open_db(restate=False):
    prices, factors, dates = _panel_data()
    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(f"{tmp}/sweep.db", create=True)
        db.create_table("prices", PRICE_SCHEMA, time_column="ts")
        db.create_table("signals", FACTOR_SCHEMA, time_column="ts")
        db.append("prices", prices)
        db.append("signals", factors)
        db.snapshot("original")
        if restate:
            _, flipped, _ = _panel_data(flip=True)
            db.write("signals", flipped)
            db.snapshot("restated")
        try:
            yield db, dates
        finally:
            db.close()


def _mean_ic(fork_db, params):
    panel = quant.build_panel(
        fork_db,
        "signals",
        "prices",
        periods=(1,),
        quantiles=params["quantiles"],
        filter_zscore=params["filter_zscore"],
        max_loss=1.0,
    )
    decay = panel.ic_decay().to_arrow().to_pylist()[0]
    loss = panel.loss_report()
    return {"mean_ic": decay["mean_ic"], "icir": decay["icir"], "loss": loss["total"]}


# -- sweeps -----------------------------------------------------------------


def test_sweep_runs_every_combination_in_its_own_fork():
    with open_db() as (db, _dates):
        result = quant.sweep(
            db,
            {"quantiles": [3, 5], "filter_zscore": [None, 2.0]},
            _mean_ic,
            prefix="ic-grid",
        )
        assert len(result) == 4
        assert len(set(result.forks)) == 4
        # The base database is untouched: the sweep wrote only inside forks.
        assert "quant_sweep" not in db.tables()
        for name in result.forks:
            assert "quant_sweep" in db.fork(name).tables()


def test_sweep_compares_every_trial_in_one_query():
    with open_db() as (db, _dates):
        result = quant.sweep(
            db,
            {"quantiles": [3, 5, 10], "filter_zscore": [None]},
            _mean_ic,
            prefix="compare",
        )
        table = result.compare().to_pandas()
    assert len(table) == 3
    assert "__fork" in table.columns, "cross-fork scans name their source fork"
    assert set(table["__fork"]) == set(result.forks)
    assert {"mean_ic", "icir", "loss"} <= set(table.columns)


def test_sweep_best_picks_the_winning_trial():
    with open_db() as (db, _dates):
        result = quant.sweep(
            db,
            {"quantiles": [3, 5, 10], "filter_zscore": [None]},
            _mean_ic,
            prefix="best",
        )
        best = result.best("icir")
        worst = result.best("icir", maximize=False)
    assert best["icir"] >= worst["icir"]
    assert best["quantiles"] in (3, 5, 10)


def test_sweep_isolates_writes_between_trials():
    """A trial that writes cannot be seen by any other trial or by the base."""

    def writer(fork_db, params):
        schema = pa.schema(
            [
                pa.field("ts", pa.timestamp("us"), nullable=False),
                pa.field("tag", pa.string()),
            ]
        )
        fork_db.create_table("scratch", schema, time_column="ts")
        fork_db.append(
            "scratch",
            pa.table(
                {"ts": [dt.datetime(2024, 1, 1)], "tag": [str(params["n"])]},
                schema=schema,
            ),
        )
        return {"n": float(params["n"])}

    with open_db() as (db, _dates):
        result = quant.sweep(db, {"n": [1, 2, 3]}, writer, prefix="iso")
        assert "scratch" not in db.tables()
        for name, expected in zip(result.forks, ["1", "2", "3"]):
            rows = db.fork(name).read("scratch").to_pylist()
            assert [r["tag"] for r in rows] == [expected]


def test_sweep_records_failures_without_aborting():
    def flaky(fork_db, params):
        if params["n"] == 2:
            raise RuntimeError("trial 2 is broken")
        return {"n": float(params["n"])}

    with open_db() as (db, _dates):
        result = quant.sweep(db, {"n": [1, 2, 3]}, flaky, prefix="flaky", keep_going=True)
        assert len(result) == 2
        assert len(result.failures) == 1
        assert "trial 2 is broken" in result.failures[0]["error"]

        with pytest.raises(RuntimeError):
            quant.sweep(db, {"n": [1, 2]}, flaky, prefix="strict")


def test_sweep_rejects_non_scalar_metrics():
    with open_db() as (db, _dates):
        with pytest.raises(TypeError, match="must be a number"):
            quant.sweep(db, {"n": [1]}, lambda f, p: {"bad": [1, 2]}, prefix="bad")


def test_sweep_forks_can_be_dropped():
    with open_db() as (db, _dates):
        result = quant.sweep(db, {"n": [1, 2]}, lambda f, p: {"n": float(p["n"])},
                             prefix="temp")
        assert set(result.forks) <= set(db.fork_names())
        result.drop()
        assert not (set(result.forks) & set(db.fork_names()))


# -- verification -----------------------------------------------------------


def test_verify_confirms_a_pinned_computation():
    with open_db() as (db, _dates):
        def build():
            return quant.build_panel(
                db, "signals", "prices", periods=(1,), quantiles=5,
                filter_zscore=None, max_loss=1.0, snapshot="original",
            )

        panel = build()
        report = quant.verify(panel, rerun=build)
    assert report["verified"] is True
    assert report["pinned"] is True
    assert report["warnings"] == []


def test_verify_refuses_an_unpinned_computation():
    with open_db() as (db, _dates):
        panel = quant.build_panel(
            db, "signals", "prices", periods=(1,), quantiles=5,
            filter_zscore=None, max_loss=1.0,
        )
        with pytest.raises(quant.VerificationError, match="unpinned"):
            quant.verify(panel)
        lenient = quant.verify(panel, strict=False)
        assert lenient["verified"] is False
        assert any("unpinned" in w for w in lenient["warnings"])


def test_verify_detects_a_changed_computation():
    with open_db() as (db, _dates):
        panel = quant.build_panel(
            db, "signals", "prices", periods=(1,), quantiles=5,
            filter_zscore=None, max_loss=1.0, snapshot="original",
        )

        def different():
            return quant.build_panel(
                db, "signals", "prices", periods=(1,), quantiles=10,
                filter_zscore=None, max_loss=1.0, snapshot="original",
            )

        with pytest.raises(quant.VerificationError, match="digest changed"):
            quant.verify(panel, rerun=different)


# -- restatement impact -----------------------------------------------------


def test_restatement_impact_measures_a_revision():
    """A vendor revision that flips the factor must show up as a moved IC."""
    with open_db(restate=True) as (db, _dates):
        def build(pin):
            return quant.build_panel(
                db, "signals", "prices", periods=(1,), quantiles=5,
                filter_zscore=None, max_loss=1.0, **pin,
            )

        def metric(panel):
            row = panel.ic_decay().to_arrow().to_pylist()[0]
            return {"mean_ic": row["mean_ic"], "icir": row["icir"]}

        report = quant.restatement_impact(
            build,
            db,
            before={"snapshot": "original"},
            after={"snapshot": "restated"},
            metric=metric,
        )
    assert report["changed"] is True
    before = report["metrics"]["mean_ic"]["before"]
    after = report["metrics"]["mean_ic"]["after"]
    np.testing.assert_allclose(after, -before, rtol=1e-9, atol=1e-12)


def test_restatement_impact_reports_no_change_when_data_is_stable():
    with open_db() as (db, _dates):
        def build(pin):
            return quant.build_panel(
                db, "signals", "prices", periods=(1,), quantiles=5,
                filter_zscore=None, max_loss=1.0, **pin,
            )

        def metric(panel):
            row = panel.ic_decay().to_arrow().to_pylist()[0]
            return {"mean_ic": row["mean_ic"]}

        report = quant.restatement_impact(
            build,
            db,
            before={"snapshot": "original"},
            after={"snapshot": "original"},
            metric=metric,
        )
    assert report["changed"] is False
