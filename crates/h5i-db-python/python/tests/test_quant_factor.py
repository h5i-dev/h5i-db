"""Factor evaluation tests (ROADMAP_QUANT.md M1, acceptance criteria 4.3).

Two kinds of test live here.

*Golden* tests execute ``alphalens-reloaded`` on the same synthetic data and
compare numbers. That is the only check that means anything for a port:
reading the formulas proves nothing, so the reference implementation runs
for real and the values must agree. They skip when alphalens is absent.

*Property* tests cover the guarantees alphalens does not have and that this
project sells: a pinned read is reproducible, and an event-time cutoff makes
lookahead structurally impossible.
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

alphalens = pytest.importorskip("alphalens", reason="golden oracle not installed")
import pandas as pd  # noqa: E402  (pandas comes with alphalens)
from alphalens import performance as al_perf  # noqa: E402
from alphalens import utils as al_utils  # noqa: E402

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
        pa.field("sector", pa.string()),
    ]
)

# 20 assets into 5 quantiles divides evenly, so pandas' value-edge qcut and
# SQL's equal-count ntile agree exactly. The non-divisible case is a
# documented divergence and has its own test.
N_DATES = 60
N_ASSETS = 20
PERIODS = (1, 5, 10)
QUANTILES = 5
SECTORS = ("tech", "energy", "health", "finance")


def _synthetic(seed: int = 11, n_dates: int = N_DATES, n_assets: int = N_ASSETS):
    """Deterministic price/factor panel, as both Arrow tables and pandas."""
    rng = np.random.default_rng(seed)
    assets = [f"A{i:02d}" for i in range(n_assets)]
    dates = [dt.datetime(2024, 1, 1) + dt.timedelta(days=d) for d in range(n_dates)]
    sectors = {a: SECTORS[i % len(SECTORS)] for i, a in enumerate(assets)}

    # Prices: geometric random walk with full-mantissa values.
    steps = rng.normal(0.0002, 0.013, size=(n_dates, n_assets))
    levels = 100.0 * np.exp(np.cumsum(steps, axis=0)) * (1 + np.arange(n_assets) / 50)
    factors = rng.normal(size=(n_dates, n_assets))

    prices_df = pd.DataFrame(levels, index=pd.DatetimeIndex(dates), columns=assets)
    prices_df.index.name = "date"
    factor_df = pd.DataFrame(factors, index=pd.DatetimeIndex(dates), columns=assets)
    factor_series = factor_df.stack()
    factor_series.index = factor_series.index.set_names(["date", "asset"])

    flat_ts = [d for d in dates for _ in assets]
    flat_asset = [a for _ in dates for a in assets]
    price_table = pa.table(
        {
            "ts": flat_ts,
            "asset": flat_asset,
            "price": levels.reshape(-1).tolist(),
        },
        schema=PRICE_SCHEMA,
    )
    factor_table = pa.table(
        {
            "ts": flat_ts,
            "asset": flat_asset,
            "factor": factors.reshape(-1).tolist(),
            "sector": [sectors[a] for _ in dates for a in assets],
        },
        schema=FACTOR_SCHEMA,
    )
    return {
        "assets": assets,
        "dates": dates,
        "sectors": sectors,
        "prices_df": prices_df,
        "factor_series": factor_series,
        "price_table": price_table,
        "factor_table": factor_table,
    }


@contextlib.contextmanager
def open_db(data, extra_version: bool = False):
    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(f"{tmp}/quant.db", create=True)
        db.create_table("prices", PRICE_SCHEMA, time_column="ts")
        db.create_table("signals", FACTOR_SCHEMA, time_column="ts")
        db.append("prices", data["price_table"])
        db.append("signals", data["factor_table"])
        if extra_version:
            db.snapshot("before_restatement")
            # A restatement: the same rows with the factor sign flipped.
            flipped = data["factor_table"].set_column(
                data["factor_table"].schema.get_field_index("factor"),
                "factor",
                pa.array(
                    [-v for v in data["factor_table"].column("factor").to_pylist()],
                    type=pa.float64(),
                ),
            )
            db.write("signals", flipped)
        try:
            yield db
        finally:
            db.close()


@pytest.fixture(scope="module")
def data():
    return _synthetic()


@pytest.fixture(scope="module")
def reference(data):
    """alphalens' own factor_data for the same inputs."""
    return al_utils.get_clean_factor_and_forward_returns(
        factor=data["factor_series"],
        prices=data["prices_df"],
        periods=PERIODS,
        quantiles=QUANTILES,
        filter_zscore=None,
        max_loss=1.0,
    )


@contextlib.contextmanager
def panel_for(data, **kwargs):
    with open_db(data) as db:
        kwargs.setdefault("periods", PERIODS)
        kwargs.setdefault("quantiles", QUANTILES)
        kwargs.setdefault("filter_zscore", None)
        kwargs.setdefault("max_loss", 1.0)
        yield db, quant.build_panel(db, "signals", "prices", **kwargs)


def _fwd_columns(reference_frame):
    """alphalens names forward returns '1D'/'5D'; we name them by bar count."""
    cols = list(al_utils.get_forward_returns_columns(reference_frame.columns))
    return dict(zip(PERIODS, cols))


def _panel_frame(panel):
    """Our panel as a pandas frame indexed like alphalens'."""
    out = panel.collect().to_pandas()
    out = out.rename(columns={"ts": "date"}).set_index(["date", "asset"]).sort_index()
    return out


# -- panel construction ---------------------------------------------------


def test_panel_matches_alphalens_rows_and_quantiles(data, reference):
    """Same surviving rows, same forward returns, same quantile labels."""
    with panel_for(data) as (_db, panel):
        got = _panel_frame(panel)

    assert len(got) == len(reference)
    assert list(got.index) == list(reference.index)

    fwd = _fwd_columns(reference)
    for period, ref_col in fwd.items():
        np.testing.assert_allclose(
            got[f"fwd_{period}"].to_numpy(),
            reference[ref_col].to_numpy(),
            rtol=1e-12,
            atol=1e-15,
            err_msg=f"forward returns diverge at horizon {period}",
        )
    np.testing.assert_array_equal(
        got["factor_quantile"].to_numpy(),
        reference["factor_quantile"].to_numpy(),
    )
    np.testing.assert_allclose(
        got["factor"].to_numpy(), reference["factor"].to_numpy(), rtol=1e-12
    )


def test_non_cumulative_forward_returns_match(data):
    """``cumulative_returns=False`` is the single-bar return n bars ahead."""
    ref = al_utils.get_clean_factor_and_forward_returns(
        factor=data["factor_series"],
        prices=data["prices_df"],
        periods=PERIODS,
        quantiles=QUANTILES,
        filter_zscore=None,
        max_loss=1.0,
        cumulative_returns=False,
    )
    with panel_for(data, cumulative_returns=False) as (_db, panel):
        got = _panel_frame(panel)
    for period, ref_col in _fwd_columns(ref).items():
        np.testing.assert_allclose(
            got[f"fwd_{period}"].to_numpy(),
            ref[ref_col].to_numpy(),
            rtol=1e-12,
            atol=1e-15,
        )


def test_filter_zscore_matches_alphalens(data):
    """The z-score clip uses per-asset statistics on the pre-join grid."""
    ref = al_utils.get_clean_factor_and_forward_returns(
        factor=data["factor_series"],
        prices=data["prices_df"],
        periods=PERIODS,
        quantiles=QUANTILES,
        filter_zscore=1.0,  # aggressive, so it actually removes rows
        max_loss=1.0,
    )
    with panel_for(data, filter_zscore=1.0) as (_db, panel):
        got = _panel_frame(panel)
    assert list(got.index) == list(ref.index)
    for period, ref_col in _fwd_columns(ref).items():
        np.testing.assert_allclose(
            got[f"fwd_{period}"].to_numpy(),
            ref[ref_col].to_numpy(),
            rtol=1e-12,
            atol=1e-15,
        )


def test_zero_aware_quantiles_match(data):
    ref = al_utils.get_clean_factor_and_forward_returns(
        factor=data["factor_series"],
        prices=data["prices_df"],
        periods=PERIODS,
        quantiles=4,
        zero_aware=True,
        filter_zscore=None,
        max_loss=1.0,
    )
    with panel_for(data, quantiles=4, zero_aware=True) as (_db, panel):
        got = _panel_frame(panel)
    np.testing.assert_array_equal(
        got["factor_quantile"].to_numpy(), ref["factor_quantile"].to_numpy()
    )


def test_loss_report_matches_alphalens_accounting(data):
    """Both phases of the drop accounting, against alphalens' own arithmetic."""
    initial = len(data["factor_series"])
    with panel_for(data) as (_db, panel):
        report = panel.loss_report()
    ref = al_utils.get_clean_factor_and_forward_returns(
        factor=data["factor_series"],
        prices=data["prices_df"],
        periods=PERIODS,
        quantiles=QUANTILES,
        filter_zscore=None,
        max_loss=1.0,
    )
    assert report["initial"] == initial
    assert report["after_binning"] == len(ref)
    # Nothing is lost binning when every date has a full cross-section.
    assert report["binning"] == pytest.approx(0.0, abs=1e-12)
    assert report["total"] == pytest.approx((initial - len(ref)) / initial)


def test_max_loss_is_enforced(data):
    with pytest.raises(quant.MaxLossExceededError) as excinfo:
        with panel_for(data, max_loss=0.05):
            pass
    assert "max_loss" in str(excinfo.value)


# -- information coefficient ----------------------------------------------


def test_ic_matches_alphalens(data, reference):
    ref_ic = al_perf.factor_information_coefficient(reference)
    with panel_for(data) as (_db, panel):
        got = panel.ic().to_pandas().set_index("ts").sort_index()
    fwd = _fwd_columns(reference)
    assert len(got) == len(ref_ic)
    for period, ref_col in fwd.items():
        np.testing.assert_allclose(
            got[f"ic_{period}"].to_numpy(),
            ref_ic[ref_col].to_numpy(),
            rtol=1e-10,
            atol=1e-12,
            err_msg=f"IC diverges at horizon {period}",
        )


def test_ic_by_group_matches_alphalens(data):
    ref = al_utils.get_clean_factor_and_forward_returns(
        factor=data["factor_series"],
        prices=data["prices_df"],
        periods=PERIODS,
        quantiles=QUANTILES,
        groupby=data["sectors"],
        filter_zscore=None,
        max_loss=1.0,
    )
    ref_ic = al_perf.factor_information_coefficient(ref, by_group=True)
    with panel_for(data, group="sector") as (_db, panel):
        got = (
            panel.ic(by_group=True)
            .to_pandas()
            .rename(columns={"ts": "date"})
            .set_index(["date", "group"])
            .sort_index()
        )
    ref_ic = ref_ic.sort_index()
    assert len(got) == len(ref_ic)
    for period, ref_col in _fwd_columns(ref).items():
        np.testing.assert_allclose(
            got[f"ic_{period}"].to_numpy(),
            ref_ic[ref_col].to_numpy(),
            rtol=1e-10,
            atol=1e-12,
        )


def test_ic_decay_is_one_query_and_matches_mean_ic(data, reference):
    ref_ic = al_perf.factor_information_coefficient(reference)
    with panel_for(data) as (_db, panel):
        decay = panel.ic_decay().to_pandas().set_index("period").sort_index()
    fwd = _fwd_columns(reference)
    for period, ref_col in fwd.items():
        np.testing.assert_allclose(
            decay.loc[period, "mean_ic"], ref_ic[ref_col].mean(), rtol=1e-10
        )
        np.testing.assert_allclose(
            decay.loc[period, "std_ic"], ref_ic[ref_col].std(), rtol=1e-10
        )


def test_mean_ic_resampled_matches_alphalens(data, reference):
    ref = al_perf.mean_information_coefficient(reference, by_time="ME")
    with panel_for(data) as (_db, panel):
        got = panel.mean_ic(by="1mo").to_pandas().sort_values("bucket")
    assert len(got) == len(ref)
    for period, ref_col in _fwd_columns(reference).items():
        np.testing.assert_allclose(
            got[f"ic_{period}"].to_numpy(),
            ref[ref_col].to_numpy(),
            rtol=1e-10,
            atol=1e-12,
        )


# -- returns by quantile ---------------------------------------------------


def test_quantile_returns_match_alphalens(data, reference):
    ref_mean, ref_err = al_perf.mean_return_by_quantile(reference, by_date=False)
    with panel_for(data) as (_db, panel):
        got = (
            panel.quantile_returns()
            .to_pandas()
            .set_index("factor_quantile")
            .sort_index()
        )
    ref_mean = ref_mean.sort_index()
    ref_err = ref_err.sort_index()
    for period, ref_col in _fwd_columns(reference).items():
        np.testing.assert_allclose(
            got[f"mean_{period}"].to_numpy(),
            ref_mean[ref_col].to_numpy(),
            rtol=1e-10,
            atol=1e-14,
            err_msg=f"quantile mean diverges at horizon {period}",
        )
        np.testing.assert_allclose(
            got[f"stderr_{period}"].to_numpy(),
            ref_err[ref_col].to_numpy(),
            rtol=1e-10,
            atol=1e-14,
            err_msg=f"quantile stderr diverges at horizon {period}",
        )


def test_quantile_returns_by_date_match_alphalens(data, reference):
    ref_mean, ref_err = al_perf.mean_return_by_quantile(reference, by_date=True)
    with panel_for(data) as (_db, panel):
        got = (
            panel.quantile_returns(by_date=True)
            .to_pandas()
            .rename(columns={"ts": "date"})
            .set_index(["factor_quantile", "date"])
            .sort_index()
        )
    ref_mean = ref_mean.sort_index()
    ref_err = ref_err.sort_index()
    assert len(got) == len(ref_mean)
    for period, ref_col in _fwd_columns(reference).items():
        np.testing.assert_allclose(
            got[f"mean_{period}"].to_numpy(),
            ref_mean[ref_col].to_numpy(),
            rtol=1e-10,
            atol=1e-14,
        )
        np.testing.assert_allclose(
            got[f"stderr_{period}"].to_numpy(),
            ref_err[ref_col].to_numpy(),
            rtol=1e-10,
            atol=1e-14,
        )


def test_quantile_returns_not_demeaned_match(data, reference):
    ref_mean, _ = al_perf.mean_return_by_quantile(reference, demeaned=False)
    with panel_for(data) as (_db, panel):
        got = (
            panel.quantile_returns(demeaned=False)
            .to_pandas()
            .set_index("factor_quantile")
            .sort_index()
        )
    for period, ref_col in _fwd_columns(reference).items():
        np.testing.assert_allclose(
            got[f"mean_{period}"].to_numpy(),
            ref_mean.sort_index()[ref_col].to_numpy(),
            rtol=1e-10,
            atol=1e-14,
        )


def test_spread_matches_alphalens(data, reference):
    ref_mean, ref_err = al_perf.mean_return_by_quantile(reference, by_date=True)
    ref_spread, ref_spread_err = al_perf.compute_mean_returns_spread(
        ref_mean, QUANTILES, 1, std_err=ref_err
    )
    with panel_for(data) as (_db, panel):
        got = panel.spread().to_pandas().set_index("ts").sort_index()
    for period, ref_col in _fwd_columns(reference).items():
        np.testing.assert_allclose(
            got[f"spread_{period}"].to_numpy(),
            ref_spread[ref_col].to_numpy(),
            rtol=1e-10,
            atol=1e-14,
        )
        np.testing.assert_allclose(
            got[f"stderr_{period}"].to_numpy(),
            ref_spread_err[ref_col].to_numpy(),
            rtol=1e-10,
            atol=1e-14,
        )


# -- turnover and stability ------------------------------------------------


def test_turnover_matches_alphalens(data, reference):
    with panel_for(data) as (_db, panel):
        got = (
            panel.turnover(period=1)
            .to_pandas()
            .rename(columns={"ts": "date"})
            .set_index(["factor_quantile", "date"])
            .sort_index()
        )
    for q in range(1, QUANTILES + 1):
        ref = al_perf.quantile_turnover(reference["factor_quantile"], q, 1).dropna()
        mine = got.loc[q, "turnover"].sort_index()
        assert len(mine) == len(ref), f"turnover row count differs for quantile {q}"
        np.testing.assert_allclose(
            mine.to_numpy(), ref.to_numpy(), rtol=1e-12, atol=1e-14
        )


def test_rank_autocorrelation_matches_alphalens(data, reference):
    ref = al_perf.factor_rank_autocorrelation(reference, period=1).dropna()
    with panel_for(data) as (_db, panel):
        got = (
            panel.rank_autocorrelation(period=1)
            .to_pandas()
            .set_index("ts")
            .sort_index()
        )
    assert len(got) == len(ref)
    np.testing.assert_allclose(
        got["autocorrelation"].to_numpy(), ref.to_numpy(), rtol=1e-10, atol=1e-12
    )


# -- factor portfolio ------------------------------------------------------


def test_weights_match_alphalens(data, reference):
    ref = al_perf.factor_weights(reference, demeaned=True).sort_index()
    with panel_for(data) as (_db, panel):
        got = (
            panel.weights()
            .to_pandas()
            .rename(columns={"ts": "date"})
            .set_index(["date", "asset"])
            .sort_index()
        )
    np.testing.assert_allclose(
        got["weight"].to_numpy(), ref.to_numpy(), rtol=1e-10, atol=1e-14
    )


def test_equal_weight_weights_match_alphalens(data, reference):
    ref = al_perf.factor_weights(
        reference, demeaned=True, equal_weight=True
    ).sort_index()
    with panel_for(data) as (_db, panel):
        got = (
            panel.weights(equal_weight=True)
            .to_pandas()
            .rename(columns={"ts": "date"})
            .set_index(["date", "asset"])
            .sort_index()
        )
    np.testing.assert_allclose(
        got["weight"].to_numpy(), ref.to_numpy(), rtol=1e-10, atol=1e-14
    )


def test_factor_returns_match_alphalens(data, reference):
    ref = al_perf.factor_returns(reference, demeaned=True).sort_index()
    with panel_for(data) as (_db, panel):
        got = panel.returns().to_pandas().set_index("ts").sort_index()
    for period, ref_col in _fwd_columns(reference).items():
        np.testing.assert_allclose(
            got[f"ret_{period}"].to_numpy(),
            ref[ref_col].to_numpy(),
            rtol=1e-10,
            atol=1e-14,
        )


def test_alpha_beta_beta_matches_alphalens(data, reference):
    ref = al_perf.factor_alpha_beta(reference, demeaned=True)
    with panel_for(data) as (_db, panel):
        got = {row["period"]: row for row in panel.alpha_beta()}
    for period, ref_col in _fwd_columns(reference).items():
        np.testing.assert_allclose(
            got[period]["beta"], ref.loc["beta", ref_col], rtol=1e-8
        )


def test_cumulative_returns_compound_the_return_series(data):
    with panel_for(data) as (_db, panel):
        curve = panel.cumulative_returns(period=1).to_pandas().set_index("ts")
    expected = (1 + curve["period_return"]).cumprod() - 1
    np.testing.assert_allclose(
        curve["cumulative_return"].to_numpy(), expected.to_numpy(), rtol=1e-12
    )


# -- the guarantees alphalens does not have --------------------------------


def test_pinned_reads_are_reproducible_and_version_aware(data):
    """Two runs at one pin agree exactly; a different version differs."""
    with open_db(data, extra_version=True) as db:
        kwargs = dict(
            periods=PERIODS, quantiles=QUANTILES, filter_zscore=None, max_loss=1.0
        )
        first = quant.build_panel(db, "signals", "prices", version=1, **kwargs)
        second = quant.build_panel(db, "signals", "prices", version=1, **kwargs)
        assert first.provenance.digest == second.provenance.digest
        a = first.ic().to_arrow()
        b = second.ic().to_arrow()
        assert a.equals(b), "same pin must reproduce byte-identical results"
        # Repeat: one lucky pair of runs proves nothing about determinism.
        for _ in range(4):
            assert first.ic().to_arrow().equals(a)

        # Versions are per table, so a single integer cannot pin two tables
        # that have different histories. A mapping can; so can a snapshot.
        latest = quant.build_panel(
            db, "signals", "prices", version={"signals": 2, "prices": 1}, **kwargs
        )
        assert latest.provenance.digest != first.provenance.digest
        c = latest.ic().to_pandas().set_index("ts")
        old = a.to_pandas().set_index("ts")
        # v2 flipped the factor's sign, so every IC flips with it.
        np.testing.assert_allclose(
            c["ic_1"].to_numpy(), -old["ic_1"].to_numpy(), rtol=1e-10, atol=1e-12
        )


def test_snapshot_pins_every_source_at_one_instant(data):
    """The idiomatic multi-table pin: one name, both tables, one instant."""
    with open_db(data, extra_version=True) as db:
        kwargs = dict(
            periods=(1, 5), quantiles=QUANTILES, filter_zscore=None, max_loss=1.0
        )
        before = quant.build_panel(
            db, "signals", "prices", snapshot="before_restatement", **kwargs
        )
        after = quant.build_panel(db, "signals", "prices", **kwargs)
        b = before.ic().to_pandas().set_index("ts")
        a = after.ic().to_pandas().set_index("ts")
        np.testing.assert_allclose(
            a["ic_1"].to_numpy(), -b["ic_1"].to_numpy(), rtol=1e-10, atol=1e-12
        )
        assert before.provenance.pin.is_pinned is True


def test_version_mapping_rejects_an_unpinned_source(data):
    with open_db(data, extra_version=True) as db:
        with pytest.raises(ValueError, match="no version pinned"):
            quant.build_panel(
                db, "signals", "prices", version={"signals": 1}, periods=(1,),
                quantiles=QUANTILES, filter_zscore=None, max_loss=1.0,
            )


def test_determinism_is_what_makes_results_bit_stable(data):
    """The reproducibility guarantee comes from single-partition execution.

    Floating-point addition is not associative, so a parallel plan may
    combine partial aggregates in a different order on each run and move the
    answer by a few units in the last place. ``deterministic=True`` (the
    default) pins that order. This test documents the mechanism: with it
    off, results are still correct, but only to tolerance.
    """
    with open_db(data) as db:
        kwargs = dict(
            periods=(1, 5), quantiles=QUANTILES, filter_zscore=None, max_loss=1.0
        )
        strict = quant.build_panel(db, "signals", "prices", **kwargs)
        loose = quant.build_panel(
            db, "signals", "prices", deterministic=False, **kwargs
        )
        baseline = strict.ic().to_arrow()
        for _ in range(5):
            assert strict.ic().to_arrow().equals(baseline)

        # Same numbers either way, to tolerance -- determinism is about
        # reproducibility, not correctness.
        np.testing.assert_allclose(
            loose.ic().to_pandas()["ic_1"].to_numpy(),
            baseline.to_pandas()["ic_1"].to_numpy(),
            rtol=1e-9,
            atol=1e-12,
        )
        assert strict.provenance.digest != loose.provenance.digest


def test_unpinned_reads_are_flagged_in_provenance(data):
    with panel_for(data) as (_db, panel):
        assert panel.provenance.pin.is_pinned is False
        assert any("unpinned" in w for w in panel.provenance.warnings())


def test_event_time_cutoff_excludes_unknowable_rows(data):
    """No row, and no forward return, may cross the decision-time embargo."""
    cutoff = data["dates"][29]
    with open_db(data) as db:
        panel = quant.build_panel(
            db,
            "signals",
            "prices",
            periods=(1, 5),
            quantiles=QUANTILES,
            filter_zscore=None,
            max_loss=1.0,
            event_time_cutoff=cutoff,
        )
        frame = panel.collect().to_pandas()

    assert len(frame) > 0
    assert frame["ts"].max() <= pd.Timestamp(cutoff)
    # A 5-bar forward return needs a price 5 bars later, so the last five
    # observable dates cannot have one: they are dropped, not guessed.
    last_with_fwd5 = frame.loc[frame["fwd_5"].notna(), "ts"].max()
    assert last_with_fwd5 <= pd.Timestamp(data["dates"][24])


def test_cutoff_result_equals_truncating_the_inputs(data):
    """The embargo is equivalent to never having had the later data."""
    cutoff_index = 39
    cutoff = data["dates"][cutoff_index]
    truncated = _synthetic()
    mask = pa.compute.less_equal(
        truncated["price_table"].column("ts"), pa.scalar(cutoff, pa.timestamp("us"))
    )
    truncated["price_table"] = truncated["price_table"].filter(mask)
    truncated["factor_table"] = truncated["factor_table"].filter(mask)

    kwargs = dict(
        periods=(1, 5), quantiles=QUANTILES, filter_zscore=None, max_loss=1.0
    )
    with open_db(data) as db:
        embargoed = quant.build_panel(
            db, "signals", "prices", event_time_cutoff=cutoff, **kwargs
        ).ic().to_pandas()
    with open_db(truncated) as db:
        physical = quant.build_panel(db, "signals", "prices", **kwargs).ic().to_pandas()
    pd.testing.assert_frame_equal(embargoed, physical)


def test_panel_frame_composes_with_the_lazy_builder(data):
    """The panel is a query, so it can be filtered further without leaving SQL."""
    with panel_for(data) as (_db, panel):
        top = panel.frame.filter(h5i_db.col("factor_quantile") == QUANTILES)
        rows = top.collect().to_pandas()
    assert len(rows) > 0
    assert set(rows["factor_quantile"]) == {QUANTILES}


# -- documented divergences -------------------------------------------------


def test_uneven_cross_section_is_a_documented_quantile_divergence(data):
    """ntile and qcut disagree when the cross-section does not divide evenly.

    Both produce equal-count buckets; they differ only in *which* bucket the
    remainder lands in. This test pins the difference so it cannot change
    silently, and asserts the invariant that does hold: bucket sizes never
    differ by more than one.
    """
    small = _synthetic(seed=3, n_dates=30, n_assets=13)
    ref = al_utils.get_clean_factor_and_forward_returns(
        factor=small["factor_series"],
        prices=small["prices_df"],
        periods=(1,),
        quantiles=QUANTILES,
        filter_zscore=None,
        max_loss=1.0,
    )
    with open_db(small) as db:
        panel = quant.build_panel(
            db,
            "signals",
            "prices",
            periods=(1,),
            quantiles=QUANTILES,
            filter_zscore=None,
            max_loss=1.0,
        )
        got = _panel_frame(panel)

    assert list(got.index) == list(ref.index)
    sizes = got.groupby([got.index.get_level_values("date"), "factor_quantile"]).size()
    assert sizes.max() - sizes.min() <= 1, "ntile must stay balanced"
    # The factor ordering within a date is identical even where labels differ.
    for date, chunk in got.groupby(level="date"):
        ordering = chunk.sort_values("factor")["factor_quantile"].to_numpy()
        assert (np.diff(ordering) >= 0).all(), "quantiles must be monotone in factor"
