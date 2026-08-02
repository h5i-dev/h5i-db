"""Research-integrity tests: overfitting, validation splits, cost calibration.

These cover the three statistics that decide whether a backtest result
means anything, so the properties asserted are the ones that would be
embarrassing to get wrong: a deflated Sharpe must fall as trials rise, a
purged split must not leak, and a cost model must not extrapolate a
straight line through a concave process.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import math
import tempfile

import numpy as np
import pyarrow as pa
import pytest

import h5i_db
from h5i_db import quant
from h5i_db.quant import costs, overfitting, validation


# -- distribution helpers ---------------------------------------------------


def test_the_normal_functions_are_mutual_inverses():
    for p in [0.001, 0.025, 0.1, 0.5, 0.9, 0.975, 0.999]:
        assert overfitting.normal_cdf(overfitting.normal_ppf(p)) == pytest.approx(
            p, abs=1e-9
        )


def test_normal_quantiles_match_known_values():
    assert overfitting.normal_ppf(0.975) == pytest.approx(1.959964, abs=1e-5)
    assert overfitting.normal_ppf(0.95) == pytest.approx(1.644854, abs=1e-5)
    assert overfitting.normal_cdf(0.0) == pytest.approx(0.5)


def test_a_probability_outside_the_open_unit_interval_is_refused():
    for bad in (0.0, 1.0, -0.1, 1.1):
        with pytest.raises(ValueError):
            overfitting.normal_ppf(bad)


# -- deflated Sharpe --------------------------------------------------------


def _returns(n=750, mu=0.0006, sigma=0.01, seed=7):
    return np.random.default_rng(seed).normal(mu, sigma, n)


def test_more_trials_always_deflate_further():
    """The property the whole statistic exists for."""
    returns = _returns()
    probabilities = [
        quant.deflated_sharpe(returns, trials=trials).probability
        for trials in (1, 10, 100, 1_000, 10_000)
    ]
    assert probabilities == sorted(probabilities, reverse=True)
    assert probabilities[0] > probabilities[-1]


def test_a_strong_result_survives_a_few_trials_and_a_weak_one_does_not():
    strong = _returns(mu=0.002, sigma=0.008, seed=3)
    weak = _returns(mu=0.0001, sigma=0.02, seed=4)
    assert quant.deflated_sharpe(strong, trials=5).is_significant
    assert not quant.deflated_sharpe(weak, trials=5).is_significant


def test_the_benchmark_rises_with_the_trial_count():
    assert overfitting.expected_maximum_sharpe(1, 0.01) == 0.0
    rising = [
        overfitting.expected_maximum_sharpe(trials, 0.01)
        for trials in (2, 10, 100, 1_000)
    ]
    assert rising == sorted(rising)


def test_a_wider_spread_of_trials_raises_the_bar():
    """A noisy search produces high maxima by chance, so it must be harder."""
    narrow = overfitting.expected_maximum_sharpe(100, 0.001)
    wide = overfitting.expected_maximum_sharpe(100, 0.01)
    assert wide > narrow


def test_negative_skew_and_fat_tails_reduce_confidence():
    """Both make a high Sharpe easier to reach by luck."""
    rng = np.random.default_rng(11)
    symmetric = rng.normal(0.001, 0.01, 800)
    # Same mean and roughly the same variance, but a long left tail.
    skewed = symmetric.copy()
    skewed[::40] -= 0.05
    skewed = skewed - skewed.mean() + symmetric.mean()
    skewed = skewed / skewed.std() * symmetric.std()

    plain = quant.deflated_sharpe(symmetric, trials=10)
    tailed = quant.deflated_sharpe(skewed, trials=10)
    assert tailed.skew < plain.skew
    assert tailed.probability < plain.probability


def test_deflation_refuses_degenerate_input():
    with pytest.raises(ValueError, match="at least 4"):
        quant.deflated_sharpe([0.01, 0.02, 0.03], trials=1)
    with pytest.raises(ValueError, match="constant"):
        quant.deflated_sharpe([0.01] * 50, trials=1)
    with pytest.raises(ValueError, match="trials"):
        quant.deflated_sharpe(_returns(), trials=0)


def test_the_trial_source_is_recorded():
    """A counted trial number must be distinguishable from an asserted one."""
    result = quant.deflated_sharpe(_returns(), trials=7)
    assert result.trials_source == "declared"
    assert result.to_dict()["trials_source"] == "declared"


def _sweep_result(trials, failures=()):
    """A SweepResult with no database behind it: from_sweep reads two lists."""
    return quant.SweepResult(
        db=None,
        forks=[trial["_fork"] for trial in trials],
        trials=list(trials),
        results_table="quant_sweep",
        failures=list(failures),
    )


def test_a_sweep_counts_the_trials_that_crashed_as_trials():
    """A trial that raised still consumed a draw from the search."""
    trials = [
        {"_fork": f"s-{i}", "_trial": i, "sharpe": 0.05 + 0.01 * i} for i in range(4)
    ]
    failures = [{"trial": 4, "fork": "s-4", "error": "boom"}]
    counted = overfitting.from_sweep(_sweep_result(trials, failures), _returns())
    quiet = overfitting.from_sweep(_sweep_result(trials), _returns())
    assert counted.trials == 5 and quiet.trials == 4
    assert "1 failed" in counted.trials_source
    # More trials is a higher bar, so a sweep cannot launder its count by
    # crashing.
    assert counted.benchmark > quiet.benchmark


def test_an_annualized_sweep_metric_is_brought_back_to_the_scale_it_is_compared_on():
    """Deflation works per observation; a stored Sharpe usually does not."""
    daily = [
        {"_fork": f"s-{i}", "_trial": i, "sharpe": value}
        for i, value in enumerate((0.02, 0.04, 0.06, 0.08))
    ]
    annual = [
        {**trial, "sharpe": trial["sharpe"] * math.sqrt(252)} for trial in daily
    ]
    per_observation = overfitting.from_sweep(_sweep_result(daily), _returns())
    rescaled = overfitting.from_sweep(
        _sweep_result(annual), _returns(), annualization=252
    )
    assert rescaled.benchmark == pytest.approx(per_observation.benchmark)
    # Left unscaled, the same trials set a benchmark sqrt(252) times higher.
    unscaled = overfitting.from_sweep(_sweep_result(annual), _returns())
    assert unscaled.benchmark > per_observation.benchmark * 10


# -- minimum track record ---------------------------------------------------


def test_minimum_track_record_shrinks_as_the_edge_grows():
    weak = quant.minimum_track_record_length(_returns(mu=0.0002, seed=21))
    strong = quant.minimum_track_record_length(_returns(mu=0.003, seed=21))
    assert strong < weak
    assert strong > 1


def test_a_strategy_below_its_benchmark_never_qualifies():
    returns = _returns(mu=0.0001)
    assert math.isinf(
        quant.minimum_track_record_length(returns, benchmark=10.0)
    )


# -- probability of backtest overfitting ------------------------------------


def test_pure_noise_gives_a_pbo_near_a_half():
    """Selection on noise carries no information."""
    rng = np.random.default_rng(5)
    matrix = rng.normal(0, 0.01, size=(600, 12))
    result = quant.probability_of_backtest_overfitting(matrix, partitions=6)
    assert 0.25 <= result.pbo <= 0.75, result.pbo
    assert result.splits == 20  # C(6, 3)
    assert result.strategies == 12


def test_a_genuinely_better_strategy_gives_a_low_pbo():
    rng = np.random.default_rng(6)
    matrix = rng.normal(0, 0.01, size=(600, 8))
    # One column has a real, persistent edge.
    matrix[:, 3] += 0.004
    result = quant.probability_of_backtest_overfitting(matrix, partitions=6)
    assert result.pbo < 0.25, result.pbo
    assert not result.is_overfit


def test_a_flat_winner_is_dropped_rather_than_ranked_last():
    """A strategy with no out-of-sample variance has no Sharpe to rank.

    Comparing against its NaN is false in every direction, which used to
    score it below every rival and count the split as overfit: a verdict
    about a missing number rather than about the selection.
    """
    rng = np.random.default_rng(11)
    matrix = rng.normal(0, 0.01, size=(240, 4))
    # One column is flat over the whole second half, so any split that tests
    # there has no Sharpe for it. It also wins in sample everywhere, by
    # sitting well above the noise in the first half.
    matrix[:120, 0] = 0.02 + rng.normal(0, 0.0001, size=120)
    matrix[120:, 0] = 0.0
    result = quant.probability_of_backtest_overfitting(matrix, partitions=4)
    assert result.splits < 6, "splits with an unrankable winner must be dropped"
    assert all(not math.isnan(rank) for rank in result.ranks)
    assert 0.0 <= result.pbo <= 1.0


def test_pbo_validates_its_inputs():
    rng = np.random.default_rng(1)
    with pytest.raises(ValueError, match="two"):
        quant.probability_of_backtest_overfitting(rng.normal(size=(100, 1)))
    with pytest.raises(ValueError, match="even"):
        quant.probability_of_backtest_overfitting(
            rng.normal(size=(100, 3)), partitions=5
        )
    with pytest.raises(ValueError, match="2-D"):
        quant.probability_of_backtest_overfitting(rng.normal(size=100))
    with pytest.raises(ValueError, match="blocks"):
        quant.probability_of_backtest_overfitting(
            rng.normal(size=(6, 3)), partitions=8
        )


# -- validation splitters ---------------------------------------------------


def test_purged_kfold_covers_every_observation_exactly_once_in_test():
    splits = list(validation.purged_kfold(100, folds=5))
    assert len(splits) == 5
    tested = sorted(index for split in splits for index in split.test)
    assert tested == list(range(100))


def test_purging_removes_training_labels_that_reach_into_the_test_set():
    """The leak this exists to stop."""
    horizons = [5] * 100
    splits = list(validation.purged_kfold(100, folds=4, horizons=horizons))
    middle = splits[1]
    first_test = min(middle.test)
    # No surviving training index may have a label reaching the test block.
    for index in middle.train:
        assert not (index <= max(middle.test) and index + 5 >= first_test)
    assert middle.purged, "a 5-bar horizon must purge something"


def test_a_longer_horizon_purges_more():
    short = list(validation.purged_kfold(200, folds=4, horizons=[1] * 200))
    long = list(validation.purged_kfold(200, folds=4, horizons=[20] * 200))
    assert sum(len(s.purged) for s in long) > sum(len(s.purged) for s in short)


def test_the_embargo_removes_observations_after_the_test_block():
    without = list(validation.purged_kfold(200, folds=4, embargo=0.0))
    with_embargo = list(validation.purged_kfold(200, folds=4, embargo=0.05))
    assert sum(len(s.purged) for s in with_embargo) > sum(
        len(s.purged) for s in without
    )
    # An embargoed index sits immediately after a test block.
    first = with_embargo[0]
    assert max(first.test) + 1 in first.purged


def test_embargo_span_is_the_single_owner_of_the_arithmetic():
    assert list(validation.embargo_span(9, 1_000, 0.01)) == list(range(10, 20))
    assert list(validation.embargo_span(9, 1_000, 0.0)) == []
    # It clips at the end of the sample rather than running past it.
    assert list(validation.embargo_span(995, 1_000, 0.01)) == list(range(996, 1_000))
    with pytest.raises(ValueError):
        validation.embargo_span(0, 100, 1.5)


def test_train_and_test_never_overlap():
    for split in validation.purged_kfold(120, folds=6, horizons=[3] * 120):
        assert not set(split.train) & set(split.test)


def test_combinatorial_purged_yields_the_expected_number_of_paths():
    splits = list(validation.combinatorial_purged(300, groups=6, test_groups=2))
    assert len(splits) == 15  # C(6, 2)
    for split in splits:
        assert not set(split.train) & set(split.test)


def test_combinatorial_splits_embargo_every_chosen_block():
    """Non-adjacent test blocks each need their own embargo."""
    splits = list(
        validation.combinatorial_purged(
            400, groups=8, test_groups=2, embargo=0.02
        )
    )
    # Find a split whose two test blocks are far apart.
    distant = next(
        split
        for split in splits
        if max(split.test) - min(split.test) > 200
    )
    blocks = []
    previous = None
    for index in sorted(distant.test):
        if previous is None or index != previous + 1:
            blocks.append(index)
        previous = index
    assert len(blocks) == 2, "expected two separate test blocks"
    # Each block's successor is purged, not only the last one's.
    ends = []
    previous = None
    for index in sorted(distant.test):
        if previous is not None and index != previous + 1:
            ends.append(previous)
        previous = index
    ends.append(previous)
    for end in ends:
        if end + 1 < 400:
            assert end + 1 in distant.purged or end + 1 in distant.test


def test_walk_forward_moves_forward_and_can_expand():
    rolling = list(validation.walk_forward(300, train_size=100, test_size=50))
    # Windows start at 0, 50, 100, 150; the next would need index 350.
    assert len(rolling) == 4
    assert rolling[0].test == tuple(range(100, 150))
    assert rolling[1].test == tuple(range(150, 200))
    assert min(rolling[1].train) > min(rolling[0].train), "rolling drops old data"

    expanding = list(
        validation.walk_forward(300, train_size=100, test_size=50, expanding=True)
    )
    assert min(expanding[1].train) == 0, "expanding keeps it"
    assert expanding[1].train_size > expanding[0].train_size


def test_walk_forward_embargoes_the_training_data_next_to_the_test_block():
    """Its training set is in the past, so the embargo has to look backwards.

    Applied forwards, as the cross-validators apply it, it would land on
    indices this splitter never trains on and remove nothing at all.
    """
    plain = list(validation.walk_forward(300, train_size=100, test_size=50))
    embargoed = list(
        validation.walk_forward(300, train_size=100, test_size=50, embargo=0.05)
    )
    # Measured against one window, not the whole sample: this splitter only
    # ever trains on `train_size` observations, so a fraction of `n` is a
    # fraction of data the fold does not have.
    width = validation.embargo_width(100 + 50, 0.05)
    assert width == 8
    for before, after in zip(plain, embargoed):
        assert after.train_size == before.train_size - width
        assert len(after.purged) == width
        # The gap sits immediately before the test block, and nothing inside
        # the test block or after it was touched.
        assert set(after.purged) == set(range(min(after.test) - width, min(after.test)))
        assert max(after.train) < min(after.test) - width


def test_a_long_sample_does_not_embargo_the_whole_training_window():
    """A width scaled to `n` outran the window it was charged against.

    At these parameters the old arithmetic asked for 500 embargoed
    observations from a 400-observation training window, so every one of the
    96 folds came back with `train=()` and fitted on nothing.
    """
    splits = list(
        validation.walk_forward(10_000, train_size=400, test_size=100, embargo=0.05)
    )
    assert len(splits) == 96
    assert all(s.train_size > 0 for s in splits)
    # 5% of one 500-observation window, not of the sample.
    assert splits[0].train_size == 400 - 25
    assert len(splits[0].purged) == 25


def test_an_entirely_purged_training_set_is_refused_not_returned():
    """`train=()` is a silently meaningless fit, so it raises instead."""
    with pytest.raises(ValueError, match="no training data left"):
        list(validation.walk_forward(1_000, train_size=10, test_size=100, embargo=0.9))


def test_the_two_embargo_directions_are_the_same_width():
    assert list(validation.embargo_gap(20, 1_000, 0.01)) == list(range(10, 20))
    assert list(validation.embargo_gap(20, 1_000, 0.0)) == []
    # It clips at the start of the sample rather than running before it.
    assert list(validation.embargo_gap(4, 1_000, 0.01)) == list(range(0, 4))
    with pytest.raises(ValueError):
        validation.embargo_gap(50, 100, 1.5)


def test_an_explicit_zero_step_is_refused_rather_than_reinterpreted():
    """`step or test_size` turned a caller's 0 into a working default."""
    with pytest.raises(ValueError, match="step must be positive"):
        list(validation.walk_forward(300, train_size=100, test_size=50, step=0))


def test_splitters_validate_their_shapes():
    with pytest.raises(ValueError):
        list(validation.purged_kfold(3, folds=5))
    with pytest.raises(ValueError, match="one entry per observation"):
        list(validation.purged_kfold(100, folds=4, horizons=[1] * 50))
    with pytest.raises(ValueError):
        list(validation.combinatorial_purged(100, groups=4, test_groups=4))


def test_a_split_that_overlaps_is_rejected_on_construction():
    with pytest.raises(ValueError, match="overlap"):
        validation.Split(train=(1, 2, 3), test=(3, 4))


# -- cost calibration -------------------------------------------------------


def _sample(direction, fill, reference, quantity, liquidity=None):
    return costs.SlippageSample(
        direction=direction,
        fill_price=fill,
        reference_price=reference,
        quantity=quantity,
        reference_size=liquidity,
    )


def test_slippage_is_signed_so_both_sides_are_costs():
    buy = _sample(1, 100.5, 100.0, 10)
    sell = _sample(-1, 99.5, 100.0, 10)
    assert buy.slippage == pytest.approx(0.5)
    assert sell.slippage == pytest.approx(0.5), "a sell below the mid also costs"
    # A buy below the mid is a saving, and shows as negative.
    assert _sample(1, 99.5, 100.0, 10).slippage == pytest.approx(-0.5)


def test_effective_spread_is_the_round_trip():
    samples = [_sample(1, 100.5, 100.0, 1), _sample(-1, 99.5, 100.0, 1)]
    assert costs.effective_spread(samples) == pytest.approx(1.0)


def test_implementation_shortfall_is_size_weighted():
    # One large expensive fill must outweigh many small cheap ones.
    samples = [_sample(1, 100.1, 100.0, 1) for _ in range(9)]
    samples.append(_sample(1, 101.0, 100.0, 91))
    weighted = costs.implementation_shortfall(samples)
    unweighted = sum(s.slippage for s in samples) / len(samples)
    assert weighted > unweighted


def test_a_square_root_impact_process_is_recovered():
    rng = np.random.default_rng(17)
    samples = []
    for _ in range(300):
        participation = float(rng.uniform(0.01, 1.0))
        # True process: 0.01 fixed + 0.05 * sqrt(participation).
        slippage = 0.01 + 0.05 * math.sqrt(participation) + rng.normal(0, 0.002)
        samples.append(
            costs.SlippageSample(
                direction=1,
                fill_price=100.0 + slippage,
                reference_price=100.0,
                quantity=participation,
                reference_size=1.0,
            )
        )
    fit = costs.fit_impact(samples, shape="sqrt")
    assert fit.intercept == pytest.approx(0.01, abs=0.005)
    assert fit.coefficient == pytest.approx(0.05, abs=0.01)
    assert fit.r_squared > 0.9
    assert fit.is_usable


def test_a_square_root_fit_beats_a_line_on_a_concave_process():
    """Why sqrt is the default: a line extrapolates badly at size."""
    rng = np.random.default_rng(23)
    samples = []
    for _ in range(300):
        participation = float(rng.uniform(0.01, 1.0))
        slippage = 0.05 * math.sqrt(participation) + rng.normal(0, 0.001)
        samples.append(
            costs.SlippageSample(
                direction=1,
                fill_price=100.0 + slippage,
                reference_price=100.0,
                quantity=participation,
                reference_size=1.0,
            )
        )
    root = costs.fit_impact(samples, shape="sqrt")
    linear = costs.fit_impact(samples, shape="linear")
    assert root.r_squared > linear.r_squared
    assert root.residual_std < linear.residual_std


def test_predictions_are_monotone_in_size():
    fit = costs.CostFit(
        intercept=0.01,
        coefficient=0.05,
        shape="sqrt",
        r_squared=0.95,
        residual_std=0.001,
        observations=100,
    )
    assert fit.predict(0.0) == pytest.approx(0.01)
    assert fit.predict(1.0) > fit.predict(0.25) > fit.predict(0.0)
    with pytest.raises(ValueError):
        fit.predict(-1.0)


def test_a_thin_fit_reports_that_it_is_thin():
    samples = [
        _sample(1, 100.0 + 0.01 * i, 100.0, float(i + 1), 1.0) for i in range(5)
    ]
    fit = costs.fit_impact(samples)
    assert not fit.is_usable, "five fills is not a cost model"
    assert fit.observations == 5


def test_cost_fitting_validates_its_inputs():
    with pytest.raises(ValueError, match="three points"):
        costs.fit_impact([_sample(1, 100.0, 100.0, 1)])
    with pytest.raises(ValueError, match="sqrt.*linear"):
        costs.fit_impact(
            [_sample(1, 100.0 + i, 100.0, 1) for i in range(5)], shape="cubic"
        )
    with pytest.raises(ValueError, match="no fills"):
        costs.effective_spread([])


def test_a_fit_with_no_variance_to_explain_is_not_reported_as_explaining_none():
    """Every fill slipped the same: the model captures all of it, not none."""
    samples = [_sample(1, 100.5, 100.0, float(i + 1), 1.0) for i in range(6)]
    fit = costs.fit_impact(samples)
    assert fit.intercept == pytest.approx(0.5)
    assert fit.coefficient == pytest.approx(0.0, abs=1e-9)
    assert fit.r_squared == 1.0
    assert fit.residual_std == pytest.approx(0.0, abs=1e-9)


# -- calibrating from a run's own fills --------------------------------------

# Mirrors crates/h5i-db-backtest/src/schema.rs::fills().
BT_FILLS = pa.schema(
    [
        pa.field("ts", pa.timestamp("ns"), nullable=False),
        pa.field("order_id", pa.int64(), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("outcome", pa.uint16(), nullable=False),
        pa.field("side", pa.string(), nullable=False),
        pa.field("price", pa.float64(), nullable=False),
        pa.field("quantity", pa.float64(), nullable=False),
        pa.field("commission", pa.float64(), nullable=False),
        pa.field("is_taker", pa.bool_(), nullable=False),
        pa.field("tag", pa.string()),
    ]
)
# Mirrors crates/h5i-db-backtest/src/schema.rs::equity(): note that it holds no
# price column at all, which is the whole reason fit_from_fills cannot quietly
# measure fills against it.
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
MARKS = pa.schema(
    [
        pa.field("ts", pa.timestamp("ns"), nullable=False),
        pa.field("mid", pa.float64(), nullable=False),
    ]
)


@contextlib.contextmanager
def _fills_db(fills, marks=None):
    """A fork-shaped database: `bt_fills`, an empty-priced `bt_equity`, marks."""
    base = dt.datetime(2026, 4, 1, 14, 0, 0)
    with tempfile.TemporaryDirectory() as tmp:
        db = h5i_db.Database(f"{tmp}/run.db", create=True)
        db.create_table("bt_fills", BT_FILLS, time_column="ts")
        db.append(
            "bt_fills",
            pa.table(
                {
                    "ts": [base + dt.timedelta(seconds=s) for s, *_ in fills],
                    "order_id": [i for i, _ in enumerate(fills)],
                    "instrument_id": ["EVENT-A"] * len(fills),
                    "outcome": [0] * len(fills),
                    "side": [side for _, side, *_ in fills],
                    "price": [price for _, _, price, _ in fills],
                    "quantity": [quantity for *_, quantity in fills],
                    "commission": [0.0] * len(fills),
                    "is_taker": [True] * len(fills),
                    "tag": [None] * len(fills),
                },
                schema=BT_FILLS,
            ),
        )
        db.create_table("bt_equity", BT_EQUITY, time_column="ts")
        db.append(
            "bt_equity",
            pa.table(
                {
                    "ts": [base + dt.timedelta(seconds=s) for s in (0, 60)],
                    "cash": [1_000.0, 1_000.0],
                    "position_value": [0.0, 0.0],
                    "equity": [1_000.0, 1_000.0],
                    "realized_pnl": [0.0, 0.0],
                    "unrealized_pnl": [0.0, 0.0],
                },
                schema=BT_EQUITY,
            ),
        )
        if marks is not None:
            db.create_table("marks", MARKS, time_column="ts")
            db.append(
                "marks",
                pa.table(
                    {
                        "ts": [base + dt.timedelta(seconds=s) for s, _ in marks],
                        "mid": [mid for _, mid in marks],
                    },
                    schema=MARKS,
                ),
            )
        try:
            yield db
        finally:
            db.close()


def test_one_fill_per_instant_is_refused_instead_of_fitting_a_free_market():
    """The reference was the other fills at the same instant; there were none.

    Every slippage sample is then exactly zero, and the fit that follows is a
    zero-cost model with a zero r-squared and no warning attached, which is
    indistinguishable from a well-fitted free market.
    """
    fills = [(second, "buy", 100.0 + 0.01 * second, 5.0) for second in range(8)]
    with _fills_db(fills) as db:
        with pytest.raises(ValueError, match="identically-zero"):
            costs.fit_from_fills(db, reference=None)


def test_fills_are_measured_against_the_reference_standing_when_they_printed():
    """The documented reference, actually read, and joined backwards in time."""
    marks = [(0, 100.0), (5, 100.0)]
    # Each buy pays 0.02 over the standing mid, on a range of sizes.
    fills = [(second, "buy", 100.02, float(second + 1)) for second in range(1, 9)]
    with _fills_db(fills, marks=marks) as db:
        fit = costs.fit_from_fills(db, reference_table="marks", reference="mid")
    assert fit.observations == 8
    assert fit.intercept == pytest.approx(0.02, abs=1e-9)
    assert fit.coefficient == pytest.approx(0.0, abs=1e-9)


def test_a_reference_column_that_does_not_exist_is_named_rather_than_ignored():
    """`bt_equity` carries no price, so the documented default cannot work."""
    fills = [(second, "buy", 100.0, 5.0) for second in range(4)]
    with _fills_db(fills) as db:
        with pytest.raises(ValueError, match="no column 'mid'"):
            costs.fit_from_fills(db)
