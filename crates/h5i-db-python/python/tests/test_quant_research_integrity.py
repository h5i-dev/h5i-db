"""Research-integrity tests: overfitting, validation splits, cost calibration.

These cover the three statistics that decide whether a backtest result
means anything, so the properties asserted are the ones that would be
embarrassing to get wrong: a deflated Sharpe must fall as trials rise, a
purged split must not leak, and a cost model must not extrapolate a
straight line through a concave process.
"""

from __future__ import annotations

import math

import numpy as np
import pytest

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


def test_walk_forward_shares_the_embargo_with_the_cross_validators():
    plain = list(validation.walk_forward(300, train_size=100, test_size=50))
    embargoed = list(
        validation.walk_forward(300, train_size=100, test_size=50, embargo=0.05)
    )
    # Training precedes the test block here, so an embargo after the test
    # cannot remove anything: the semantics are shared, not copied.
    assert [s.train_size for s in plain] == [s.train_size for s in embargoed]


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
