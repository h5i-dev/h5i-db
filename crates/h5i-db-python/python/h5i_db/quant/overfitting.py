"""Overfitting statistics (ROADMAP_QUANT.md Q3.4, VII-D1).

A Sharpe ratio selected as the best of many trials is not the Sharpe of a
strategy; it is the maximum of a sample, and the maximum of enough samples
of noise is impressive. Correcting for that needs the number of trials, and
in practice that number is guessed or omitted because nothing records it.

Here it is not guessed. :func:`from_sweep` reads it off a
:class:`~h5i_db.quant.SweepResult`, which knows exactly how many parameter
combinations ran because it created a fork for each one. Passing a trial
count by hand is allowed and is recorded as such, so a reader can tell a
counted claim from an asserted one.

Implemented from Bailey & López de Prado: the Deflated Sharpe Ratio, the
minimum track record length, and the Probability of Backtest Overfitting via
combinatorially symmetric cross-validation.

No scipy: the two distribution functions needed are computed here, so the
module has no dependency beyond the standard library and numpy.
"""

from __future__ import annotations

import itertools
import math
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

__all__ = [
    "deflated_sharpe",
    "minimum_track_record_length",
    "expected_maximum_sharpe",
    "probability_of_backtest_overfitting",
    "from_sweep",
    "DeflatedSharpe",
    "PBOResult",
]

#: Euler-Mascheroni, which appears in the expected maximum of N draws.
_EULER = 0.577215664901532860606512090082

# Acklam's rational approximation to the inverse normal CDF. Accurate to
# about 1.15e-9 across the whole range, which is far tighter than anything
# these statistics resolve.
_A = (
    -3.969683028665376e01,
    2.209460984245205e02,
    -2.759285104469687e02,
    1.383577518672690e02,
    -3.066479806614716e01,
    2.506628277459239e00,
)
_B = (
    -5.447609879822406e01,
    1.615858368580409e02,
    -1.556989798598866e02,
    6.680131188771972e01,
    -1.328068155288572e01,
)
_C = (
    -7.784894002430293e-03,
    -3.223964580411365e-01,
    -2.400758277161838e00,
    -2.549732539343734e00,
    4.374664141464968e00,
    2.938163982698783e00,
)
_D = (
    7.784695709041462e-03,
    3.224671290700398e-01,
    2.445134137142996e00,
    3.754408661907416e00,
)


def normal_cdf(x: float) -> float:
    """Standard normal CDF."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def normal_ppf(p: float) -> float:
    """Standard normal quantile (the inverse of :func:`normal_cdf`)."""
    if not 0.0 < p < 1.0:
        raise ValueError(f"a probability must lie strictly in (0, 1), got {p}")
    if p < 0.02425:
        q = math.sqrt(-2.0 * math.log(p))
        return (((((_C[0] * q + _C[1]) * q + _C[2]) * q + _C[3]) * q + _C[4]) * q + _C[5]) / (
            (((_D[0] * q + _D[1]) * q + _D[2]) * q + _D[3]) * q + 1.0
        )
    if p > 1.0 - 0.02425:
        q = math.sqrt(-2.0 * math.log(1.0 - p))
        return -(((((_C[0] * q + _C[1]) * q + _C[2]) * q + _C[3]) * q + _C[4]) * q + _C[5]) / (
            (((_D[0] * q + _D[1]) * q + _D[2]) * q + _D[3]) * q + 1.0
        )
    q = p - 0.5
    r = q * q
    return (((((_A[0] * r + _A[1]) * r + _A[2]) * r + _A[3]) * r + _A[4]) * r + _A[5]) * q / (
        ((((_B[0] * r + _B[1]) * r + _B[2]) * r + _B[3]) * r + _B[4]) * r + 1.0
    )


def _moments(returns: Sequence[float]) -> tuple:
    """Per-observation Sharpe, skew and kurtosis of a returns series.

    The Sharpe here is **not annualized**. Deflation compares a statistic to
    its own sampling distribution, and annualizing first would scale the
    statistic without scaling the distribution, which quietly inflates every
    result.
    """
    import numpy as np

    values = np.asarray(list(returns), dtype=float)
    values = values[~np.isnan(values)]
    n = values.size
    if n < 4:
        raise ValueError(
            f"deflation needs at least 4 observations to estimate a fourth "
            f"moment, got {n}"
        )
    mean = float(values.mean())
    std = float(values.std(ddof=1))
    if std == 0.0:
        raise ValueError("a constant returns series has no Sharpe ratio to deflate")
    centred = values - mean
    m2 = float((centred**2).mean())
    skew = float((centred**3).mean()) / m2**1.5
    kurtosis = float((centred**4).mean()) / m2**2
    return mean / std, skew, kurtosis, n


def expected_maximum_sharpe(trials: int, variance: float) -> float:
    """The Sharpe you would expect from the best of `trials` worthless strategies.

    This is the benchmark a real strategy has to beat. `variance` is the
    variance of the Sharpe ratios *across* trials: a wide spread of results
    raises the bar, because a wide spread produces high maxima by chance.
    """
    if trials < 1:
        raise ValueError("trials must be at least 1")
    if variance < 0:
        raise ValueError("variance must not be negative")
    if trials == 1:
        return 0.0
    spread = math.sqrt(variance)
    return spread * (
        (1.0 - _EULER) * normal_ppf(1.0 - 1.0 / trials)
        + _EULER * normal_ppf(1.0 - 1.0 / (trials * math.e))
    )


@dataclass
class DeflatedSharpe:
    """The result of deflating an observed Sharpe ratio."""

    #: Observed Sharpe, per observation (not annualized).
    sharpe: float
    #: The benchmark implied by the trial count.
    benchmark: float
    #: Probability the strategy's true Sharpe exceeds the benchmark.
    probability: float
    trials: int
    observations: int
    skew: float
    kurtosis: float
    #: Where the trial count came from, so a counted claim is
    #: distinguishable from an asserted one.
    trials_source: str = "declared"

    @property
    def is_significant(self) -> bool:
        """Whether the strategy clears the benchmark at 95%."""
        return self.probability >= 0.95

    def to_dict(self) -> dict:
        return {
            "sharpe": self.sharpe,
            "benchmark": self.benchmark,
            "deflated_sharpe": self.probability,
            "trials": self.trials,
            "trials_source": self.trials_source,
            "observations": self.observations,
            "skew": self.skew,
            "kurtosis": self.kurtosis,
            "significant_at_95": self.is_significant,
        }


def deflated_sharpe(
    returns: Sequence[float],
    *,
    trials: int,
    trial_sharpe_variance: Optional[float] = None,
    benchmark: Optional[float] = None,
    trials_source: str = "declared",
) -> DeflatedSharpe:
    """Deflate a Sharpe ratio for the number of trials that produced it.

    ``trials`` is how many strategies were tried before this one was chosen.
    ``trial_sharpe_variance`` is the variance of those trials' Sharpe ratios;
    when it is unknown the returns' own sampling variance is used, which is
    the conservative substitute rather than an assumption of zero.

    Returns the probability that the true Sharpe exceeds the benchmark. A
    value below 0.95 means the result is indistinguishable from the best of
    that many coin flips.
    """
    sharpe, skew, kurtosis, n = _moments(returns)
    if trials < 1:
        raise ValueError("trials must be at least 1")

    if benchmark is None:
        variance = (
            trial_sharpe_variance
            if trial_sharpe_variance is not None
            else (1.0 + 0.5 * sharpe**2) / n
        )
        benchmark = expected_maximum_sharpe(trials, variance)

    # The Sharpe's own standard error, corrected for non-normal returns:
    # negative skew and fat tails both make a high Sharpe easier to obtain
    # by luck, so both widen the distribution the statistic is judged
    # against.
    denominator = 1.0 - skew * sharpe + 0.25 * (kurtosis - 1.0) * sharpe**2
    if denominator <= 0:
        raise ValueError(
            "the returns' higher moments give a non-positive variance for the "
            "Sharpe estimator; the series is too short or too degenerate to "
            "deflate"
        )
    statistic = (sharpe - benchmark) * math.sqrt(n - 1) / math.sqrt(denominator)
    return DeflatedSharpe(
        sharpe=sharpe,
        benchmark=benchmark,
        probability=normal_cdf(statistic),
        trials=trials,
        observations=n,
        skew=skew,
        kurtosis=kurtosis,
        trials_source=trials_source,
    )


def minimum_track_record_length(
    returns: Sequence[float],
    *,
    benchmark: float = 0.0,
    confidence: float = 0.95,
) -> float:
    """How many observations are needed for a Sharpe to be believable.

    Returns the number of observations at which the observed Sharpe would
    become statistically distinguishable from ``benchmark``. A track record
    shorter than this is not evidence, however good it looks.
    """
    sharpe, skew, kurtosis, _n = _moments(returns)
    if sharpe <= benchmark:
        return float("inf")
    z = normal_ppf(confidence)
    variance = 1.0 - skew * sharpe + 0.25 * (kurtosis - 1.0) * sharpe**2
    return 1.0 + variance * (z / (sharpe - benchmark)) ** 2


@dataclass
class PBOResult:
    """Probability of backtest overfitting, via CSCV."""

    #: Fraction of splits where the in-sample winner underperformed the
    #: out-of-sample median.
    pbo: float
    #: Out-of-sample relative ranks of the in-sample winner, one per split.
    ranks: list = field(default_factory=list)
    splits: int = 0
    strategies: int = 0

    @property
    def is_overfit(self) -> bool:
        """Whether selection carries no information (PBO at or above 0.5)."""
        return self.pbo >= 0.5

    def to_dict(self) -> dict:
        return {
            "pbo": self.pbo,
            "splits": self.splits,
            "strategies": self.strategies,
            "overfit": self.is_overfit,
        }


def probability_of_backtest_overfitting(
    performance: Any,
    *,
    partitions: int = 8,
) -> PBOResult:
    """Combinatorially symmetric cross-validation (Bailey et al.).

    ``performance`` is a matrix of shape ``(observations, strategies)``:
    each column is one trial's returns over the same period. The sample is
    cut into ``partitions`` contiguous blocks; every way of choosing half of
    them forms an in-sample set with the rest out of sample. For each split,
    the strategy that won in sample is looked up out of sample, and PBO is
    how often that winner landed in the bottom half.

    A PBO near 0.5 means selection carried no information: the in-sample
    winner is as likely as not to be a below-median performer next period.
    """
    import numpy as np

    matrix = np.asarray(performance, dtype=float)
    if matrix.ndim != 2:
        raise ValueError("performance must be a 2-D (observations, strategies) matrix")
    observations, strategies = matrix.shape
    if strategies < 2:
        raise ValueError("PBO compares strategies; at least two are needed")
    if partitions < 2 or partitions % 2 != 0:
        raise ValueError("partitions must be an even number of at least 2")
    if observations < partitions * 2:
        raise ValueError(
            f"{observations} observations cannot be cut into {partitions} usable "
            "blocks; use fewer partitions or a longer sample"
        )

    blocks = np.array_split(np.arange(observations), partitions)
    half = partitions // 2
    ranks = []
    for chosen in itertools.combinations(range(partitions), half):
        rest = [index for index in range(partitions) if index not in chosen]
        in_sample = np.concatenate([blocks[index] for index in chosen])
        out_sample = np.concatenate([blocks[index] for index in rest])

        in_scores = _sharpe_columns(matrix[in_sample])
        out_scores = _sharpe_columns(matrix[out_sample])
        if np.all(np.isnan(in_scores)) or np.all(np.isnan(out_scores)):
            continue
        winner = int(np.nanargmax(in_scores))
        if np.isnan(out_scores[winner]):
            # The winner was flat out of sample, so it has no Sharpe to rank.
            # Every comparison against NaN is false, which would score it
            # below every rival and count the split as overfit; that is a
            # verdict about a missing number, not about the selection. The
            # split carries no evidence either way, so it is dropped like the
            # other degenerate ones above.
            continue
        # Relative rank of the winner out of sample, in [0, 1].
        finite = out_scores[~np.isnan(out_scores)]
        if finite.size < 2:
            continue
        rank = float((finite < out_scores[winner]).sum()) / (finite.size - 1)
        ranks.append(rank)

    if not ranks:
        raise ValueError("no usable split produced a comparison")
    below_median = sum(1 for rank in ranks if rank < 0.5)
    return PBOResult(
        pbo=below_median / len(ranks),
        ranks=ranks,
        splits=len(ranks),
        strategies=strategies,
    )


def _sharpe_columns(block: Any):
    """Per-column Sharpe of a block, NaN where it is undefined."""
    import numpy as np

    mean = block.mean(axis=0)
    std = block.std(axis=0, ddof=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(std > 0, mean / std, np.nan)


def from_sweep(
    sweep_result: Any,
    returns: Sequence[float],
    *,
    metric: str = "sharpe",
    annualization: float = 1.0,
) -> DeflatedSharpe:
    """Deflate a Sharpe using the trial count a sweep actually ran.

    This is the honest wiring: the sweep created one fork per parameter
    combination, so the number of trials is a fact about the run rather than
    a number someone remembered. The variance of the trials' own metric is
    used as the spread, which is what the benchmark is supposed to reflect.

    A failed trial still consumed a draw from the search: it was tried, and it
    could have been the one that won. Counting only the trials that finished
    would let a sweep launder its trial count by crashing, so the failures are
    counted too.

    ``annualization`` is the factor the stored metric was annualized by (252
    for daily bars, 1 when the trials stored a per-observation Sharpe, which
    is what :func:`deflated_sharpe` works in). The spread is divided back down
    by its square root before use: an annualized variance against a
    per-observation statistic overstates the benchmark by the same factor and
    fails every strategy in the sweep.
    """
    import numpy as np

    if annualization <= 0:
        raise ValueError(f"annualization must be positive, got {annualization}")
    completed = len(sweep_result.trials)
    failed = len(getattr(sweep_result, "failures", ()) or ())
    trials = completed + failed
    if trials < 1:
        raise ValueError("the sweep ran no trials")
    scale = math.sqrt(annualization)
    values = [
        float(trial[metric]) / scale
        for trial in sweep_result.trials
        if trial.get(metric) is not None
    ]
    variance = float(np.var(values, ddof=1)) if len(values) > 1 else None
    source = f"sweep of {trials} trials"
    if failed:
        source += f" ({failed} failed)"
    return deflated_sharpe(
        returns,
        trials=trials,
        trial_sharpe_variance=variance,
        trials_source=source,
    )
