"""Scoring a probability forecast against the market's own.

An event contract quotes a probability, so a strategy that trades it is making
a competing forecast whether or not it says so. That makes one comparison
available which no equity curve provides: was your probability better than the
price you paid?

The Brier score is the mean squared error of a probability, `(p - y)^2`. The
advantage is `market_brier - strategy_brier`, so positive means you were closer
to the truth. It is worth more than a P&L on small samples, because it uses the
whole distance to the outcome rather than only the sign of the trade, and it
cannot be inflated by sizing.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Optional, Sequence

__all__ = [
    "BrierAdvantage",
    "brier_advantage",
    "brier_decomposition",
    "reliability_curve",
]


def _clip(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def _validate(
    strategy: Sequence[float], market: Sequence[float], outcomes: Sequence[float]
) -> tuple[list[float], list[float], list[float]]:
    if not (len(strategy) == len(market) == len(outcomes)):
        raise ValueError(
            "strategy, market and outcome series must be the same length; got "
            f"{len(strategy)}, {len(market)}, {len(outcomes)}"
        )
    if not strategy:
        raise ValueError("nothing to score: the series are empty")
    kept_strategy: list[float] = []
    kept_market: list[float] = []
    kept_outcome: list[float] = []
    for index, (p_user, p_market, y) in enumerate(zip(strategy, market, outcomes)):
        if p_user is None or p_market is None or y is None:
            continue
        if any(isinstance(value, float) and math.isnan(value) for value in (p_user, p_market, y)):
            continue
        outcome = float(y)
        if outcome not in (0.0, 1.0):
            raise ValueError(
                f"observation {index}: outcome must be 0 or 1, got {outcome}. "
                "A Brier score is defined against a realized binary event."
            )
        kept_strategy.append(_clip(p_user))
        kept_market.append(_clip(p_market))
        kept_outcome.append(outcome)
    if not kept_strategy:
        raise ValueError("every observation was missing; nothing to score")
    return kept_strategy, kept_market, kept_outcome


@dataclass(frozen=True)
class BrierAdvantage:
    """How much better than the market a forecast was, and on what sample."""

    observations: int
    strategy_brier: float
    market_brier: float
    advantage: float
    advantage_per_observation: tuple[float, ...]
    cumulative: tuple[float, ...]
    win_rate: float
    skill_score: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "observations": self.observations,
            "strategy_brier": self.strategy_brier,
            "market_brier": self.market_brier,
            "advantage": self.advantage,
            "win_rate": self.win_rate,
            "skill_score": self.skill_score,
        }


def brier_advantage(
    strategy: Sequence[float],
    market: Sequence[float],
    outcomes: Sequence[float],
) -> BrierAdvantage:
    """Score a forecast against the market price it traded against.

    All three series are aligned positionally: observation `i` is one market,
    with the strategy's probability, the market's probability, and the realized
    0/1 outcome. Observations missing any of the three are dropped, because a
    forecast with no outcome is not yet scoreable.

    `skill_score` is the advantage as a fraction of the market's own Brier, the
    conventional normalisation: 0.0 means no better than the market, 1.0 means
    perfect against a market that was not.
    """
    p_user, p_market, y = _validate(strategy, market, outcomes)
    user_scores = [(p - outcome) ** 2 for p, outcome in zip(p_user, y)]
    market_scores = [(p - outcome) ** 2 for p, outcome in zip(p_market, y)]
    per_observation = [
        market_score - user_score
        for market_score, user_score in zip(market_scores, user_scores)
    ]
    running = []
    total = 0.0
    for value in per_observation:
        total += value
        running.append(total)
    strategy_brier = sum(user_scores) / len(user_scores)
    market_brier = sum(market_scores) / len(market_scores)
    return BrierAdvantage(
        observations=len(user_scores),
        strategy_brier=strategy_brier,
        market_brier=market_brier,
        advantage=market_brier - strategy_brier,
        advantage_per_observation=tuple(per_observation),
        cumulative=tuple(running),
        win_rate=sum(1 for value in per_observation if value > 0) / len(per_observation),
        skill_score=(
            (market_brier - strategy_brier) / market_brier if market_brier > 0 else 0.0
        ),
    )


def brier_decomposition(
    forecasts: Sequence[float],
    outcomes: Sequence[float],
    *,
    edges: Optional[Sequence[float]] = None,
) -> dict[str, float]:
    """Murphy's three-term decomposition of the Brier score.

    `brier = reliability - resolution + uncertainty`. Reliability is how far the
    buckets sit off the diagonal, resolution is how much the forecast separates
    outcomes at all, uncertainty is the irreducible variance of the base rate.
    Two forecasts with the same score can differ entirely in which term earned
    it, which is why the score alone is not a verdict.
    """
    if edges is None:
        edges = (0.0, 0.1, 0.2, 0.35, 0.5, 0.65, 0.8, 0.9, 1.0)
    bounds = sorted(float(edge) for edge in edges)
    if len(bounds) < 2:
        raise ValueError("edges must define at least one bucket")
    kept_forecasts, _, kept_outcomes = _validate(forecasts, forecasts, outcomes)
    total = len(kept_forecasts)
    base = sum(kept_outcomes) / total

    def bucket_of(value: float) -> int:
        for index in range(len(bounds) - 1):
            low, high = bounds[index], bounds[index + 1]
            if (value > low or (index == 0 and value >= low)) and value <= high:
                return index
        return len(bounds) - 2

    groups: dict[int, list[tuple[float, float]]] = {}
    for forecast, outcome in zip(kept_forecasts, kept_outcomes):
        groups.setdefault(bucket_of(forecast), []).append((forecast, outcome))

    reliability = 0.0
    resolution = 0.0
    for members in groups.values():
        count = len(members)
        mean_forecast = sum(item[0] for item in members) / count
        mean_outcome = sum(item[1] for item in members) / count
        reliability += count * (mean_forecast - mean_outcome) ** 2
        resolution += count * (mean_outcome - base) ** 2
    reliability /= total
    resolution /= total
    uncertainty = base * (1.0 - base)
    brier = sum(
        (forecast - outcome) ** 2
        for forecast, outcome in zip(kept_forecasts, kept_outcomes)
    ) / total
    return {
        "brier": brier,
        "reliability": reliability,
        "resolution": resolution,
        "uncertainty": uncertainty,
        "identity": reliability - resolution + uncertainty,
        "observations": float(total),
        "base_rate": base,
    }


def reliability_curve(
    forecasts: Sequence[float],
    outcomes: Sequence[float],
    *,
    edges: Optional[Sequence[float]] = None,
) -> list[dict[str, float]]:
    """Bucketed forecast against realized frequency, one row per bucket.

    The signed gap is the tradeable part: a bucket whose realized frequency
    sits below its mean quote was overpriced.
    """
    if edges is None:
        edges = (0.0, 0.1, 0.2, 0.35, 0.5, 0.65, 0.8, 0.9, 1.0)
    bounds = sorted(float(edge) for edge in edges)
    kept_forecasts, _, kept_outcomes = _validate(forecasts, forecasts, outcomes)
    rows = []
    for index in range(len(bounds) - 1):
        low, high = bounds[index], bounds[index + 1]
        members = [
            (forecast, outcome)
            for forecast, outcome in zip(kept_forecasts, kept_outcomes)
            if (forecast > low or (index == 0 and forecast >= low)) and forecast <= high
        ]
        if not members:
            continue
        count = len(members)
        mean_forecast = sum(item[0] for item in members) / count
        realized = sum(item[1] for item in members) / count
        rows.append(
            {
                "low": low,
                "high": high,
                "observations": float(count),
                "mean_forecast": mean_forecast,
                "realized": realized,
                "gap": realized - mean_forecast,
            }
        )
    return rows
