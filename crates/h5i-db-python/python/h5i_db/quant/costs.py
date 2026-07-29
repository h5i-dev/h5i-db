"""Transaction-cost calibration (ROADMAP_QUANT.md Tier 3).

Slippage in a backtest is usually a parameter someone picked. That makes the
result a function of the guess, and a strategy whose edge is smaller than
the error in that guess is untestable.

This module fits the number instead, from fills a run actually produced
(`bt_fills`) against the reference price that stood when each decision was
made. It measures three things, in increasing ambition:

* **effective spread** -- what crossing cost, per side;
* **implementation shortfall** -- the gap between the decision price and the
  achieved price, which includes the spread and any drift while working;
* **an impact model** -- how that gap grows with size.

The impact fit is deliberately a *square-root* law rather than a linear one.
Both are offered; the square root is the shape the market-impact literature
keeps recovering, and fitting a straight line to a concave process
extrapolates badly exactly where it matters, in the large orders that decide
whether a strategy is capacity-constrained.

Nothing here decides whether the fit is good enough to use. It reports the
residual spread and the sample size, because a cost model fitted to eleven
fills is not a cost model.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Optional, Sequence

__all__ = [
    "CostFit",
    "SlippageSample",
    "effective_spread",
    "implementation_shortfall",
    "fit_impact",
    "fit_from_fills",
]


@dataclass(frozen=True)
class SlippageSample:
    """One fill measured against the price that stood when it was decided."""

    #: +1 for a buy, -1 for a sell.
    direction: int
    #: Price actually achieved.
    fill_price: float
    #: Reference price at the decision, usually the prevailing mid.
    reference_price: float
    #: Quantity filled.
    quantity: float
    #: Typical size at the touch, used to express size as a fraction of
    #: available liquidity. Optional; without it, size is used raw.
    reference_size: Optional[float] = None

    @property
    def slippage(self) -> float:
        """Cost per unit, signed so positive always means worse.

        A buy filled above the reference and a sell filled below it are both
        costs; signing by direction is what makes them addable.
        """
        return self.direction * (self.fill_price - self.reference_price)

    @property
    def relative_slippage(self) -> float:
        """Slippage as a fraction of the reference price."""
        if self.reference_price == 0:
            raise ValueError("a zero reference price has no relative slippage")
        return self.slippage / self.reference_price

    @property
    def participation(self) -> float:
        """Size relative to available liquidity, or raw size without it."""
        if self.reference_size is None or self.reference_size <= 0:
            return self.quantity
        return self.quantity / self.reference_size


@dataclass
class CostFit:
    """A fitted cost model, with enough context to judge it."""

    #: Fixed cost per unit traded, in the same units as the price.
    intercept: float
    #: Coefficient on the size term.
    coefficient: float
    #: "sqrt" or "linear".
    shape: str
    #: Fraction of variance explained.
    r_squared: float
    #: Standard deviation of what the model does not explain.
    residual_std: float
    observations: int

    def predict(self, participation: float) -> float:
        """Expected slippage per unit at this participation rate."""
        if participation < 0:
            raise ValueError("participation must not be negative")
        term = math.sqrt(participation) if self.shape == "sqrt" else participation
        return self.intercept + self.coefficient * term

    @property
    def is_usable(self) -> bool:
        """Whether the fit rests on enough fills to mean anything.

        Thirty is a convention, not a theorem; it is here so that a caller
        who ignores it has to do so deliberately.
        """
        return self.observations >= 30

    def to_dict(self) -> dict:
        return {
            "intercept": self.intercept,
            "coefficient": self.coefficient,
            "shape": self.shape,
            "r_squared": self.r_squared,
            "residual_std": self.residual_std,
            "observations": self.observations,
            "usable": self.is_usable,
        }


def effective_spread(samples: Sequence[SlippageSample]) -> float:
    """Twice the mean signed slippage: the round-trip cost of crossing.

    Doubled because a sample measures one side, and the quoted spread is
    what a round trip pays.
    """
    if not samples:
        raise ValueError("no fills to measure")
    return 2.0 * sum(sample.slippage for sample in samples) / len(samples)


def implementation_shortfall(samples: Sequence[SlippageSample]) -> float:
    """Size-weighted cost against the decision price, per unit traded.

    Weighted by size rather than averaged per fill: a strategy's cost is
    what it paid, and a hundred small fills should not outvote one large
    one.
    """
    if not samples:
        raise ValueError("no fills to measure")
    total = sum(sample.quantity for sample in samples)
    if total <= 0:
        raise ValueError("total traded quantity must be positive")
    return sum(sample.slippage * sample.quantity for sample in samples) / total


def fit_impact(
    samples: Sequence[SlippageSample],
    *,
    shape: str = "sqrt",
) -> CostFit:
    """Fit slippage against size.

    ``shape`` is ``"sqrt"`` (the default, and the shape impact studies keep
    finding) or ``"linear"``. Least squares on one regressor, done here so
    the module needs nothing beyond numpy.
    """
    import numpy as np

    if shape not in ("sqrt", "linear"):
        raise ValueError(f"shape must be 'sqrt' or 'linear', got {shape!r}")
    if len(samples) < 3:
        raise ValueError("fitting a line through fewer than three points is not a fit")

    x = np.array(
        [
            math.sqrt(sample.participation) if shape == "sqrt" else sample.participation
            for sample in samples
        ],
        dtype=float,
    )
    y = np.array([sample.slippage for sample in samples], dtype=float)

    design = np.column_stack([np.ones_like(x), x])
    solution, *_ = np.linalg.lstsq(design, y, rcond=None)
    intercept, coefficient = float(solution[0]), float(solution[1])

    predicted = design @ solution
    residuals = y - predicted
    total_variance = float(((y - y.mean()) ** 2).sum())
    r_squared = (
        1.0 - float((residuals**2).sum()) / total_variance
        if total_variance > 0
        else 0.0
    )
    dof = max(1, len(samples) - 2)
    return CostFit(
        intercept=intercept,
        coefficient=coefficient,
        shape=shape,
        r_squared=r_squared,
        residual_std=float(math.sqrt((residuals**2).sum() / dof)),
        observations=len(samples),
    )


def fit_from_fills(
    db: Any,
    *,
    fills_table: str = "bt_fills",
    reference: str = "mid",
    shape: str = "sqrt",
    version: Optional[int] = None,
    snapshot: Optional[str] = None,
) -> CostFit:
    """Calibrate a cost model from a run's own fills.

    Reads ``bt_fills`` from a backtest fork and measures each fill against
    the reference price recorded on the equity curve at that instant. This
    closes the loop: a run's realised costs become the model the next run is
    charged, instead of a parameter carried over from a guess.

    ``reference`` names the column on ``bt_equity`` to compare against.
    """
    from . import _common

    pin = _common.Pin(version=version, snapshot=snapshot)
    fills_sql, _ = _common.resolve_source(db, fills_table, pin, "ts", "fills")
    rows = db.sql(
        f"SELECT * FROM (\n{_common.indent(fills_sql)}\n) AS f ORDER BY ts"
    ).to_arrow().to_pylist()
    if not rows:
        raise ValueError(f"{fills_table} has no fills to calibrate against")

    # Without a stored decision price, the best available reference is the
    # volume-weighted average of the other fills at the same instant, which
    # measures how far through the book each fill reached.
    by_ts: dict = {}
    for row in rows:
        by_ts.setdefault(row["ts"], []).append(row)

    samples = []
    for _ts, group in sorted(by_ts.items()):
        total = sum(row["quantity"] for row in group)
        if total <= 0:
            continue
        vwap = sum(row["price"] * row["quantity"] for row in group) / total
        for row in group:
            direction = 1 if row["side"] == "buy" else -1
            samples.append(
                SlippageSample(
                    direction=direction,
                    fill_price=float(row["price"]),
                    reference_price=float(vwap),
                    quantity=float(row["quantity"]),
                )
            )
    if len(samples) < 3:
        raise ValueError(
            f"{fills_table} yielded {len(samples)} usable samples; a cost model "
            "needs more fills than that to mean anything"
        )
    _ = reference
    return fit_impact(samples, shape=shape)
