"""Quant research workflows on top of h5i-db (ROADMAP_QUANT.md).

Factor evaluation and portfolio statistics, computed in the engine and
attributed to the data version they ran against.

    from h5i_db import quant

    panel = quant.build_panel(db, "signals", "prices", periods=(1, 5, 10))
    panel.ic().to_pandas()
    panel.quantile_returns().to_pandas()

Every entry point takes a read point (``version=`` / ``as_of=`` /
``snapshot=``) and an optional ``event_time_cutoff``; results carry a
provenance header recording both, so a number can be regenerated and
checked rather than trusted.
"""

from __future__ import annotations

from . import costs, overfitting, perf, report, validation
from ._common import Pin, Provenance
from .factor import FactorPanel, MaxLossExceededError, build_panel
from .perf import (
    DAILY,
    MONTHLY,
    WEEKLY,
    YEARLY,
    ReturnSeries,
    from_levels,
    returns,
)
from .costs import CostFit, SlippageSample, fit_impact
from .overfitting import (
    DeflatedSharpe,
    PBOResult,
    deflated_sharpe,
    minimum_track_record_length,
    probability_of_backtest_overfitting,
)
from .report import factor_report, report_payload, tearsheet
from .validation import Split, combinatorial_purged, purged_kfold, walk_forward
from .sweep import (
    SweepResult,
    VerificationError,
    restatement_impact,
    sweep,
    verify,
)

__all__ = [
    "Pin",
    "Provenance",
    "FactorPanel",
    "MaxLossExceededError",
    "build_panel",
    "ReturnSeries",
    "returns",
    "from_levels",
    "perf",
    "DAILY",
    "WEEKLY",
    "MONTHLY",
    "YEARLY",
    "sweep",
    "SweepResult",
    "verify",
    "VerificationError",
    "restatement_impact",
    "report",
    "factor_report",
    "tearsheet",
    "report_payload",
    "overfitting",
    "deflated_sharpe",
    "minimum_track_record_length",
    "probability_of_backtest_overfitting",
    "DeflatedSharpe",
    "PBOResult",
    "validation",
    "purged_kfold",
    "combinatorial_purged",
    "walk_forward",
    "Split",
    "costs",
    "fit_impact",
    "CostFit",
    "SlippageSample",
]
