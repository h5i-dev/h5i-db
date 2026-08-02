"""Backtest-native parameter studies on isolated, versioned forks.

A study is a search, and searches inflate their own best result. So the shape
of the search is explicit here rather than implied: which windows a candidate is
scored on, how candidates are sampled, and how many survive to the holdout.

Three pieces compose:

- :class:`ValidationWindows` is one train/holdout pair; :class:`WalkForward`
  is a sequence of them, scored by median so one lucky window cannot carry a
  candidate.
- :class:`TopK` makes the holdout a second stage. Candidates are ranked on
  train, only the best `k` are run out of sample, and the final ranking breaks
  ties on the train score. That is the honest order of operations: the holdout
  is spent once, on a shortlist, not mined by every candidate.
- :class:`GridSearch`, :class:`RandomSearch` and :class:`TPESearch` decide
  which points get tried at all. Grid is exhaustive and the default; the others
  need :class:`Range` for anything continuous.

Nothing guesses a split. A fold is where the caller says it is, an embargo is
whatever gap the caller leaves between train and holdout, and both are recorded
in the result.
"""

from __future__ import annotations

import html
import itertools
import json
import math
import random
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import nullcontext, suppress
from dataclasses import dataclass, field, fields, replace
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, Union

from .backtest_attention import (
    AttentionState,
    TrialAttention,
    attention_for_trial,
    rollup_attention,
)
from .backtest_config import BacktestConfig
from .backtest_result import BacktestResult

__all__ = [
    "AttentionState",
    "BacktestStudy",
    "GridSearch",
    "Range",
    "RandomSearch",
    "StudyResult",
    "TPESearch",
    "TopK",
    "TrialAttention",
    "ValidationWindows",
    "WalkForward",
    "study",
]

_CONFIG_SECTIONS = {"data", "execution", "portfolio", "risk", "output"}
_NON_TUNABLE = {
    "data.signals",
    "data.commands",
    "data.strategy_id",
    "data.snapshot",
    "data.version",
    "data.as_of",
    "data.window",
}

#: Metrics lifted from every phase of every trial onto the leaderboard row.
_METRICS = (
    "records_processed",
    "orders",
    "fills",
    "final_cash",
    "realized_pnl",
    "commissions",
    "coverage",
)


@dataclass(frozen=True)
class ValidationWindows:
    """Explicit train/holdout windows; no split or embargo is guessed."""

    train: tuple[Any, Any]
    holdout: tuple[Any, Any]

    def __post_init__(self) -> None:
        from .backtest import _to_nanos

        if len(self.train) != 2 or len(self.holdout) != 2:
            raise ValueError("train and holdout must each contain (start, end)")
        train_start, train_end = map(_to_nanos, self.train)
        holdout_start, holdout_end = map(_to_nanos, self.holdout)
        if train_start >= train_end or holdout_start >= holdout_end:
            raise ValueError("validation window starts must precede their ends")
        if train_end > holdout_start:
            raise ValueError("train must end at or before holdout starts")

    @property
    def embargo_ns(self) -> int:
        """The gap left between train and holdout, in nanoseconds.

        Not enforced, reported. A zero embargo is legitimate when labels are
        instantaneous and wrong when they are not, and only the caller knows
        which; recording it puts the choice in the result.
        """
        from .backtest import _to_nanos

        return _to_nanos(self.holdout[0]) - _to_nanos(self.train[1])


@dataclass(frozen=True)
class WalkForward:
    """Several train/holdout folds, scored by median across them.

    A candidate that wins one window and loses three is not a finding. The
    median over folds is the statistic this ranks on, and it is the reason to
    prefer several short folds over one long one.
    """

    folds: tuple[ValidationWindows, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "folds", tuple(self.folds))
        if not self.folds:
            raise ValueError("a walk-forward needs at least one fold")
        from .backtest import _to_nanos

        starts = [_to_nanos(fold.train[0]) for fold in self.folds]
        if starts != sorted(starts):
            raise ValueError(
                "walk-forward folds must be given in time order, so fold "
                "indices in the result mean what they look like"
            )

    @classmethod
    def of(cls, *folds: ValidationWindows) -> "WalkForward":
        return cls(folds=folds)

    def __len__(self) -> int:
        return len(self.folds)


def _as_walk_forward(
    validation: Optional[Union[ValidationWindows, WalkForward]],
) -> Optional[WalkForward]:
    if validation is None:
        return None
    if isinstance(validation, WalkForward):
        return validation
    return WalkForward(folds=(validation,))


@dataclass(frozen=True)
class TopK:
    """Spend the holdout on a shortlist, ranked on train.

    `k` candidates survive the train stage. Nothing else is ever run out of
    sample, which is what keeps the holdout a holdout: a set every candidate
    touched is a second training set with a different name.
    """

    k: int
    metric: str = "realized_pnl"
    maximize: bool = True

    def __post_init__(self) -> None:
        # `bool` is an `int`, and a `float` k silently truncated a shortlist
        # to a different size than it read as.
        if not isinstance(self.k, int) or isinstance(self.k, bool) or self.k < 1:
            raise ValueError("k must be a positive integer")
        if self.metric not in _METRICS:
            raise ValueError(f"metric must be one of {_METRICS}, got {self.metric!r}")


@dataclass(frozen=True)
class Range:
    """A continuous or stepped interval, for samplers that need bounds.

    Grid search cannot use an unstepped range: there is nothing to enumerate.
    That is an error rather than an arbitrary discretisation.
    """

    low: float
    high: float
    step: Optional[float] = None
    log: bool = False
    integer: bool = False

    def __post_init__(self) -> None:
        if self.high <= self.low:
            raise ValueError(f"range high must exceed low, got ({self.low}, {self.high})")
        if self.step is not None and self.step <= 0:
            raise ValueError("range step must be positive")
        if self.log and self.low <= 0:
            raise ValueError("a log range must be strictly positive")

    def enumerate(self) -> list[Any]:
        """Every value on the grid, or an error when the range is continuous."""
        if self.step is None:
            raise ValueError(
                "a grid needs a step: an unstepped Range is continuous, so use "
                "RandomSearch or TPESearch, or give step="
            )
        values: list[Any] = []
        count = int(math.floor((self.high - self.low) / self.step + 1e-9)) + 1
        for index in range(count):
            value = self.low + index * self.step
            values.append(int(round(value)) if self.integer else value)
        return values

    def sample(self, rng: random.Random) -> Any:
        if self.step is not None:
            choices = self.enumerate()
            return choices[rng.randrange(len(choices))]
        if self.log:
            drawn = math.exp(
                rng.uniform(math.log(self.low), math.log(self.high))
            )
        else:
            drawn = rng.uniform(self.low, self.high)
        return int(round(drawn)) if self.integer else drawn


@dataclass(frozen=True)
class GridSearch:
    """Every combination, in a stable order. The default."""

    def plan(self, space: Mapping[str, Any]) -> list[dict[str, Any]]:
        names = sorted(space)
        axes = []
        for name in names:
            spec = space[name]
            values = spec.enumerate() if isinstance(spec, Range) else list(spec)
            if not values:
                raise ValueError(f"parameter {name!r} has no candidate values")
            axes.append(values)
        return [dict(zip(names, combo)) for combo in itertools.product(*axes)]


@dataclass(frozen=True)
class RandomSearch:
    """`trials` independent draws from the space, seeded.

    Beats a grid when the space is wide and most axes do not matter, which is
    the usual case. Duplicate draws are kept rather than resampled: dropping
    them would quietly change the trial count the deflated-Sharpe correction
    needs.
    """

    trials: int
    seed: int = 0

    def __post_init__(self) -> None:
        if isinstance(self.trials, bool) or self.trials < 1:
            raise ValueError("trials must be a positive integer")

    def plan(self, space: Mapping[str, Any]) -> list[dict[str, Any]]:
        rng = random.Random(self.seed)
        names = sorted(space)
        out = []
        for _ in range(self.trials):
            point = {}
            for name in names:
                spec = space[name]
                if isinstance(spec, Range):
                    point[name] = spec.sample(rng)
                else:
                    values = list(spec)
                    if not values:
                        raise ValueError(f"parameter {name!r} has no candidate values")
                    point[name] = values[rng.randrange(len(values))]
            out.append(point)
        return out


@dataclass(frozen=True)
class TPESearch:
    """Tree-structured Parzen estimator, via optuna.

    Sequential by construction: each point is proposed from the results so far,
    so this ignores `max_workers` and runs one trial at a time. Optuna is an
    optional dependency; asking for TPE without it is an error naming the
    install rather than a silent fallback to random search.
    """

    trials: int
    seed: int = 0
    metric: str = "realized_pnl"
    maximize: bool = True

    def __post_init__(self) -> None:
        if isinstance(self.trials, bool) or self.trials < 1:
            raise ValueError("trials must be a positive integer")
        if self.metric not in _METRICS:
            raise ValueError(f"metric must be one of {_METRICS}, got {self.metric!r}")

    def sampler(self):  # pragma: no cover - exercised only when optuna present
        try:
            import optuna
        except ImportError as error:  # pragma: no cover
            raise ImportError(
                "TPESearch needs optuna: pip install 'h5i-db[optuna]' or "
                "pip install optuna. Use RandomSearch for a seeded search with "
                "no extra dependency."
            ) from error
        optuna.logging.set_verbosity(optuna.logging.WARNING)
        return optuna


def _parameterized(
    config: BacktestConfig, parameters: Mapping[str, Any]
) -> BacktestConfig:
    sections: dict[str, Any] = {}
    for path, value in parameters.items():
        if path in _NON_TUNABLE:
            raise ValueError(
                f"{path!r} is a data identity field and cannot vary inside a study"
            )
        parts = path.split(".")
        if len(parts) != 2 or parts[0] not in _CONFIG_SECTIONS:
            raise ValueError(
                f"parameter {path!r} must name section.field in "
                f"{sorted(_CONFIG_SECTIONS)}"
            )
        section_name, field_name = parts
        section = sections.get(section_name, getattr(config, section_name))
        valid = {item.name for item in fields(section)}
        if field_name not in valid:
            raise ValueError(
                f"unknown parameter {path!r}; {section_name} fields are {sorted(valid)}"
            )
        sections[section_name] = replace(section, **{field_name: value})
    return replace(config, **sections)


def _grid(parameters: Mapping[str, Sequence[Any]]) -> list[dict[str, Any]]:
    if not parameters:
        raise ValueError("a study needs at least one parameter")
    return GridSearch().plan(parameters)


def _median(values: Sequence[Any]) -> Optional[float]:
    numeric = [float(value) for value in values if isinstance(value, (int, float))]
    if not numeric:
        return None
    return statistics.median(numeric)


@dataclass
class StudyResult:
    study_id: str
    trials: list[dict[str, Any]]
    results: list[BacktestResult] = field(default_factory=list, repr=False)
    failures: list[dict[str, Any]] = field(default_factory=list)
    folds: int = 1
    selection: Optional[TopK] = None

    def leaderboard(
        self,
        metric: str = "realized_pnl",
        *,
        maximize: bool = True,
        tie_breaker: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Successful trials ranked on `metric`, best first.

        `tie_breaker` names a second column, used when the first ties. With a
        two-stage study that is how the train score earns its keep: it orders
        candidates the holdout could not separate.
        """
        eligible = [
            row
            for row in self.trials
            if row.get("status") == "ok" and isinstance(row.get(metric), (int, float))
        ]
        if not eligible:
            raise ValueError(f"no successful trial produced metric {metric!r}")
        sign = -1.0 if maximize else 1.0

        def key(row: dict[str, Any]) -> tuple[float, float]:
            primary = sign * float(row[metric])
            secondary = 0.0
            if tie_breaker is not None and isinstance(
                row.get(tie_breaker), (int, float)
            ):
                secondary = sign * float(row[tie_breaker])
            return (primary, secondary)

        return sorted(eligible, key=key)

    def best(
        self,
        metric: str = "realized_pnl",
        *,
        maximize: bool = True,
        tie_breaker: Optional[str] = None,
    ) -> dict[str, Any]:
        return self.leaderboard(metric, maximize=maximize, tie_breaker=tie_breaker)[0]

    def ranked(self) -> list[dict[str, Any]]:
        """The ranking the study's own selection policy implies.

        With a `TopK` selection over a validated study: median holdout score,
        train score as the tie-break, shortlist only. Without one it falls back
        to the selection metric over whatever phase ran.
        """
        metric = self.selection.metric if self.selection else "realized_pnl"
        maximize = self.selection.maximize if self.selection else True
        for primary, secondary in (
            (f"holdout_median_{metric}", f"train_median_{metric}"),
            (f"holdout_{metric}", f"train_{metric}"),
            (f"run_median_{metric}", None),
            (metric, None),
        ):
            try:
                return self.leaderboard(
                    primary, maximize=maximize, tie_breaker=secondary
                )
            except ValueError:
                continue
        raise ValueError(f"no trial produced a rankable {metric!r} column")

    @property
    def selected(self) -> list[dict[str, Any]]:
        """Trials that reached the holdout stage."""
        return [row for row in self.trials if row.get("selected")]

    def drop(self) -> int:
        dropped = 0
        for result in self.results:
            if result.get("cached", False):
                continue
            try:
                dropped += int(result.drop())
            except Exception:
                if result.fork_name in result._db.fork_names():
                    raise
        return dropped

    def attention(self) -> list[TrialAttention]:
        """Trials in the order a reviewer should visit them."""
        items = [attention_for_trial(row) for row in self.trials]
        return sorted(items, key=lambda item: (-item.priority, item.trial))

    @property
    def attention_state(self) -> AttentionState:
        return rollup_attention(self.attention())

    @property
    def warning_badge(self) -> int:
        return sum(bool(item.warnings) and not item.seen for item in self.attention())

    def open_trial(self, trial: int) -> dict[str, Any]:
        """Open a detail row and mark it seen; list views never mark it."""
        for row in self.trials:
            if int(row.get("trial", -1)) == trial:
                row["seen"] = True
                return row
        raise KeyError(f"trial {trial} is not present in study {self.study_id!r}")

    def to_html(
        self,
        path: Optional[Union[str, Path]] = None,
        *,
        metric: str = "realized_pnl",
        maximize: bool = True,
    ) -> str:
        rows = self.leaderboard(metric, maximize=maximize)
        columns = sorted({key for row in rows for key in row})
        header = "".join(f"<th>{html.escape(column)}</th>" for column in columns)
        body = "".join(
            "<tr>"
            + "".join(
                f"<td>{html.escape(str(row.get(column, '')))}</td>"
                for column in columns
            )
            + "</tr>"
            for row in rows
        )
        document = (
            "<!doctype html><meta charset='utf-8'>"
            f"<title>{html.escape(self.study_id)}</title>"
            "<style>body{font:14px system-ui;margin:32px}"
            "table{border-collapse:collapse}"
            "th,td{border:1px solid #ccc;padding:6px 9px;text-align:right}"
            "th{background:#f3f3f3}</style>"
            f"<h1>{html.escape(self.study_id)}</h1><table><thead><tr>{header}</tr>"
            f"</thead><tbody>{body}</tbody></table>"
        )
        if path is not None:
            Path(path).write_text(document, encoding="utf-8")
        return document


@dataclass(frozen=True)
class BacktestStudy:
    """A reproducible search over a typed base configuration."""

    study_id: str
    base: BacktestConfig
    parameters: Mapping[str, Any]
    validation: Optional[Union[ValidationWindows, WalkForward]] = None
    search: Union[GridSearch, RandomSearch, TPESearch] = field(
        default_factory=GridSearch
    )
    selection: Optional[TopK] = None
    max_workers: int = 1
    keep_going: bool = False
    _cached_flags: dict[int, list[bool]] = field(
        default_factory=dict, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if not self.study_id:
            raise ValueError("study_id must be non-empty")
        if self.base.data.strategy_kind == "callback":
            raise ValueError(
                "BacktestStudy requires a signals or commands strategy; "
                "callback objects are not serializable trial inputs"
            )
        if isinstance(self.max_workers, bool) or self.max_workers < 1:
            raise ValueError("max_workers must be a positive integer")
        if not self.parameters:
            raise ValueError("a study needs at least one parameter")
        object.__setattr__(
            self,
            "parameters",
            {
                name: (value if isinstance(value, Range) else tuple(value))
                for name, value in self.parameters.items()
            },
        )
        if self.selection is not None and self.validation is None:
            raise ValueError(
                "a TopK selection needs validation windows: without a holdout "
                "there is no second stage to select into"
            )
        object.__setattr__(self, "_cached_flags", {})
        # Validate every path and value the search can produce, before any fork
        # exists. A grid does this exhaustively; a sampler does it on its plan.
        for candidate in self._plan():
            _parameterized(self.base, candidate)

    def _plan(self) -> list[dict[str, Any]]:
        if isinstance(self.search, TPESearch):
            # TPE proposes points from results, so there is no static plan.
            # Validate the space's endpoints instead.
            probe: dict[str, Any] = {}
            for name, spec in self.parameters.items():
                probe[name] = spec.low if isinstance(spec, Range) else list(spec)[0]
            return [probe]
        return self.search.plan(self.parameters)

    def run(self, db: Any) -> StudyResult:
        walk = _as_walk_forward(self.validation)
        folds = len(walk) if walk else 1
        if isinstance(self.search, TPESearch):
            return self._run_tpe(db, walk, folds)
        candidates = self.search.plan(self.parameters)
        return self._run_static(db, candidates, walk, folds)

    # -- phase execution ---------------------------------------------------

    def _phases(
        self, walk: Optional[WalkForward], stage: str
    ) -> list[tuple[int, str, Any]]:
        """(fold index, phase name, window) tuples for one stage."""
        if walk is None:
            return [(0, "run", self.base.data.window)]
        if stage == "train":
            return [(index, "train", fold.train) for index, fold in enumerate(walk.folds)]
        return [(index, "holdout", fold.holdout) for index, fold in enumerate(walk.folds)]

    def _column(self, folds: int, fold: int, phase: str, metric: str) -> str:
        """Where a metric lands on the row.

        Single-fold studies keep the flat `train_*`/`holdout_*` names, because
        those are the columns callers already rank on. Multi-fold studies add a
        per-fold prefix and the medians that make folds comparable.
        """
        if phase == "run":
            return metric
        if folds == 1:
            return f"{phase}_{metric}"
        return f"fold{fold}_{phase}_{metric}"

    def _execute_stage(
        self,
        db: Any,
        row: dict[str, Any],
        parameters: Mapping[str, Any],
        walk: Optional[WalkForward],
        folds: int,
        stage: str,
    ) -> list[BacktestResult]:
        base = _parameterized(self.base, parameters)
        outputs: list[BacktestResult] = []
        collected: dict[str, list[Any]] = {metric: [] for metric in _METRICS}
        cached_flags: list[bool] = self._cached_flags.setdefault(int(row["trial"]), [])
        try:
            self._run_phases(
                db,
                row,
                parameters,
                walk,
                folds,
                stage,
                base,
                outputs,
                collected,
                cached_flags,
            )
        except Exception:
            # A stage that fails partway leaves the forks its earlier phases
            # created behind. Each is a complete run carrying its own trial
            # digest, so `trial_count` counts trials this study never scored
            # and every multiple-testing correction computed from it is
            # deflated by runs that produced nothing. Only forks this stage
            # created are dropped: a cached result names another trial's fork,
            # and dropping that would delete a run still in use.
            #
            # "Created" is not "owned", though. `execute` releases the digest
            # lock when the run returns, so a concurrent trial hashing to the
            # same `trial_digest` can be handed this very fork by `find_trial`
            # and hold it as a cache hit. The drop therefore happens under the
            # same lock and only after asking: with the lock held no lookup
            # can be in flight, so the fork is either already shared (skip it,
            # the other holder's result names it) or still unreachable by the
            # lookup that would share it.
            from .backtest import _trial_ledger_lock, fork_is_shared

            for created in outputs:
                if created.get("cached"):
                    continue
                digest = created.get("trial_digest")
                guard = (
                    _trial_ledger_lock(db, digest)
                    if isinstance(digest, str) and digest
                    else nullcontext()
                )
                with suppress(Exception), guard:
                    if fork_is_shared(created.fork_name):
                        continue
                    db.drop_fork(created.fork_name)
            raise
        if walk is not None and folds > 1:
            phase = "train" if stage == "train" else "holdout"
            for metric in _METRICS:
                row[f"{phase}_median_{metric}"] = _median(collected[metric])
        return outputs

    def _run_phases(
        self,
        db: Any,
        row: dict[str, Any],
        parameters: Mapping[str, Any],
        walk: Optional[WalkForward],
        folds: int,
        stage: str,
        base: BacktestConfig,
        outputs: list[BacktestResult],
        collected: dict[str, list[Any]],
        cached_flags: list[bool],
    ) -> None:
        """The phase loop itself, so its caller can clean up after it."""
        from .backtest import execute

        for fold, phase, window in self._phases(walk, stage):
            suffix = phase if folds == 1 else f"{phase}{fold}"
            run_id = f"{self.study_id}-{int(row['trial']):04d}-{suffix}"
            configured = replace(
                base,
                run_id=run_id,
                data=replace(base.data, window=window),
                metadata={
                    **base.metadata,
                    "study_id": self.study_id,
                    "trial": int(row["trial"]),
                    "phase": phase,
                    "fold": fold,
                    "parameters": dict(parameters),
                },
            )
            result = execute(db, configured)
            outputs.append(result)
            for metric in _METRICS:
                value = result.get(metric)
                row[self._column(folds, fold, phase, metric)] = value
                collected[metric].append(value)
            row[self._column(folds, fold, phase, "fork")] = result.fork_name
            row[self._column(folds, fold, phase, "digest")] = result.get("digest")
            row[self._column(folds, fold, phase, "trial_digest")] = (
                configured.trial_digest
            )
            was_cached = bool(result.get("cached", False))
            row[self._column(folds, fold, phase, "cached")] = was_cached
            # Kept out of band: the "run" phase writes the bare name "cached",
            # so the aggregate cannot be derived from the row's own columns.
            cached_flags.append(was_cached)
            row.setdefault("warnings", []).extend(
                str(item) for item in result.get("warnings", [])
            )

    def _new_row(self, index: int, parameters: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "trial": index,
            "status": "ok",
            "parameters": json.dumps(parameters, sort_keys=True, default=str),
            "seen": False,
            "needs_decision": bool(self.base.metadata.get("needs_decision", False)),
            "warnings": [],
            "selected": self.selection is None,
        }

    def _finalise_cached(self, rows: Sequence[Optional[dict[str, Any]]]) -> None:
        """A trial counts as cached only when every phase it ran was cached."""
        for row in rows:
            if row is None or row.get("status") != "ok":
                continue
            flags = self._cached_flags.get(int(row["trial"]), [])
            row["cached"] = bool(flags) and all(flags)

    def _selection_column(self, folds: int) -> str:
        metric = self.selection.metric if self.selection else "realized_pnl"
        return f"train_median_{metric}" if folds > 1 else f"train_{metric}"

    # -- drivers -----------------------------------------------------------

    def _run_static(
        self,
        db: Any,
        candidates: list[dict[str, Any]],
        walk: Optional[WalkForward],
        folds: int,
    ) -> StudyResult:
        rows: list[Optional[dict[str, Any]]] = [None] * len(candidates)
        results: list[BacktestResult] = []
        failures: list[dict[str, Any]] = []

        first_stage = "train" if walk is not None else "run"

        def run_candidate(index: int, parameters: dict[str, Any]) -> tuple:
            row = self._new_row(index, parameters)
            outputs = self._execute_stage(
                db, row, parameters, walk, folds, first_stage
            )
            return row, outputs

        def record_failure(index: int, parameters: dict[str, Any], exc: Exception) -> None:
            failures.append(
                {"trial": index, "parameters": dict(parameters), "error": repr(exc)}
            )
            rows[index] = {
                "trial": index,
                "status": "failed",
                "parameters": json.dumps(parameters, sort_keys=True, default=str),
                "error": repr(exc),
                "selected": False,
            }

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {
                pool.submit(run_candidate, index, parameters): (index, parameters)
                for index, parameters in enumerate(candidates)
            }
            for future in as_completed(futures):
                index, parameters = futures[future]
                try:
                    row, outputs = future.result()
                    rows[index] = row
                    results.extend(outputs)
                except Exception as exc:
                    record_failure(index, parameters, exc)
                    if not self.keep_going:
                        for pending in futures:
                            pending.cancel()
                        raise

        # Second stage: the holdout, spent on a shortlist.
        if walk is not None and self.selection is not None:
            column = self._selection_column(folds)
            scored = [
                row
                for row in rows
                if row is not None
                and row.get("status") == "ok"
                and isinstance(row.get(column), (int, float))
            ]
            ordered = sorted(
                scored,
                key=lambda row: float(row[column]),
                reverse=self.selection.maximize,
            )
            shortlist = ordered[: self.selection.k]
            for row in shortlist:
                row["selected"] = True
            with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                futures = {
                    pool.submit(
                        self._execute_stage,
                        db,
                        row,
                        json.loads(row["parameters"]),
                        walk,
                        folds,
                        "holdout",
                    ): row
                    for row in shortlist
                }
                for future in as_completed(futures):
                    row = futures[future]
                    try:
                        results.extend(future.result())
                    except Exception as exc:
                        row["status"] = "failed"
                        row["error"] = repr(exc)
                        failures.append(
                            {"trial": row["trial"], "error": repr(exc), "phase": "holdout"}
                        )
                        if not self.keep_going:
                            raise
        elif walk is not None:
            # No selection policy: every candidate is run out of sample, which
            # is a legitimate choice for a small grid and is recorded as such.
            for row in [item for item in rows if item is not None and item["status"] == "ok"]:
                try:
                    results.extend(
                        self._execute_stage(
                            db,
                            row,
                            json.loads(row["parameters"]),
                            walk,
                            folds,
                            "holdout",
                        )
                    )
                except Exception as exc:
                    row["status"] = "failed"
                    row["error"] = repr(exc)
                    failures.append(
                        {"trial": row["trial"], "error": repr(exc), "phase": "holdout"}
                    )
                    if not self.keep_going:
                        raise

        self._finalise_cached(rows)
        return StudyResult(
            study_id=self.study_id,
            trials=[row for row in rows if row is not None],
            results=results,
            failures=failures,
            folds=folds,
            selection=self.selection,
        )

    def _run_tpe(
        self, db: Any, walk: Optional[WalkForward], folds: int
    ) -> StudyResult:  # pragma: no cover - requires optuna
        search = self.search
        assert isinstance(search, TPESearch)
        optuna = search.sampler()

        rows: list[dict[str, Any]] = []
        results: list[BacktestResult] = []
        failures: list[dict[str, Any]] = []
        column = (
            f"train_median_{search.metric}"
            if folds > 1 and walk is not None
            else (f"train_{search.metric}" if walk is not None else search.metric)
        )
        opt = optuna.create_study(
            direction="maximize" if search.maximize else "minimize",
            sampler=optuna.samplers.TPESampler(seed=search.seed),
        )
        for index in range(search.trials):
            trial = opt.ask()
            point: dict[str, Any] = {}
            for name in sorted(self.parameters):
                spec = self.parameters[name]
                if isinstance(spec, Range):
                    if spec.integer:
                        point[name] = trial.suggest_int(
                            name, int(spec.low), int(spec.high), log=spec.log
                        )
                    else:
                        point[name] = trial.suggest_float(
                            name, spec.low, spec.high, step=spec.step, log=spec.log
                        )
                else:
                    point[name] = trial.suggest_categorical(name, list(spec))
            row = self._new_row(index, point)
            try:
                outputs = self._execute_stage(
                    db, row, point, walk, folds, "train" if walk else "run"
                )
                results.extend(outputs)
                score = row.get(column)
                opt.tell(
                    trial,
                    float(score) if isinstance(score, (int, float)) else None,
                    state=None
                    if isinstance(score, (int, float))
                    else optuna.trial.TrialState.FAIL,
                )
            except Exception as exc:
                row["status"] = "failed"
                row["error"] = repr(exc)
                failures.append({"trial": index, "parameters": point, "error": repr(exc)})
                opt.tell(trial, state=optuna.trial.TrialState.FAIL)
                if not self.keep_going:
                    rows.append(row)
                    raise
            rows.append(row)

        if walk is not None:
            shortlist = rows
            if self.selection is not None:
                scored = [
                    row
                    for row in rows
                    if row["status"] == "ok" and isinstance(row.get(column), (int, float))
                ]
                shortlist = sorted(
                    scored,
                    key=lambda row: float(row[column]),
                    reverse=self.selection.maximize,
                )[: self.selection.k]
                for row in shortlist:
                    row["selected"] = True
            for row in shortlist:
                if row["status"] != "ok":
                    continue
                results.extend(
                    self._execute_stage(
                        db, row, json.loads(row["parameters"]), walk, folds, "holdout"
                    )
                )
        self._finalise_cached(rows)
        return StudyResult(
            study_id=self.study_id,
            trials=rows,
            results=results,
            failures=failures,
            folds=folds,
            selection=self.selection,
        )


def study(
    db: Any,
    *,
    study_id: str,
    base: BacktestConfig,
    parameters: Mapping[str, Any],
    validation: Optional[Union[ValidationWindows, WalkForward]] = None,
    search: Optional[Union[GridSearch, RandomSearch, TPESearch]] = None,
    selection: Optional[TopK] = None,
    max_workers: int = 1,
    keep_going: bool = False,
) -> StudyResult:
    """Construct and immediately execute a :class:`BacktestStudy`."""
    return BacktestStudy(
        study_id=study_id,
        base=base,
        parameters=parameters,
        validation=validation,
        search=search or GridSearch(),
        selection=selection,
        max_workers=max_workers,
        keep_going=keep_going,
    ).run(db)
