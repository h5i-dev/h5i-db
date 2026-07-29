"""Backtest-native parameter studies on isolated, versioned forks."""

from __future__ import annotations

import html
import itertools
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, fields, replace
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, Union

from .backtest_config import BacktestConfig
from .backtest_result import BacktestResult

__all__ = ["BacktestStudy", "StudyResult", "ValidationWindows", "study"]

_CONFIG_SECTIONS = {"data", "execution", "portfolio", "risk", "output"}
_NON_TUNABLE = {
    "data.signals",
    "data.commands",
    "data.snapshot",
    "data.version",
    "data.as_of",
    "data.window",
}


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
    names = sorted(parameters)
    values = []
    for name in names:
        candidates = list(parameters[name])
        if not candidates:
            raise ValueError(f"parameter {name!r} has no candidate values")
        values.append(candidates)
    return [dict(zip(names, combination)) for combination in itertools.product(*values)]


@dataclass
class StudyResult:
    study_id: str
    trials: list[dict[str, Any]]
    results: list[BacktestResult] = field(default_factory=list, repr=False)
    failures: list[dict[str, Any]] = field(default_factory=list)

    def leaderboard(
        self,
        metric: str = "realized_pnl",
        *,
        maximize: bool = True,
    ) -> list[dict[str, Any]]:
        eligible = [
            row
            for row in self.trials
            if row.get("status") == "ok" and isinstance(row.get(metric), (int, float))
        ]
        if not eligible:
            raise ValueError(f"no successful trial produced metric {metric!r}")
        return sorted(eligible, key=lambda row: row[metric], reverse=maximize)

    def best(
        self,
        metric: str = "realized_pnl",
        *,
        maximize: bool = True,
    ) -> dict[str, Any]:
        return self.leaderboard(metric, maximize=maximize)[0]

    def drop(self) -> int:
        dropped = 0
        for result in self.results:
            try:
                dropped += int(result.drop())
            except Exception:
                if result.fork_name in result._db.fork_names():
                    raise
        return dropped

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
    """A reproducible grid over a typed base configuration."""

    study_id: str
    base: BacktestConfig
    parameters: Mapping[str, Sequence[Any]]
    validation: Optional[ValidationWindows] = None
    max_workers: int = 1
    keep_going: bool = False

    def __post_init__(self) -> None:
        if not self.study_id:
            raise ValueError("study_id must be non-empty")
        if isinstance(self.max_workers, bool) or self.max_workers < 1:
            raise ValueError("max_workers must be a positive integer")
        object.__setattr__(
            self,
            "parameters",
            {name: tuple(values) for name, values in self.parameters.items()},
        )
        # Validate paths and combinations before any forks are created.
        for candidate in _grid(self.parameters):
            _parameterized(self.base, candidate)

    def run(self, db: Any) -> StudyResult:
        from .backtest import execute

        combinations = _grid(self.parameters)

        def run_trial(index: int, parameters: dict[str, Any]) -> tuple:
            base = _parameterized(self.base, parameters)
            phases = (
                (("train", self.validation.train), ("holdout", self.validation.holdout))
                if self.validation is not None
                else (("run", base.data.window),)
            )
            row: dict[str, Any] = {
                "trial": index,
                "status": "ok",
                "parameters": json.dumps(parameters, sort_keys=True, default=str),
            }
            outputs: list[BacktestResult] = []
            for phase, window in phases:
                run_id = f"{self.study_id}-{index:04d}-{phase}"
                configured = replace(
                    base,
                    run_id=run_id,
                    data=replace(base.data, window=window),
                    metadata={
                        **base.metadata,
                        "study_id": self.study_id,
                        "trial": index,
                        "phase": phase,
                        "parameters": dict(parameters),
                    },
                )
                result = execute(db, configured)
                outputs.append(result)
                prefix = "" if phase == "run" else f"{phase}_"
                for metric in (
                    "records_processed",
                    "orders",
                    "fills",
                    "final_cash",
                    "realized_pnl",
                    "commissions",
                    "coverage",
                ):
                    row[f"{prefix}{metric}"] = result.get(metric)
                row[f"{prefix}fork"] = result.fork_name
                row[f"{prefix}digest"] = result.get("digest")
            return row, outputs

        rows: list[Optional[dict[str, Any]]] = [None] * len(combinations)
        results: list[BacktestResult] = []
        failures: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {
                pool.submit(run_trial, index, parameters): (index, parameters)
                for index, parameters in enumerate(combinations)
            }
            for future in as_completed(futures):
                index, parameters = futures[future]
                try:
                    row, outputs = future.result()
                    rows[index] = row
                    results.extend(outputs)
                except Exception as exc:
                    failure = {
                        "trial": index,
                        "parameters": dict(parameters),
                        "error": repr(exc),
                    }
                    failures.append(failure)
                    rows[index] = {
                        "trial": index,
                        "status": "failed",
                        "parameters": json.dumps(
                            parameters, sort_keys=True, default=str
                        ),
                        "error": repr(exc),
                    }
                    if not self.keep_going:
                        for pending in futures:
                            pending.cancel()
                        raise
        return StudyResult(
            study_id=self.study_id,
            trials=[row for row in rows if row is not None],
            results=results,
            failures=failures,
        )


def study(
    db: Any,
    *,
    study_id: str,
    base: BacktestConfig,
    parameters: Mapping[str, Sequence[Any]],
    validation: Optional[ValidationWindows] = None,
    max_workers: int = 1,
    keep_going: bool = False,
) -> StudyResult:
    """Construct and immediately execute a :class:`BacktestStudy`."""
    return BacktestStudy(
        study_id=study_id,
        base=base,
        parameters=parameters,
        validation=validation,
        max_workers=max_workers,
        keep_going=keep_going,
    ).run(db)
