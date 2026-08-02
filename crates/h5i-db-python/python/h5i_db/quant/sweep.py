"""Fork sweeps, verification, and restatement impact (ROADMAP_QUANT.md Q3).

The three things this layer can do that a pandas research stack cannot,
because they are properties of the store rather than of the statistics:

* :func:`sweep` runs a parameter grid with one fork per combination, so
  every trial is isolated, attributable, and comparable in a single query.
* :func:`verify` re-executes a computation from its provenance header and
  reports whether the numbers still come out the same.
* :func:`restatement_impact` re-runs one computation at two data versions
  and reports how much the answer moved when the data was revised.
"""

from __future__ import annotations

import hashlib
import itertools
import json
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional, Sequence

import pyarrow as pa

from ._common import Provenance

__all__ = [
    "sweep",
    "SweepResult",
    "verify",
    "VerificationError",
    "restatement_impact",
]

RESULTS_TABLE = "quant_sweep"


class VerificationError(Exception):
    """A recomputation did not reproduce the numbers it claimed."""


def _grid(params: Mapping[str, Sequence]) -> list:
    """The cartesian product of a parameter grid, in a stable order."""
    if not params:
        raise ValueError("sweep() needs at least one parameter")
    keys = list(params)
    values = []
    for key in keys:
        options = params[key]
        if isinstance(options, (str, bytes)) or not isinstance(options, Sequence):
            raise TypeError(
                f"parameter {key!r} must be a sequence of values to try"
            )
        if len(options) == 0:
            raise ValueError(f"parameter {key!r} has no values")
        values.append(list(options))
    return [dict(zip(keys, combo)) for combo in itertools.product(*values)]


def _scalar_columns(metrics: Mapping[str, Any]) -> dict:
    out = {}
    for key, value in metrics.items():
        if isinstance(value, bool):
            out[key] = value
        elif isinstance(value, (int, float)) or value is None:
            out[key] = None if value is None else float(value)
        else:
            raise TypeError(
                f"sweep metric {key!r} must be a number or None, got "
                f"{type(value).__name__}; put anything else in the trial's "
                "own tables"
            )
    if not out:
        raise ValueError("the sweep function returned no metrics")
    return out


@dataclass
class SweepResult:
    """The outcome of a parameter sweep: one fork per trial."""

    db: Any
    forks: list
    trials: list
    results_table: str
    failures: list = field(default_factory=list)
    #: Forks created for trials that raised. They hold no results row, so they
    #: are kept out of `forks` (and out of `compare()`), but they exist and
    #: :meth:`drop` has to clean them up or the sweep leaks them.
    failed_forks: list = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.trials)

    def __repr__(self) -> str:
        return (
            f"SweepResult(trials={len(self.trials)}, "
            f"failed={len(self.failures)}, table={self.results_table!r})"
        )

    def compare(self):
        """Every trial's parameters and metrics, aggregated across forks.

        One query over all the forks at once: they share their base's
        segments, so comparing a hundred trials costs about what reading one
        does plus what each actually wrote.
        """
        return self.db.fork_scan(self.results_table, self.forks).collect()

    def to_pandas(self):
        return self.compare().to_pandas()

    def best(self, metric: str, maximize: bool = True) -> dict:
        """The trial with the best value of ``metric``."""
        rows = [t for t in self.trials if t.get(metric) is not None]
        if not rows:
            raise ValueError(f"no trial produced a value for {metric!r}")
        return (max if maximize else min)(rows, key=lambda t: t[metric])

    def drop(self) -> int:
        """Delete every fork the sweep made, failed trials included.

        The comparison table goes with them.
        """
        return self.db.drop_forks(list(self.forks) + list(self.failed_forks))


def sweep(
    db: Any,
    params: Mapping[str, Sequence],
    fn: Callable[[Any, dict], Mapping[str, Any]],
    *,
    prefix: str = "sweep",
    results_table: str = RESULTS_TABLE,
    note: Optional[str] = None,
    keep_going: bool = False,
) -> SweepResult:
    """Run a parameter grid, one fork per combination.

    ``fn(fork_db, params) -> {metric: number}`` is called once per
    combination with a :class:`~h5i_db.Database` opened on that trial's own
    fork. Anything it writes lands in the fork and nowhere else, so trials
    cannot contaminate each other or the base data.

    Each trial's parameters and metrics are written into its fork as
    ``results_table``; :meth:`SweepResult.compare` reads them all back in
    one cross-fork query.

    With ``keep_going`` a failing trial is recorded in
    :attr:`SweepResult.failures` instead of aborting the sweep.
    """
    combos = _grid(params)
    names = [f"{prefix}-{i:04}" for i in range(len(combos))]
    db.create_forks(
        names,
        note=note or f"quant sweep over {', '.join(params)}",
        meta={"quant_sweep": prefix, "trials": len(combos)},
    )

    trials = []
    failures = []
    failed_forks = []
    for index, (name, combo) in enumerate(zip(names, combos)):
        fork = db.fork(name)
        try:
            metrics = fn(fork, dict(combo))
            if metrics is None:
                raise ValueError("the sweep function returned None")
            metrics = _scalar_columns(metrics)
        except Exception as exc:  # noqa: BLE001 -- recorded, not swallowed
            if not keep_going:
                raise
            failures.append({"trial": index, "fork": name, "error": repr(exc)})
            failed_forks.append(name)
            continue
        row = {"_trial": index, "_fork": name, "_params": json.dumps(combo, default=str)}
        row.update(metrics)
        _write_row(fork, results_table, row)
        trials.append({**combo, **metrics, "_trial": index, "_fork": name})

    return SweepResult(
        db=db,
        forks=[t["_fork"] for t in trials],
        trials=trials,
        results_table=results_table,
        failures=failures,
        failed_forks=failed_forks,
    )


def _write_row(fork_db: Any, table: str, row: dict) -> None:
    fields = []
    for key, value in row.items():
        if key in ("_trial",):
            fields.append(pa.field(key, pa.int64(), nullable=False))
        elif isinstance(value, str):
            fields.append(pa.field(key, pa.string()))
        elif isinstance(value, bool):
            fields.append(pa.field(key, pa.bool_()))
        else:
            fields.append(pa.field(key, pa.float64()))
    schema = pa.schema(fields)
    if table not in fork_db.tables():
        # `_trial` is the time column: a sweep's natural order is trial order,
        # and every table in h5i-db is ordered by something.
        fork_db.create_table(table, schema, time_column="_trial")
    fork_db.append(table, pa.table({k: [v] for k, v in row.items()}, schema=schema))


def _values_digest(subject: Any) -> Optional[str]:
    """A content hash of the rows a computation produces, or None.

    The digest in the provenance header covers the pin, the parameters and the
    SQL, so it answers "is this the same computation" and not "does it still
    produce the same numbers". An engine change moves the second without
    touching the first, which is exactly the case verification exists for, so
    the rows are read back and hashed too.

    Chunking is normalised away before hashing: two runs of one query may
    split their batches differently without disagreeing about a single value.
    """
    collect = getattr(subject, "collect", None)
    if not callable(collect):
        frame = getattr(subject, "frame", None)
        collect = getattr(frame, "collect", None)
    if not callable(collect):
        return None
    result = collect()
    table = result.to_arrow() if hasattr(result, "to_arrow") else result
    table = table.combine_chunks()
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


def verify(
    subject: Any,
    *,
    rerun: Optional[Callable[[], Any]] = None,
    strict: bool = True,
) -> dict:
    """Re-execute a computation and check it still produces its numbers.

    ``subject`` is anything carrying a :class:`~h5i_db.quant.Provenance`
    (a factor panel, a returns series). The check has two halves: the
    provenance digest must be unchanged, and the recomputed values must
    match. Checking the values means collecting both runs, which is the price
    of the second half: a digest over the SQL cannot notice an engine that
    computes the same query differently. An unpinned subject is reported as
    unverifiable rather than passed, because reproducing it is not something
    the header can promise.
    """
    provenance = getattr(subject, "provenance", None)
    if not isinstance(provenance, Provenance):
        raise TypeError("verify() needs an object carrying a Provenance")

    report = {
        "kind": provenance.kind,
        "digest": provenance.digest,
        "pinned": provenance.pin.is_pinned,
        "warnings": list(provenance.warnings()),
        "verified": False,
    }
    # An unpinned computation cannot be verified even if a recomputation
    # agrees with it. The digest covers the pin, the parameters and the SQL,
    # not the bytes those read; two runs against "latest" agreeing proves
    # only that nothing changed in the seconds between them.
    if not provenance.pin.is_pinned:
        report["reason"] = "unpinned computation"
        if strict:
            raise VerificationError(
                "cannot verify an unpinned computation: it read the latest "
                "version, which may already have moved. Re-run it against a "
                "snapshot, an as-of time, or an explicit version."
            )
        return report

    if rerun is None:
        rerun = getattr(subject, "_default_verification", None)
    if rerun is None:
        report["reason"] = "no recomputation supplied"
        return report

    before = _values_digest(subject)
    again = rerun()
    other = getattr(again, "provenance", None)
    if isinstance(other, Provenance) and other.digest != provenance.digest:
        report["reason"] = "provenance digest changed"
        if strict:
            raise VerificationError(
                f"digest changed: {provenance.digest[:12]} -> {other.digest[:12]}"
            )
        return report

    after = _values_digest(again)
    report["values_compared"] = before is not None and after is not None
    if not report["values_compared"]:
        # Nothing to read back: the subject carries a header but no rows. Say
        # so rather than reporting a values check that did not happen.
        report["reason"] = "values not comparable"
        if strict:
            raise VerificationError(
                "the subject cannot be collected, so only its header could be "
                "checked; pass something that produces rows, or verify with "
                "strict=False to accept a header-only check"
            )
        return report
    report["values_digest"] = before
    if before != after:
        report["reason"] = "recomputed values differ"
        if strict:
            raise VerificationError(
                f"the computation reproduces its header but not its numbers: "
                f"values {before[:12]} -> {after[:12]}"
            )
        return report
    report["verified"] = True
    return report


def restatement_impact(
    build: Callable[[Any], Any],
    db: Any,
    before: Mapping[str, Any],
    after: Optional[Mapping[str, Any]] = None,
    *,
    metric: Callable[[Any], Any],
    tolerance: float = 1e-9,
) -> dict:
    """How much a data revision moved an answer.

    ``build(pin_kwargs)`` produces the computation at a read point and
    ``metric(built)`` reduces it to comparable numbers (a dict of scalars or
    an Arrow table). The two are run at ``before`` and ``after``, and the
    differences are reported per key.

    This is the question a versioned store can answer and a pile of parquet
    files cannot: not "what is my alpha" but "did the vendor's revision
    change it".
    """
    after = dict(after or {})
    first = metric(build(dict(before)))
    second = metric(build(after))

    if isinstance(first, Mapping) and isinstance(second, Mapping):
        changes = {}
        for key in sorted(set(first) | set(second)):
            a, b = first.get(key), second.get(key)
            if isinstance(a, (int, float)) and isinstance(b, (int, float)):
                delta = float(b) - float(a)
                changes[key] = {
                    "before": float(a),
                    "after": float(b),
                    "delta": delta,
                    "changed": abs(delta) > tolerance,
                }
            elif a != b:
                changes[key] = {"before": a, "after": b, "changed": True}
        return {
            "before_pin": dict(before),
            "after_pin": after,
            "changed": any(c.get("changed") for c in changes.values()),
            "metrics": changes,
        }

    raise TypeError(
        "metric() must reduce the computation to a mapping of scalars"
    )
