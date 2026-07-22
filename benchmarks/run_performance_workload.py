#!/usr/bin/env python3
"""Run the checked-in P0 query workload without linking a Rust test binary."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import platform
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from check_performance_report import parse_report, validate


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def nearest_rank(values: list[int], percentile: float) -> int:
    if not values:
        raise ValueError("cannot summarize an empty sample")
    ordered = sorted(values)
    rank = max(1, math.ceil(percentile * len(ordered)))
    return ordered[rank - 1]


def summarize(values: list[int]) -> dict[str, int]:
    return {
        "min": min(values),
        "median": nearest_rank(values, 0.5),
        "p95": nearest_rank(values, 0.95),
        "max": max(values),
    }


def optional_output(command: list[str]) -> str | None:
    try:
        result = subprocess.run(command, text=True, capture_output=True, check=False)
    except OSError:
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip() or result.stderr.strip() or None


def load_workload(path: Path) -> dict[str, Any]:
    workload = json.loads(path.read_text(encoding="utf-8"))
    if workload.get("schema_version") != 1:
        raise ValueError("unsupported workload schema_version")
    cases = workload.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError("workload cases must be a non-empty array")
    names: set[str] = set()
    for case in cases:
        if not isinstance(case, dict):
            raise ValueError("each workload case must be an object")
        name = case.get("name")
        sql = case.get("sql")
        if not isinstance(name, str) or not name or name in names:
            raise ValueError("workload case names must be non-empty and unique")
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError(f"case {name!r} has no SQL")
        names.add(name)
    return workload


def execute_once(binary: Path, db: Path, case: dict[str, Any]) -> dict[str, Any]:
    command = [
        str(binary),
        "--format",
        "json",
        "query",
        str(db),
        case["sql"],
        "--stats",
    ]
    started = time.perf_counter_ns()
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    wall_ns = time.perf_counter_ns() - started
    if result.returncode != 0:
        raise RuntimeError(
            f"case {case['name']!r} failed with exit {result.returncode}:\n{result.stderr}"
        )
    report = parse_report(result.stderr)
    validate(report)
    if case.get("require_scan") and report["bytes_scanned"] <= 0:
        raise ValueError(f"case {case['name']!r} expected a physical scan")
    return {
        "result_sha256": hashlib.sha256(result.stdout.encode("utf-8")).hexdigest(),
        "query_fingerprint": report["query_fingerprint"],
        "sample": {
            "wall_ns": wall_ns,
            "query_ns": report["planning_ns"] + report["execution_ns"],
            "planning_ns": report["planning_ns"],
            "execution_ns": report["execution_ns"],
            "bytes_scanned": report["bytes_scanned"],
            "scan_output_rows": report["scan_output_rows"],
            "output_rows": report["output_rows"],
            "row_groups_pruned": report["row_groups_pruned"],
            "page_index_rows_pruned": report["page_index_rows_pruned"],
            "spill_count": report["spill_count"],
            "spilled_bytes": report["spilled_bytes"],
        },
    }


def run_case(
    binary: Path,
    db: Path,
    case: dict[str, Any],
    warmups: int,
    repetitions: int,
) -> dict[str, Any]:
    for _ in range(warmups):
        execute_once(binary, db, case)

    executions = [execute_once(binary, db, case) for _ in range(repetitions)]
    fingerprints = {execution["query_fingerprint"] for execution in executions}
    checksums = {execution["result_sha256"] for execution in executions}
    if len(fingerprints) != 1:
        raise ValueError(f"case {case['name']!r} produced inconsistent fingerprints")
    if len(checksums) != 1:
        raise ValueError(f"case {case['name']!r} produced inconsistent results")

    samples = [execution["sample"] for execution in executions]
    summary = {
        metric: summarize([sample[metric] for sample in samples])
        for metric in samples[0]
    }
    return {
        "name": case["name"],
        "regression_gate": bool(case.get("regression_gate")),
        "query_fingerprint": fingerprints.pop(),
        "result_sha256": checksums.pop(),
        "samples": samples,
        "summary": summary,
    }


def compare_baseline(
    current: dict[str, Any], baseline: dict[str, Any], max_regression_percent: float
) -> tuple[list[dict[str, Any]], list[str]]:
    old_cases = {case["name"]: case for case in baseline.get("cases", [])}
    comparisons: list[dict[str, Any]] = []
    failures: list[str] = []
    for case in current["cases"]:
        if not case["regression_gate"]:
            continue
        old = old_cases.get(case["name"])
        if old is None:
            failures.append(f"baseline is missing gated case {case['name']!r}")
            continue
        if old.get("query_fingerprint") != case["query_fingerprint"]:
            failures.append(f"query fingerprint changed for {case['name']!r}")
            continue
        if old.get("result_sha256") != case["result_sha256"]:
            failures.append(f"result checksum changed for {case['name']!r}")
            continue
        previous = old["summary"]["query_ns"]["median"]
        observed = case["summary"]["query_ns"]["median"]
        regression = 0.0 if previous == 0 else (observed - previous) * 100.0 / previous
        comparisons.append(
            {
                "name": case["name"],
                "baseline_query_ns_median": previous,
                "current_query_ns_median": observed,
                "regression_percent": regression,
            }
        )
        if regression > max_regression_percent:
            failures.append(
                f"{case['name']!r} regressed {regression:.2f}% "
                f"(limit {max_regression_percent:.2f}%)"
            )
    return comparisons, failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, default=Path("target/debug/h5i-db"))
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument(
        "--workload",
        type=Path,
        default=Path(__file__).with_name("performance_workload.json"),
    )
    parser.add_argument("--warmups", type=int)
    parser.add_argument("--repetitions", type=int)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--max-regression-percent", type=float, default=10.0)
    args = parser.parse_args()

    try:
        workload = load_workload(args.workload)
        warmups = workload["warmups"] if args.warmups is None else args.warmups
        repetitions = (
            workload["repetitions"] if args.repetitions is None else args.repetitions
        )
        if warmups < 0 or repetitions <= 0:
            raise ValueError("warmups must be non-negative and repetitions must be positive")
        if not args.binary.is_file():
            raise ValueError(f"binary does not exist: {args.binary}")
        if not args.db.exists():
            raise ValueError(f"database does not exist: {args.db}")

        payload: dict[str, Any] = {
            "schema_version": 1,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "environment": {
                "platform": platform.platform(),
                "python": platform.python_version(),
                "rustc": optional_output(["rustc", "-Vv"]),
                "binary": str(args.binary),
                "binary_sha256": file_sha256(args.binary),
                "binary_version": optional_output([str(args.binary), "--version"]),
                "database": str(args.db),
                "workload": str(args.workload),
                "workload_sha256": file_sha256(args.workload),
                "warmups": warmups,
                "repetitions": repetitions,
            },
            "cases": [
                run_case(args.binary, args.db, case, warmups, repetitions)
                for case in workload["cases"]
            ],
        }
        failures: list[str] = []
        if args.baseline:
            baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
            comparisons, failures = compare_baseline(
                payload, baseline, args.max_regression_percent
            )
            payload["baseline_comparison"] = {
                "baseline": str(args.baseline),
                "max_regression_percent": args.max_regression_percent,
                "cases": comparisons,
                "failures": failures,
            }

        rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
        if args.output:
            args.output.write_text(rendered, encoding="utf-8")
        else:
            sys.stdout.write(rendered)
        for failure in failures:
            print(f"performance gate failed: {failure}", file=sys.stderr)
        return 1 if failures else 0
    except (OSError, KeyError, TypeError, ValueError, RuntimeError, json.JSONDecodeError) as error:
        print(f"performance workload failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
