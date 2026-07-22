#!/usr/bin/env python3
"""Validate P0 metrics through an already-built h5i-db executable.

This checker has no Rust or DataFusion dependency. It deliberately consumes the
public CLI contract so the real adapter is exercised without linking another
DataFusion-heavy test binary.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


REQUIRED_INTEGERS = (
    "planning_ns",
    "execution_ns",
    "output_batches",
    "output_rows",
    "bytes_scanned",
    "scan_output_rows",
    "row_groups_pruned",
    "page_index_rows_pruned",
    "pushdown_rows_pruned",
    "spill_count",
    "spilled_bytes",
    "sort_operators",
)


def parse_report(stderr: str) -> dict[str, Any]:
    for line in reversed(stderr.splitlines()):
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and "query_id" in value and "query_fingerprint" in value:
            return value
    raise ValueError("stderr did not contain a query performance report")


def validate(report: dict[str, Any]) -> None:
    if report.get("status") != "succeeded":
        raise ValueError(f"query status is not succeeded: {report.get('status')!r}")
    fingerprint = report.get("query_fingerprint")
    if not isinstance(fingerprint, str) or len(fingerprint) != 64:
        raise ValueError("query_fingerprint is not a 64-character blake3 hex digest")
    try:
        int(fingerprint, 16)
    except ValueError as error:
        raise ValueError("query_fingerprint is not hexadecimal") from error
    for name in REQUIRED_INTEGERS:
        value = report.get(name)
        if not isinstance(value, int) or value < 0:
            raise ValueError(f"{name} is not a non-negative integer: {value!r}")
    if not isinstance(report.get("scans"), list):
        raise ValueError("scans is not an array")
    if not isinstance(report.get("operators"), list):
        raise ValueError("operators is not an array")
    forbidden = {"sql", "query", "query_text", "normalized_query"}
    leaked = forbidden.intersection(report)
    if leaked:
        raise ValueError(f"report exposes query text fields: {sorted(leaked)}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, default=Path("target/debug/h5i-db"))
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--sql", required=True)
    parser.add_argument("--require-scan", action="store_true")
    args = parser.parse_args()

    command = [
        str(args.binary),
        "--format",
        "json",
        "query",
        str(args.db),
        args.sql,
        "--stats",
    ]
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        return result.returncode
    try:
        report = parse_report(result.stderr)
        validate(report)
        if args.require_scan and report["bytes_scanned"] <= 0:
            raise ValueError("--require-scan was set but bytes_scanned is zero")
    except ValueError as error:
        print(f"performance report check failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
