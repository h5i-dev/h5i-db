#!/usr/bin/env python3
"""Demonstrate the P2 predicate-cache win on skewed (episodic) symbol data.

The checked-in performance workload shows the predicate cache yielding no
byte reduction: its symbols interleave uniformly, so every row group contains
every symbol and row-group-granular pruning has nothing to eliminate. This
scenario builds the case the cache exists for — an episodic symbol that
trades only inside a narrow time window of time-ordered segments, with a
name that sorts inside the liquid symbols' min/max range so per-column
statistics cannot prune it. Only evaluating the predicate itself reveals
which row groups are empty, which is exactly what the cache memoizes.

Drives an already-built CLI binary (like run_performance_workload.py), so it
adds no Rust/DataFusion link target. Requires pyarrow for data generation.

Exit codes: 0 = warm hits reduced physical scan bytes with identical results,
1 = the cache failed to reduce bytes or results diverged, 2 = setup error.
"""

from __future__ import annotations

import argparse
import json
import random
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from check_performance_report import parse_report, validate

EPISODIC = "SYM0031_EPI"  # sorts between SYM0031 and SYM0032
QUERY = (
    "SELECT count(*) AS n, min(price) AS lo, max(price) AS hi, "
    f"sum(size) AS vol FROM trades WHERE symbol = '{EPISODIC}'"
)


def generate_parquet(path: Path, rows: int, episode_frac: float, seed: int) -> int:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as error:
        raise RuntimeError(f"pyarrow is required to generate the dataset: {error}")

    rng = random.Random(seed)
    episode = (int(rows * 0.50), int(rows * (0.50 + episode_frac)))
    ts, sym, price, size = [], [], [], []
    t = 1_750_000_000_000_000_000
    prices = {f"SYM{i:04d}": 20.0 + rng.random() * 480 for i in range(64)}
    prices[EPISODIC] = 100.0
    episodic_rows = 0
    for i in range(rows):
        t += rng.randint(1_000, 2_000_000)
        if episode[0] <= i < episode[1] and rng.random() < 0.05:
            s = EPISODIC
            episodic_rows += 1
        else:
            s = f"SYM{rng.randrange(64):04d}"
        prices[s] = max(0.01, prices[s] + (rng.random() - 0.5) * 0.1)
        ts.append(t)
        sym.append(s)
        price.append(prices[s])
        size.append(rng.randint(1, 10_000))
    table = pa.table(
        {
            "ts": pa.array(ts, pa.timestamp("ns", tz="UTC")),
            "symbol": pa.array(sym, pa.string()),
            "price": pa.array(price, pa.float64()),
            "size": pa.array(size, pa.int64()),
        }
    )
    pq.write_table(table, path)
    return episodic_rows


def cli(binary: Path, *args: str) -> str:
    result = subprocess.run(
        [str(binary), *args], text=True, capture_output=True, check=False
    )
    if result.returncode != 0:
        raise RuntimeError(f"{args[0]} failed with exit {result.returncode}:\n{result.stderr}")
    return result.stdout


def query_once(binary: Path, db: Path, predicate_cache: bool) -> tuple[str, dict]:
    args = [str(binary), "--format", "json", "query", str(db), QUERY, "--stats"]
    if predicate_cache:
        args.append("--predicate-cache")
    result = subprocess.run(args, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"query failed with exit {result.returncode}:\n{result.stderr}")
    report = parse_report(result.stderr)
    validate(report)
    return result.stdout.strip(), report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, default=Path("target/release/h5i-db"))
    parser.add_argument("--dir", type=Path, help="working directory (temp when omitted)")
    parser.add_argument("--rows", type=int, default=4_000_000)
    parser.add_argument(
        "--episode-frac",
        type=float,
        default=0.03,
        help="fraction of the time axis the episodic symbol trades in",
    )
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    try:
        if not args.binary.is_file():
            raise RuntimeError(f"binary does not exist: {args.binary}")
        tmp = None
        if args.dir is None:
            tmp = tempfile.TemporaryDirectory(prefix="h5i-predicate-cache-")
            workdir = Path(tmp.name)
        else:
            workdir = args.dir
            workdir.mkdir(parents=True, exist_ok=True)

        parquet = workdir / "skewed.parquet"
        db = workdir / "skewed.db"
        episodic_rows = generate_parquet(parquet, args.rows, args.episode_frac, args.seed)
        print(
            f"dataset: {args.rows:,} rows, {episodic_rows:,} episodic rows "
            f"in a {args.episode_frac:.0%} window",
        )
        if db.exists():
            shutil.rmtree(db)
        cli(args.binary, "init", str(db))
        cli(args.binary, "create-table", str(db), "trades", "--like", str(parquet))
        cli(args.binary, "ingest", str(db), "trades", str(parquet))
    except (OSError, RuntimeError) as error:
        print(f"setup failed: {error}", file=sys.stderr)
        return 2

    try:
        baseline_rows, baseline = None, None
        for _ in range(args.reps):
            baseline_rows, baseline = query_once(args.binary, db, predicate_cache=False)
        cold_rows, cold = query_once(args.binary, db, predicate_cache=True)
        warm_rows, warm = None, None
        for _ in range(args.reps):
            warm_rows, warm = query_once(args.binary, db, predicate_cache=True)

        def line(label: str, report: dict) -> None:
            print(
                f"  {label:14s} bytes_scanned={report['bytes_scanned']:>12,} "
                f"scan_rows={report['scan_output_rows']:>12,} "
                f"hits={report['predicate_cache_hits']} "
                f"builds={report['predicate_cache_builds']}"
            )

        print(f"query: {QUERY}")
        line("no cache", baseline)
        line("cold (build)", cold)
        line("warm (hits)", warm)

        if not (baseline_rows == cold_rows == warm_rows):
            print("FAIL: results diverged between cache modes", file=sys.stderr)
            return 1
        if warm["predicate_cache_hits"] == 0:
            print("FAIL: warm run did not hit the predicate cache", file=sys.stderr)
            return 1
        if warm["bytes_scanned"] >= baseline["bytes_scanned"]:
            print("FAIL: warm hit did not reduce physical scan bytes", file=sys.stderr)
            return 1
        reduction = 1 - warm["bytes_scanned"] / baseline["bytes_scanned"]
        print(f"OK: warm hits scan {reduction:.0%} fewer physical bytes, identical results")
        return 0
    except (RuntimeError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"scenario failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
