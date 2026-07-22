#!/usr/bin/env python3
"""Polars end-to-end comparison over the SAME Parquet segment files that
h5i-db wrote (disk-backed `scan_parquet`, not preloaded DataFrames — see
DESIGN_CLAUDE.md benchmark framing: the honest comparison is storage
included).

Usage:
    polars_compare.py <bench.db path> [--repeat 3]

Prints JSON to stdout; human lines to stderr.
"""

import glob
import json
import sys
import time
import uuid

import polars as pl


def table_segments(db, want_time_col_set):
    """Locate each table's segment glob + metadata via the catalog files."""
    out = {}
    for cat in glob.glob(f"{db}/catalog/tables/*.json"):
        entry = json.load(open(cat))
        table_id = entry["table_id"]
        head = json.load(open(f"{db}/tables/{table_id}/HEAD"))
        manifest = json.load(
            open(f"{db}/tables/{table_id}/manifests/{head['sequence']:012d}.json")
        )
        out[entry["name"]] = {
            "glob": f"{db}/tables/{table_id}/segments/*.parquet",
            "time_range": manifest.get("time_range"),
            "rows": manifest.get("rows"),
        }
    return out


def timed(name, repeat, fn):
    best = None
    for _ in range(repeat):
        t0 = time.perf_counter()
        out = fn()
        dt = (time.perf_counter() - t0) * 1000
        best = dt if best is None else min(best, dt)
        _ = out
    print(f"  {name:<44} {best:>10.1f} ms", file=sys.stderr)
    return {"name": name, "wall_ms": best}


def main():
    db = sys.argv[1]
    repeat = 3
    if "--repeat" in sys.argv:
        repeat = int(sys.argv[sys.argv.index("--repeat") + 1])
    tables = table_segments(db, {"ts"})
    trades, quotes = tables["trades"], tables["quotes"]
    t_min, t_max = trades["time_range"]
    span = t_max - t_min
    results = []

    print(f"polars {pl.__version__} over {trades['rows']:,} trade rows", file=sys.stderr)

    # 1. full aggregation (disk-backed lazy scan)
    results.append(timed("polars: full aggregation (group by symbol)", repeat, lambda: (
        pl.scan_parquet(trades["glob"])
        .group_by("symbol")
        .agg(pl.len(), pl.col("price").mean(), pl.col("size").sum())
        .collect()
    )))

    # 2. narrow time-range scans
    for label, frac in [("0.01%", 0.0001), ("1%", 0.01)]:
        lo = t_min + int(span * 0.4)
        hi = lo + int(span * frac)
        lo_dt = pl.from_epoch(pl.lit(lo), time_unit="ns").dt.replace_time_zone("UTC")
        hi_dt = pl.from_epoch(pl.lit(hi), time_unit="ns").dt.replace_time_zone("UTC")
        results.append(timed(f"polars: time-range scan {label}", repeat, lambda lo_dt=lo_dt, hi_dt=hi_dt: (
            pl.scan_parquet(trades["glob"])
            .filter((pl.col("ts") >= lo_dt) & (pl.col("ts") < hi_dt))
            .select(pl.len(), pl.col("price").mean())
            .collect()
        )))

    # 3. 1-minute OHLCV + VWAP
    results.append(timed("polars: 1-minute OHLCV + VWAP rollup", repeat, lambda: (
        pl.scan_parquet(trades["glob"])
        .sort("ts")
        .group_by_dynamic("ts", every="1m", group_by="symbol")
        .agg(
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            pl.col("size").sum().alias("volume"),
            ((pl.col("price") * pl.col("size")).sum() / pl.col("size").sum()).alias("vwap"),
        )
        .collect()
    )))

    # 4. ASOF join trades x quotes by symbol (both sides from disk)
    results.append(timed("polars: ASOF join trades x quotes", repeat, lambda: (
        pl.scan_parquet(trades["glob"]).sort("ts")
        .join_asof(
            pl.scan_parquet(quotes["glob"]).sort("ts"),
            on="ts", by="symbol", strategy="backward",
        )
        .select(pl.len())
        .collect()
    )))

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
