#!/usr/bin/env python3
"""Baseline end-to-end comparisons over the SAME Parquet segment files that
h5i-db wrote (disk-backed, not preloaded DataFrames — see DESIGN_CLAUDE.md
benchmark framing: the honest comparison is storage included).

Engines: polars, duckdb, pandas, pyarrow, arcticdb — each runs the same five
workloads the Rust bench runs, reading from disk every iteration. Engines
whose package is not installed are skipped with a note. ArcticDB reads from
its own LMDB store (populated once from the same data, sibling directory
`<db>.arctic`), since reading foreign Parquet is not its model; its OHLCV and
ASOF compute happens in pandas over ArcticDB reads, which is the idiomatic
ArcticDB usage.

Usage:
    compare_baselines.py <bench.db path> [--repeat 3]
                         [--engines polars,duckdb,pandas,pyarrow,arcticdb]
                         [--pyarrow-asof]   # run pyarrow's experimental (very slow) join_asof

Prints JSON to stdout; human lines to stderr.
"""

import glob
import json
import os
import shutil
import sys
import time


def table_segments(db):
    """Locate each table's segment files + metadata via the catalog files."""
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
            "files": sorted(glob.glob(f"{db}/tables/{table_id}/segments/*.parquet")),
            "time_range": manifest.get("time_range"),
            "rows": manifest.get("rows"),
        }
    return out


def timed(engine, name, repeat, fn):
    best = None
    for _ in range(repeat):
        t0 = time.perf_counter()
        out = fn()
        dt = (time.perf_counter() - t0) * 1000
        best = dt if best is None else min(best, dt)
        _ = out
    label = f"{engine}: {name}"
    print(f"  {label:<44} {best:>10.1f} ms", file=sys.stderr)
    return {"engine": engine, "name": name, "wall_ms": best}


def windows(trades):
    """The two narrow time windows used by the scan workloads (ns epochs)."""
    t_min, t_max = trades["time_range"]
    span = t_max - t_min
    out = []
    for label, frac in [("0.01%", 0.0001), ("1%", 0.01)]:
        lo = t_min + int(span * 0.4)
        hi = lo + int(span * frac)
        out.append((label, lo, hi))
    return out


# --------------------------------------------------------------------------
# polars


def run_polars(trades, quotes, repeat):
    import polars as pl

    print(f"polars {pl.__version__}", file=sys.stderr)
    results = []
    r = lambda name, fn: results.append(timed("polars", name, repeat, fn))

    r("full aggregation (group by symbol)", lambda: (
        pl.scan_parquet(trades["glob"])
        .group_by("symbol")
        .agg(pl.len(), pl.col("price").mean(), pl.col("size").sum())
        .collect()
    ))

    for label, lo, hi in windows(trades):
        lo_dt = pl.from_epoch(pl.lit(lo), time_unit="ns").dt.replace_time_zone("UTC")
        hi_dt = pl.from_epoch(pl.lit(hi), time_unit="ns").dt.replace_time_zone("UTC")
        r(f"time-range scan {label}", lambda lo_dt=lo_dt, hi_dt=hi_dt: (
            pl.scan_parquet(trades["glob"])
            .filter((pl.col("ts") >= lo_dt) & (pl.col("ts") < hi_dt))
            .select(pl.len(), pl.col("price").mean())
            .collect()
        ))

    r("1-minute OHLCV + VWAP rollup", lambda: (
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
    ))

    r("ASOF join trades x quotes", lambda: (
        pl.scan_parquet(trades["glob"]).sort("ts")
        .join_asof(
            pl.scan_parquet(quotes["glob"]).sort("ts"),
            on="ts", by="symbol", strategy="backward",
        )
        .select(pl.len())
        .collect()
    ))

    return results


# --------------------------------------------------------------------------
# duckdb


def run_duckdb(trades, quotes, repeat):
    import duckdb

    print(f"duckdb {duckdb.__version__}", file=sys.stderr)
    results = []
    r = lambda name, fn: results.append(timed("duckdb", name, repeat, fn))
    tg, qg = trades["glob"], quotes["glob"]

    # Build ns-exact timestamp literals matching how duckdb reads the ts column.
    con = duckdb.connect()
    ts_type = con.execute(
        f"SELECT typeof(ts) FROM read_parquet('{tg}') LIMIT 1"
    ).fetchone()[0]
    con.close()
    if "_NS" in ts_type:
        lit = lambda ns: f"make_timestamp_ns({ns})"
    else:  # TIMESTAMPTZ / TIMESTAMP: microsecond precision
        lit = lambda ns: f"make_timestamptz({ns // 1000})"

    def q(sql):
        def fn():
            con = duckdb.connect()
            out = con.execute(sql).arrow()  # columnar; avoids per-row py-object conversion
            con.close()
            return out
        return fn

    r("full aggregation (group by symbol)", q(f"""
        SELECT symbol, count(*), avg(price), sum(size)
        FROM read_parquet('{tg}') GROUP BY symbol
    """))

    for label, lo, hi in windows(trades):
        r(f"time-range scan {label}", q(f"""
            SELECT count(*), avg(price) FROM read_parquet('{tg}')
            WHERE ts >= {lit(lo)} AND ts < {lit(hi)}
        """))

    r("1-minute OHLCV + VWAP rollup", q(f"""
        SELECT symbol, date_trunc('minute', ts) AS bucket,
               first(price ORDER BY ts) AS open,
               max(price)               AS high,
               min(price)               AS low,
               last(price ORDER BY ts)  AS close,
               sum(size)                AS volume,
               sum(price * size) / sum(size) AS vwap
        FROM read_parquet('{tg}')
        GROUP BY symbol, bucket
    """))

    r("ASOF join trades x quotes", q(f"""
        SELECT count(*)
        FROM read_parquet('{tg}') t
        ASOF LEFT JOIN read_parquet('{qg}') q
          ON t.symbol = q.symbol AND t.ts >= q.ts
    """))

    return results


# --------------------------------------------------------------------------
# pandas


def run_pandas(trades, quotes, repeat):
    import pandas as pd

    print(f"pandas {pd.__version__}", file=sys.stderr)
    results = []
    r = lambda name, fn: results.append(timed("pandas", name, repeat, fn))
    tf, qf = trades["files"], quotes["files"]

    r("full aggregation (group by symbol)", lambda: (
        pd.read_parquet(tf, columns=["symbol", "price", "size"])
        .groupby("symbol")
        .agg(n=("price", "count"), price_mean=("price", "mean"), size_sum=("size", "sum"))
    ))

    for label, lo, hi in windows(trades):
        lo_dt = pd.Timestamp(lo, unit="ns", tz="UTC")
        hi_dt = pd.Timestamp(hi, unit="ns", tz="UTC")
        def scan(lo_dt=lo_dt, hi_dt=hi_dt):
            df = pd.read_parquet(
                tf, columns=["ts", "price"],
                filters=[("ts", ">=", lo_dt), ("ts", "<", hi_dt)],
            )
            return len(df), df["price"].mean()
        r(f"time-range scan {label}", scan)

    def ohlcv():
        df = pd.read_parquet(tf, columns=["ts", "symbol", "price", "size"])
        df = df.sort_values("ts", kind="stable")
        df["pv"] = df["price"] * df["size"]
        out = df.groupby(["symbol", pd.Grouper(key="ts", freq="1min")]).agg(
            open=("price", "first"), high=("price", "max"),
            low=("price", "min"), close=("price", "last"),
            volume=("size", "sum"), pv=("pv", "sum"),
        )
        out["vwap"] = out["pv"] / out["volume"]
        return out
    r("1-minute OHLCV + VWAP rollup", ohlcv)

    def asof():
        # Column-pruned reads: polars/duckdb answer count(*) over the join with
        # projection pushdown and never materialize unused columns — eagerly
        # loading every column here would be both unfair and an OOM risk at 20M
        # rows on small machines.
        t = pd.read_parquet(tf, columns=["ts", "symbol", "price"]).sort_values("ts", kind="stable")
        q = pd.read_parquet(qf, columns=["ts", "symbol", "bid"]).sort_values("ts", kind="stable")
        return len(pd.merge_asof(t, q, on="ts", by="symbol", direction="backward"))
    r("ASOF join trades x quotes", asof)

    return results


# --------------------------------------------------------------------------
# pyarrow


def run_pyarrow(trades, quotes, repeat):
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.dataset as ds

    print(f"pyarrow {pa.__version__}", file=sys.stderr)
    results = []
    r = lambda name, fn: results.append(timed("pyarrow", name, repeat, fn))
    tf, qf = trades["files"], quotes["files"]
    ts_type = ds.dataset(tf).schema.field("ts").type

    r("full aggregation (group by symbol)", lambda: (
        ds.dataset(tf)
        .to_table(columns=["symbol", "price", "size"])
        .group_by("symbol")
        .aggregate([("price", "count"), ("price", "mean"), ("size", "sum")])
    ))

    for label, lo, hi in windows(trades):
        flt = (pc.field("ts") >= pa.scalar(lo, ts_type)) & (
            pc.field("ts") < pa.scalar(hi, ts_type)
        )
        def scan(flt=flt):
            t = ds.dataset(tf).to_table(columns=["ts", "price"], filter=flt)
            return t.num_rows, pc.mean(t["price"])
        r(f"time-range scan {label}", scan)

    def ohlcv():
        t = ds.dataset(tf).to_table(columns=["ts", "symbol", "price", "size"])
        t = t.sort_by("ts")
        t = t.append_column("bucket", pc.floor_temporal(t["ts"], unit="minute"))
        t = t.append_column("pv", pc.multiply(t["price"], t["size"]))
        # use_threads=False keeps first/last order-deterministic
        out = t.group_by(["symbol", "bucket"], use_threads=False).aggregate([
            ("price", "first"), ("price", "max"), ("price", "min"),
            ("price", "last"), ("size", "sum"), ("pv", "sum"),
        ])
        return out.append_column(
            "vwap", pc.divide(out["pv_sum"], out["size_sum"])
        )
    r("1-minute OHLCV + VWAP rollup", ohlcv)

    def asof():
        # Column-pruned like the pandas variant (see comment there).
        t = ds.dataset(tf).to_table(columns=["ts", "symbol", "price"]).sort_by("ts")
        q = ds.dataset(qf).to_table(columns=["ts", "symbol", "bid"]).sort_by("ts")
        return t.join_asof(
            q, on="ts", by="symbol", tolerance=-(2**62)
        ).num_rows
    if "--pyarrow-asof" in sys.argv:
        try:
            # join_asof is experimental and orders of magnitude slower than the
            # other engines here (44 s at 2M rows vs 57 ms for polars), so it is
            # opt-in and runs a single iteration.
            results.append(timed("pyarrow", "ASOF join trades x quotes", 1, asof))
        except Exception as e:  # may be absent/broken depending on pyarrow build
            print(f"  pyarrow: ASOF join skipped ({e})", file=sys.stderr)
    else:
        print("  pyarrow: ASOF join skipped (experimental join_asof is ~1000x "
              "slower; pass --pyarrow-asof to run it)", file=sys.stderr)

    return results


# --------------------------------------------------------------------------
# arcticdb


def run_arcticdb(trades, quotes, repeat):
    import arcticdb as adb
    import pandas as pd

    print(f"arcticdb {adb.__version__}", file=sys.stderr)
    results = []

    def safe(name, rep, fn):
        """Version-tolerant: a workload an older ArcticDB cannot express is
        skipped with a note instead of aborting the engine."""
        try:
            results.append(timed("arcticdb", name, rep, fn))
        except Exception as e:  # noqa: BLE001 — engine coverage over precision
            print(f"  arcticdb: {name} skipped ({type(e).__name__}: {e})", file=sys.stderr)

    # One-time ingest of the same data into ArcticDB's own LMDB store — its
    # native format is the honest storage-included comparison, not foreign
    # Parquet. Chunked per segment file to bound memory on small machines.
    db_root = trades["glob"].split("/tables/")[0].rstrip("/")
    store = os.path.abspath(db_root + ".arctic")
    shutil.rmtree(store, ignore_errors=True)
    ac = adb.Arctic(f"lmdb://{store}?map_size=20GB")
    lib = ac.get_library("bench", create_if_missing=True)

    def ingest():
        for symbol, meta in (("trades", trades), ("quotes", quotes)):
            for i, path in enumerate(meta["files"]):
                df = pd.read_parquet(path).set_index("ts")
                if i == 0:
                    lib.write(symbol, df)
                else:
                    lib.append(symbol, df, validate_index=False)

    safe("ingest into LMDB store (one-time)", 1, ingest)
    if not results:
        return results  # ingest failed; nothing below can run

    def full_agg():
        try:
            q = adb.QueryBuilder().groupby("symbol").agg(
                {"price": ["count", "mean"], "size": "sum"}
            )
            return lib.read("trades", query_builder=q).data
        except Exception:  # older QueryBuilder: single aggregate per column
            q = adb.QueryBuilder().groupby("symbol").agg(
                {"price": "mean", "size": "sum"}
            )
            return lib.read("trades", query_builder=q).data

    safe("full aggregation (group by symbol)", repeat, full_agg)

    for label, lo, hi in windows(trades):
        lo_ts = pd.Timestamp(lo, unit="ns", tz="UTC")
        hi_ts = pd.Timestamp(hi - 1, unit="ns", tz="UTC")  # date_range is inclusive

        def scan(lo_ts=lo_ts, hi_ts=hi_ts):
            df = lib.read("trades", date_range=(lo_ts, hi_ts), columns=["price"]).data
            return len(df), df["price"].mean()

        safe(f"time-range scan {label}", repeat, scan)

    def ohlcv():
        df = lib.read("trades", columns=["symbol", "price", "size"]).data
        df["pv"] = df["price"] * df["size"]
        out = df.groupby(["symbol", pd.Grouper(level=0, freq="1min")]).agg(
            open=("price", "first"), high=("price", "max"),
            low=("price", "min"), close=("price", "last"),
            volume=("size", "sum"), pv=("pv", "sum"),
        )
        out["vwap"] = out["pv"] / out["volume"]
        return out

    safe("1-minute OHLCV + VWAP rollup", repeat, ohlcv)

    def asof():
        # Column-pruned like the pandas variant; ArcticDB has no native ASOF
        # join, so this measures its idiomatic path: store reads + merge_asof.
        t = lib.read("trades", columns=["symbol", "price"]).data.reset_index()
        q = lib.read("quotes", columns=["symbol", "bid"]).data.reset_index()
        t = t.sort_values("ts", kind="stable")
        q = q.sort_values("ts", kind="stable")
        return len(pd.merge_asof(t, q, on="ts", by="symbol", direction="backward"))

    safe("ASOF join trades x quotes", repeat, asof)
    return results


# --------------------------------------------------------------------------

ENGINES = {
    "polars": run_polars,
    "duckdb": run_duckdb,
    "pandas": run_pandas,
    "pyarrow": run_pyarrow,
    "arcticdb": run_arcticdb,
}


def main():
    db = sys.argv[1]
    repeat = 3
    if "--repeat" in sys.argv:
        repeat = int(sys.argv[sys.argv.index("--repeat") + 1])
    engines = list(ENGINES)
    if "--engines" in sys.argv:
        engines = sys.argv[sys.argv.index("--engines") + 1].split(",")

    tables = table_segments(db)
    trades, quotes = tables["trades"], tables["quotes"]
    print(f"{trades['rows']:,} trade rows, {quotes['rows']:,} quote rows, "
          f"repeat={repeat}", file=sys.stderr)

    results = []
    for eng in engines:
        try:
            results.extend(ENGINES[eng](trades, quotes, repeat))
        except ImportError as e:
            print(f"{eng}: not installed ({e}) — skipped", file=sys.stderr)

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
