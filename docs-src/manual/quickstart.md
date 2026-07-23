---
title: Quickstart
description: From nothing to a queried, versioned market database in five commands — CLI and Python.
order: 2
---

# Quickstart

Five minutes from an empty directory to time-series SQL with time travel. The
CLI and the Python library drive the same engine and the same on-disk format —
pick either, or mix freely.

## CLI

```console
$ cargo install h5i-db-cli
$ h5i-db init market.db
$ h5i-db create-table market.db trades --like ticks.parquet --time-column ts
$ h5i-db ingest market.db trades ticks.parquet
$ h5i-db query market.db "SELECT symbol, vwap(price, size) AS vwap FROM trades GROUP BY symbol"
```

`init` creates the database — a plain directory, no server. `create-table`
infers the schema from an existing Parquet/CSV/Arrow file (or takes explicit
JSON via `--schema`); `--time-column` declares the time axis, which is what
makes pruning and the time-series operators work. `ingest` appends the file's
rows as one atomic, durable commit.

Query output goes to stdout in your chosen `--format`:

```console
$ h5i-db query market.db "SELECT time_bucket('1m', ts) AS bar, vwap(price, size) AS vwap
                          FROM trades GROUP BY bar ORDER BY bar" --format csv | head -3
bar,vwap
2026-07-01T09:30:00,101.24
2026-07-01T09:31:00,101.31
```

Every write created a version. Look at the history, read an old version, pin a
reproducible snapshot:

```console
$ h5i-db versions market.db trades
$ h5i-db query market.db "SELECT count(*) FROM h5i('trades', 1)"   # time travel
$ h5i-db snapshot create market.db eod-2026-07-01
```

And when something needs fixing, preview before you commit:

```console
$ h5i-db delete-range market.db trades --start 2026-07-01T09:30:00Z \
    --end 2026-07-01T09:31:00Z --plan
{"plan_id": "5c41…", "summary": {"rows_affected": 12481, "segments_reused": 127}}
$ h5i-db plan show market.db trades 5c41…     # before/after samples
$ h5i-db plan apply market.db trades 5c41…    # metadata-only swap
```

Finally, `h5i-db ui market.db` serves a loopback-only review surface: pending
plans with previews, the version timeline, and an SQL scratchpad.

## Python

```console
$ pip install h5i-db
```

```python
import pyarrow as pa
import h5i_db

schema = pa.schema([
    pa.field("ts", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("symbol", pa.string()),
    pa.field("price", pa.float64()),
    pa.field("size", pa.int64()),
])

db = h5i_db.Database("market.db", create=True)
db.create_table("trades", schema, time_column="ts")
db.append("trades", ticks)                 # pyarrow.Table / RecordBatch(es)

bars = db.sql("""
    SELECT time_bucket('1m', ts) AS bar, symbol,
           vwap(price, size) AS vwap, sum(size) AS volume
    FROM trades GROUP BY bar, symbol ORDER BY bar
""").to_pandas()
```

Time travel and reproducibility:

```python
old = db.read("trades", version=3)                         # exact version
asof = db.sql("SELECT * FROM h5i('trades', '2026-07-01T00:00:00Z')")
db.snapshot("eod-2026-07-01")                              # pin all tables
```

Previewable mutation, the same plan/apply flow as the CLI:

```python
plan = db.plan_delete_range("trades", start_us, end_us)    # raw µs bounds
plan.summary                 # {"rows_affected": …, "segments_reused": …}
plan.before_sample           # pyarrow.Table of rows to be removed
plan.apply()                 # or plan.discard()
```

!!! note "Raw time units in Python"
    SQL takes RFC3339 strings, but the Python range APIs
    (`plan_delete_range`, `read(time_start=…)`) take **raw integers in the
    time column's unit** — microseconds for `timestamp[us]`. See
    [Core concepts](concepts.html#the-time-axis).

Errors are typed and machine-actionable:

```python
try:
    db.append("trades", batch)
except h5i_db.ConflictError as e:     # another writer won the race
    print(e.code, e.retryable)        # "version_conflict", True
```

## Where next

- [Core concepts](concepts.html) — what a version, segment, snapshot, and plan
  actually are.
- [SQL reference](sql.html) — `h5i()`, `asof_join`, `time_bucket`, `gapfill`,
  and the rest of the function library.
- The [Cookbook quickstart notebook](../cookbook/00_fundamentals/01_quickstart.html)
  — this page as an executed notebook on real data.
