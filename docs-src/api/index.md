---
title: Overview
description: The h5i_db Python library — install, the five-minute tour, data interchange, and error handling.
order: 0
---

# Python API

<p class="doc-lede">The <code>h5i_db</code> package is an ergonomic wrapper over the
native Rust engine. All tabular data crosses the boundary as Arrow, so it plugs
directly into pyarrow, pandas, and Polars.</p>

<div class="doc-divider"></div>

```console
$ pip install h5i-db
```

The only required dependency is `pyarrow >= 14`. `to_pandas()` /
`to_polars()` activate when pandas / Polars are installed.

## The five-minute tour

```python
import pyarrow as pa
import h5i_db

db = h5i_db.Database("market.db", create=True)

schema = pa.schema([
    pa.field("ts", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("symbol", pa.string()),
    pa.field("price", pa.float64()),
    pa.field("size", pa.int64()),
])
db.create_table("trades", schema, time_column="ts")

db.append("trades", table)                    # pyarrow Table / RecordBatch(es)
df = db.sql("SELECT * FROM trades").to_pandas()

old = db.read("trades", version=3)            # time travel
plan = db.plan_delete_range("trades", t0_us, t1_us)   # previewable mutation
plan.apply()                                  # or plan.discard()

db.close()                                    # or use `with h5i_db.Database(...) as db:`
```

## The pieces

<div class="card-grid">
  <a class="card" href="database.html">
    <span class="card-no">CLASS</span>
    <span class="card-title">Database</span>
    <span class="card-desc">Open/create databases; tables, ingest, SQL, time travel, snapshots, plans, policy, maintenance.</span>
  </a>
  <a class="card" href="results-and-plans.html">
    <span class="card-no">CLASSES</span>
    <span class="card-title">QueryResult &amp; MutationPlan</span>
    <span class="card-desc">Result conversion to Arrow/pandas/Polars, and the preview → apply mutation flow.</span>
  </a>
  <a class="card" href="exceptions.html">
    <span class="card-no">EXCEPTIONS</span>
    <span class="card-title">Error types</span>
    <span class="card-desc">The typed hierarchy under H5iError, with .code, .hint, and .retryable on every error.</span>
  </a>
</div>

## Data in, data out

Everything tabular is Arrow:

- **In**: `write()` / `append()` accept a `pyarrow.Table`, a `RecordBatch`,
  or a sequence of batches (`TableLike`). Coming from pandas or Polars:
  `pa.Table.from_pandas(df)` / `pl_df.to_arrow()`.
- **Out**: `read()` returns a `pyarrow.Table`; `sql()` returns a
  [`QueryResult`](results-and-plans.html#queryresult) with `.to_arrow()`,
  `.to_pandas()`, `.to_polars()`.

Because the interchange is Arrow IPC, there is no per-row conversion cost and
no type fidelity loss.

## Error handling

Every failure raises a subclass of `h5i_db.H5iError` carrying the same
structured envelope the CLI prints: `.code` (stable string), `.hint`
(actionable), `.retryable` (worth retrying?).

```python
try:
    db.sql("SELECT * FROM h5i('trades')", timeout=30, max_rows=1_000_000)
except h5i_db.TimeoutError:
    ...                                   # raise the timeout or narrow the query
except h5i_db.LimitError as e:
    print(e.code)                         # "limit_exceeded"
except h5i_db.ConflictError:
    ...                                   # retryable — another writer won the race
```

See [Exceptions](exceptions.html) for the full hierarchy and code table.

## Versioning note

`h5i_db.__version__` reports the installed engine version. The pure-Python
wrapper (`Database`, `QueryResult`, `MutationPlan`) sits on the private
native module `h5i_db._native`; treat everything not exported from `h5i_db`
itself as internal.
