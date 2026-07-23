---
title: Migrating from DuckDB
description: "Move tables from a .duckdb file into a versioned h5i-db store using DuckDB's own Parquet export, plus a SQL dialect and workflow checklist for what changes."
order: 2.5
---

# Migrating from DuckDB

h5i-db does **not** read `.duckdb` files directly — and by design. DuckDB's
on-disk format is an internal, engine-versioned block format; a native reader
would mean tracking DuckDB's internals release by release for a run-once
operation. Instead, migration goes through the interchange format both engines
already speak fluently: **Parquet**. DuckDB exports it in one statement, and
`h5i-db ingest` reads it directly.

This page is the recipe plus the "what changes" checklist. Budget a few minutes
per table.

!!! note "Is h5i-db the right destination?"
    h5i-db is a **versioned, time-series** store, not a general-purpose OLAP
    engine. It shines when your data has a **time column** and is
    **append-mostly** — market ticks, metrics, event logs — and you want
    version history, time travel, ASOF joins, and crash-safe commits underneath.
    If you use DuckDB for ad-hoc general analytics, star-schema BI, or heavy
    in-place `UPDATE`/`DELETE`, that is not what h5i-db is for. See
    [Core concepts](concepts.html).

---

## The recipe

### 1. Export from DuckDB to Parquet

For a whole database, `EXPORT DATABASE` writes one Parquet file per table plus
the schema, into a directory:

```sql
-- in the duckdb shell, against your existing file
$ duckdb market.duckdb
D EXPORT DATABASE 'export_dir' (FORMAT PARQUET);
```

For a single table, `COPY` is enough:

```sql
D COPY trades TO 'trades.parquet' (FORMAT PARQUET);
```

Parquet preserves column names, types, and (unlike CSV) timestamp precision and
nullability — so it is always preferable to CSV for migration.

### 2. Create the table in h5i-db

Infer the schema straight from the exported Parquet, and — this is the one step
with no DuckDB equivalent — **declare the time column**. This is what unlocks
manifest pruning, streaming rollups, and ASOF joins; pick the column your
queries filter and order by.

```console
$ h5i-db init market.db
$ h5i-db create-table market.db trades --like export_dir/trades.parquet --time-column ts
```

If you want an explicit sort key beyond the time column (e.g. time then
symbol), pass `--sort-key ts,symbol`. See
[`create-table`](cli.html#h5i-db-create-table) for the full type list.

### 3. Ingest the data

```console
$ h5i-db ingest market.db trades export_dir/trades.parquet
```

Repeat steps 2–3 per table. For many tables, loop in your shell:

```console
$ for f in export_dir/*.parquet; do
    t=$(basename "$f" .parquet)
    h5i-db create-table market.db "$t" --like "$f" --time-column ts
    h5i-db ingest      market.db "$t" "$f"
  done
```

### 4. Verify

```console
$ h5i-db tables market.db                       # row counts + time ranges per table
$ h5i-db query  market.db "SELECT count(*) FROM trades"
```

Row counts should match DuckDB's `SELECT count(*)`. Every ingest is committed as
version 1 (and up); nothing is destructive, so a mistaken load is one
[`restore`](cli.html#h5i-db-restore) away from undone.

!!! tip "Python instead of the CLI"
    The same flow works from Python — read the Parquet with `pyarrow` and
    `db.append(table)`. DuckDB can hand you Arrow with zero copy via
    `duckdb.sql("SELECT * FROM trades").arrow()`, which you pass straight to
    `db.append("trades", ...)`. See the [Quickstart](quickstart.html).

---

## SQL dialect: what ports and what changes

The query engine is [DataFusion](sql.html), so standard analytical SQL — joins,
CTEs, window functions, `date_trunc`, `stddev`, `corr`,
`approx_percentile_cont`, `INTERVAL` arithmetic — moves over unchanged. String
literals are single-quoted; identifiers are case-insensitive. The differences
worth knowing:

### Ports cleanly

- **`time_bucket(...)`** — h5i-db adopts DuckDB/TimescaleDB semantics
  deliberately, so bucketed rollups port as-is.
- **ASOF joins** — supported, and semantically differential-tested against
  DuckDB as the oracle (ties, NULLs, strict/non-strict). The syntax differs
  slightly — `ASOF JOIN … MATCH_CONDITION` or the `asof_join(...)` table
  function; see the [SQL reference](sql.html).
- Standard aggregates, window functions, and scalar date/time functions.

### Needs rewriting

DuckDB-specific extensions have no DataFusion equivalent — rewrite these:

| DuckDB construct | In h5i-db |
|---|---|
| `read_parquet(...)`, `read_csv_auto(...)` in `FROM` | Data is **ingested first**; query plain table names |
| `PIVOT` / `UNPIVOT`, `SUMMARIZE` | Rewrite with explicit `CASE` / aggregation |
| `QUALIFY` | Wrap the window expression in a subquery + `WHERE` |
| `SELECT * EXCLUDE (...) / REPLACE (...)`, `COLUMNS(...)` | List columns explicitly |
| `USING SAMPLE` | `ORDER BY random() LIMIT n` or app-side sampling |
| List/struct/map sugar, list comprehensions | Restructure; nested types are not the target model |
| `HUGEINT`, `DECIMAL(p,s)`, nested `STRUCT`/`LIST` columns | Cast to a supported type at export (see below) |

### Time travel is different syntax

DuckDB's `AT (VERSION => …)` is rejected by its native storage. In h5i-db, time
travel is first-class — read any past version with the `h5i()` table function or
the CLI/`--version` flag:

```sql
SELECT count(*) FROM h5i('trades', 1);   -- the table as of version 1
```

See [Reading tables & time travel](sql.html) for `as_of` timestamp resolution.

---

## Workflow differences

- **Mutations are previewable, not free-form DML.** There is no ad-hoc
  `UPDATE`/`DELETE` mid-query. Range deletes and updates go through a
  **plan → apply** gate you can inspect (and policy can require approval for) —
  `plan_delete_range(...).apply()` in Python, the mutation verbs on the CLI. See
  [Core concepts](concepts.html).
- **Writes are commits.** Each `ingest` is an atomic, immutable version. There
  is no transaction you `COMMIT`/`ROLLBACK`; instead you `restore` to an earlier
  version if a load was wrong.
- **Sessions see a consistent snapshot.** `FROM trades` is snapshot-bound when
  the session opens, so every query in a session sees one consistent set of
  versions — use `h5i('trades', N)` to reach across versions explicitly.

---

## Type mapping notes

h5i-db's type set is intentionally lean (see the
[`create-table` types](cli.html#h5i-db-create-table)): the integer/float family,
`utf8`, `bool`, `timestamp_s/ms/us/ns` (UTC), `date32/date64`. If a DuckDB table
uses `HUGEINT`, `DECIMAL`, `TIME`, `INTERVAL`, or nested `STRUCT`/`LIST`/`MAP`
columns, cast them to a supported representation **during export**, e.g.:

```sql
D COPY (
    SELECT ts,
           symbol,
           CAST(notional AS DOUBLE) AS notional  -- DECIMAL -> float64
    FROM trades
  ) TO 'trades.parquet' (FORMAT PARQUET);
```

Timezone-aware DuckDB `TIMESTAMPTZ` values are stored UTC on the h5i-db side;
export as UTC and keep your time column in a single `timestamp_*` precision.

---

## What you gain

Once migrated, the same data carries capabilities DuckDB does not offer:
O(1) reads of any historical [version](concepts.html), previewable and
policy-gated mutations, crash-safe-by-construction commits, and time-series
operators (`vwap`, `ewma`, gapfill/resample, sort-free ASOF) that run on
declared-sorted, immutable segments. That structure is also why OHLCV+VWAP
rollups and ASOF joins are faster here than on DuckDB-over-Parquet — see the
[benchmark results](https://github.com/h5i-dev/h5i-db/blob/main/benchmarks/RESULTS.md).
