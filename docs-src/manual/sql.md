---
title: SQL reference
description: "h5i-db's SQL surface beyond stock DataFusion: time travel with h5i(), ASOF joins, gapfill/resample, tail, time_bucket, vwap, ewma, and rolling window sugar."
order: 5
---

# SQL reference

h5i-db speaks full DataFusion SQL — joins, CTEs, window functions,
`date_trunc`, `stddev`, `corr`, `approx_percentile_cont`, `INTERVAL`
arithmetic — plus the time-series extensions documented here. String literals
are single-quoted; identifiers are case-insensitive.

The idiomatic OHLCV query exercises most of the library at once:

```sql
SELECT time_bucket('5m', ts) AS bar, symbol,
       first_value(price ORDER BY ts) AS open,
       max(price)                     AS high,
       min(price)                     AS low,
       last_value(price ORDER BY ts)  AS close,
       sum(size)                      AS volume,
       vwap(price, size)              AS vwap
FROM trades
GROUP BY bar, symbol
ORDER BY bar;
```

!!! note "Raw time units"
    Numeric time arguments — `gapfill` step, ASOF tolerance — are **raw
    integers in the time column's unit**. For the common `timestamp[us]`
    column: `5000000` is 5 seconds, `60000000` is one minute.

## Reading tables & time travel

### Plain names vs `h5i()`

| Form | Resolves |
|---|---|
| `FROM trades` | Snapshot-bound when the session opens — every query in a session sees one consistent set of versions |
| `FROM h5i('trades')` | Latest version, re-resolved at each query |
| `FROM h5i('trades', 42)` | Exact version number |
| `FROM h5i('trades', '2026-07-01T00:00:00Z')` | As-of: latest version whose **commit time** ≤ the RFC3339 timestamp |
| `FROM h5i('trades', 'eod-2026-07-18')` | Version pinned by a named snapshot |

`h5i()` is a standard table function — no special grammar — so it composes
with everything:

```sql
-- diff two versions
SELECT count(*) FROM h5i('trades', 2) b
JOIN h5i('trades', 1) a ON a.ts = b.ts;
```

Any string second argument that does not parse as RFC3339 is treated as a
snapshot name — so avoid naming snapshots like timestamps.

## Table functions

### `asof_join`

```sql
asof_join('left', 'right', 'left_on', 'right_on'
          [, 'by_cols' [, 'backward'|'forward' [, tolerance]]])
```

For each left row, find the most recent right row at or before it
(`'backward'`, the default) or the first at or after it (`'forward'`),
optionally matching equality keys — the canonical trades-vs-quotes join:

```sql
SELECT * FROM asof_join('trades', 'quotes', 'ts', 'ts', 'symbol');
SELECT * FROM asof_join('trades', 'quotes', 'ts', 'ts', 'symbol', 'backward', 5000000);
```

- `by_cols` is comma-separated; each entry is `'col'` (same name both sides)
  or `'lcol=rcol'`.
- `tolerance` is an integer in raw time units: the maximum allowed
  `|left.ts − right.ts|`.
- The join is always **LEFT and 1:1 with the left side** — unmatched left rows
  keep NULLs. A useful invariant to assert: `len(output) == len(left)`.
- Right-side columns that collide with left names get a `_right` suffix.
- The right side is buffered in memory (charged to the query memory budget);
  the left side streams. Left-only filters and `LIMIT` push down into the
  left scan.
- Both tables are read at **latest**; to ASOF-join historical versions, use
  the keyword form over session-bound names, or materialize first.

The keyword syntax is also supported (bare table names only, no aliases):

```sql
SELECT * FROM trades ASOF JOIN quotes
  MATCH_CONDITION (trades.ts >= quotes.ts)     -- >= backward, <= forward
  ON trades.symbol = quotes.symbol;
```

### `gapfill` / `resample`

```sql
gapfill('table', 'time_column', step [, 'null'|'locf'|'interpolate'])
```

Turn an irregular series into a regular grid from the first to the last
observed timestamp, stepping by `step` raw time units. `resample(...)` is an
exact alias.

```sql
SELECT ts, price FROM gapfill('bars_1m', 'ts', 60000000, 'locf') ORDER BY ts;
```

Fill modes for synthesized instants:

| Mode | Behavior |
|---|---|
| `'null'` (default) | Non-time columns are NULL |
| `'locf'` | Last observation carried forward (NULL before the first) |
| `'interpolate'` | Linear interpolation for numeric columns (ints rounded); non-numeric falls back to previous value |

!!! warning "gapfill is per-table, not per-key"
    There is **no per-key grouping**: on a multi-symbol table, `locf` carries
    whichever symbol last ticked. Gapfill single-instrument tables, or filter
    to one key first. Also note: observations that don't land exactly on the
    grid are dropped from the output; duplicate timestamps collapse to the
    last row; at most 1,000,000 rows are generated (`limit_exceeded` beyond).

### `tail`

```sql
tail('table' [, after_version [, poll_ms]])
```

Stream rows appended after a version — a message-log view of an append-only
table. With no version it starts after the current head (future appends
only). `poll_ms` defaults to 250 (minimum 10).

```sql
SELECT ts, price FROM tail('trades', 812) LIMIT 500;
```

- The result is **unbounded** — always apply `LIMIT` (or cancel the query).
  `tail` blocks until `LIMIT` rows arrive; pass a query timeout as a backstop.
- Requires a **pure-append version chain** after `after_version`; any
  delete/replace/restore/write in the range errors with a hint.
- Size `LIMIT` from `versions` row deltas to fetch "exactly what's new since
  version N" — no timestamp-cursor guesswork.

## Scalar, aggregate & window functions

### `time_bucket`

```sql
time_bucket(interval, ts)
time_bucket(interval, ts, origin_or_timezone)
time_bucket(interval, ts, origin, timezone)
```

Floor timestamps into fixed buckets — DuckDB/TimescaleDB semantics. The
interval is a literal: an SQL `INTERVAL` or a string like `'30s'`, `'5m'`,
`'1.5h'`, `'1d'`, `'1w'`, `'1mo'`, `'1y'`. Fixed widths align to the origin
`2000-01-03T00:00:00Z` (a Monday, so weeks start Monday); month/year widths
use calendar bucketing.

```sql
SELECT time_bucket('5m', ts) AS bar, … GROUP BY bar;
SELECT time_bucket('1d', ts, 'America/New_York') AS session_day, …   -- local-time days
```

The third argument is a timezone when it parses as an IANA name (or contains
`/`), otherwise an origin timestamp; use the 4-argument form to pass both.
With a timezone, bucketing happens in local wall time and handles DST
(ambiguous → earliest, gap → first valid instant). Out-of-range inputs yield
NULL buckets rather than errors.

### `vwap` / `wavg`

```sql
vwap(price, size)     -- value first, weight second
wavg(size, price)     -- kdb argument order: weight first
```

Weighted mean as a streaming, mergeable aggregate — the same computation with
two argument conventions. Returns `Float64`; NULL when the group is empty or
the weight sum is zero; rows with a NULL in either argument are skipped.
Supports retraction, so sliding-window use is O(n):

```sql
SELECT vwap(price, size) OVER (ORDER BY ts ROWS BETWEEN 99 PRECEDING AND CURRENT ROW)
FROM trades;
```

### `ewma`

```sql
ewma(value, alpha) OVER (PARTITION BY … ORDER BY ts)
```

Exponentially weighted moving average, one ordered pass per partition:
`y₀ = x₀; yᵢ = α·xᵢ + (1−α)·yᵢ₋₁`. `alpha` must be a constant in `[0, 1]`.
NULL inputs carry the previous smoothed value forward. Matches
`pandas.ewm(alpha=…, adjust=False)`.

```sql
SELECT ewma(price, 0.06) OVER (PARTITION BY symbol ORDER BY ts) AS px_smooth
FROM trades;
```

### `rolling_avg` / `rolling_sum` / `rolling_min` / `rolling_max`

```sql
rolling_avg(value, order_by, rows)
```

Convenience sugar, expanded before parsing into the standard window frame
`AVG(value) OVER (ORDER BY order_by ROWS BETWEEN rows−1 PRECEDING AND
CURRENT ROW)`. `rows` must be an integer literal in 1…1,000,000.

!!! warning "Not partitioned"
    The sugar has **no `PARTITION BY`** — it is a trailing n-row window in
    global order and will mix symbols on a multi-symbol table. Use it on
    single-key subsets, or write the explicit window:
    `AVG(x) OVER (PARTITION BY symbol ORDER BY ts ROWS BETWEEN n−1 PRECEDING
    AND CURRENT ROW)`. It also cannot take its own `OVER` clause.

### `first_value` / `last_value`

Stock DataFusion, but worth knowing the idiom: `last_value(x ORDER BY ts)`
inside a `GROUP BY` is how you take "closing" values without a self-join —
see the OHLCV query at the top of this page.

## Sessions, pruning & performance

- Narrow time-range predicates prune segments via manifest statistics before
  any I/O — verify with `h5i-db query … --stats` or the UI's SQL scratchpad.
- Select only the columns you need; Parquet projection pushdown is
  column-granular.
- Memory budgets (`--memory-limit-mb` / `sql(memory_limit=…)`) enable disk
  spilling instead of OOM; `--max-rows` and timeouts turn runaway queries
  into clean, typed errors.
- `information_schema` is available for introspection
  (`SELECT * FROM information_schema.tables`).

For a guided tour with real data, see the cookbook:
[A SQL tour for quants](../cookbook/00_fundamentals/04_sql_tour_for_quants.html)
and [Performance tuning](../cookbook/03_risk_and_production/10_performance_tuning.html).
