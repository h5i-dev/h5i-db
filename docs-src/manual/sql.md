---
title: SQL reference
description: "h5i-db's SQL beyond stock DataFusion: time travel with h5i(), ASOF joins, gapfill and resample, tail, time_bucket, vwap, ewma, and rolling sugar."
order: 5
---

# SQL reference

h5i-db speaks full DataFusion SQL (joins, CTEs, window functions,
`date_trunc`, `stddev`, `corr`, `approx_percentile_cont`, `INTERVAL`
arithmetic), plus the time-series extensions documented here. String literals
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
    Numeric time arguments (the `gapfill` step, the ASOF tolerance) are **raw
    integers in the time column's unit**. For the common `timestamp[us]`
    column: `5000000` is 5 seconds, `60000000` is one minute.

## Reading tables & time travel

### Plain names vs `h5i()`

| Form | Resolves |
|---|---|
| `FROM trades` | Snapshot-bound when the session opens; every query in a session sees one consistent set of versions |
| `FROM h5i('trades')` | Latest version, re-resolved at each query |
| `FROM h5i('trades', 42)` | Exact version number |
| `FROM h5i('trades', '2026-07-01T00:00:00Z')` | As-of: latest version whose **commit time** ≤ the RFC3339 timestamp |
| `FROM h5i('trades', 'eod-2026-07-18')` | Version pinned by a named snapshot |

`h5i()` is a standard table function with no special grammar, so it composes
with everything:

```sql
-- diff two versions
SELECT count(*) FROM h5i('trades', 2) b
JOIN h5i('trades', 1) a ON a.ts = b.ts;
```

Any string second argument that does not parse as RFC3339 is treated as a
snapshot name, so avoid naming snapshots like timestamps.

## Table functions

### `asof_join`

```sql
asof_join('left', 'right', 'left_on', 'right_on'
          [, 'by_cols' [, 'backward'|'forward' [, tolerance]]])
```

For each left row, find the most recent right row at or before it
(`'backward'`, the default) or the first at or after it (`'forward'`),
optionally matching equality keys. This is the canonical trades-vs-quotes join:

```sql
SELECT * FROM asof_join('trades', 'quotes', 'ts', 'ts', 'symbol');
SELECT * FROM asof_join('trades', 'quotes', 'ts', 'ts', 'symbol', 'backward', 5000000);
```

- `by_cols` is comma-separated; each entry is `'col'` (same name both sides)
  or `'lcol=rcol'`.
- `tolerance` is an integer in raw time units: the maximum allowed
  `|left.ts − right.ts|`.
- The join is always **LEFT and 1:1 with the left side**: unmatched left rows
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

### `forks`

```sql
forks('table' [, 'fork-a,fork-b'])
```

Read a table from **every fork at once**, each row labelled with a `__fork`
column (`''` for the base). This is the cross-fork aggregation step of a
branch-per-hypothesis sweep: N agents write N forks, one query compares
them.

```sql
SELECT __fork, count(*), avg(price) FROM forks('trades') GROUP BY __fork;
SELECT __fork, vwap(price, size) FROM forks('trades', 'exp-a,exp-b') GROUP BY __fork;
```

- Segments shared between forks are opened **once** no matter how many forks
  reference them; you pay for distinct data, not for fork count.
- All included forks must agree on the table's schema; a mismatch errors and
  names the fork rather than unioning loosely.
- The second argument narrows to a comma-separated fork list; omit it for
  base plus every fork. See [Forks](concepts.html#forks) for the model.

### `tail`

```sql
tail('table' [, after_version [, poll_ms]])
```

Stream rows appended after a version: a message-log view of an append-only
table. With no version it starts after the current head (future appends
only). `poll_ms` defaults to 250 (minimum 10).

```sql
SELECT ts, price FROM tail('trades', 812) LIMIT 500;
```

- The result is **unbounded**, so always apply `LIMIT` (or cancel the query).
  `tail` blocks until `LIMIT` rows arrive; pass a query timeout as a backstop.
- Requires a **pure-append version chain** after `after_version`; any
  delete/replace/restore/write in the range errors with a hint.
- Size `LIMIT` from `versions` row deltas to fetch "exactly what's new since
  version N", with no timestamp-cursor guesswork.

### `latest_on`

```sql
latest_on('table', 'by_column')
```

One row per group: the most recent row per symbol, per instrument, per
whatever `by_column` names. Both arguments are string literals, and the group
column must be string-like.

```sql
SELECT ts, symbol, price FROM latest_on('trades', 'symbol');
```

It precomputes each immutable segment's last row per group and caches that as
a checksummed sidecar, so an append-only table reuses every prior segment's
contribution and scans only what is new — segments × groups rather than rows.
The cache is a pure accelerator: a miss, a corrupt entry or a version
mismatch rebuilds from the segment and never changes the answer. Sidecar
writing follows the session's
[`--predicate-cache`](cli.html#h5i-db-query) setting, so a default query reads
caches without writing any.

## Scalar, aggregate & window functions

### `time_bucket`

```sql
time_bucket(interval, ts)
time_bucket(interval, ts, origin_or_timezone)
time_bucket(interval, ts, origin, timezone)
```

Floor timestamps into fixed buckets, following DuckDB/TimescaleDB semantics. The
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

Weighted mean as a streaming, mergeable aggregate; the two spellings are the
same computation with different argument order. Returns `Float64`; NULL when
the group is empty or the weight sum is zero; rows with a NULL in either
argument are skipped.
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
rolling_avg(value, order_by, rows, partition_by)
```

Convenience sugar, expanded before parsing into the standard window frame
`AVG(value) OVER (PARTITION BY partition_by ORDER BY order_by ROWS BETWEEN
rows−1 PRECEDING AND CURRENT ROW)`. `rows` must be an integer literal in
1…1,000,000. The `PARTITION BY` is emitted only when the fourth argument is
given.

!!! warning "The three-argument form is not partitioned"
    Without a fourth argument the window is a trailing n-row window in
    **global** order, so on a multi-symbol table it averages across symbols
    and returns a plausible, wrong number. Pass the partition column, use the
    sugar on single-key subsets, or write the window out in full. Either form
    still cannot take its own `OVER` clause.

### Rolling window functions

Eight window functions DataFusion does not have. Unlike the `rolling_avg`
sugar above these are real window functions, so they take their own `OVER`
clause and any frame you like:

| Function | Over the frame |
|---|---|
| `mad(x)` | Mean absolute deviation |
| `skew(x)` | Sample skewness |
| `kurt(x)` | Excess kurtosis |
| `ts_rank(x)` | The current row's rank within the frame, scaled to `(0, 1]` |
| `idxmax(x)` / `idxmin(x)` | 1-based position of the frame's maximum / minimum |
| `ts_corr(x, y)` / `ts_cov(x, y)` | Correlation and covariance of two columns |

```sql
SELECT ts, symbol,
       mad(price)             OVER w AS mad_5,
       ts_rank(price)         OVER w AS rank_5,
       ts_corr(price, size)   OVER w AS corr_5
FROM trades
WINDOW w AS (PARTITION BY symbol ORDER BY ts ROWS 4 PRECEDING);
```

The rest of the rolling family (`rolling_mean`, `rolling_std`,
`rolling_var`, `rolling_sum`, `rolling_min`, `rolling_max`, `rolling_count`)
is reachable through stock DataFusion aggregates in an `OVER` clause; the
[DataFrame builder](../api/dataframe.html#rolling-and-cross-sectional-operators)
names them uniformly and compiles to exactly that.

### Cross-sectional window functions

Two operators for the "against everything else at this instant" shape.
Partition by the timestamp, not by the asset:

| Function | Result |
|---|---|
| `cs_rank(x)` | Rank within the partition, scaled to `(0, 1]` |
| `cs_winsorize(x, lower_pct, upper_pct)` | `x` clipped to the partition's percentile bounds |

```sql
SELECT ts, symbol,
       cs_rank(signal)                    OVER (PARTITION BY ts) AS rank,
       cs_winsorize(signal, 0.05, 0.95)   OVER (PARTITION BY ts) AS clipped
FROM signals;
```

`cs_demean` and `cs_zscore` are plain SQL over the same partition
(`x - avg(x) OVER (PARTITION BY ts)`), and the DataFrame builder spells all
four the same way.

### `first_value` / `last_value`

Stock DataFusion, but the idiom is easy to miss: `last_value(x ORDER BY ts)`
inside a `GROUP BY` is how you take "closing" values without a self-join. See
the OHLCV query at the top of this page.

## Sessions, pruning & performance

- Narrow time-range predicates prune segments via manifest statistics before
  any I/O; verify with `h5i-db query … --stats` or the UI's SQL scratchpad.
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
