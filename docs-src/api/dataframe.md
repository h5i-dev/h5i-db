---
title: DataFrame builder
description: "Build queries as Python instead of SQL strings: db.table(), lazy verbs, expressions, rolling and cross-sectional operators, ASOF joins, and the SQL each one compiles to."
order: 1.5
---

# DataFrame builder

`db.table(...)` starts a **lazy** query you build up with methods instead of
writing a SQL string. Nothing runs until a terminal method like `.collect()`.

```python
from h5i_db import col

(db.table("trades", as_of="2026-07-01T00:00:00Z")
   .filter(col("symbol").is_in(["AAPL", "MSFT"]))
   .group_by("symbol")
   .agg(col("price").mean().alias("px"))
   .collect())
```

The builder is a **compiler, not a second engine**. Every verb lowers to SQL
run through [`Database.sql()`](database.html#databasesql), so a built query
sees the same session, the same [table functions](../manual/sql.html) and the
same version pins as the equivalent string. `.sql()` shows exactly what it
produced:

```sql
SELECT "symbol", avg("price") AS "px"
FROM h5i('trades', '2026-07-01T00:00:00Z')
WHERE "symbol" IN ('AAPL', 'MSFT')
GROUP BY "symbol"
```

!!! note "When to reach for it"
    For a query you write once, SQL is usually shorter and clearer. The
    builder pays off when queries are **generated** — a factor library
    sweeping windows and columns in a loop — where f-string SQL means quoting
    bugs, and when you want a partially-built pipeline you can reuse and
    extend. Both surfaces stay first-class; `.sql()` is the door between them.

## Reading a table

### `Database.table`

```python
table(name, version=None, as_of=None, snapshot=None) -> LazyFrame
```

Start a query. Pass at most one read point; passing two raises
`InvalidInputError`.

| Call | Reads |
|---|---|
| `db.table("trades")` | The bare table name — snapshot-bound for the query, so two references to it inside one query always agree |
| `db.table("trades", version=42)` | `h5i('trades', 42)` |
| `db.table("trades", as_of="2026-07-01T00:00:00Z")` | `h5i('trades', '…')` — latest version committed at or before that instant |
| `db.table("trades", snapshot="eod-2026-07-18")` | `h5i('trades', 'eod-…')` |

Because pins lower to `h5i()`, a pinned builder query is bound at the source
exactly like hand-written SQL — including under a research-mode pin.

## Verbs

Every verb returns a **new** frame, so a partial pipeline is safe to reuse as
a base for several queries.

| Verb | Does |
|---|---|
| `.filter(*preds)` | Keep matching rows; several predicates are ANDed |
| `.select(*exprs, **named)` | Replace the projection |
| `.with_columns(*exprs, replace=None, **named)` | Add columns, keeping the rest |
| `.group_by(*keys).agg(...)` | Aggregate; keys are projected alongside |
| `.group_by(*keys).count()` | Rows per group |
| `.sort(by, descending=False)` | Order the result |
| `.limit(n, offset=0)` / `.head(n)` | Take rows (still lazy) |
| `.unique()` | `SELECT DISTINCT` |
| `.join(other, on=…, how=…)` | Join two frames |
| `.join_asof(other, on=…, by=…)` | ASOF join via `asof_join` |
| `.pipe(fn, *args)` | `fn(frame, *args)` — for reusable helpers |

Strings are column names wherever an expression is accepted, so
`.group_by("symbol")` and `.group_by(col("symbol"))` are the same thing.
Keyword arguments name the result: `with_columns(ret=col("close") - 1)`.

`with_columns` **adds**. Naming a column that already exists is an error,
because `SELECT *` would then carry two of it. Say so explicitly to
overwrite one:

```python
db.table("trades").with_columns(price=col("price") * 2, replace="price")
```

```sql
SELECT * EXCEPT ("price"), "price" * 2 AS "price"
FROM "trades"
```

The builder never reads the schema — that is what keeps it lazy — so a
`replace` name that does not exist is caught by the engine, not at build
time.

### Terminal methods

| Method | Returns |
|---|---|
| `.collect(memory_limit=, timeout=, max_rows=)` | [`QueryResult`](results-and-plans.html) |
| `.to_arrow()` / `.to_pandas()` / `.to_polars()` | The frame, converted |
| `.sql()` | The generated SQL, as a string |
| `.explain(analyze=False)` | `EXPLAIN` / `EXPLAIN ANALYZE` of it |
| `.schema()` | Result schema via a `LIMIT 0` run — no data read |

`.collect()` takes the same guardrails as `db.sql()`: `max_rows` raises
`LimitError` as soon as the result exceeds it, and `timeout` raises
`TimeoutError`.

## Expressions

`col(name)` references a column, `lit(value)` a constant. The name is one
identifier and is never split on `.`, so a column really named `a.b` works;
pass `col("price", relation="l")` to qualify a side of a join.

```python
from h5i_db import col, lit, when

col("price") * col("size")              # arithmetic
(col("price") > 100) & (col("size") < 5)   # & | ~ , not and/or/not
col("symbol").is_in(["AAPL", "MSFT"])
col("px").is_null()
col("ts").between(t0, t1)
col("symbol").like("A%")
col("size").cast("DOUBLE")
when(col("price") > 100).then(lit(1)).otherwise(lit(0))
```

!!! warning "Use `&`, `|`, `~`"
    Python cannot overload `and` / `or` / `not`, so an Expr raises
    `TypeError` if used as a truth value. Mind the precedence too: `&` binds
    tighter than `>`, so the comparisons need parentheses.

!!! note "Operators mean what SQL means"
    Expressions compile to SQL and keep SQL's semantics, not Python's. Most
    visibly, `/` between two integer columns is **integer** division —
    `col("size") / 4` truncates. Cast for true division:
    `col("size").cast("DOUBLE") / 4`. The rule is deliberate: the same
    expression must not mean one thing here and another in `db.sql()`.

Identifiers and literals are quoted at a single site, and identifiers are
always quoted so case survives (`col("Symbol")` finds a field named
`Symbol`, which bare SQL would fold to lowercase). A value containing
`'; DROP TABLE trades; --` is a string, never syntax.

Aggregates are methods: `.sum()`, `.mean()`, `.min()`, `.max()`, `.count()`,
`.n_unique()`, `.std()`, `.var()`, `.median()`, `.quantile(q)`,
`.first(order_by=)`, `.last(order_by=)`. So are scalars: `.abs()`, `.log()`,
`.log10()`, `.exp()`, `.sqrt()`, `.sign()`, `.round(n)`, `.floor()`,
`.ceil()`, `.coalesce(...)`, `.greatest(...)`, `.least(...)`. Plus
`count_star()`, `vwap(price, size)`, `wavg(weight, value)` and
`time_bucket(interval, ts)` as functions.

A `when(...).then(...)` chain is already a complete expression — a `CASE`
with no `ELSE` yields NULL — so it can be aliased without `.otherwise()`.

The idiomatic OHLCV query, built:

```python
from h5i_db import col, time_bucket, vwap

(db.table("trades")
   .group_by(time_bucket("5m", col("ts")).alias("bar"), "symbol")
   .agg(col("price").first("ts").alias("open"),
        col("price").max().alias("high"),
        col("price").min().alias("low"),
        col("price").last("ts").alias("close"),
        col("size").sum().alias("volume"),
        vwap(col("price"), col("size")).alias("vwap"))
   .sort("bar"))
```

```sql
SELECT time_bucket('5m', "ts") AS "bar", "symbol",
       first_value("price" ORDER BY "ts") AS "open", max("price") AS "high",
       min("price") AS "low", last_value("price" ORDER BY "ts") AS "close",
       sum("size") AS "volume", vwap("price", "size") AS "vwap"
FROM "trades"
GROUP BY "bar", "symbol"
ORDER BY "bar"
```

## Rolling and cross-sectional operators

Rolling methods take a `window` and an `order_by`, and optionally a
`partition_by`. The window is either a row count or a duration string
(`'30s'`, `'5m'`, `'1.5h'`, `'1d'`, `'1w'`, `'1mo'`, `'1y'`).

```python
col("close").rolling_mean(20, order_by="ts", partition_by="symbol")
```

```sql
avg("close") OVER (PARTITION BY "symbol" ORDER BY "ts"
                   ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
```

Unlike the [`rolling_avg` SQL sugar](../manual/sql.html), these carry a
`PARTITION BY`, so they do not mix symbols on a multi-symbol table.

| Method | Lowers to |
|---|---|
| `.rolling_mean` `.rolling_sum` `.rolling_min` `.rolling_max` | `avg` `sum` `min` `max` |
| `.rolling_std` `.rolling_var` `.rolling_count` | `stddev` `var_samp` `count` |
| `.rolling_mad` `.rolling_skew` `.rolling_kurt` | `mad` `skew` `kurt` |
| `.rolling_rank` | `ts_rank` — percentile of the current value in the window |
| `.rolling_idxmax` `.rolling_idxmin` | `idxmax` `idxmin` — 1-based position |
| `.rolling_corr(other, …)` `.rolling_cov(other, …)` | `ts_corr` `ts_cov` |
| `.ewma(alpha, order_by, partition_by)` | `ewma` |

Cross-sectional methods rank a value against its peers *at the same instant*,
so they take the bucket to compare within:

| Method | Lowers to |
|---|---|
| `.cs_rank(partition_by)` | `cs_rank(x) OVER (PARTITION BY …)` |
| `.cs_winsorize(lower, upper, partition_by)` | `cs_winsorize(x, lo, hi) OVER (…)` |
| `.cs_demean(partition_by)` | `x - avg(x) OVER (…)` — plain SQL |
| `.cs_zscore(partition_by)` | `(x - avg(x) OVER (…)) / stddev(x) OVER (…)` |

For a raw window frame, `.over(partition_by=, order_by=, rows=, duration=)`
applies to a single aggregate, where `rows` is a trailing count or a
`(preceding, following)` pair with `None` for unbounded. SQL attaches `OVER`
to one function call, so window each part of a compound expression
separately — `col("a").sum().over(...) / count_star().over(...)`, not
`(col("a").sum() / count_star()).over(...)`, which is rejected.

## Joins

`.join()` renders both sides as subqueries aliased `l` and `r`. Those aliases
are part of the contract: reach a specific side with
`col("price", relation="l")`, or an arbitrary condition with
`predicate=sql_expr('l."a" > r."b"')`.

`how=` is `inner` (default), `left`, `right`, `full`, `cross`, `semi` or
`anti`. Keys come from `on=` (same name both sides) or `left_on=`/`right_on=`.

!!! warning "Column names are not deduplicated"
    `SELECT *` over a join of two tables sharing a column name yields both
    copies. Project explicitly to avoid the ambiguity.

Comparing one table at two pinned versions — the "same query across N
versions" pattern:

```python
def mean_px(version):
    return (db.table("trades", version=version)
              .group_by("symbol")
              .agg(col("price").mean().alias("px")))

drift = (mean_px(1).join(mean_px(2), on="symbol")
         .select(col("symbol", relation="l").alias("symbol"),
                 (col("px", relation="r") - col("px", relation="l")).alias("drift")))
```

```sql
SELECT "l"."symbol" AS "symbol", "r"."px" - "l"."px" AS "drift"
FROM (
  SELECT "symbol", avg("price") AS "px"
  FROM h5i('trades', 1)
  GROUP BY "symbol"
) AS "l"
INNER JOIN (
  SELECT "symbol", avg("price") AS "px"
  FROM h5i('trades', 2)
  GROUP BY "symbol"
) AS "r"
  ON "l"."symbol" = "r"."symbol"
```

### `join_asof`

```python
join_asof(other, on=None, by=None, direction="backward", tolerance=None,
          left_on=None, right_on=None) -> LazyFrame
```

For each left row, take the most recent right row at or before it
(`"backward"`) or the first at or after it (`"forward"`). The result is LEFT
and 1:1 with the left side. `tolerance` is an integer in the time column's
raw units — for a `timestamp[us]` column, `5000000` is five seconds.

```python
db.table("trades").join_asof(db.table("quotes"), on="ts", by="symbol",
                             tolerance=5_000_000)
```

```sql
SELECT *
FROM asof_join('trades', 'quotes', 'ts', 'ts', 'symbol', 'backward', 5000000)
```

!!! warning "Both sides must be plain, unpinned tables"
    The `asof_join` table function takes table names and reads both at
    **latest**. So `join_asof` refuses a side that already has verbs applied,
    and refuses a pinned side outright rather than silently ignoring the pin.
    Filter *after* the join, or for a pinned ASOF use `db.sql()` with the
    [`ASOF JOIN` keyword form](../manual/sql.html#asof_join) over
    session-bound names.

## How pipelines become SQL

Most pipelines compile to one flat `SELECT`. A stage that reads a column an
earlier stage *computed* gets its own level, because SQL resolves `WHERE` and
`SELECT` against the `FROM`, not against sibling projections:

```python
(db.table("bars")
   .with_columns(ret=col("close") / col("open") - 1)
   .filter(col("ret") > 0)          # reads a computed column -> subquery
   .sort("ret", descending=True)
   .limit(10))
```

```sql
SELECT *
FROM (
  SELECT *, "close" / "open" - 1 AS "ret"
  FROM "bars"
) AS "_s1"
WHERE "ret" > 0
ORDER BY "ret" DESC
LIMIT 10
```

Filtering a *base* column instead stays flat, and independent `with_columns`
calls coalesce into one projection. Aggregation, `LIMIT` and `DISTINCT`
always close a level, since whatever follows acts on their output. So does a
projection holding a **window function or aggregate**: SQL evaluates `WHERE`
before the select list, so filtering in the same level would recompute the
window over only the surviving rows.

Because levels close, a stage can only see what the stage before it emitted.
Filtering or sorting by a column an earlier `.select()` dropped is an error
naming that column, as it would be for a DataFrame.

The generated SQL is deterministic, so it is safe to snapshot-test or diff.

## Escape hatch

`sql_expr()` embeds a raw fragment anywhere an expression is accepted. Full
SQL coverage through builder verbs is deliberately not a goal — reach for
this rather than waiting for a method:

```python
from h5i_db import sql_expr

(db.table("trades")
   .group_by("symbol")
   .agg(sql_expr("approx_percentile_cont(price, 0.99)").alias("p99")))
```

Its text is inserted verbatim, so it is the one place quoting is yours to get
right; never build it from untrusted input. Because its column references are
opaque to the builder, it conservatively forces a subquery when the current
stage defines any computed name.

When a pipeline outgrows the builder, `.sql()` gives you the query to paste
into `db.sql()` and keep going from there.
