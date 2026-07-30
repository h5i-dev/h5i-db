# h5i-db from Python

```python
import h5i_db
db = h5i_db.Database("market.db")            # read_only=True for analysis-only
df = db.sql("SELECT * FROM h5i('trades', 42)").to_pandas()
plan = db.plan_delete_range("trades", t0, t1); plan.summary; plan.apply()
```

## What is in the package

The storage and query surface is the top level. Three subpackages hold the rest,
and each has its own reference because each is larger than this file.

| import | covers | reference |
|---|---|---|
| `h5i_db` | `Database`, expressions (`col lit when sql_expr time_bucket vwap wavg count_star`), `MutationPlan`, the `H5iError` tree | this file |
| `h5i_db.backtest` | configs, preflight, runs on forks, settlement, studies, the strategy pack | [backtest.md](backtest.md) |
| `h5i_db.quant` | factor panels, tearsheets, deflated Sharpe, PBO, purged CV, basket reports | [quant.md](quant.md) |
| `h5i_db.venues` | vendor archives into canonical tables, ledger replay | [data-onramp.md](data-onramp.md) |

One import trap. `backtest` and `venues` are bound when you `import h5i_db`, so
`h5i_db.backtest.execute(...)` works. `quant` is not: reach it with
`from h5i_db import quant` (after which `h5i_db.quant` also resolves). Writing
`import h5i_db` and then `h5i_db.quant.tearsheet(...)` raises `AttributeError`.

Installed wheels also expose two console scripts, `h5i-backtest` and
`h5i-venues`, which are the same entry points as `python -m h5i_db.backtest` and
`python -m h5i_db.venues`. Use the `python -m` form when working from a source
checkout, since the scripts only exist after an install.

Errors are one tree: `H5iError` with `.code`, `.retryable` and `.hint`, and the
subclasses `ConflictError NotFoundError InvalidInputError PolicyError
CorruptionError LimitError TimeoutError StorageError`. Retry on `.retryable`,
never on a `ConflictError` from `promote`, and stop on `CorruptionError`.

## DataFrame builder

`db.table(...)` builds a query instead of writing SQL. It is lazy: nothing
runs until `.collect()`. Every verb compiles to SQL run through `db.sql()`,
so pins, table functions and operators behave identically — `.sql()` shows
what it produced.

```python
from h5i_db import col, lit, sql_expr, time_bucket, vwap, when

(db.table("trades", version=42)              # -> h5i('trades', 42); unpinned -> bare name
   .filter(col("symbol").is_in(["AAPL"]))    # & | ~ for boolean ops, not and/or/not
   .with_columns(notional=col("price") * col("size"))
   .group_by(time_bucket("5m", col("ts")).alias("bar"))
   .agg(vwap(col("price"), col("size")).alias("v"))
   .sort("bar").limit(10)
   .collect())                               # or .to_arrow/.to_pandas/.to_polars/.sql/.explain/.schema
```

Verbs: `filter select with_columns group_by().agg() sort limit head unique
join join_asof pipe`. Rolling: `col(x).rolling_mean(20, order_by="ts",
partition_by="symbol")` (window is a row count or `'30m'`); also
`rolling_{sum,min,max,std,var,count,mad,skew,kurt,rank,idxmax,idxmin,corr,cov}`
and `.ewma(alpha, order_by, partition_by)`. Cross-sectional:
`.cs_rank(partition_by)`, `.cs_winsorize(lo, hi, partition_by)`,
`.cs_demean`, `.cs_zscore`. `sql_expr("…")` embeds raw SQL anywhere.

`join_asof` needs plain, **unpinned** tables on both sides — the `asof_join`
table function reads both at latest, so it refuses a pin rather than ignore
it. Use `db.sql()` with the `ASOF JOIN` keyword form when you need a pin.

Gotchas the builder enforces rather than papering over: `with_columns` adds,
so overwriting needs `replace="name"`; `.over()` attaches to one aggregate,
so window each part of a compound expression separately; `/` between two
integer columns truncates, because expressions keep SQL semantics.

Full reference: `docs-src/api/dataframe.md`.

## Where to go next

Building a strategy or scoring one → [backtest.md](backtest.md) and
[quant.md](quant.md). Loading somebody else's market data →
[data-onramp.md](data-onramp.md). Running several hypotheses at once →
[forks.md](forks.md). Keeping the future out of a read →
[research-mode.md](research-mode.md).
