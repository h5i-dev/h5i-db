# h5i-db from Python

```python
import h5i_db
db = h5i_db.Database("market.db")            # read_only=True for analysis-only
df = db.sql("SELECT * FROM h5i('trades', 42)").to_pandas()
plan = db.plan_delete_range("trades", t0, t1); plan.summary; plan.apply()
```

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

Full reference: `docs-src/api/dataframe.md`.
