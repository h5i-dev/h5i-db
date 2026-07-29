---
title: Quant workflows
description: "Factor evaluation and performance tearsheets on pinned data: h5i_db.quant, its alphalens/empyrical parity, and the divergences that are deliberate."
order: 7
---

# Quant workflows

`h5i_db.quant` runs the standard quant research loop against the engine:
factor evaluation in the shape of `alphalens`, performance statistics in the
shape of `pyfolio` (arithmetic from `empyrical`), and reports that record
which version of the data produced them.

Two things make it different from the pandas libraries it mirrors. Every
computation runs against a **pinned read**, so a number can be reproduced
later or refused as unreproducible. And every panel-scale computation
compiles to SQL, so the data stays in the engine and only the aggregate
comes back.

```python
from h5i_db import quant

panel = quant.build_panel(
    db, "signals", "prices",
    periods=(1, 5, 10),      # forward-return horizons, in bars
    quantiles=5,
    snapshot="2024-q1",      # the pin
)
panel.ic().to_pandas()             # per-date rank IC, one column per horizon
panel.quantile_returns()           # mean forward return per bucket
quant.factor_report(panel, path="factor.html")
```

## Inputs

Both sources are long format and may be a table name or a `LazyFrame`:

| Source | Columns |
|---|---|
| factor | `ts`, `asset`, `factor`, optionally a group column |
| prices | `ts`, `asset`, `price` |

Column names are arguments (`ts=`, `asset=`, `factor_column=`,
`price_column=`); pass a `LazyFrame` when the shape needs more than renaming.

## Pinning and the embargo

`version=`, `as_of=` and `snapshot=` are the storage read point, and
`event_time_cutoff=` is the decision-time embargo: every source read is
restricted to `ts <= cutoff`, so a forward return that would need a price
from after the cutoff is dropped rather than computed. The result is
identical to never having had the later data, which is asserted in the test
suite.

**Versions are per table.** `version=2` means "version 2 of every source",
which is only meaningful when the sources really do share a lineage. To pin
several tables to one instant use `snapshot=` or `as_of=`; to pin them
independently pass a mapping:

```python
quant.build_panel(db, "signals", "prices",
                  version={"signals": 2, "prices": 1})
```

## Reproducibility

`deterministic=True` (the default) runs every query single-partition.
Floating-point addition is not associative, so a parallel plan may combine
partial aggregates in a different order on each run and move a result by a
few units in the last place. That is invisible in a chart and fatal to a
number you intend to cite, so the default trades intra-query parallelism for
bit-stability. Parallelism across runs (a fork sweep) is unaffected.

`quant.verify(subject, rerun=...)` re-executes a computation and checks the
provenance digest still matches. An unpinned computation is *refused* rather
than passed: reproducing it is not something its header can promise.

## Divergences from the reference implementations

Every difference from `alphalens` / `empyrical` is listed here and pinned by
a test. A divergence that is not in this table is a bug.

| # | Where | h5i-db | Reference | Why |
|---|---|---|---|---|
| D1 | Quantile assignment | `ntile(q)`, equal count, remainder to the earliest buckets | `pd.qcut`, quantile value edges | Identical whenever the cross-section divides evenly by `q`. When it does not, both give equal-count buckets and differ only in *which* bucket absorbs the remainder. Bucket sizes never differ by more than one, and quantiles stay monotone in the factor: both asserted. |
| D2 | Forward-return labels | `fwd_1`, `fwd_5` — bar counts | `'1D'`, `'5D'` — pandas frequency strings | Horizons are bar counts here, so no trading calendar has to be inferred to name a column. A 5-bar horizon is 5 bars whether the bars are days or minutes. |
| D3 | Calendar inference | none; horizons are positional | infers a `CustomBusinessDay` calendar from the observed index | Calendar inference is what makes alphalens fragile on irregular data. Where a real calendar is needed, resample first. |
| D4 | `alpha_beta` annualization | `annualization / period`, with `annualization` an argument (252 by default) | `pd.Timedelta('252Days') / pd.Timedelta(period)` | Follows from D2. Pass `annualization=` for non-daily bars. |
| D5 | Percentiles (`tail_ratio`) | exact rank interpolation | `np.percentile`, linear | Matches numpy exactly. DataFusion's own `percentile_cont` is approximate and disagrees around the eighth significant digit, which is enough to break parity. |
| D6 | Rolling statistics | null until the window is full | pandas `min_periods=window` | Same behaviour; noted because a SQL frame's default is the opposite, and an unguarded frame reports a "63-bar Sharpe" from two observations. |
| D7 | Omega with no winning bars | `0.0` numerator | `sum([]) == 0` | Same value; reached by coalescing a SQL `NULL` rather than by summing an empty list. |
| D8 | Event-study family | not implemented | `average_cumulative_return_by_quantile` etc. | Needs a windowed range join around each signal date, which is a tracked engine gap. The cookbook covers the manual pattern meanwhile. |

## Performance statistics

```python
series = quant.returns(db, "strategy_returns", annualization=quant.DAILY)
series.stats()                      # the headline set, as one SQL row
series.drawdown_table(top=10)       # worst non-overlapping episodes
series.rolling_sharpe(63)
quant.tearsheet(series, path="tearsheet.html")
```

`stats()` returns annual return and volatility, cumulative return, Sharpe,
Sortino, downside risk, max drawdown, Calmar, Omega, stability, tail ratio,
skew, kurtosis and daily VaR, plus alpha and beta when a `benchmark=` series
is passed. Values match `empyrical` to 1e-9; skew and kurtosis follow
`scipy` (biased), which is what pyfolio reports.

`annualization` is bars per year: `quant.DAILY` (252), `quant.WEEKLY`,
`quant.MONTHLY`, `quant.YEARLY`, or any number — `24 * 365` for hourly
crypto bars.

## Sweeps

A sweep runs a parameter grid with one fork per combination. Trials cannot
contaminate each other or the base data, and they compare in one query
because forks share their base's segments.

```python
def trial(fork_db, params):
    panel = quant.build_panel(fork_db, "signals", "prices",
                              quantiles=params["quantiles"], periods=(1,))
    row = panel.ic_decay().to_arrow().to_pylist()[0]
    return {"mean_ic": row["mean_ic"], "icir": row["icir"]}

result = quant.sweep(db, {"quantiles": [3, 5, 10]}, trial)
result.compare().to_pandas()    # every trial, one cross-fork query
result.best("icir")
result.drop()                   # forks and their results go together
```

## Reports

`quant.factor_report(panel)` and `quant.tearsheet(series)` render one
self-contained HTML file: inline CSS and JS, data embedded as JSON, no
network access at view time. Section one is always the provenance header, so
a reader sees the data version before the numbers. `quant.report_payload()`
returns the same content as a dict, which is what agents and the CLI should
read instead of scraping HTML.
