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

Both constructors take the same pins as `build_panel`. `quant.returns` reads
a returns series (one row per bar, simple decimal returns);
`quant.from_levels` reads an equity *level* series and differences it, which
is what a backtest's `bt_equity` table is. Alongside `stats()` the series
carries `equity_curve()`, `underwater()`, `drawdown_table(top=…)`,
`rolling_sharpe(w)`, `rolling_volatility(w)` and `rolling_beta(w, benchmark)`.

## Selection bias gets first-class statistics

A number found by searching is worth less than the same number found once, so
these are not optional footnotes.

```python
rets = db.sql("SELECT ret FROM strategy_returns ORDER BY ts").to_arrow()["ret"].to_pylist()

quant.deflated_sharpe(rets, trials=40)        # .sharpe .benchmark .probability
quant.minimum_track_record_length(rets)       # observations still needed

trials = [[r * (1 + i / 10) for r in rets] for i in range(8)]
quant.probability_of_backtest_overfitting(list(zip(*trials)))   # (observations, trials)
```

`deflated_sharpe` discounts a Sharpe by the size of the search that produced
it, and reports `probability`: the chance the true Sharpe beats the
benchmark. Below 0.95 the result is indistinguishable from the best of that
many coin flips. When the variance of the trials' Sharpes is unknown it
substitutes the returns' own sampling variance, which is conservative rather
than an assumption of zero; pass `trial_sharpe_variance=` when you have it.
`trials_source` records whether the trial count was declared or measured, so
a report cannot quietly pass off a guess as a count.

`minimum_track_record_length` returns `inf` when the observed Sharpe sits
below the deflated benchmark. That is not a bug: it means no amount of
further data makes *this* result significant, and the honest report is that
the search found nothing.

`probability_of_backtest_overfitting` takes a matrix of shape
`(observations, trials)` — one column per trial's returns over the same
period — and splits it into `partitions` (default 8). A PBO near 0.5 means
the in-sample winner carried no information.

Read the moments first. A hold-to-resolution book has an equity curve that is
flat and then jumps, so its returns are one outlier surrounded by noise, and
Sharpe assumes something much closer to normal. High skew and kurtosis mean
the Sharpe is the wrong summary, not that the strategy is bad.

## Purged cross-validation

```python
n = 90
list(quant.purged_kfold(n, folds=5, horizons=[10] * n, embargo=0.01))
list(quant.combinatorial_purged(n, groups=6, test_groups=2, horizons=[10] * n))
list(quant.walk_forward(n, train_size=40, test_size=10, horizons=[10] * n))
```

Each yields `Split(train, test, purged)` index arrays. `horizons[i]` is how
many observations forward observation *i*'s label depends on, so a label
needing the next ten bars cannot leak into its own training fold. Omitting
`horizons` says labels are instantaneous, which is rarely true and is never
assumed silently; `embargo` is an additional gap as a fraction of `n`.

`walk_forward` takes `step=` (defaults to `test_size`) and `expanding=True`
for a growing rather than rolling training window.

## Cost calibration

```python
samples = [
    quant.SlippageSample(direction=1, fill_price=100.0 + 0.02 * q,
                         reference_price=100.0, quantity=q, reference_size=500.0)
    for q in (10.0, 25.0, 50.0, 80.0, 120.0, 200.0)
]
fit = quant.fit_impact(samples, shape="sqrt")     # or "linear"
fit.predict(0.1), fit.is_usable, fit.r_squared
```

Calibrates a slippage model from realised fills instead of assuming a
constant. A backtest run writes `calibration_samples` ready for this;
`fit.is_usable` is the guard against fitting a curve to four fills.

## Scoring a forecast against the market's own

Event contracts quote a probability, so a strategy trading them is making a
competing forecast. This is the one comparison an equity curve cannot make,
and sizing cannot inflate it.

```python
strategy = [0.7, 0.4, 0.6, 0.2, 0.9]
market   = [0.6, 0.5, 0.5, 0.3, 0.8]
outcomes = [1.0, 0.0, 1.0, 0.0, 1.0]

adv = quant.brier_advantage(strategy, market, outcomes)
adv.advantage         # market_brier - strategy_brier; positive means you were closer
adv.skill_score       # as a fraction of the market's own Brier
adv.cumulative        # the path, for plotting

quant.brier_decomposition(strategy, outcomes)  # reliability - resolution + uncertainty
quant.reliability_curve(strategy, outcomes)    # bucketed, with the signed gap
```

The decomposition says *why* a score is what it is: sitting off the diagonal
(reliability) is a different failure from not separating outcomes at all
(resolution). Reliability is small in absolute terms — bucket gaps of five to
nine points square down to thousandths — so the *sign structure* of the
reliability curve is the tradeable finding, not the magnitude of that term.

## Comparing many runs at once

A sweep leaves one fork per trial, and opening twenty tearsheets is not
comparing them. `basket_report` assembles one document from stored tables
only, with no re-simulation:

```
quant.basket_report(db, {"th50": result_a, "th60": result_b},
                    path="basket.html",
                    panels=quant.PORTFOLIO_PANELS + ("equity", "price"),
                    snapshot="panel-v1")
```

(`result_a` and `result_b` are [backtest results](backtest.html#opening-a-run-again);
the [backtesting page](backtest.html#comparing-many-runs-at-once) has the
worked version.)

`quant.PORTFOLIO_PANELS` (`total_equity`, `total_drawdown`,
`total_rolling_sharpe`, `total_cash_equity`, `periodic_pnl`, `leaderboard`)
are safe at any basket size. Per-run panels draw one series each and are
dropped past `per_run_limit` with the reason recorded in `report.skipped`,
because silently thinning lines misrepresents the basket. `quant.PANELS` is
every panel name. The `price` panel puts fill markers on the book the fills
actually met, read at the same pin the runs used, and `brier_advantage` is
the one panel needing an input the report cannot derive — your strategy's own
probability — so it is passed in and skipped with a reason when absent.

`quant.basket_payload(...)` returns the same content as a `BasketReport`
object without rendering HTML.

## Restatement impact

```python
quant.restatement_impact(
    lambda pin: quant.build_panel(db, "signals", "prices", periods=(1,), **pin),
    db,
    before={"version": {"signals": 1, "prices": 1}},
    after={"version": {"signals": 2, "prices": 1}},
    metric=lambda panel: {"mean_ic": panel.mean_ic()},
)
```

Runs one computation at two read points and reports what a vendor's revision
moved — a question only a versioned store can answer. `build(pin)` receives
the pin kwargs (`version`, `as_of`, `snapshot`) and produces the computation;
`metric(built)` reduces it to a mapping of scalars, and the result carries
`before`, `after`, `delta` and `changed` per key, with `tolerance` (default
`1e-9`) deciding what counts as moved. Omitting `after` compares against the
unpinned read.

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

`quant.factor_report(panel)`, `quant.tearsheet(series)` and
`quant.backtest_report(result)` render one self-contained HTML file: inline
CSS and JS, data embedded as JSON, no network access at view time. Section
one is always the provenance header, so a reader sees the data version before
the numbers. `quant.report_payload()` (and the per-kind
`quant.backtest_payload` / `quant.basket_payload`) returns the same content
as a dict, which is what agents and scripts should read instead of scraping
HTML.

## From the shell

```bash
python -m h5i_db.quant factor --db market.db --factor signals --prices prices \
    --snapshot 2024-q1 --format html --out factor.html
python -m h5i_db.quant tearsheet --db market.db --returns strategy_returns --out run.html
python -m h5i_db.quant stats  --db market.db --returns strategy_returns --format json
python -m h5i_db.quant verify --db market.db --factor signals --prices prices
```

Every verb takes the same pin flags (`--version`, `--as-of`, `--snapshot`,
`--event-time-cutoff`) and `--format json|html|table`. `stats` prints the
headline set without rendering a document, and `verify` re-runs a factor
panel and checks it still produces its numbers.

## Provenance objects

Every computation carries a `Provenance` (`digest`, `warnings`) and the `Pin`
it ran under (`version`, `as_of`, `snapshot`, `event_time_cutoff`, and
`is_pinned`). `quant.verify(subject, rerun=…)` checks both halves: the
provenance digest must be unchanged *and* the recomputed values must match,
because a digest over the SQL cannot notice an engine that computes the same
query differently. An unpinned subject raises `VerificationError` (or, with
`strict=False`, is reported unverifiable) rather than passing: two runs
against "latest" agreeing proves only that nothing changed in the seconds
between them.

The objects these return are part of the API, not implementation detail:

| Type | Returned by | Carries |
|---|---|---|
| `FactorPanel` | `build_panel` | `ic`, `ic_decay`, `mean_ic`, `quantile_returns`, `spread`, `turnover`, `rank_autocorrelation`, `alpha_beta`, `weights`, `loss_report` |
| `ReturnSeries` | `returns`, `from_levels` | `stats`, `equity_curve`, `underwater`, `drawdown_table`, `rolling_sharpe`, `rolling_volatility`, `rolling_beta` |
| `SweepResult` | `sweep` | `compare`, `best`, `to_pandas`, `drop` |
| `BasketReport` | `basket_payload` | `drawn`, `skipped`, `to_dict`, `to_html` |
| `DeflatedSharpe` | `deflated_sharpe` | `sharpe`, `benchmark`, `probability`, `trials`, `skew`, `kurtosis`, `is_significant` |
| `PBOResult` | `probability_of_backtest_overfitting` | `pbo`, `ranks`, `splits`, `strategies`, `is_overfit` |
| `CostFit` | `fit_impact` | `intercept`, `coefficient`, `shape`, `r_squared`, `observations`, `predict`, `is_usable` |
| `BrierAdvantage` | `brier_advantage` | `strategy_brier`, `market_brier`, `advantage`, `cumulative`, `win_rate`, `skill_score` |

Both `FactorPanel` and `ReturnSeries` expose `sql()`, so the SQL a number was
computed from is inspectable rather than implied.

`build_panel` raises `MaxLossExceededError` when more of the input is dropped
than `max_loss` allows (default 0.35), mirroring alphalens, so a panel built
from a third of the data cannot be mistaken for a panel built from all of it.
`panel.loss_report()` gives the same accounting alphalens prints: what was
lost joining to forward returns, and what was lost assigning quantiles.
