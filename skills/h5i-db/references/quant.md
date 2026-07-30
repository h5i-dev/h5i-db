# Quant analytics with h5i-db

`h5i_db.quant` runs the standard research loop against the engine. Factor
statistics match `alphalens-reloaded` and portfolio statistics match
`empyrical-reloaded`, so the numbers are the familiar ones; what is new is that
they are attributable. A report leads with the version SHA and the pin it ran
under, an unpinned run says so, and `quant.verify()` refuses to certify a result
that cannot be reproduced.

## Factor evaluation

```python
from h5i_db import quant

panel = quant.build_panel(db, "signals", "prices",
                          periods=(1, 5, 10), quantiles=5,
                          snapshot="2024-q1")      # the pin

panel.ic()                  # per-date rank IC, one column per horizon
panel.quantile_returns()    # mean forward return per bucket
quant.factor_report(panel, path="factor.html")
```

`event_time_cutoff=` restricts every read to what was knowable at a decision
time, so a forward return needing a later price is dropped rather than computed.

## Performance statistics

```python
series = quant.returns(db, "strategy_returns")       # or
series = quant.from_levels(fork, "bt_equity")        # straight off a backtest run
series.stats()
series.drawdown_table()
quant.tearsheet(series, path="run.html")
```

Everything takes a *returns series*: one row per bar, simple (non-cumulative)
decimal returns. Annualisation constants are `quant.DAILY WEEKLY MONTHLY YEARLY`;
pass a bar count directly for anything else.

## Selection bias gets first-class statistics

A number found by searching is worth less than the same number found once, so
these are not optional footnotes.

```python
quant.deflated_sharpe(returns, trials=n)          # .sharpe .benchmark .probability
quant.minimum_track_record_length(returns)        # observations still needed
quant.probability_of_backtest_overfitting(matrix) # .pbo over (observations, trials)
```

`deflated_sharpe` discounts a Sharpe by the size of the search that produced it
and reports `probability`, the chance the true Sharpe beats the benchmark. Below
0.95 the result is indistinguishable from the best of that many coin flips. When
the variance of the trials' Sharpes is unknown it substitutes the returns' own
sampling variance, which is conservative rather than an assumption of zero.

`minimum_track_record_length` returns `inf` when the observed Sharpe sits below
the deflated benchmark. That is not a bug: it means no amount of further data
makes *this* result significant, and the honest report is that the search found
nothing.

Read the moments first. A hold-to-resolution book has an equity curve that is
flat and then jumps, so its returns are one outlier surrounded by noise, and
Sharpe assumes something much closer to normal. High skew and kurtosis mean the
Sharpe is the wrong summary, not that the strategy is bad.

`probability_of_backtest_overfitting` takes a matrix of shape
`(observations, trials)`, one column per trial's returns over the same period. A
PBO near 0.5 means the in-sample winner carried no information.

## Purged cross-validation

```python
quant.purged_kfold(n, folds=5, horizons=[10] * n, embargo=0.01)
quant.combinatorial_purged(n, folds=8, size=2, horizons=...)
quant.walk_forward(n, folds=5, horizons=...)
```

`horizons[i]` is how many observations forward observation *i*'s label depends
on, so a label needing the next ten bars cannot leak into its own training fold.
Omitting `horizons` says labels are instantaneous, which is rarely true and is
never assumed silently.

## Cost calibration

```python
fit = quant.fit_impact(samples)     # CostFit from SlippageSample observations
```

Calibrates a slippage model from realised fills instead of assuming a constant.

## Scoring a forecast against the market's own

Event contracts quote a probability, so a strategy trading them is making a
competing forecast. This is the one comparison an equity curve cannot make, and
sizing cannot inflate it.

```python
adv = quant.brier_advantage(strategy_probs, market_probs, outcomes)
adv.advantage        # market_brier - strategy_brier; positive means you were closer
adv.skill_score      # as a fraction of the market's own Brier
adv.cumulative       # the path, for plotting

quant.brier_decomposition(forecasts, outcomes)   # reliability - resolution + uncertainty
quant.reliability_curve(forecasts, outcomes)     # bucketed, with the signed gap
```

The decomposition says *why* a score is what it is: sitting off the diagonal
(reliability) is a different failure from not separating outcomes at all
(resolution). Note reliability is small in absolute terms — bucket gaps of five
to nine points square down to thousandths — so the *sign structure* of the
reliability curve is the tradeable finding, not the magnitude of that term.

## Comparing many runs at once

A sweep leaves one fork per trial, and opening them one at a time is not
comparing them.

```python
quant.basket_report(db, {"th50": result_a, "th60": result_b},
                    path="basket.html",
                    panels=quant.PORTFOLIO_PANELS + ("equity", "price"),
                    snapshot="panel-v1")
```

`quant.PORTFOLIO_PANELS` are safe at any basket size; per-run panels draw one
series each and are dropped past `per_run_limit` with the reason recorded in
`report.skipped`, because silently thinning lines misrepresents the basket. The
`price` panel puts fill markers on the book the fills actually met, read at the
same pin the runs used. Charts are inline SVG with no external requests, so the
document is readable without a plotting library installed.

`brier_advantage` is the one panel needing an input the report cannot derive —
your strategy's own probability — so it is passed in and skipped with a reason
when absent.

## Versioning-native workflows

```python
quant.sweep(...)                  # parameter grid, one fork per trial
quant.verify(...)                 # refuses to certify an unreproducible result
quant.restatement_impact(...)     # same computation at two data versions
```

`restatement_impact` re-runs one computation at two versions and reports what a
vendor's revision moved, which is a question only a versioned store can answer.
