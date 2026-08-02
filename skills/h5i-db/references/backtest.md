# Backtesting with h5i-db

An event-driven backtester whose data plane is the database. It never routes a
live order. A run executes inside a fork and writes its results there as
ordinary tables, so `bt_fills` is queryable with the same SQL as market data and
two runs diff at fill level.

Python only: there is no `h5i-db backtest` CLI verb. The shell entry point is
`python -m h5i_db.backtest`.

## The shortest complete run

```python
from h5i_db import backtest

config = backtest.BacktestConfig(
    run_id="momentum-001",
    data=backtest.DataConfig(signals="signals", snapshot="2024-q1"),   # the pin
    portfolio=backtest.PortfolioConfig(starting_cash=100_000.0),
    execution=backtest.ExecutionConfig(fee_kind="kalshi", fee_rate=0.07),
)

backtest.inspect(db, config).raise_for_errors()   # refuse unsupported claims
result = backtest.execute(db, config)             # runs in fork "bt-momentum-001"

result.summary()      # fills, final cash, how far it actually simulated
result.explain()      # why orders were rejected or never filled
result.fills          # Arrow; or SQL it on the fork
result.verify()       # re-execute the stored config and compare
result.report("run.html")   # one self-contained page of the whole run
```

`report()` writes a single HTML file with no dependencies and no network
access at view time: the replay fidelity and the pin first, then the
performance panels, then order lifecycle, rejections and fills, and the
configuration verbatim at the end so the page is enough to re-run itself. In a
notebook the result renders as that page. A run with no equity curve still
reports; the performance panels drop out and the execution evidence stays.

`execute` returns a `BacktestResult`, which is a dict *and* an object: `result["fills"]`
and `result.fills` are different things. The mapping keys are summary metrics; the
attributes are tables (`run orders fills positions equity`) and methods.

## The five config sections

`BacktestConfig` round-trips through JSON, so a config file is a complete
reproduction recipe. Only these fields exist; anything else is a typo the
constructor rejects.

| section | fields |
|---|---|
| `DataConfig` | `signals` `commands` `strategy_id` `snapshot` `version` `as_of` `window` `minimum_coverage` |
| `ExecutionConfig` | `fee_kind` `fee_rate` `maker_rebate` `maker_fee_rate` `queue_position` `optimistic_queue` `latency_nanos` `slippage_ticks` `margin_kind` `leverage` `maintenance_margin_rate` |
| `PortfolioConfig` | `starting_cash` |
| `RiskConfig` | `max_order_quantity` `max_abs_position` `max_open_orders` |
| `OutputConfig` | `equity_interval_nanos` |

`fee_kind` is `"prediction_market"`, `"proportional"`, `"kalshi"` or `None`. On
event contracts the fee is `rate * quantity * p * (1 - p)`, not `notional * rate`:
it peaks at even odds and vanishes at certainty, and it is usually the largest
cost component. Combinations that describe no real venue are refused at
construction, so `queue_position=True` with `slippage_ticks=1` raises rather than
silently letting one win.

`margin_kind` is `"cash"`, `"linear"` or `None`. Leaving it `None` means no
margin model at all: leverage is unbounded and nothing can be liquidated, so
the run's `liquidations` and `rejected_for_margin` counts are zero because
nothing was measuring, not because the account was never at risk. The report
says which of the two it was.

## Stamp a signal *after* the quote it came from

The single most common way to get a wrong fill price.

```python
{"ts": decision_ts + datetime.timedelta(microseconds=1), ...}
```

Market data is merged on `(ts_init, stream priority, stream, arrival)`. Signals
are not part of that order: an intent is released on the first record reaching
its timestamp. So an order stamped *exactly* on a book instant may match the
previous snapshot, and whether it does depends on where its instrument falls
among records sharing that timestamp. Submitting a microsecond later is
deterministic and fills at exactly the bid or ask the decision snapshot carried.

## Three strategy shapes

- **Signals** (`backtest.signal_table`, schema `SIGNAL_SCHEMA`): timestamped
  order intent. The strategy is *data*, so the trial ledger can identify it by
  content and `verify()` can reproduce it. Prefer this.
- **Commands** (`command_table`, `COMMAND_SCHEMA`): adds `submit`/`amend`/`cancel`
  lifecycle and `client_order_id`.
- **Callback** (`EventStrategy`, or the Rust `Strategy` trait): path-dependent
  logic. Has no content identity, so `BacktestStudy` refuses it and trial dedup
  does not apply.

`backtest.strategies` ships eleven rules as signal *generators* over a quote
panel, plus `pair_arbitrage` which reads both outcomes itself:

```python
panel = backtest.quote_panel(db, snapshot="panel-v1")   # stops at expiration_ns
signal_plan = backtest.strategies.late_favorite_hold(panel, min_price=0.75)
db.append("signals", signal_plan.signals)   # signal_plan.parameters travels with it
```

`quote_panel` stops at expiry deliberately: the data keeps quoting after
resolution, and a momentum rule reading that jump to 0 or 1 finds a signal that
is really an answer.

## Preflight refuses what the data cannot support

```python
inspection = backtest.inspect(db, config)
inspection.ok            # False when a claim is unsupported
inspection.fidelity      # ReplayFidelity: what this data can honestly model
inspection.errors        # each with .code and .message
inspection.warnings
```

`unsupported_queue_claim` is the one to know: queue-position fills need every
delta between snapshots, and no care recovers that from a periodic grid.
`execute` refuses too, so the check cannot be skipped by not calling `inspect`.
`snapshot_only` is a warning, not an error: market orders against a known book
are fine.

## Settlement is gated on observability

Not an event in the stream. A policy applied after the run, asking one question:
did the replay reach the instant the resolution became *observable*?

```python
positions = result.positions.to_pandas()
positions.settlement_pnl     # what it became at resolution; null when refused
positions.market_exit_pnl    # what it was worth at the last mark
```

`realized_pnl` covers closed round trips only, so a book held to resolution
reads zero there and the result lives in `settlement_pnl`. Read both: a rule with
negative `realized_pnl` and positive settlement was paid by what it held, not by
what it traded.

`run.settlement_applied` is true when settlement reached **any** position, so
`True` beside several refusals is normal on a panel whose markets resolve at
different times. The authoritative per-position signal is whether
`settlement_pnl` is null.

## Searching without fooling yourself

```python
search = backtest.study(
    db, study_id="threshold", base=config,
    parameters={"execution.fee_rate": backtest.Range(0.0, 0.08)},
    search=backtest.RandomSearch(trials=40, seed=7),      # or GridSearch/TPESearch
    validation=backtest.WalkForward.of(fold_a, fold_b, fold_c),
    selection=backtest.TopK(k=5, metric="realized_pnl"),
)
search.ranked()    # holdout median, train score as tie-break
search.selected    # only the trials that reached the holdout
```

`WalkForward` scores each candidate over several folds and reports the median, so
one lucky window cannot carry it. `TopK` makes the holdout a second stage: only
`k` candidates ever run out of sample. **Check that only the shortlist has
holdout columns** — if every candidate has an out-of-sample score, the
out-of-sample set is a second training set.

Columns: single-fold studies keep flat `train_*`/`holdout_*`; multi-fold adds
`fold{i}_train_*` plus `train_median_*` and `holdout_median_*`.

Two traps. `data.signals` is a data-identity field, so a study cannot vary the
*strategy* — searching strategy parameters means one signals table and one run
per candidate. And pick a metric that is closed at the window edge: `final_cash`
inside a fold measures deployed capital, not performance, because a position open
at the boundary has spent cash it has not recovered.

`TPESearch` needs the optional `optuna` extra and runs sequentially. Duplicate
random draws are kept rather than resampled, because dropping them would change
the trial count that `quant.deflated_sharpe` needs.

## Trials are identified by content

A pinned, declarative config hashes to a `trial_digest` over every replay input,
ignoring `run_id` and descriptive `metadata`. Re-submitting the same semantic
trial returns the recorded result with `cached=True` instead of forking again,
and lookup-plus-creation is serialized across local processes, so a retry loop
cannot spend a second run or double-count a score. Unpinned configs and callback
strategies are not reused, because their identity is not complete.

## From the shell

```bash
python -m h5i_db.backtest inspect market.db config.json
python -m h5i_db.backtest run     market.db config.json [--allow-preflight-errors]
python -m h5i_db.backtest list    market.db
python -m h5i_db.backtest report  market.db momentum-001 --output run.html [--tearsheet]
python -m h5i_db.backtest verify  market.db momentum-001
```

Exit codes are distinct so a script need not parse stdout: `2` a refused config,
`3` a run that failed to reproduce.

## Feeding it data

Canonical tables are `book_deltas`, `trades`, `bars`, `instruments`,
`resolutions`, `funding` (`backtest.MARKET_DATA_TABLES`). One book event is many
rows sharing an `event_index` with `is_last` on the final one, and **every row
under one `event_index` must carry the same `instrument_id` and `outcome`** — a
mixed event is refused, because merging both outcomes into one book gives it a
best ask belonging to the other side of the market. Index values need only change
between events; they need not increase with `ts_init`.

To turn vendor archives into those tables, see
[data-onramp.md](data-onramp.md). To score a result, see [quant.md](quant.md).
