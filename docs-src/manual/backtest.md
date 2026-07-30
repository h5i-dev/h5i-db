---
title: Backtesting
description: "Deterministic event-driven backtesting on versioned data: canonical tables, runs that live on forks, and settlement that refuses to book what a run never reached."
order: 8
---

# Backtesting

`h5i-db-backtest` simulates venues against recorded market data. It never
routes a live order.

Three properties shape the design, and each is a test rather than an
intention:

- **Determinism.** A run is a pure function of (data pin, strategy, config).
  No wall clock, no unseeded randomness, no iteration over a hash map
  without sorting first.
- **No look-ahead, structurally.** Records carry `ts_event` and `ts_init`
  and replay in `ts_init` order, so late data arrives late. A strategy has
  no route to a market's resolution, because resolutions are read after the
  run finishes.
- **Data honesty.** Windows are half-open and owned in one place; a gap in
  incremental data invalidates the book rather than being replayed across;
  requested and loaded windows stay separate facts.

## The tables

Market data is venue-neutral. A Polymarket loader and a Hyperliquid loader
both produce the same tables, and the kernel never learns which vendor a row
came from.

| Table | Holds |
|---|---|
| `book_deltas` | snapshots, incremental level changes, and explicit gaps |
| `trades` | prints, with an optional aggressor |
| `bars` | aggregates |
| `instruments` | one row per outcome, so a categorical market is N rows |
| `resolutions` | how a market ended; never read on the strategy path |

Every market-data table is time-indexed on `ts_init`, the column replay
sorts by, so a range scan prunes on exactly the right column.

Numbers are `Float64` on disk and fixed point in the kernel. The conversion
is exact for every value the fixed-point type can represent (nine decimal
places, magnitudes below about 9e9); a test walks the entire 0.0001 tick
grid to confirm it rather than assuming.

### Snapshots are grouped

A snapshot is many rows sharing an `event_index`, with `is_last` on the
final one. Applying half a snapshot would leave a crossed or hollow book, so
a reader that meets a truncated one refuses it instead of reconstructing
something plausible from the fragment.

## A run

```rust
use h5i_db_backtest::{run_in_fork, RunSpec, Money};

let mut strategy = SignalReplay::new(intents)?;
let report = run_in_fork(
    &db,
    RunSpec::new("momentum-001", Money::from_units(10_000)?)
        .window(window)
        .read_at(ReadAt::Snapshot("2024-q1".into()))
        .minimum_coverage(0.95),
    &mut strategy,
    |engine| engine.fee_model(Box::new(PredictionMarketFees::new(0.07)?)),
).await?;
```

The run creates a fork, replays inside it, and writes its results there:

| Table | Holds |
|---|---|
| `bt_run` | the manifest: pin, config digest, cash, how far it simulated |
| `bt_orders` | every order and its final status |
| `bt_fills` | every execution |
| `bt_positions` | where it finished, with settlement attribution |
| `bt_equity` | the equity curve |

Because results are ordinary tables on a branch, everything else already
works: `fork_diff` compares two runs at fill level, a cross-fork scan
aggregates a sweep, `promote` publishes a blessed run, and `drop_fork_tree`
disposes of the rest.

`bt_fills` is authoritative. Positions are a fold over it and nothing else,
so a stored run can be rebuilt and checked rather than trusted.

## From a run to a tearsheet

```python
from h5i_db import quant

fork = db.fork("bt-momentum-001")
series = quant.from_levels(fork, "bt_equity")
quant.tearsheet(series, path="run.html")
```

The statistics are the same empyrical-parity set documented in
[Quant workflows](quant.html); nothing about them is backtest-specific.

## Settlement

Settlement is not an event in the replay stream. It is a policy applied
after the run, gated on one question: did the run reach the instant the
resolution became observable?

A three-day replay of a six-month market ends holding a position. Marking it
to the eventual winner books a profit nobody trading those three days could
have collected, so settlement applies only when
`simulated_through >= observable_at`. Otherwise the mark-to-market result
stands and the report says why.

Both numbers survive. `market_exit_pnl` is what the position was worth at
the last mark, `settled_pnl` is what it became at resolution, and the
difference is reported as an explicit adjustment rather than folded in
silently.

## Venue models

Four small traits carry every behavioural variation:

| Trait | Decides |
|---|---|
| `FeeModel` | what a fill costs |
| `FillModel` | what book an order meets |
| `LatencyModel` | when the venue hears about an order |
| `VenueModule` | periodic processes (funding, liquidation) |

New behaviour is a new implementation, never a new flag. `FillModel` has one
escape hatch worth knowing: `book_for_fill` may return a *synthetic* book,
and matching runs against it unchanged, so slippage, bar-derived quotes and
synthetic depth all reuse one matching path.

`PredictionMarketFees` implements the curved fee these venues actually
charge, `rate · quantity · p · (1 - p)`, which peaks at even odds and
vanishes at certainty. A flat `notional × rate` is the wrong shape and
overcharges the tails, which is where these markets trade most.

## Kalshi data

`h5i-db-venues::kalshi` converts Kalshi market metadata, REST order-book
snapshots, websocket snapshots and deltas, trades, and candlesticks into the
canonical tables above. It normalises NO bids into YES asks, so strategies see
one binary-contract book. The stateful websocket decoder enforces sequence
numbers, emits a `Gap` when one is missing, and refuses further deltas until a
new snapshot arrives.

An exact queue-aware Kalshi backtest requires data captured prospectively from
the authenticated `orderbook_delta` websocket:

1. use one decoder per market ticker;
2. record local receipt time separately from exchange event time;
3. persist the initial snapshot and every ordered delta;
4. on a gap, persist the gap, resubscribe or request a snapshot, and do not
   treat the stale interval as continuous coverage.

Kalshi's historical API supplies trades and minute-or-coarser candlesticks, not
historical L2 deltas. Those records support trade-driven or bar research, but
cannot reconstruct queue position and must not be presented as exact L2
replay. REST snapshots likewise describe only the instant at which they were
requested.

Use `KalshiFees` (or Python's `fee_kind="kalshi"`) with rates pinned from the
applicable series fee schedule. It implements the quadratic curve, centicent
trade-fee rounding, whole-cent cash movement, and per-order partial-fill
rounding accumulator. The adapter accepts uniform tick schedules and rejects
variable or tapered schedules rather than silently snapping them to the wrong
grid.

See Kalshi's
[order-book websocket](https://docs.kalshi.com/websockets/orderbook-updates),
[historical-data](https://docs.kalshi.com/getting_started/historical_data),
[fixed-point](https://docs.kalshi.com/getting_started/fixed_point_migration),
and [fee-rounding](https://docs.kalshi.com/getting_started/fee_rounding)
documentation before operating a recorder.

## Strategies

**Tier 1, signal replay** is the strategy as data: a list of timestamped
order intents replayed through the full matching, fee and latency path. It
covers most systematic research with no callback code and no language
boundary in the loop.

**Tier 2** is the `Strategy` trait, for path-dependent logic. Its callbacks
receive a `Context` that exposes the clock, books, positions and cash --
and nothing else. There is no route from it to a resolution.

Two orderings inside the loop are load-bearing:

- the venue sees data **before** the strategy, so a strategy cannot act on a
  price the matching engine has not processed;
- strategy commands are **queued**, never executed inside the callback that
  produced them, which removes reentrancy and makes latency a property of
  the queue rather than of every call site.

## Agent trial ledger

`backtest.execute(db, config)` treats every pinned, declarative
`BacktestConfig` as one score-producing trial. Its `trial_digest` hashes every
replay input but excludes `run_id` and descriptive `metadata`. Re-submitting
the same semantic config returns the recorded result with
`result["cached"] == True`; it does not create another fork or increase
`backtest.trial_count(db)`. Lookup plus creation is serialized per database,
including across local agent processes.

Unpinned configs and Python callback strategies still create normal recorded
runs, but are not reused: current table heads and callback implementations do
not have a complete identity in the typed config. Use a snapshot, version, or
as-of pin and a signals/commands strategy when retry-safe deduplication matters.

The `h5i-db-ui` experiments view is an attention router rather than a
leaderboard wrapper. Its default tab orders trials as:

1. human decision required;
2. failed or warned;
3. finished and unseen;
4. running;
5. seen.

The experiment sidebar rolls up the maximum child priority and counts unseen
warnings. Merely scanning a list does not mark work reviewed:
`StudyResult.open_trial(n)` marks in-process state, while `h5i-db-ui` marks a
trial seen only when its detail is opened and persists that review state in
the browser. The leaderboard remains a separate tab.

## What is not here

No live order routing, no brokerage adapters, no portfolio optimisation, no
plotting API. The boundary is simulation and evaluation; see
`ROADMAP_QUANT.md` §11 for the full list and the reasoning.
