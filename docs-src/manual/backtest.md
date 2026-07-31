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

One event is one book: every row under an `event_index` must carry the same
`instrument_id` and `outcome`. A feed that puts two outcomes under one event is
refused for the same reason a truncated snapshot is. The alternative is worse
than an error, because the levels would merge into a single book whose best ask
belongs to the other side of the market, and a buy would fill against it
without complaint.

The index values themselves only have to *change* between events. They are not
required to increase with `ts_init`, since grouping follows row order and ends
at `is_last`, so a recorder that writes instrument-major is fine.

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

## From the shell

The Python bindings carry a typed configuration, `BacktestConfig`, whose
sections are `data`, `execution`, `portfolio`, `risk` and `output`. It
round-trips through JSON, so a config file is a complete reproduction recipe
and the same contract drives both `backtest.execute(db, config)` and a
command line:

```bash
python -m h5i_db.backtest inspect market.db config.json
python -m h5i_db.backtest run     market.db config.json
python -m h5i_db.backtest list    market.db
python -m h5i_db.backtest report  market.db momentum-001 --output run.html
python -m h5i_db.backtest verify  market.db momentum-001
```

| Verb | Prints | Exit |
|---|---|---|
| `inspect` | the preflight inspection: replay fidelity, per-table stats, errors and warnings | `2` when the config is refused |
| `run` | the run summary, after refusing on preflight errors | |
| `list` | one run summary per `bt-` fork, in fork-name order | |
| `report` | the path it wrote | |
| `verify` | whether re-executing the stored config reproduced it | `3` when it did not |

Two flags are worth knowing. `run --allow-preflight-errors` records the
findings and runs anyway, for the case where you know why the data is thin.
`report --execution-only` renders the execution manifest instead of an equity
tearsheet; `report` falls back to it on its own when a run produced no equity
curve, because a tearsheet of nothing is worse than a manifest.

Exit codes are distinct on purpose: a refused config (`2`) and a run that
failed to reproduce (`3`) call for different responses, and a script should not
have to parse stdout to tell them apart.

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

### Not every market picks a winner

A `Resolution` carries a `Payout`, which is one of three things:

| Payout | What happened |
|---|---|
| `Winner(outcome)` | one outcome took the whole dollar |
| `Split(payouts)` | a scalar or partial settlement, payouts summing to one |
| `Void { outcomes }` | the question was unanswerable; a complete set refunds at cost |

The last two are not edge cases worth skipping. A voided binary pays both
sides fifty cents; recording it as a winner is wrong by the full notional on
each side, in opposite directions. `Resolution::split` refuses a payout
vector that does not sum to exactly one, because a settlement that does not
conserve a complete set mints or burns cash.

## Trading stops before resolving

A market's `expiration` is when trading stops, and the engine enforces it:
an order submitted after it is rejected, an order whose latency carries it
past it is rejected on arrival, and a resting order is cancelled at the
bell rather than left working against a book that no longer exists.

Data keeps arriving after a market closes -- late prints, a book teardown,
the settlement itself -- and observing it is fine. Filling against it is not.

## Outcomes cannot be borrowed

There is no stock loan for a share of "YES". A venue will not let you sell
an outcome you do not hold, so neither will the engine: a sell beyond the
held position is rejected with a message naming the trade that expresses the
same view. To be short YES, buy NO. It costs `1 - p` and pays the same.

This matters twice over. A short's worst case is `1 - p`, not `p`, so
collateralising it at the mark understates the requirement by up to the
whole notional -- selling a three-cent longshot posts three cents against
ninety-seven cents of risk. `CashMargin` charges the complement on a
probability short for that reason.

`EngineBuilder::allow_naked_shorts(true)` lifts the constraint. It is for
measuring what the constraint costs, not for producing a result to act on.

## Complete sets

Prediction-market venues will exchange a complete set of outcomes against
one unit of cash, in both directions. That is the contract that makes the
sum-to-one relationship *transactable* rather than merely observable, and
without it complete-set market making and the arbitrage that pins a book to
a dollar are both unreachable.

```rust
ctx.mint(&market, sets);    // pay one per set, receive one of every outcome
ctx.redeem(&market, sets);  // hand the set back, receive one per set
```

A Python callback strategy returns the same operations as actions:

```python
return [
    {"action": "mint", "instrument_id": market, "quantity": 100.0},
    {"action": "redeem", "instrument_id": market, "quantity": 100.0},
    {"action": "convert", "instrument_id": market,
     "outcomes": [0, 1], "quantity": 50.0},
]
```

Three things about how this is modelled:

- **Legs are fills.** A mint emits one fill per outcome, priced so the legs
  sum to exactly one per set. `Portfolio::replay` rebuilds every position
  from `bt_fills` alone, and a position that moved without a fill to explain
  it is a run whose stored result and audit disagree.
- **It is not instant.** Set operations queue behind the same insertion
  latency as an order. A mint that lands immediately is an arbitrage nobody
  could have taken.
- **It costs a flat fee, not a rate.** `SetOperationCosts` is charged once
  per operation, because a mint is one chain transaction. That is what makes
  a one-cent complete-set edge unprofitable at small size and profitable at
  large; a model without it reports every such edge as free money.

Minting is allowed only where the venue actually offers it:
`Instrument::supports_complete_set` is true for any two-outcome market, and
for a wider one only when `neg_risk` says the venue wired the outcomes into
a single exclusive set. A group of independent conditions displayed under
one heading is several instruments here, not one with many outcomes, and
minting across them would create a dollar out of nothing.

### Negative-risk conversion

`ctx.convert(market, held, quantity)` is Polymarket's conversion, addressed
the way its adapter is: `held` names the outcomes whose NO side you hold.

In this crate's N-outcome model it is provably a redemption. NO(i) is
"everything except i", so a basket of NO over `k` outcomes holds each named
outcome `k - 1` times and each unnamed one `k` times -- that is `k - 1`
complete sets plus the residual. The venue hands back `k - 1` in cash and
keeps the residual, which is exactly what redeeming `k - 1` sets does. The
primitive is offered under its own name because strategies reason in NO
contracts and the derivation is not obvious; a test pins the equivalence.

## Scoring a forecast

A market price on a prediction market *is* a forecast, so the natural
question about a strategy is whether it forecast better. Fills cannot answer
it: they record what a strategy did, not what it believed.

```rust
ctx.record_forecast(&market, outcome, Price::from_f64(0.65)?)?;
```

```python
return [{"action": "forecast", "instrument_id": market,
         "outcome": 0, "probability": 0.65}]
```

`RunReport::calibration_samples()` joins those statements to the market's
own price at the same instant and to what the outcome actually paid, giving
the triples a Brier score, a reliability curve or an advantage-over-market
series is computed from. The Python report returns them as
`calibration_samples`, ready for `h5i_db.quant.calibration`.

Two kinds of forecast are dropped rather than scored, and both are reported
by `unscored_forecasts()` so the sample is never quietly smaller than it
looks: a market with no known resolution, and a market that paid every
outcome the same. Against a void, every forecast scores identically --
including a confidently wrong one -- so including it does not measure a
forecaster, it dilutes the sample that does.

The market's own probabilities are sampled onto the equity curve's clock as
`mark_curve`, for the comparison series. Turn it off with
`record_mark_curve(false)` on a run spanning thousands of markets.

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

## Perpetuals

A derivatives venue does not value your position at the mid, and modelling
it as if it did produces losses that look like strategy results.

### Three prices, not one

Hyperliquid publishes an **oracle** built from spot exchanges and a **mark**
derived from it and the book. Margin, unrealised PnL and liquidation read
the mark; funding is charged on the oracle; the book's mid is neither.

`MarketEvent::Reference` carries both, in a `references` table alongside
`trades` and `funding`. The engine keeps the book-derived, venue-mark and
oracle prices apart and exposes one effective mark, so `MarkSource` is a
policy rather than a rewrite. It defaults to using the venue's mark where
one exists, which is a no-op on data that carries none.

Two consequences worth stating:

- A thin book or a one-print wick moves the mid far enough to liquidate a
  position the venue was still valuing calmly. On the mark it does not.
- Funding on the mid reintroduces exactly the manipulation the oracle
  exists to prevent, and at hourly settlement that compounds over a carry.

Reference records replay at priority 7, ahead of the book, for the same
reason corporate actions do: every per-record check that prices against the
mark must already have it.

### Prices the venue will actually accept

Hyperliquid caps a price at five significant figures **and** at
`6 - szDecimals` decimal places, per coin. A flat tick cannot say that: it
accepts prices the venue refuses at the top of a range and refuses ones it
accepts at the bottom. `PriceRule::SignificantFigures` says it, and
`hyperliquid::parse_meta` derives it per coin along with `maxLeverage`,
`onlyIsolated` and the lot size.

Delisted coins are kept, not dropped. Dropping them is survivorship bias
applied at ingestion, where it is hardest to notice.

### Data

`candleSnapshot` returns bars and the REST `l2Book` returns only the book as
it is right now, so the archive is the only source of book history the venue
offers. `read_archive_lz4` reads the hourly files; `parse_ws_message`
handles a live capture, dispatching `l2Book` and `trades` by the coin in the
payload so one call covers a capture spanning many markets.

`s3://hyperliquid-archive` (us-east-1) is a **Requester Pays** bucket:
anonymous reads get a 403 whatever headers you send, and you need an AWS
account and pay the transfer. Pull from us-east-1 and the transfer is free;
you pay only GET requests, which are a fraction of a cent.

```bash
aws s3 ls s3://hyperliquid-archive/market_data/20250101/0/l2Book/ \
    --request-payer requester
aws s3 cp s3://hyperliquid-archive/market_data/20250101/0/l2Book/BTC.lz4 . \
    --request-payer requester
```

Measured on `20250101/0`: 166 coins, 77 MB compressed for the hour, so
roughly 1.8 GB a day and 670 GB a year. BTC alone is 700 KB an hour, 6,301
snapshots, one every 0.57 seconds.

Two things the archive is **not**:

- **It has no trades.** `market_data/<date>/<hour>/` contains `l2Book/` and
  nothing else. Prints have to come from a live websocket capture, and
  without them the queue-position fill model has nothing to work with. This
  is the single biggest remaining gap for a Hyperliquid market-making
  backtest.
- **It is not bucketed by venue time.** Files are grouped by when the
  archiver received a message, so the `00` hour file opens with a message
  the venue stamped at `23:59:59.877` the day before. Load a window with an
  hour of slack on each side.

Each line is `{"time": <archiver receive, ISO nanoseconds>, "ver_num": 1,
"raw": <the live websocket envelope>}`, and the venue's own stamp is inside
at `raw.data.time`. **Both matter.** The archiver trails the venue by 57 ms
at the median and 3.2 seconds at the worst, so the outer stamp is `ts_init`
and the inner one is `ts_event`; reading only the inner one hands a strategy
up to three seconds of look-ahead on every book update. `read_archive` keeps
them apart, and clamps `ts_init` to at least `ts_event` so a skewed clock
cannot claim a message was known before it was sent.

The live endpoints need no credentials, which is where
`tests/fixtures/hyperliquid` came from. Refresh them with:

```bash
curl -sX POST https://api.hyperliquid.xyz/info \
  -H 'Content-Type: application/json' -d '{"type":"metaAndAssetCtxs"}'
```

Fixtures captured from the venue are worth the bytes. A hand-written one
tests that a parser matches what its author believed; a real one tests that
it matches what the venue sends. The `fundingHistory` request shape here was
wrong -- it nested its arguments under `req`, which the API rejects with a
422 that names no field -- for exactly as long as only the first kind
existed.

Two shapes to know, both caught this way: `fundingHistory` takes its
arguments flat while `candleSnapshot` nests them under `req`, and funding
timestamps carry tens of milliseconds of jitter around the hour rather than
landing on it exactly.

`ArchiveRead` counts what produced nothing and `require_yield` refuses a
file that is mostly junk -- usually the wrong channel or the wrong
decompression, and worse to replay as a thin book than to refuse outright.

Trades matter more than they look: without prints the queue model has
nothing to consume the size ahead of a resting order, so every touched
limit fills immediately.

`asset_ctxs/<date>.csv.lz4` is the historical mark and oracle — a daily CSV
at one-minute cadence across the universe. Its `funding` column is the
standing rate sampled once a minute, **not** a payment, so
`asset_context_records` deliberately emits reference prices and no funding
events; sixty samples an hour would charge a carry sixty times. Settlements
come from `fundingHistory`.

`hyperliquid_archive::load_archive` walks a synced directory and commits it,
so the whole path is one call:

```rust
let spec = ArchiveSpec::new("./archive").date("20250101").coin("BTC");
load_archive(&db, &spec, &universe, known_at).await?;
```

Coins filter on the *file name*, so a one-coin load reads 700 KB instead of
77 MB. A date, hour or coin that is not on disk lands in `ArchiveLoad::missing`
rather than reading as no data — an incomplete sync found at replay time
looks like a thin book. Instruments come from a supplied `meta` universe,
not from the archive, which carries prices and no metadata.

### Post-only

`OrderRequest::post_only()` is Hyperliquid's ALO. It is checked on arrival,
not on submission, because what matters is the book the venue sees when the
order gets there -- an order sent into a wide market and delivered into a
crossed one is rejected, which is the risk a maker takes. A post-only order
that later fills does so as a maker, since by construction it could never
have taken.

### Stops and take-profits

`OrderRequest::with_trigger` holds an order off the book until a price
reaches it. That is the difference from a limit at the same price: a limit
is liquidity someone can trade against, and a stop is not — so resting one
would invent depth that was never there. Untriggered orders carry their own
`OrderStatus::Untriggered`.

They fire on the **mark**, not the book, for the same reason margin does: a
one-print wick should not stop you out of a position the venue still values
calmly. Once fired the order is ordinary and meets the book from there, so a
stop can and does fill well below its trigger — the gap it exists to protect
against is the gap it suffers.

```rust
ctx.submit(
    OrderRequest::market(market, outcome, Side::Sell, size)
        .with_trigger(Trigger::stop_loss(Side::Sell, price)),
);
```

```python
return [{"action": "submit", "client_order_id": "stop", "instrument_id": m,
         "side": "sell", "quantity": 1.0,
         "trigger_price": 90.0, "trigger_direction": "stop_loss"}]
```

### TWAP

Hyperliquid's TWAP is a native order type, not a client-side loop: the venue
slices it into equal children on a fixed cadence (thirty seconds) and works
them until the duration is up.

```rust
ctx.twap(TwapRequest::new(market, outcome, Side::Buy, size, duration_nanos));
```

Modelling it as one large market order gets the answer wrong in the
direction that flatters. A size worth slicing is a size that moves the book,
and the whole reason to slice is that it does — so the children here are
ordinary market orders crossing through the same matching path, and a TWAP
into a thin book suffers exactly the slippage that book implies. The last
slice carries the rounding, so the schedule works exactly the size it was
given rather than quietly working less and reporting a better average.

### Fees that fall with volume

`TieredFees` prices on rolling traded notional. Every serious venue does
this and most simulators do not, which decides whether a market-making
strategy has a positive expectancy at all: at the top of a real schedule the
maker fee is *negative*.

A fill is priced at the tier reached before it, and volume ages out of a
trailing window (`hyperliquid::FEE_VOLUME_WINDOW_NANOS` is fourteen days).
The schedule is supplied rather than baked in -- venues republish theirs, and
a stale table compiled into a backtester is worse than none because it looks
authoritative.

### Margin

`PerInstrumentMargin` grants leverage per coin, which is how the venue
grants it: forty times on the majors and three on the long tail.
`hyperliquid::margin_from_meta` builds one from the universe, falling back
for an unlisted coin to the *tightest* leverage in it rather than the
loosest.

Two policies that both default to the previous behaviour:

- `LiquidationPolicy::Partial` closes positions, largest maintenance
  requirement first, only until the account is above maintenance again. A
  venue closes what it must, not what it can, and the two produce materially
  different results from the same data.
- `EngineBuilder::isolate(instrument)` margins one instrument on its own
  collateral. The bucket is sized at the position's entry and stays there,
  so it does not top itself up from cross cash as the trade moves against
  you -- an isolated position can lose exactly its bucket, and a cross
  account in profit will not rescue it.

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

### Stamp a signal after the quote it came from

Market data is merged on the total order `(ts_init, stream priority, stream,
arrival)`, and the priorities are explicit: gaps before corporate actions
before snapshots before deltas before prints. Signals are not part of that
order. An intent is released on the first record whose timestamp reaches it,
which puts one detail in the analyst's hands.

A signal timestamped *exactly* on a book instant is released while the venue is
partway through that instant. Whether its own instrument has been updated yet
depends on where that instrument falls among the records sharing the timestamp,
so with one market the signal sees the new book and with sixty it may match
against the previous one. The replay stays deterministic, because the merge
order is total and the same data always produces the same answer; what varies
is which book a same-timestamp signal meets, and that varies with the shape of
the panel rather than with anything the strategy did.

Stamp the intent strictly after the quote it was decided from and the question
disappears:

```python
signals = backtest.signal_table([
    {"ts": decision_ts + datetime.timedelta(microseconds=1),
     "instrument_id": market, "outcome": 0, "side": "buy", "quantity": 20.0},
])
```

The order then fills at exactly the bid or ask carried by the decision
snapshot, stamped at the next event. That is also the honest reading of a
backtest: you transacted at a price that was knowable when you chose to trade.
Tier 2 strategies are unaffected, because a callback is already invoked after
the venue has processed the record it is reacting to.

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

## Bringing your own data

`h5i_db.venues` turns vendor archives already on disk into the canonical
tables. It does not fetch: downloading belongs in a script, where credentials
and rate limits belong, and this layer is the part that must be testable
offline and byte-reproducible.

```python
from h5i_db import venues

specs = venues.polymarket_markets_from_json(payloads)   # slug -> outcomes, tokens
venues.write_markets(db, specs)                         # instruments, resolutions
report = venues.ingest_archive(                         # book_deltas, trades
    db,
    files=venues.discover("/mnt/pmxt"),
    markets=specs,
    layout=venues.PMXT_LAYOUT,
    window=(start_ns, end_ns),
)
report.coverage, report.gaps, report.replayed
```

The same three steps run from a shell, with market definitions travelling as a
JSON file rather than as flags:

```bash
python -m h5i_db.venues markets market.db specs.json
python -m h5i_db.venues ingest  market.db specs.json --root /mnt/pmxt \
    --start-ns 1777000000000000000 --end-ns 1777003600000000000 --min-coverage 0.95
python -m h5i_db.venues inspect market.db
```

Four properties are worth knowing before pointing it at a mirror.

**Re-running is a replay, not a duplicate.** Every commit is keyed by the hash
of the normalised rows it carries, so identical inputs produce identical keys
and h5i-db recognises them. An interrupted backfill is safe to restart, and two
sources serving the same hour converge on one commit rather than two.

**Requested and loaded windows stay separate facts.** `report.coverage` is the
loaded span over the requested one, and it is `None` when no window was asked
for, because a ratio against an unbounded request would be meaningless.
`--min-coverage` exits non-zero rather than letting a short load pass quietly.

**A vendor dialect is data, not a code path.** `ArchiveLayout` carries the
column names, event vocabulary, timestamp unit and level shape.
`PMXT_LAYOUT` and `TELONEX_LAYOUT` are literals of that type, and a third
vendor is a new literal. An event type present in the file but absent from the
layout is counted and reported, never guessed at.

**Outcome order is positional and refused when ambiguous.** A market spec pairs
`outcome_labels` with `tokens` by index, a token claimed by two markets is an
error, and a resolution with no observability instant is an error too, since
settlement is gated on when the result became knowable.

## Searching without fooling yourself

`backtest.study` runs a grid by default. Three additions make the search
shape explicit when it matters.

```python
from h5i_db import backtest

result = backtest.study(
    db,
    study_id="threshold",
    base=config,
    parameters={"execution.fee_rate": backtest.Range(0.0, 0.08)},
    search=backtest.RandomSearch(trials=40, seed=7),
    validation=backtest.WalkForward.of(fold_one, fold_two, fold_three),
    selection=backtest.TopK(k=5, metric="final_cash"),
)
result.ranked()          # holdout median, train score as the tie-break
result.selected          # only the trials that reached the holdout
```

`WalkForward` scores a candidate on several folds and reports the median, so one
lucky window cannot carry it; per-fold columns are `fold{i}_train_*` and
`fold{i}_holdout_*`, with `train_median_*` and `holdout_median_*` alongside. A
single `ValidationWindows` keeps the flat `train_*` / `holdout_*` names.

`TopK` makes the holdout a second stage: candidates are ranked on train, only
`k` are run out of sample, and nothing else ever touches it. A holdout every
candidate touched is a second training set with a different name.

`RandomSearch` beats a grid when the space is wide and most axes do not matter.
`TPESearch` needs the optional `optuna` extra and runs sequentially, because
each point is proposed from the results so far. Duplicate draws are kept rather
than resampled: dropping them would change the trial count that the
deflated-Sharpe correction in `quant.deflated_sharpe` depends on.

Subprocess isolation per trial is deliberately absent. A study refuses callback
strategies, so a trial is a declarative config that cannot crash the driver;
the isolation the reference stacks need is buying safety this API already has.

## Comparing many runs at once

A sweep produces one fork per trial, and opening twenty tearsheets is not
comparing them. `quant.basket_report` assembles one document from stored tables
only, with no re-simulation:

```python
from h5i_db import quant

quant.basket_report(
    db,
    {"th50": result_a, "th60": result_b},
    path="basket.html",
    panels=quant.PORTFOLIO_PANELS + ("equity", "price"),
    snapshot="panel-v1",
)
```

Portfolio panels (`total_equity`, `total_drawdown`, `total_rolling_sharpe`,
`total_cash_equity`, `periodic_pnl`, `leaderboard`) are safe at any size.
Per-run panels draw one series each and are dropped, with a reason recorded in
`report.skipped`, once the basket exceeds `per_run_limit`: silently thinning
lines to fit would misrepresent the basket. The `price` panel puts fill markers
on the book the fills actually met, read at the same pin the runs used.

The charts are inline SVG with no external requests, because a report that needs
a plotting library installed to be *read* is not a report.

`brier_advantage` is the one panel that needs an input the report cannot
derive: your strategy's own probability. `market_brier - strategy_brier` says
whether the forecast beat the price it paid, which is a comparison an equity
curve cannot make and which cannot be inflated by sizing.

## A pack of strategies, as data

`backtest.strategies` ships the standard rules as signal *generators*: each
takes a quote panel and returns a signals table, so the trial ledger can
identify it and `verify()` can reproduce it.

```python
panel = backtest.quote_panel(db, snapshot="panel-v1")
plan = backtest.strategies.late_favorite_hold(panel, min_price=0.75)
db.append("signals", plan.signals)
```

`quote_panel` stops at `expiration_ns`, so no rule can read the resolution jump
as a price move. Every generator stamps its orders a microsecond after the quote
they were decided from. `STRATEGIES` maps name to generator for sweeping the
pack itself; `pair_arbitrage` is outside it because it reads both outcomes from
the database rather than one side's panel.

## Replaying an account's ledger

The strictest realism question available: given the trades an account actually
took, does the engine reproduce the same portfolio? Usually not, and that is the
point. `venues.commands_from_ledger` compiles a ledger into *intent* rather than
into fills, so the historical book accepts or refuses each order on its merits:
limit orders at the ledger's own price, immediate-or-cancel, and sells as
`reduce_only` so a replay cannot invent short exposure the ledger never showed.

```python
commands = venues.commands_from_ledger(rows, specs)
db.append("commands", commands)
result = backtest.execute(db, config)          # data=DataConfig(commands="commands", ...)
venues.compare_to_ledger(result, typed_rows)   # per-market reconciliation
```

A forced-fill simulator would reproduce the ledger by construction and test
nothing. The comparison reports per-market shortfalls rather than one
pass/fail, because *where* the book refused is the finding.
