# h5i-db

**English** · [Español](README.es.md) · [Français](README.fr.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md)

**A fast, agent-native time-series *d*atabase and *b*acktesting engine for quant research.
Embedded, written in Rust.**

- (DB) **Fast for time-series shape:** over 4.5× faster than DuckDB and Polars
  on OHLCV+VWAP rollups over 20M rows.
- (DB) **Native time-series SQL:** ASOF join, timezone-aware `time_bucket`,
  gapfill/resample, rolling windows, `vwap`, `ewma`.
- (DB) **Point-in-time reads:** pin a decision time and the frame that reaches
  pandas cannot contain rows from after it. No lookahead bias, by construction.
- (BT) **Efficient event-driven backtester:** 3.05M events/s through
  the replay kernel, 11.7× NautilusTrader and 31× LEAN on a shared
  top-of-book workload.
- (BT) **Native support for popular markets:** Kalshi, Polymarket and Hyperliquid
  payloads decode into one canonical set of tables, each with the venue's real fee
  curve and funding rather than a generic `notional × rate`.
- (BT) **The usual statistics, plus how much to trust them:** factor and
  performance numbers match `alphalens` and `empyrical`; deflated Sharpe and
  overfitting probability say how much of a result was just the search that
  found it.
- (AI) **Fork a database in milliseconds:** forks share data instead of copying it. 
  Agents can run wide trial-and-error loops (fork, mutate, evaluate, discard) 
  at almost zero cost.
- (AI) **Every write is an atomic, versioned commit:** any past version reads in
  O(1), so a bad ingest (human or agent) is one `restore` away from undone.
- (AI) **Safety policies for agent writes:** previewable mutations, policy gates,
  fail-closed constraints that block destructive operations, and an audit
  trail of what changed and why.

📖 **[Documentation](https://db.h5i.dev/manual/)** · [Manual](https://db.h5i.dev/manual/) · [Python API](https://db.h5i.dev/api/) ·
[Cookbook](https://github.com/h5i-dev/h5i-db-cookbook) · [Agent skill](skills/h5i-db/SKILL.md)

---

## Quickstart

**CLI**

```bash
cargo install h5i-db-cli
```

```bash
h5i-db init market.db
h5i-db create-table market.db trades --like ticks.parquet --time-column ts
h5i-db ingest market.db trades ticks.parquet --idempotency-key load-1
h5i-db context market.db                                           # orient in one call
h5i-db query market.db "SELECT symbol, vwap(price,size) FROM trades GROUP BY symbol"
h5i-db query market.db "SELECT count(*) FROM trades" \
  --decision-time 2026-07-01T00:00:00Z                             # the future is unreadable
h5i-db ui market.db                                                # review + experiments surface
```

**Python Library**

```bash
pip install h5i-db
```

```python
import pyarrow as pa
import h5i_db

db = h5i_db.Database("market.db", create=True)

db.create_table(
    "trades",
    pa.schema([("ts", pa.timestamp("us")), ("symbol", pa.string()), ("price", pa.float64())]),
    time_column="ts",
)
db.append("trades", pa.table({
    "ts": pa.array([1_700_000_000_000_000, 1_700_000_060_000_000], pa.timestamp("us")),
    "symbol": ["AAPL", "MSFT"], "price": [187.4, 411.2],
}))

df = db.sql("SELECT symbol, avg(price) AS px FROM trades GROUP BY symbol").to_pandas()
# df = db.table("trades").group_by("symbol").agg(px=col("price").mean()).to_pandas()
old = db.read("trades", version=1)                # time travel: read any past version

plan = db.plan_delete_range("trades", 1_700_0_000_000)
print(plan.summary)                               # preview the mutation before it lands
plan.apply()
```

**Backtest** (same install, no server, no separate data pipeline)

```python
from h5i_db import backtest

config = backtest.BacktestConfig(
    run_id="momentum-001",
    data=backtest.DataConfig(signals="signals", snapshot="2024-q1"),   # the pin
    portfolio=backtest.PortfolioConfig(starting_cash=100_000.0),
    execution=backtest.ExecutionConfig(fee_kind="kalshi", fee_rate=0.07),
    risk=backtest.RiskConfig(max_order_quantity=500.0),
)

backtest.inspect(db, config).raise_for_errors()  # refuse claims the data can't support
result = backtest.execute(db, config)            # replays inside fork "bt-momentum-001"

result.summary()                  # fills, final cash, how far it actually simulated
result.explain()                  # why orders were rejected or never filled
result.fills                      # Arrow, or just SQL it: SELECT * FROM bt_fills
result.tearsheet("run.html")
result.verify()                   # re-execute the stored config and compare
```

A parameter grid becomes one fork per trial, and the winner is ranked without
an export step. Give it explicit train and holdout windows and every trial runs
both phases, so the leaderboard can be read out of sample:

```python
board = backtest.study(
    db, study_id="fees", base=config,
    parameters={"execution.fee_rate": [0.0, 0.02, 0.07]},
    validation=backtest.ValidationWindows(
        train=("2024-01-01", "2024-04-01"), holdout=("2024-04-01", "2024-07-01")
    ),
).leaderboard("holdout_final_cash")
```

The same typed contract runs from the shell, so a config file is the whole
reproduction recipe:

```bash
python -m h5i_db.backtest inspect market.db config.json   # fidelity + preflight findings
python -m h5i_db.backtest run     market.db config.json
python -m h5i_db.backtest report  market.db momentum-001 --output run.html
python -m h5i_db.backtest verify  market.db momentum-001
```

**Agent skill** (Claude Code, Codex, Cursor, …)

```bash
npx skills add h5i-dev/h5i-db        # installs the h5i-db skill from skills/h5i-db/
```

---

## Why

| | DuckDB | Polars | pandas | PyArrow | ArcticDB | **h5i-db** |
|---|---|---|---|---|---|---|
| User-facing versioning / time travel | ✗¹ | ✗ | ✗ | ✗ | ✓ | ✓ (O(1) version reads) |
| SQL joins/windows/CTEs | ✓ | partial | ✗ | ✗ | ✗ | ✓ (DataFusion) |
| ASOF join | ✓ | ✓ | ✓ | ✗² | ✗ | ✓⁴ (sort-free on sorted storage) |
| Previewable mutations (plan/apply) | ✗ | ✗ | ✗ | ✗ | ✗ | ✓, policy-enforceable |
| Concurrent writers | MVCC | n/a | n/a | n/a | unsafe³ | CAS + explicit conflict |
| 20M-row narrow time-range scan | 45.5 ms | 28.1 ms | 23.9 ms | 22.8 ms | **4.2 ms**⁵ | 10.0 ms |
| 20M-row 1-min OHLCV+VWAP | 7237 ms | 7309 ms | 5115 ms | 7121 ms | 3504 ms | **1558 ms** |
| 20M-row ASOF join (by symbol) | 11566 ms | **1485 ms** | 6624 ms | ✗² | 7008 ms | 1548 ms |

¹ `AT (VERSION …)` syntax exists but native storage rejects it.
² Experimental `join_asof` exists but is ~1000× slower, impractical at this scale.
³ Documented single-writer-per-symbol assumption.
⁴ Native `ASOF JOIN … MATCH_CONDITION` SQL syntax and an `asof_join(...)`
  table function (SQL and Python).
⁵ ArcticDB's native time index wins narrow point reads from its own LMDB
  store; h5i-db's manifest pruning is second and beats every general engine.

Full methodology in [benchmarks/RESULTS.md](benchmarks/RESULTS.md).

---

## Why it's fast

- **Manifest pruning:** every version's manifest carries per-segment time
  ranges and column min/max. Narrow queries prune whole segments before a
  single file is opened.
- **Declared sort order:** segments are stored time-sorted and the query
  layer tells DataFusion so. OHLCV rollups stream instead of sorting 20M rows
  first (every baseline pays that sort), and the ASOF join is sort-free.
- **Immutable segments:** footer metadata is cached unconditionally (sound
  because segments never change), cutting ~40% off warm scans.
- **Version-aware aggregate states:** OHLCV/VWAP rollups persist mergeable
  states per immutable segment; re-queries merge states in milliseconds
  instead of recomputing, scanning only newly appended segments.
- **No kernel heroics:** generic scans and aggregations run on stock
  DataFusion and tie the best engines; h5i-db only adds structure where
  time-series shape makes that structure pay.

---

## Quant workflows

`h5i_db.quant` runs the standard research loop against the engine, and every
result records the data version it was computed from.

```python
from h5i_db import quant

panel = quant.build_panel(db, "signals", "prices",
                          periods=(1, 5, 10), quantiles=5,
                          snapshot="2024-q1")     # the pin

panel.ic()                  # per-date rank IC, one column per horizon
panel.quantile_returns()    # mean forward return per bucket
quant.factor_report(panel, path="factor.html")
```

Factor statistics match `alphalens-reloaded` and portfolio statistics match
`empyrical-reloaded`, so the numbers are the ones you already trust; what is
new is that they are attributable. A report leads with the version SHA and
the pin it ran under, an unpinned run says so, and `quant.verify()` refuses
to certify a result that cannot be reproduced.

Three things follow from the storage layer rather than the statistics:

- **`event_time_cutoff=`** restricts every read to what was knowable at a
  decision time, so a forward return that would need a later price is
  dropped rather than computed.
- **`quant.sweep()`** runs a parameter grid with one fork per trial, so
  trials cannot contaminate each other and all of them compare in a single
  cross-fork query.
- **`quant.restatement_impact()`** re-runs one computation at two data
  versions and reports what a vendor's revision moved.

Selection bias gets first-class statistics rather than a footnote, because a
number found by searching is worth less than the same number found once:

- **`quant.deflated_sharpe(returns, trials=N)`** discounts a Sharpe ratio by
  the size of the search that found it, and `minimum_track_record_length()`
  says how long a record must be before the ratio means anything.
- **`quant.probability_of_backtest_overfitting(matrix)`** runs combinatorially
  symmetric cross-validation over a sweep's trials: a PBO near 0.5 means the
  in-sample winner carried no information.
- **`quant.purged_kfold()`**, **`combinatorial_purged()`** and
  **`walk_forward()`** split on horizons and embargo, so a label that depends
  on the next ten bars cannot leak into its own training fold. Horizons are
  never guessed: omitting them says labels are instantaneous.
- **`quant.fit_impact()`** calibrates a slippage model from realised fills
  instead of assuming a cost constant.

### Backtesting

`h5i-db-backtest` is an event-driven backtester whose data plane is the
database. A run executes inside a fork and writes `bt_orders`, `bt_fills`,
`bt_positions` and `bt_equity` there, so results are queryable with the same
SQL as market data and two runs diff at fill level with `fork_diff`.

```python
fork = db.fork("bt-momentum-001")
quant.tearsheet(quant.from_levels(fork, "bt_equity"), path="run.html")
```

It is also fast, because replay reads decoded records straight out of the
storage layer rather than crossing a language boundary per event. On one
shared workload (200k top-of-book updates, 200 market orders, one instrument,
every adapter verifying it saw all of them):

| engine | measured boundary | median | throughput |
|---|---|---:|---:|
| **h5i-db** | decoded records through the replay kernel | **65.7 ms** | **3.05 M events/s** |
| **h5i-db** | full persisted run: scan, decode, fork, replay, write | 331 ms | 605 k events/s |
| NautilusTrader 1.230.0 | in-memory objects through `BacktestEngine.run()` | 767 ms | 261 k events/s |
| LEAN `11ba019f6` | first `Slice` callback to `OnEndOfAlgorithm`, disk-fed | 2,033 ms | 98.4 k events/s |

Even the persisted boundary, which does strictly more work than the other two,
is 2.3× NautilusTrader's in-memory engine and 6.1× LEAN's measured callback
throughput. This is one narrow event-driven workload, not a ranking of
backtest systems; the boundaries differ and the benchmark checks event and
order counts, not PnL equivalence. Methodology, raw samples and the reasons
each boundary was drawn where it was:
[benchmarks/backtest_compare/RESULTS.md](benchmarks/backtest_compare/RESULTS.md).

What the simulation itself covers:

- **A run is a pure function of (data pin, strategy, config).** No wall clock,
  no unseeded randomness, no unsorted hash-map iteration. `result.verify()`
  re-executes a stored run and reports whether it reproduced.
- **Look-ahead is closed structurally, not by convention.** Records carry
  `ts_event` and `ts_init` and replay in `ts_init` order, so late data arrives
  late; a strategy has no route to a market's resolution.
- **Settlement is gated on observability.** A three-day replay of a six-month
  market leaves its position unsettled and says why, rather than booking a
  profit nobody trading that window could have collected. Both numbers
  survive: mark-to-market and settled PnL, with the difference reported as an
  explicit adjustment.
- **Corporate actions apply forward, never backward.** Nobody ever traded the
  split-adjusted price, so splits, dividends and delistings arrive as events
  at the instant they take effect and act on positions, resting limits and
  marks. Adjustment factors are point-in-time data; an unannounced action is
  simply not in the stream. A ticker resolves to an instrument over half-open
  spans, and an ambiguous lookup is refused with the candidates named.
- **Accounts are multi-currency,** with margin, liquidation, perpetual
  funding, order amendment, self-trade prevention and pre-trade risk limits.
- **Preflight refuses claims the data cannot support.** `backtest.inspect()`
  reports a replay fidelity, and asking for queue-position fills from periodic
  snapshots is an error rather than a plausible-looking number.
- **Strategies come in three shapes:** signals or command tables (the
  strategy as data, no callback code and no language boundary in the loop),
  Python `EventStrategy` callbacks, and the native Rust `Strategy` trait.
- **Venue coverage:** prediction markets are the first venue with N-outcome
  markets as the general case, via Kalshi, Polymarket and Hyperliquid loaders
  that all produce the same canonical tables. `KalshiFees` implements the
  actual quadratic fee curve, centicent rounding and per-order partial-fill
  accumulator, not `notional × rate`.

See the [quant](https://db.h5i.dev/manual/quant/) and
[backtesting](https://db.h5i.dev/manual/backtest/) manual pages.

---

## Why for agents

- **Reproducible inputs:** every read resolves to a version, so "which data did
this run see" has an answer, and re-running against that version is O(1) rather
than an archaeology project.

- **Point-in-time pulls:** a read point can be pinned on two axes: event time
(`--decision-time`) and arrival (`--as-of`). The frame you hand to pandas is
then bounded at the source, which is the only place a bound survives the trip
into Python. `arrival-delta` measures, after the fact, how much of a result
depended on data that arrived later.

- **Don't let a result destroy the context window.** `H5I_DB_PROFILE=agent` caps
every query and spills the rest to Parquet, reporting the true row count and
where the withheld rows live.

- **One call to get oriented:** `h5i-db context <db>` returns every table's
schema, size, time range and head version, the operations policy gates, and
any plan already staged.

- **Errors that can be acted on:** the stderr envelope carries `next_actions`
(runnable commands), `did_you_mean` for typos, and a `retryable` flag.

- **Branch without copying.** `fork` opens a writable workspace over a pinned
view of every table and duplicates no data, so an edit or an experiment costs
one small file and is as cheap to discard as to keep. `forks('trades')` then
reads that table across every branch at once with a `__fork` column, so
comparing what each one produced needs no export step.

- **A trial is identified by its content, not its name.** A pinned, declarative
`BacktestConfig` hashes to a `trial_digest` over every replay input, ignoring the
run id and descriptive metadata. Re-submitting the same semantic trial returns
the recorded result with `cached=True` instead of forking and replaying again,
and lookup-plus-creation is serialized across local agent processes, so a retry
loop cannot spend a second run or double-count a score.

- **The review surface routes attention rather than ranking.** `h5i-db ui` orders
trials by what needs a human next: decision required, then failed or warned, then
finished and unseen, then running, then seen. Scanning a list does not mark work
reviewed; a trial counts as seen only when its detail is opened. The leaderboard
is a separate tab, because "best so far" and "what did I not look at" are
different questions.

- **Mistakes are cheap.** Mutations preview through `plan`/`apply` and policy can
require that gate; `--idempotency-key` makes a retried ingest replay instead of
double-appending; an opt-in `data-policy` rejects malformed rows fail-closed;
commits are fsync-before-swap with a manifest hash chain, tested by killing the
writer at every step.

---

## When *not* to use h5i-db

- **Distributed, multi-terabyte warehouses:** single-node and embedded by
  design. Reach for ClickHouse, Snowflake or a lakehouse.
- **OLTP or high-concurrency serving:** one writer at a time, no row-level
  MVCC, no interactive transactions. Use Postgres.
- **Sub-microsecond tick capture:** the write cadence this is built for is
  minute bars, end-of-day, and vendor files, not the capture layer itself.
  That is kdb+ territory.
- **Databases with no time column:** the whole design assumes a time index;
  without one you lose pruning, the ASOF join, and point-in-time reads.
- **Live trading:** the backtester never routes a real order. There are no
  brokerage adapters, no portfolio optimiser and no plotting API; the boundary
  is simulation and evaluation.

---

## Development

```bash
cargo test --workspace          # ~290 tests incl. crash-safety fault injection
cargo run -p h5i-db-bench --profile bench-fast -- --trades 1000000
cargo run -p h5i-db-bench --profile bench-fast --bin h5i-db-fork-bench
python3 benchmarks/backtest_compare/run.py \
  --output benchmarks/backtest_compare/results.json   # vs NautilusTrader and LEAN
```

Workspace crates under `crates/`: `core` (versioned storage kernel), `query`
(DataFusion layer), `backtest` (replay kernel, venue models, settlement),
`venues` (Kalshi, Polymarket, Hyperliquid loaders), `cli` (the agent-facing
binary), `ui` (review surface), `observability`, `python`
(`pip install h5i-db`), `bench`.

---

## License

Apache-2.0. See [LICENSE](./LICENSE).
