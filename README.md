# h5i-db

**A fast, agent-native time-series database for quant research.
Embedded, written in Rust.**

- **Fast for time-series shape:** over 4.5× faster than DuckDB and Polars
  on OHLCV+VWAP rollups over 20M rows.
- **Native time-series SQL:** ASOF join, timezone-aware `time_bucket`,
  gapfill/resample, rolling windows, `vwap`, `ewma`.
- **Fork a database in milliseconds:** forks share data instead of copying it. 
  Agents can run wide trial-and-error loops (fork, mutate, evaluate, discard) 
  at almost zero cost.
- **Every write is an atomic, versioned commit:** any past version reads in
  O(1), so a bad ingest (human or agent) is one `restore` away from undone.
- **Safety policies for agent writes:** previewable mutations, policy gates,
  fail-closed constraints that block destructive operations, and an audit
  trail of what changed and why.
- **Point-in-time reads:** pin a decision time and the frame that reaches
  pandas cannot contain rows from after it. No lookahead bias, by construction.
- **Embedded:** one directory, no server, no daemon. Apache-2.0.

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
h5i-db ui market.db                                                # review surface
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
old = db.read("trades", version=1)                # time travel: read any past version

plan = db.plan_delete_range("trades", 1_700_0_000_000)
print(plan.summary)                               # preview the mutation before it lands
plan.apply()
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

---

## Development

```bash
cargo test --workspace          # ~290 tests incl. crash-safety fault injection
cargo run -p h5i-db-bench --profile bench-fast -- --trades 1000000
cargo run -p h5i-db-bench --profile bench-fast --bin h5i-db-fork-bench
```

Workspace crates under `crates/`: `core` (versioned storage kernel), `query`
(DataFusion layer), `cli` (the agent-facing binary), `ui` (review surface),
`python` (`pip install h5i-db`), `bench`.

---

## License

Apache-2.0. See [LICENSE](./LICENSE).
