# h5i-db

**The database that can show an agent the past and nothing else.**

An embedded, fully versioned analytical database for quant research, written in
Rust and Apache-2.0.

Look-ahead bias is the correctness bug in backtesting, and it gets worse when an
agent runs forty backtests overnight — nobody reviews forty results closely
enough to catch the one that quietly read tomorrow's close. h5i-db makes that
structurally impossible rather than checkable after the fact: pin a session to a
decision instant and no query inside it can read a row stamped later, or a
commit that had not arrived yet.

- **Point-in-time enforced, not offered.** `--decision-time` bounds every scan
  in the session; `--as-of` pins which commits exist. Both are part of the
  table, not a filter a query can forget or widen.
- **Immutable & versioned.** Every write is an atomic commit; any past version
  reads in O(1), and `leakage-check` quantifies what a restatement changed.
- **Built for agents, not chatbots.** One-call orientation, output budgets that
  protect a context window, and error envelopes carrying runnable recovery
  commands. No LLM inside the database.
- **Fast where time-series shape allows.** Over 4.5× faster than DuckDB and
  Polars for OHLCV+VWAP rollups on 20M rows, with full SQL via DataFusion
  (ASOF join, `time_bucket`, gapfill/resample, `vwap`, `ewma`).

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

The skill teaches an agent the safe driving pattern — discover → query with
limits → plan/apply for mutations — and ships in-repo at
[`skills/h5i-db/`](skills/h5i-db/SKILL.md) so it always matches this
repository's CLI.

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

- **Manifest pruning.** Every version's manifest carries per-segment time
  ranges and column min/max. Narrow queries prune whole segments before a
  single file is opened.
- **Declared sort order.** Segments are stored time-sorted and the query
  layer tells DataFusion so. OHLCV rollups stream instead of sorting 20M rows
  first (every baseline pays that sort), and the ASOF join is sort-free.
- **Immutable segments.** Footer metadata is cached unconditionally (sound
  because segments never change), cutting ~40% off warm scans.
- **Version-aware aggregate states.** OHLCV/VWAP rollups persist mergeable
  states per immutable segment; re-queries merge states in milliseconds
  instead of recomputing, scanning only newly appended segments.
- **No kernel heroics.** Generic scans and aggregations run on stock
  DataFusion and tie the best engines; h5i-db only adds structure where
  time-series shape makes it structurally faster.

---

## Why for agents

The premise is that the agent lives *outside* the database. Nothing here
generates SQL or embeds a model; the database's job is to be legible and
impossible to corrupt, and everything below follows from that.

- **Show it the past, and nothing else.** A research session is pinned on two
axes — event time (`--decision-time`, so a window cannot overrun forwards) and
arrival (`--as-of`, so a later restatement stays invisible). Both are enforced
in the table, so a query that explicitly asks for the future still gets none.
A table that cannot be bounded refuses the session rather than being quietly
exempt.

```bash
export H5I_DB_DECISION_TIME=2026-07-01T00:00:00Z   # pins the whole session
h5i-db query market.db "SELECT vwap(price, size) FROM trades"
```

- **Don't let a result destroy the context window.** `H5I_DB_PROFILE=agent` caps
every query and spills the rest to Parquet, reporting the true row count and
where the withheld rows live. Output never changes based on whether stdout is
a terminal.

- **One call to get oriented.** `h5i-db context <db>` returns every table's
schema, size, time range and head version, the operations policy gates, and
any plan already staged — deterministic, so it can be cached, and `--budget`
caps it in tokens.

- **Errors that can be acted on.** The stderr envelope carries `next_actions`
(runnable commands), `did_you_mean` for typos, and a `retryable` flag. A CI
test executes the commands the binary suggests, so they cannot rot into
plausible fiction.

- **Mistakes are cheap.** Mutations preview through `plan`/`apply` and policy can
require that gate; `--idempotency-key` makes a retried ingest replay instead of
double-appending; an opt-in `data-policy` rejects malformed rows fail-closed;
commits are fsync-before-swap with a manifest hash chain, tested by killing the
writer at every step.

---

## When *not* to use h5i-db

- **Distributed, multi-terabyte warehouses.** Single-node and embedded by
  design. Reach for ClickHouse, Snowflake or a lakehouse.
- **OLTP or high-concurrency serving.** One writer at a time, no row-level
  MVCC, no interactive transactions. Use Postgres.
- **Sub-microsecond tick capture.** The write cadence this is built for is
  minute bars, end-of-day, and vendor files — not the capture layer itself.
  That is kdb+ territory.
- **Databases with no time column.** The whole design assumes a time index;
  without one you lose pruning, the ASOF join, and research mode entirely.

---

## Development

```bash
cargo test --workspace          # 60+ tests incl. crash-safety fault injection
cargo run -p h5i-db-bench --profile bench-fast -- --trades 1000000
```

Workspace crates under `crates/`: `core` (versioned storage kernel), `query`
(DataFusion layer), `cli` (the agent-facing binary), `ui` (review surface),
`python` (`pip install h5i-db`), `bench`.

---

## License

Apache-2.0. See [LICENSE](./LICENSE).
