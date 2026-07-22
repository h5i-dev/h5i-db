# h5i-db

**An embedded, versioned time-series database for quant workloads and AI
agents, written in Rust.**

Every write is an atomic commit producing an immutable version. Full SQL via
DataFusion with native time-series operators (SQL ASOF join, timezone-aware
`time_bucket`, gapfill/resample, rolling windows, `vwap`, `ewma`) and
append-only streaming tails. Mutations can be previewed before they commit and
gated by policy. Crash-safe by construction and proven by tests that kill the
writer at every commit step.

📖 **[Documentation & demo films](https://h5i-dev.github.io/h5i-db/)** ·
[Design document](DESIGN.md) · [Benchmarks](benchmarks/RESULTS.md) ·
[Operations guide](docs/OPERATIONS.md) · [Agent guide](SKILL.md)

## Quickstart

```bash
cargo install --path crates/h5i-db-cli
# Python: pip install h5i-db  (first PyPI release pending — until then:
#   maturin build --release -m crates/h5i-db-python/Cargo.toml)

h5i-db init market.db
h5i-db create-table market.db trades --like ticks.parquet --time-column ts
h5i-db ingest market.db trades ticks.parquet
h5i-db query market.db "SELECT symbol, vwap(price,size) FROM trades GROUP BY symbol"
h5i-db query market.db "SELECT count(*) FROM h5i('trades', 1)"     # time travel
h5i-db ui market.db                                                # review surface
```

## Why

| | DuckDB | Polars | pandas | PyArrow | ArcticDB | **h5i-db** |
|---|---|---|---|---|---|---|
| User-facing versioning / time travel | ✗¹ | ✗ | ✗ | ✗ | ✓ | ✓ (O(1) version reads) |
| SQL joins/windows/CTEs | ✓ | partial | ✗ | ✗ | ✗ | ✓ (DataFusion) |
| ASOF join | ✓ | ✓ | ✓ | ✗² | ✗ | ✓⁴ (sort-free on sorted storage) |
| Previewable mutations (plan/apply) | ✗ | ✗ | ✗ | ✗ | ✗ | ✓, policy-enforceable |
| Concurrent writers | MVCC | n/a | n/a | n/a | unsafe³ | CAS + explicit conflict |
| 20M-row narrow time-range scan | 17.0 ms | 15.0 ms | 13.5 ms | 10.1 ms | — | **7.3 ms** |
| 20M-row 1-min OHLCV+VWAP | 774 ms | 2 392 ms | 4 242 ms | 2 369 ms | — | **164 ms** |

¹ `AT (VERSION …)` syntax exists but native storage rejects it.
² Experimental `join_asof` exists but is ~1000× slower — impractical at this scale.
³ Documented single-writer-per-symbol assumption.
⁴ Via the `asof_join(...)` table function in SQL (and Python) — not the
  `ASOF JOIN` keyword syntax DuckDB uses.

All engines disk-backed over identical Parquet segments, measured in one
session; full methodology in [benchmarks/RESULTS.md](benchmarks/RESULTS.md).

## Why it's fast

The speed comes from the *versioning*, not from custom kernels:

- **Manifest pruning.** Every version's manifest carries per-segment time
  ranges and column min/max. Narrow queries prune whole segments before a
  single file is opened — the baselines must at least touch the footers of
  all 50 files in the glob.
- **Declared sort order.** Segments are stored time-sorted and the query
  layer tells DataFusion so. OHLCV rollups stream instead of sorting 20M rows
  first (every baseline pays that sort), and the ASOF join is sort-free.
- **Immutable segments.** Footer metadata is cached unconditionally — sound
  because segments never change — cutting ~40% off warm scans.
- **No kernel heroics.** Generic scans and aggregations run on stock
  DataFusion and tie the best engines; h5i-db only adds structure where
  time-series shape makes it structurally faster.

## Why for agents

An agent's failure modes are exactly what the storage model removes:

- **Every write is an atomic, immutable commit** — a bad ingest or mutation
  is one `restore` away from undone, and old versions read in O(1).
- **Previewable mutations.** `plan` shows exactly what a `DELETE`/`UPDATE`
  will touch before `apply`, and policy can require that gate — the agent
  proposes, the human (or a rule) approves.
- **Crash-safe by construction.** fsync-before-swap, checksums, a manifest
  hash chain — proven by tests that kill the writer at every commit step. An
  agent killed mid-write cannot corrupt the store.
- **An auditable trail.** Version history records what changed and when;
  the review UI gives humans a diff-and-approve surface over it.

## Development

```bash
cargo test --workspace          # 60+ tests incl. crash-safety fault injection
cargo run -p h5i-db-bench --profile bench-fast -- --trades 1000000
```

Workspace crates under `crates/`: `core` (versioned storage kernel), `query`
(DataFusion layer), `cli` (the agent-facing binary), `ui` (review surface),
`python` (`pip install h5i-db`), `bench`.

License: Apache-2.0
