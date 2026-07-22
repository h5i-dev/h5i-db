# h5i-db

**An embedded, versioned time-series database for quant workloads and AI
agents, written in Rust.**

Every write is an atomic commit producing an immutable version. Full SQL via
DataFusion with native time-series operators (ASOF join, `time_bucket`,
`vwap`, `ewma`). Mutations can be previewed before they commit and gated by
policy. Crash-safe by construction and proven by tests that kill the writer
at every commit step.

📖 **[Documentation & demo films](https://koukyosyumei.github.io/h5i-db/)** ·
[Design document](DESIGN_CLAUDE.md) · [Benchmarks](benchmarks/RESULTS.md) ·
[Agent guide](SKILL.md)

## Quickstart

```bash
cargo install --path crates/h5i-db-cli      # or: pip install h5i-db

h5i-db init market.db
h5i-db create-table market.db trades --like ticks.parquet --time-column ts
h5i-db ingest market.db trades ticks.parquet
h5i-db query market.db "SELECT symbol, vwap(price,size) FROM trades GROUP BY symbol"
h5i-db query market.db "SELECT count(*) FROM h5i('trades', 1)"     # time travel
h5i-db ui market.db                                                # review surface
```

## Why

| | DuckDB | Polars | ArcticDB | **h5i-db** |
|---|---|---|---|---|
| User-facing versioning / time travel | ✗¹ | ✗ | ✓ | ✓ (O(1) version reads) |
| SQL joins/windows/CTEs | ✓ | partial | ✗ | ✓ (DataFusion) |
| ASOF join | ✓ | ✓ | ✗ | ✓ (sort-free on sorted storage) |
| Previewable mutations (plan/apply) | ✗ | ✗ | ✗ | ✓, policy-enforceable |
| Concurrent writers | MVCC | n/a | unsafe² | CAS + explicit conflict |
| 20M-row narrow time-range scan | — | 14.9 ms | — | **5.3 ms** |
| 20M-row 1-min OHLCV+VWAP | — | 1 575 ms | — | **142 ms** |

¹ `AT (VERSION …)` syntax exists but native storage rejects it.
² Documented single-writer-per-symbol assumption.

## Workspace

| crate | role |
|---|---|
| `h5i-db-core` | versioned storage kernel (no query engine dependency) |
| `h5i-db-query` | DataFusion layer: pruning provider, ASOF join, time-series functions |
| `h5i-db-cli` | `h5i-db` binary — the agent-facing contract |
| `h5i-db-ui` | loopback review UI (plan approval, version diff, SQL scratchpad) |
| `h5i-db-python` | `pip install h5i-db` (pyarrow interop) |
| `h5i-db-bench` | benchmark harness + Polars comparison |

## Development

```bash
cargo test --workspace          # 60+ tests incl. crash-safety fault injection
cargo run -p h5i-db-bench --profile bench-fast -- --trades 1000000
```

License: Apache-2.0
