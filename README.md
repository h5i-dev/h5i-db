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

| | DuckDB | Polars | pandas | PyArrow | ArcticDB | **h5i-db** |
|---|---|---|---|---|---|---|
| User-facing versioning / time travel | ✗¹ | ✗ | ✗ | ✗ | ✓ | ✓ (O(1) version reads) |
| SQL joins/windows/CTEs | ✓ | partial | ✗ | ✗ | ✗ | ✓ (DataFusion) |
| ASOF join | ✓ | ✓ | ✓ | ✗² | ✗ | ✓ (sort-free on sorted storage) |
| Previewable mutations (plan/apply) | ✗ | ✗ | ✗ | ✗ | ✗ | ✓, policy-enforceable |
| Concurrent writers | MVCC | n/a | n/a | n/a | unsafe³ | CAS + explicit conflict |
| 20M-row narrow time-range scan | 17.0 ms | 15.0 ms | 13.5 ms | 10.1 ms | — | **7.3 ms** |
| 20M-row 1-min OHLCV+VWAP | 774 ms | 2 392 ms | 4 242 ms | 2 369 ms | — | **164 ms** |

¹ `AT (VERSION …)` syntax exists but native storage rejects it.
² Experimental `join_asof` exists but is ~1000× slower — impractical at this scale.
³ Documented single-writer-per-symbol assumption.

All engines disk-backed over identical Parquet segments, measured in one
session; full methodology in [benchmarks/RESULTS.md](benchmarks/RESULTS.md).

## Workspace

| crate | role |
|---|---|
| `h5i-db-core` | versioned storage kernel (no query engine dependency) |
| `h5i-db-query` | DataFusion layer: pruning provider, ASOF join, time-series functions |
| `h5i-db-cli` | `h5i-db` binary — the agent-facing contract |
| `h5i-db-ui` | loopback review UI (plan approval, version diff, SQL scratchpad) |
| `h5i-db-python` | `pip install h5i-db` (pyarrow interop) |
| `h5i-db-bench` | benchmark harness + polars/duckdb/pandas/pyarrow comparison |

## Development

```bash
cargo test --workspace          # 60+ tests incl. crash-safety fault injection
cargo run -p h5i-db-bench --profile bench-fast -- --trades 1000000
```

License: Apache-2.0
