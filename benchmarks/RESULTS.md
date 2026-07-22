# h5i-db benchmark results

Date: 2026-07-22 · Machine: WSL2 Linux, 10 cores, 7 GiB RAM (aarch64) ·
Build: `--profile bench-fast` (opt, thin-LTO off) · Seed 42.

Dataset: **20 M trades + 5 M quotes**, 64 symbols, random-walk prices,
nanosecond timestamps, ingested as 50 append commits → 50 Parquet segments,
147 MiB on disk (zstd).

Reproduce:

```bash
cargo run -p h5i-db-bench --profile bench-fast -- --trades 20000000 --quotes 5000000
python crates/h5i-db-bench/compare_baselines.py <dir>/bench.db \
    [--engines polars,duckdb,pandas,pyarrow]   # needs those packages
```

On small machines run one engine per invocation (`--engines <one>`), ideally
under a cgroup cap (`systemd-run --user --scope -p MemoryMax=...`): a 20 M-row
eager sort/join can otherwise OOM the whole machine. `ulimit -v` is *not* a
usable guard — polars/duckdb reserve virtual address space far beyond RSS and
abort spuriously.

## Core numbers (single run after one warm-up)

| Workload | h5i-db |
|---|---:|
| Ingest (50 append commits, full durability) | 3.74 M rows/s |
| Full aggregation `GROUP BY symbol` | 47 ms |
| Time-range scan 0.01 % (48/50 segments pruned) | 7.3 ms |
| Time-range scan 1 % (48/50 pruned) | 6.3 ms |
| Time-range scan 100 % | 26 ms |
| 1-minute OHLCV + VWAP rollup | 164 ms |
| ASOF join trades × quotes (by symbol) | 439 ms |
| Quant pipeline (asof → OHLCV → log-returns) | 534 ms |
| Cold read of version 3 (metadata only) | 1.4 ms |
| `as_of` timestamp resolution (binary search) | 2.6 ms |

## vs raw DataFusion over the identical Parquet files

Isolates the cost of h5i-db's versioned-metadata layer.

| Workload | h5i-db | raw DataFusion |
|---|---:|---:|
| Full aggregation | 47 ms | 33 ms¹ |
| 1 % time-range scan | 6.3 ms | 9.4 ms |

¹ With equally warm caches (interleaved control runs) the full-aggregation gap
is ~20 % (23 vs 19 ms at 10 M rows); the headline numbers retain benchmark-order
cache bias. On *selective* queries h5i-db is at parity or faster because
manifest pruning skips whole objects before any I/O.

## vs disk-backed baselines (best of 3, same segment files)

The honest comparison per DESIGN_CLAUDE.md: storage included on both sides —
every engine reads the identical Parquet segments from disk each iteration.
Preloaded in-memory DataFrames are a different (unfair) contest. All engines
were measured back-to-back in one session on the same dataset.

Baselines: Polars 1.43 (lazy `scan_parquet`), DuckDB 1.5.4 (`read_parquet`
SQL, Arrow output), pandas 3.0.3 (pyarrow-backed `read_parquet`), PyArrow 25.0
(`dataset` + Acero compute).

| Workload | h5i-db | Polars | DuckDB | pandas | PyArrow |
|---|---:|---:|---:|---:|---:|
| Full aggregation | 47 ms | 90 ms | 47 ms | 653 ms | 113 ms |
| Time-range scan 0.01 % | **7.3 ms** | 15.0 ms | 17.0 ms | 13.5 ms | 10.1 ms |
| Time-range scan 1 % | **6.3 ms** | 14.1 ms | 15.0 ms | 13.3 ms | 10.3 ms |
| 1-min OHLCV + VWAP | **164 ms** | 2 392 ms | 774 ms | 4 242 ms | 2 369 ms |
| ASOF join (by symbol) | 439 ms | 408 ms | 952 ms | 4 535 ms | n/a² |

² PyArrow's experimental `Table.join_asof` measured 44.5 s at 2 M rows
(~780× slower than Polars) — excluded at 20 M; `--pyarrow-asof` runs it anyway.
The pandas/pyarrow ASOF and scan workloads read only the columns the query
needs, matching the projection pushdown the optimizer engines apply — eager
full-width reads would be both unfair and an OOM risk at this scale.

Why the pattern looks like this (and was predicted to):

- **Narrow scans**: the version manifest carries per-segment time ranges +
  column min/max, so pruning happens before any file is opened. Every baseline
  must at least touch footers/statistics of all 50 files in the glob.
- **OHLCV rollup**: h5i-db storage is already time-sorted and the provider
  declares that ordering, so DataFusion streams the bucketed aggregation. All
  four baselines pay for not knowing the data is sorted: Polars/pandas/pyarrow
  need an explicit 20 M-row sort first; DuckDB's ordered aggregates
  (`first(price ORDER BY ts)`) do a per-group sort. This is the widest gap
  (4.7–26×) and the most structural: it comes from versioned-manifest metadata,
  not kernel quality.
- **ASOF join**: Polars and h5i-db are effectively tied — both have a real
  asof operator over sorted inputs; h5i-db skips the sort when scan order is
  already (time), which storage guarantees. DuckDB's ASOF JOIN and pandas'
  `merge_asof` trail; pyarrow has no practical asof at this scale.
- **Full scans**: pure decode+hash throughput — DuckDB matches h5i-db,
  Polars/pyarrow are close, and h5i-db intentionally does not try to
  out-kernel them (DESIGN_CLAUDE.md §7 Tier 3). Versioning adds ≤ ~20 % on
  this shape. pandas pays eager-materialization cost even here.

Positioning: *analytical performance at parity with the best disk-backed
engines on generic shapes, structurally faster on time-series shapes* — plus
the things none of these baselines do at all: time travel, snapshots,
previewable mutations, crash-safe atomic commits.

## Performance changes discovered while benchmarking

1. **Parquet footer-metadata cache** wired into the provider
   (`CachedParquetFileReaderFactory`): segments are immutable, so caching is
   unconditionally sound; saved ~40 % of warm full-scan latency at 50 segments.
2. **Row-level filter pushdown disabled** (`with_pushdown_filters`): measured
   ~2× slower on selective tick scans than decode-then-filter, because
   manifest pruning already removed irrelevant segments. Row-group/page
   pruning via the predicate stays on.
