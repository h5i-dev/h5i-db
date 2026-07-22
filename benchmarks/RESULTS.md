# h5i-db benchmark results

Date: 2026-07-22 · Machine: WSL2 Linux, 10 cores, 7 GiB RAM (aarch64) ·
Build: `--profile bench-fast` (opt, thin-LTO off) · Seed 42.

Dataset: **20 M trades + 5 M quotes**, 64 symbols, random-walk prices,
nanosecond timestamps, ingested as 50 append commits → 50 Parquet segments,
147 MiB on disk (zstd).

Reproduce:

```bash
cargo run -p h5i-db-bench --profile bench-fast -- --trades 20000000 --quotes 5000000
python crates/h5i-db-bench/polars_compare.py <dir>/bench.db   # needs polars
```

## Core numbers (single run after one warm-up)

| Workload | h5i-db |
|---|---:|
| Ingest (50 append commits, full durability) | 3.16 M rows/s |
| Full aggregation `GROUP BY symbol` | 61 ms |
| Time-range scan 0.01 % (48/50 segments pruned) | 5.3 ms |
| Time-range scan 1 % (48/50 pruned) | 6.7 ms |
| Time-range scan 100 % | 22 ms |
| 1-minute OHLCV + VWAP rollup | 142 ms |
| ASOF join trades × quotes (by symbol) | 364 ms |
| Quant pipeline (asof → OHLCV → log-returns) | 465 ms |
| Cold read of version 3 (metadata only) | 1.5 ms |
| `as_of` timestamp resolution (binary search) | 3.7 ms |

## vs raw DataFusion over the identical Parquet files

Isolates the cost of h5i-db's versioned-metadata layer.

| Workload | h5i-db | raw DataFusion |
|---|---:|---:|
| Full aggregation | 61 ms | 35 ms¹ |
| 1 % time-range scan | 6.7 ms | 7.1 ms |

¹ With equally warm caches (interleaved control runs) the full-aggregation gap
is ~20 % (23 vs 19 ms at 10 M rows); the headline numbers retain benchmark-order
cache bias. On *selective* queries h5i-db is at parity or faster because
manifest pruning skips whole objects before any I/O.

## vs Polars 1.43 (disk-backed `scan_parquet`, best of 3)

The honest comparison per DESIGN_CLAUDE.md: storage included on both sides,
same segment files. Preloaded in-memory DataFrames are a different (unfair)
contest.

| Workload | h5i-db | Polars | ratio |
|---|---:|---:|---:|
| Full aggregation | 61 ms | 54 ms | 0.9× |
| Time-range scan 0.01 % | **5.3 ms** | 14.9 ms | **2.8×** |
| Time-range scan 1 % | **6.7 ms** | 10.5 ms | **1.6×** |
| 1-min OHLCV + VWAP | **142 ms** | 1 575 ms | **11×** |
| ASOF join (by symbol) | **364 ms** | 475 ms | **1.3×** |

Why the pattern looks like this (and was predicted to):

- **Narrow scans**: the version manifest carries per-segment time ranges +
  column min/max, so pruning happens before any file is opened. Polars must
  at least touch footers/statistics of every file in the glob.
- **OHLCV rollup**: h5i-db storage is already time-sorted and the provider
  declares that ordering, so DataFusion streams the bucketed aggregation;
  Polars' `group_by_dynamic` needs an explicit 20 M-row sort first.
- **ASOF join**: both engines are strong; h5i-db's operator skips the sort
  when scan order is already (time), which storage guarantees.
- **Full scans**: pure decode+hash throughput — Polars is excellent here and
  h5i-db intentionally does not try to out-kernel it (DESIGN_CLAUDE.md §7
  Tier 3). Versioning adds ≤ ~20 % on this shape.

Positioning: *Polars-class analytical performance with durable versioning and
storage-aware time-series execution* — plus the things a DataFrame library
does not do at all: time travel, snapshots, previewable mutations, crash-safe
atomic commits.

## Performance changes discovered while benchmarking

1. **Parquet footer-metadata cache** wired into the provider
   (`CachedParquetFileReaderFactory`): segments are immutable, so caching is
   unconditionally sound; saved ~40 % of warm full-scan latency at 50 segments.
2. **Row-level filter pushdown disabled** (`with_pushdown_filters`): measured
   ~2× slower on selective tick scans than decode-then-filter, because
   manifest pruning already removed irrelevant segments. Row-group/page
   pruning via the predicate stays on.
