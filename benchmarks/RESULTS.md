# h5i-db benchmark results

Date: 2026-07-22 · Machine: WSL2 Linux, 10 cores, 7 GiB RAM (aarch64) ·
Build: `--profile bench-fast` (opt, thin-LTO off) · Seed 42.

Dataset: **20 M trades + 5 M quotes**, 64 symbols, random-walk prices,
nanosecond timestamps, ingested as 50 append commits → 50 Parquet segments,
147 MiB on disk (zstd).

Reproduce:

```bash
cargo run -p h5i-db-bench --profile bench-fast -- --trades 20000000 --quotes 5000000
python benchmarks/compare_baselines.py <dir>/bench.db \
    [--engines polars,duckdb,pandas,pyarrow,arcticdb]   # needs those packages
    # engines whose package is missing are skipped with a note; arcticdb has
    # no Linux aarch64 wheels, so it only runs on x86_64/macOS machines. It
    # reads from its own LMDB store (populated once, sibling `<db>.arctic`).
```

The query-local P0 regression workload reuses the generated `bench.db` and an
already-built CLI binary, so it does not compile or link another DataFusion
test target:

```bash
python benchmarks/run_performance_workload.py \
    --binary target/bench-fast/h5i-db \
    --db <dir>/bench.db \
    --output current-performance.json

# On the same machine and dataset, require identical results and no gated
# median query-time regression greater than 10%.
python benchmarks/run_performance_workload.py \
    --binary target/bench-fast/h5i-db \
    --db <dir>/bench.db \
    --baseline baseline-performance.json \
    --output current-performance.json
```

The checked-in workload runs one warm-up and five measured repetitions of a
cross-sectional control and the symbol/time predicate shape targeted by P2.
Each result records physical scan metrics, a result checksum, the binary and
workload hashes, and machine/compiler metadata. Raw SQL and result rows are not
copied into the result artifact.

The Rust harness also emits `aggregate states: cold OHLCV + VWAP` and
`aggregate states: warm OHLCV + VWAP`. Their detail objects report state hits,
builds, scanned segments/rows/bytes, corruption recovery, and eviction counts.
The warm result is value-equivalent in-process and reports zero segments
scanned. Full 20 M-row run (50 segments, 64 symbols, release profile,
2026-07-22, WSL2):

| case | wall | detail |
|---|---|---|
| aggregate states: cold OHLCV + VWAP | 2445 ms | 50 built, 20 M rows scanned |
| aggregate states: warm OHLCV + VWAP | 30.9 ms | 50/50 reused, 0 rows scanned |

Warm reuse requires serde_json's `float_roundtrip` feature (workspace-enabled):
sidecar verification re-serializes parsed JSON, and the default lossy f64
parse made every full-mantissa state fail its checksum and silently rebuild —
warm equaled cold until that fix. The regression is pinned by a unit test in
`aggregate_state.rs`.

Two honest limits observed on this dataset: the checked-in P2 workload case
(`symbol = … AND ts range`) gets **no** physical-scan reduction from the
predicate cache because symbols interleave uniformly, so every surviving row
group contains the symbol; and the predicate shape that *does* cluster
(symbol + price band) is rejected because Float64 columns are outside the
eligibility contract. The cache's byte-reduction exit gate currently only
fires on correlated int/string/timestamp predicates like the one in
`query_misc.rs`.

The case the cache exists for is demonstrated by
`benchmarks/predicate_cache_scenario.py` (needs pyarrow and a built CLI): an
episodic symbol trading only inside a narrow window of time-ordered segments,
named so per-column min/max statistics cannot prune it. At the default
4 M rows, warm hits scan **75% fewer physical bytes** (38.2 MB → 9.7 MB,
3.69 M → 0.93 M rows) with identical results; the reduction tracks the
clustering ratio and row-group granularity, and translates to proportionally
fewer range GETs once segments live on remote object storage. Wall time
barely moves against a warm local page cache — physical bytes, not local
latency, is the metric this prototype optimizes.

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

## 2026-07-23 cloud-VM run: ArcticDB added (x86_64, 20 M rows)

First run including the ArcticDB baseline (no Linux aarch64 wheels, so it
cannot run on the WSL2 dev machine). Environment: shared cloud CPU VM —
Intel(R) Xeon(R) CPU @ 2.20 GHz, 31 GB RAM, Ubuntu 22.04.5 LTS. Absolute
times are roughly 5–10× slower than the bare-metal numbers above and are
**not comparable across sections**; within-session ratios are the signal.
All six engines measured back-to-back in one session, `--repeat 5`.
ArcticDB 6.19 reads from its own LMDB store (populated once from the same
data — one-time ingest 9.7 s); its OHLCV/ASOF compute is pandas over store
reads, its idiomatic usage.

| Workload | h5i-db | Polars 1.35 | DuckDB 1.3 | pandas 2.3 | PyArrow 24 | ArcticDB 6.19 |
|---|---:|---:|---:|---:|---:|---:|
| Full aggregation | 353 ms | 370 ms | **228 ms** | 2 578 ms | 553 ms | 886 ms |
| Time-range scan 0.01 % | 10.0 ms | 28.1 ms | 45.5 ms | 23.9 ms | 22.8 ms | **4.2 ms** |
| Time-range scan 1 % | 12.8 ms | 29.9 ms | 40.9 ms | 25.6 ms | 24.0 ms | **5.6 ms** |
| 1-min OHLCV + VWAP | **1 558 ms** | 7 309 ms | 7 237 ms | 5 115 ms | 7 121 ms | 3 504 ms |
| … re-run, unchanged data | **21.7 ms**³ | recompute | recompute | recompute | recompute | recompute |
| ASOF join (by symbol) | 1 548 ms | **1 485 ms** | 11 566 ms | 6 624 ms | n/a² | 7 008 ms |
| Ingest | 1.92 M rows/s (50 durable commits) | n/a | n/a | n/a | n/a | 25 M rows in 9.7 s (one-time load) |

³ Version-aware aggregate states: warm `finance_rollup` reused all 50
per-segment states (cold build 5 508 ms on this VM). The other engines have
no equivalent — re-running the query costs the full rollup again.

The pattern holds from the earlier section — OHLCV 2.2–4.7× faster than every
engine, ASOF tied with Polars and 4–7× faster than the rest, cross-sectional
within 1.5× of DuckDB — with one honest new loss: **ArcticDB's native time
index wins narrow time-range point reads** (4.2 vs 10.0 ms) against h5i-db's
manifest pruning (48/50 segments pruned), both far ahead of the
footer-scanning general engines. That gap is the P2 predicate-cache /
finer-time-index territory in `ROADMAP.md` Part II.

## Performance changes discovered while benchmarking

1. **Parquet footer-metadata cache** wired into the provider
   (`CachedParquetFileReaderFactory`): segments are immutable, so caching is
   unconditionally sound; saved ~40 % of warm full-scan latency at 50 segments.
2. **Row-level filter pushdown disabled** (`with_pushdown_filters`): measured
   ~2× slower on selective tick scans than decode-then-filter, because
   manifest pruning already removed irrelevant segments. Row-group/page
   pruning via the predicate stays on.
