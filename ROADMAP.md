# h5i-db Roadmap

Living roadmap. Last full update 2026-07-22 (branch `improve-performance`);
Parts III–IV added 2026-07-23 (branch `improve-tests`); Part V (agent-facing
surfaces, from a 2024–26 AI-agent×DB paper survey) added 2026-07-23.

This document merges the former `ROADMAP_PERFORMANCE.md` into the
production-readiness roadmap; the separate file is gone. Part I tracks
production readiness, Part II the structural performance program, Part III a
fresh production-grade gap analysis against DuckDB, Part IV a
QuestDB-inspired performance program, and Part V the agent-facing surface layer
(no-RAG/no-vector/no-local-model features mined from 2024–26 top-conference
papers, with per-item paper attribution). Statuses in the 2026-07-22 update were
re-verified against the source (grep/tests/benchmarks), not carried forward
from earlier revisions; Parts III–IV were sourced from a source-level study of
`~/Ref/duckdb` and `~/Ref/questdb` cross-checked against a full inventory of
`crates/h5i-db-core` and `crates/h5i-db-query`.

---

# Part I — Production readiness

Originates from the full-codebase review of 2026-07-22 (branch `improve-poc`).
Since then the codebase has delivered nearly all of that review. Item numbers
from the original review are kept for traceability.

## Delivered since the review (verified in source)

**Correctness & durability (all §1 blockers closed):**
segment fsync before HEAD swap (1.1, `database.rs` sync-paths batch) ·
`time_bucket` validation with `checked_mul` (1.2) · OS-level `flock` writer
lock (1.3, `backend.rs`) · runtime-flavor guard before `block_in_place`
(1.4, `udtf.rs`) · unwind wheel profile (1.5, `[profile.wheel]`) · UI
bearer token + limits (1.6).

**Performance (§2, most closed):**
ASOF filter/projection/limit pushdown with declared ordering and memory-pool
accounting (2.1/2.2/2.5/2.6) · `TableProvider::statistics()` with exact
manifest stats and metadata-only `COUNT(*)` (2.3) · streaming scan/CLI/Python
output and bounded sorted writes (2.4) · retractable VWAP/`wavg` (2.7) ·
exact ≤128-value distinct-set pruning for entity columns (2.8, first tier) ·
`H5iSession::refresh()` + shared runtime (2.9) · pairwise O(n) sortedness
check (2.11).

**Operational (§3, most closed):**
PyPI trusted publishing in release.yml (3.1) · tracing init in CLI and UI
(3.2) · retention/GC (`retention.rs`, retention floor in version resolution)
(3.3) · staging leases protecting in-flight commits from vacuum (3.4) ·
catalog CAS via create-if-absent (3.5) · UI query timeout/limits (3.7) ·
Python GIL release via `py.detach` (3.8) · schema-only empty results (3.9) ·
`docs/OPERATIONS.md` (3.10) · CI: Windows job, MSRV (1.88), supply-chain
audit, perf-trend, bench-smoke (3.11) · broken-pipe quiet exit, `--max-bytes`
(3.12).

**Features (§4):**
schema evolution (`evolution.rs`) · gapfill/LOCF (`gapfill.rs`) · incremental
version diffs (`incremental.rs`) · tailing (`tail.rs`) · S3/GCS/Azure/MinIO
object-store backend on conditional writes (`backend_object.rs`) ·
multi-table atomic commits (`transaction.rs`).

## Still open

| # | Item | Notes |
|---|------|-------|
| 2.8b | Bloom filters for high-cardinality entity columns | Only exact ≤128 distinct sets ship; no probabilistic tier. Also relevant to P2 below — a bloom answers point predicates more cheaply than a predicate-cache build. |
| 2.10 | Manifest deltas / compact encoding / WAL | Every commit still rewrites the full segment list; small frequent appends pay O(segments). |
| — | Generic-scan overhead vs raw DataFusion ~20% | Design ledger goal was ≤10%. Re-measure before optimizing; the gap may have moved. |
| — | SQL-native `ASOF JOIN` syntax | Custom planner + `asof_join` UDTF exist; bare SQL `ASOF JOIN` parity with DESIGN §6.4 unverified this pass. |
| 3.11b | Fuzz smoke in CI | Delivered, then **deliberately disabled 2026-07-22** (job commented out in ci.yml; `./fuzz` targets remain). Re-enable by uncommenting when the project wants the canary back. |
| — | Benchmark methodology debt | Non-WSL bare-metal rerun, ArcticDB baseline, Polars `set_sorted` variant, segment/version scaling curves (from the original credibility list). |

## Strengths worth preserving

- HEAD swap is textbook (temp + fsync + rename + dir fsync, CAS revalidated in
  the critical section); fault-injection `CommitHook` exercises every commit
  step on the shipped code path; the object-store backend gets the same
  guarantees from conditional PUTs.
- Integrity design: blake3 parent-checksum chains, self-checksummed
  specs/catalog/snapshots/plans, precise `Corruption {object, detail}` errors.
- Genuinely streaming scan path with sound declared ordering.
- Pruning fails open everywhere; correctness never depends on stats — the
  same rule now governs the performance sidecars (Part II §invariants).
- Plan/apply review flow: checksummed, TTL'd, vacuum-protected, fail-closed.
- Coherent error contract verified by tests that run the real binary.
- Honest benchmark write-ups; OOM-safe CI matrix.

---

# Part II — Structural performance program

Formerly `ROADMAP_PERFORMANCE.md`. The performance identity:

> A workload- and version-aware analytical database that skips data and
> reuses work across immutable versions.

This fits the architecture: versions are immutable manifests, segments have
stable checksums, statistics exist, scans use DataFusion pruning, and
destructive rewrites go through plan/apply. A cache miss may cost time but
must never affect correctness.

## Cost model

Attribute every optimization to a term of:

```text
query latency = planning + metadata I/O + data bytes read + rows decoded
              + sort/shuffle + join/aggregation + result materialization
```

Report cold and warm runs (a warm filesystem cache is not a predicate-cache
hit), median and p95 over ≥5 measured runs, with input rows/segments/bytes and
peak memory. Physical bytes — not warm-local wall time — is the honest metric
for skip-work features; wall-time payoff arrives with cold/remote storage.

## Phase status

| Phase | Status | Deliverable |
|---|---|---|
| P0 | **done** | Query-local reports, bounded telemetry, no-link benchmark gate |
| P1 | **done** | Planner stats, exact-set pruning, pushdowns (blooms optional ext.) |
| P2 | **prototype done** | Immutable predicate cache (row-group granularity) |
| P3 | **prototype done** | Version-aware finance aggregate states (OHLCV/VWAP) |
| P4 | planned | `LayoutSpec`, layout health, previewable partial reclustering |
| P5 | partial | Parquet adaptation; optional hot tier / custom encodings later |

### P0 — observability (done)

`H5iSession::sql_reported` gives each execution a query-local report:
scan-range bytes, scan output rows, row-group/page pruning, operator timing,
sorts, spills, predicate/aggregate cache counters. Exposed through Rust, CLI
`query --stats`, and the UI. Concurrent executions cannot mix scan records
(query-ID scoped, tested). Report construction is **gated on `--stats`** in
the CLI — the default path builds no report. Telemetry is a bounded opt-in
ring (`telemetry_capacity`, 0 = off) holding fingerprints, never SQL text;
flush is an explicit disposable sidecar write.
`benchmarks/run_performance_workload.py` drives a pre-built CLI (no extra
DataFusion link target), pins result checksums, and gates on median query-time
regressions. Baselines are machine-specific — pin a reference machine before
trusting the 10% gate across environments.

Open refinements: split metadata/requested/compressed/decompressed bytes;
expose reports through Python.

### P1 — cheap pruning (done)

Manifest statistics folded into planner `Statistics` (exact where
representable), metadata-only `COUNT(*)`, exact ≤128 distinct-set entity
pruning, ASOF pushdown, O(n) sortedness, retractable VWAP. The probabilistic
bloom tier remains the natural next step when entity cardinality exceeds the
exact-set threshold (see Part I open items).

### P2 — immutable predicate cache (prototype done; graduation pending)

Checksum-keyed row-group selections for deterministic conjunctions
(equality-required, typed column/literal terms; casts/functions/nulls
rejected). Sidecars under `cache/predicates/v1/` — checksummed,
create-if-absent, 256 MiB bound with oldest-first eviction, corruption
degrades to a miss and rebuild; DataFusion still re-evaluates the original
predicate above the scan. Opt-in via `--predicate-cache`.

**Measured reality (2026-07-22, 20 M-row benchmarks):**

- On uniformly interleaved symbols (the checked-in workload) a warm hit
  eliminates **nothing** — every row group contains every symbol.
- The predicate shape that clusters on real tick data (symbol + price band)
  is **ineligible**: Float64 is outside the contract.
- The case the cache exists for is demonstrated by
  `benchmarks/predicate_cache_scenario.py` — an episodic symbol inside
  per-row-group min/max ranges: warm hits scan **75% fewer physical bytes**
  with identical results. Wall time barely moves against a warm page cache;
  the payoff multiplies on the object-store backend (fewer range GETs).

**Graduation criteria (kill-or-graduate):** demonstrate wall-clock or cost
wins on the object-store backend; extend eligibility to Float64 or move to
row-level selections; a dedicated schema-evolution key case. If no real
workload exercises it by then, delete the prototype rather than maintain it.

### P3 — version-aware aggregate states (prototype done)

`AggregateStateStore::finance_rollup` persists one mergeable OHLCV/VWAP state
per (segment checksum, schema revision, plan hash, semantics version) under
`cache/aggregates/v1/`, resolves the exact manifest per call, scans only
misses, merges in manifest order. Append-only versions reuse all old states;
compaction misses cleanly; historical versions hit their old states. The
contract is deliberately narrower than SQL equivalence (non-null columns,
finite values, int64-volume exactness, deterministic open/close tie-breaker).

**Measured (20 M rows, 50 segments):** cold 2445 ms → warm **30.9 ms (79×)**,
50/50 states reused, zero corruption.

**Incident worth remembering:** the sealed-entry checksum verifies by
re-serializing parsed JSON, which requires parse∘serialize to be the f64
identity. serde_json's default lossy float parse (~1 ulp) made every
full-mantissa state fail verification and silently rebuild — warm equaled
cold, unit tests (short-decimal floats) passed, and only the benchmark's
`corrupt_entries` counter exposed it. Fixed via the `float_roundtrip`
workspace feature + a 512-group full-mantissa regression test. Design lesson:
prefer checksums over stored bytes rather than re-serialization identity —
apply if either sidecar format is revised.

Remaining before any SQL optimizer rewrite: restore/overwrite/schema-evolution
cases, fixed time buckets, then rewrites only on proved exact matches.

### P4 — workload-aware previewable reclustering (planned)

Unchanged in scope: format-versioned `LayoutSpec` (partitioning, ordering,
segment targets, per-segment layout revision — distinct from the object-path
`layout.rs`), layout health from manifests + telemetry, `optimize --plan`
before `--apply` on the existing plan/apply machinery, WAIR-style
boundary-segment selection with rewrite budgets. Never infer table-wide
ordering from a rewritten subset. Exit gate: predicted vs observed bytes
saved calibrated on held-out queries; partial reclustering beats full rewrite
in read-plus-rewrite cost.

### P5 — ingest tiers and adaptive encoding (partial; evidence-gated)

Bounded Parquet segments, streaming paths, and compaction are in. Mixed
hot/cold formats (Arrow IPC ingest tier), per-column encoding policies, and
FastLanes/LeCo-style formats stay benchmark-gated: prototype only when
profiles show decoder CPU or post-pruning bytes as a top-two cost.

## Deliberately deferred

Selective late materialization (crosses DataFusion internals; take projection
pushdown and row selection first) · active-storage services (conflicts with
embedded scope; keep format tags extensible) · full predicate-derived layouts
(do explicit finance keys + boundary reclustering first) · arbitrary
incremental SQL / full IVM (fixed mergeable states cover the high-value
finance cases with a far smaller correctness surface).

## Correctness and operability invariants

1. A version manifest remains the sole authority for a snapshot's rows.
2. Cache absence, eviction, corruption, or version mismatch causes a miss,
   never a query error or a different result.
3. Every persistent cache key includes segment identity and
   expression/engine semantics; schema revision alone is insufficient.
4. Optimizer rewrites are exact and testable against a forced uncached plan.
5. Layout optimization uses plan/apply, exact input checksums, rewrite
   budgets, and temporary-space estimates.
6. Old snapshots remain readable after optimization until retention/vacuum
   explicitly removes them.
7. Performance claims include end-to-end latency and controls; theoretical
   scan reductions are labeled as such — and warm-page-cache wall time is
   never presented as evidence for byte-skipping features.

## Research basis

Mechanisms adopted, not promised outcomes: Predicate Caching (SIGMOD 2024) →
P2 · WAIR (SIGMOD 2026) + MDDL (SIGMOD 2024) + Pando (VLDB 2023) → P4 ·
OpenIVM (SIGMOD 2024) → P3's fixed mergeable states · Selective Late
Materialization (VLDB 2025), FastLanes (VLDB 2025), LeCo (SIGMOD 2024),
Vortex (SIGMOD 2024), Active Data Lakes (VLDB 2026) → deferred /
benchmark-gated (§P5, §deferred). Re-check venues and measurements before
citing externally; no implementation decision here depends on a paper's
headline speedup.

---

# Part III — Production-grade gap analysis vs DuckDB (2026-07-23)

Sourced from a source-level comparison against `~/Ref/duckdb` cross-checked
against a full inventory of the storage kernel and query layer.

**Framing (important — do not read this as "become DuckDB").** h5i-db is
already past POC on the axes people usually worry about: crash-safety, CAS
commits, checksummed hash-chained manifests, snapshot isolation, spill-to-disk
(`FairSpillPool` + `DiskManager`, `session.rs:70-78`), and object-store CAS are
genuinely strong — often stronger than DuckDB's single-file MVCC storage. The
path to production-grade is therefore **not** chasing DuckDB's OLAP breadth
(the §9 non-goals in `DESIGN.md` correctly rule that out). It is two things
DuckDB *earns trust through* that h5i-db has not yet, plus a small set of
structural gaps specific to the tick/quant workload. Tiers are ordered by
return-on-trust, not by size.

## Tier 0 — Correctness & trust (highest priority)

This is the single largest gap, and it is about *evidence*, not features.
DuckDB ships millions of SQLLogicTest assertions + SQLSmith fuzzing +
TPC-H/DS correctness. h5i-db has ~78 hand-written tests, **zero property-based
tests**, and its 3 fuzz targets are **disabled in CI** (`ci.yml` fuzz-smoke
commented out 2026-07-22). `DESIGN.md` itself calls DuckDB the "semantic
oracle" and Phase 2 promised "SQL differential tests against DuckDB" — the
honesty ledger admits this does not exist.

| # | Item | Rationale | Acceptance criteria |
|---|------|-----------|---------------------|
| T0.1 | **Differential correctness harness vs DuckDB/DataFusion.** Adopt `sqllogictest-rs` (the crate DataFusion itself uses); generate random data + random queries over the supported subset (scan/filter/group/window/ASOF/`time_bucket`/time-travel), run through h5i-db and DuckDB-over-Parquet, assert equal. | The promised-but-missing Phase 2 gate; the only way to trust ASOF ties/NULLs, `time_bucket` DST edges, time-travel, and aggregate-state-cache = SQL-equivalence. | A CI job runs ≥1,000 generated queries/run with 0 result mismatches vs DuckDB on the supported subset; every ASOF/`time_bucket`/gapfill semantic in `DESIGN.md` has a golden `.slt` case. |
| T0.2 | **Property-based tests (`proptest`).** Storage invariants over generated inputs: append-then-scan preserves the row multiset; `compact` preserves rows & bounds; `delete_range` removes exactly the range; time-travel roundtrip; schema-evolution null-backfill; retract-VWAP ≡ fresh recompute. | Zero exist today; these catch the bug classes example tests never will, on the immutable-manifest core where correctness is everything. | ≥8 invariants encoded as `proptest` cases in CI, each with a shrinking-verified minimal counterexample path; runs on every PR. |
| T0.3 | **Re-enable fuzzing in CI + commit seed corpora.** Uncomment fuzz-smoke; add seed corpora for `manifest_json`/`csv_ingest`/`sql_parse`; add a target for the string SQL rewriters (T0.4). | 3 targets exist but are dormant (`ci.yml` fuzz-smoke disabled); a dormant fuzzer is no fuzzer (ROADMAP 3.11b). | Fuzz-smoke runs on every PR with committed corpora; a nightly longer run; 0 panics/crashes at merge. |
| T0.4 | **Harden the string-based SQL rewriters.** `ASOF JOIN` and `rolling_*` are rewritten by naive quote-aware paren scanners (`session.rs:368-465`), not a parser. | Live correctness *and* injection risk — mis-parsing aliases/nested parens silently produces wrong plans. | Move to a DataFusion `ExprPlanner`/`RelationPlanner` or a custom `sqlparser` dialect; fuzz target (T0.3) finds no mis-parse; aliased/nested-paren ASOF forms parse correctly or error explicitly, never mis-plan. |

Do this tier first — every item below is worth less until the engine is
*proven* correct.

## Tier 1 — Structural gaps specific to the tick/quant workload

| # | Item | Rationale | Acceptance criteria |
|---|------|-----------|---------------------|
| T1.1 | **Small-write amplification / ingest buffering** (extends 2.10). Manifest-delta / log-structured manifest (format already reserves the slot) and/or a WAL-backed ingest buffer that batches small appends before sealing a target-size segment. | The canonical tick workload is high-frequency *small* appends; today every commit rewrites the full segment list O(segments) (`manifest.rs:151`) with no WAL. This is the #1 structural blocker for h5i-db's own headline use case. | 10k sequential small appends cost O(1) amortized manifest bytes per append (not O(segments)); ingest throughput on 1-row-batch appends within 2× of bulk append; recovery test survives a crash mid-buffer. |
| T1.2 | **Decimal128 as a first-class type.** Wire `Decimal128` through `json_stat_to_scalar` (`provider.rs:35-70`, `pruning.rs:17-52`) and the aggregate-state type gate (`aggregate_state.rs:466`). | Table stakes for a finance DB (prices, notionals); today decimal columns get no pruning and no aggregate-state acceleration — `util.rs:83` already has a `Decimal128(18,6)` test fixture. | Decimal columns prune on min/max like Int/Float; OHLCV/VWAP aggregate-state cache accepts Decimal price/volume; differential test (T0.1) covers decimal arithmetic. |
| T1.3 | **Bloom filters for high-cardinality entity columns** (delivers 2.8b; see also A2). Enable Parquet split-block bloom filters in the segment writer; wire into the existing `contained()` pruning path. | Exact ≤128-distinct-set pruning does not help when `symbol` cardinality is in the thousands (crypto/equities); this is directly on the hot `symbol = …` path. | A point-symbol query on a high-cardinality table skips row groups that a min/max-only plan scans; measured physical-byte reduction reported cold and warm (Part II invariant 7). |
| T1.4 | **Real S3/object-store runtime tests.** MinIO/LocalStack integration tests exercising commit, CAS conflict, concurrent writers, and read against a live object store. | The entire Phase 5 value prop has zero runtime coverage — `roadmap_features.rs:206` only asserts the backend *constructs*; `DESIGN.md §10` flags that CAS semantics vary across S3-compatible stores. | CI job runs the commit/CAS/conflict/read suite against MinIO; a documented capability-probe refuses multi-writer mode on stores without conditional PUT. |

## Tier 2 — Query engine & optimizer

| # | Item | Rationale | Acceptance criteria |
|---|------|-----------|---------------------|
| T2.1 | **Make the ASOF join scale** (see also B1). Hash-repartition on the `by` keys; spillable right buffer. | Flagship operator is single-partition and buffers the entire right side in memory (`asof.rs:366` `TODO(perf)`, `:543`); large right sides OOM and it does not parallelize. | ASOF over a right side larger than the memory limit completes via spill; multi-partition plan shows near-linear speedup with cores on a by-keyed join. |
| T2.2 | **Stream gapfill.** Respect time-range pushdown; stream instead of loading the whole table into a `MemTable` (`gapfill.rs:212`). | Gapfill over a year of ticks OOMs today. | Gapfill peak memory is bounded independent of table size; time-range predicate prunes segments before gapfill. |
| T2.3 | **Predicate-based DELETE/UPDATE.** Predicate-delete that rewrites affected segments, or (bigger) deletion vectors / merge-on-read. **Deliberate-decision flag:** this pushes against the "range mutations only" simplicity in `DESIGN.md` — adopt only with an explicit call, not by default. | Only time-range copy-on-write exists (`database.rs:1400` rejects the rest); "delete a delisted symbol's rows" / GDPR corrections are not expressible. | `DELETE … WHERE <predicate>` and `UPDATE … SET` on non-time predicates work through plan/apply; previewable like existing mutations; differential-tested. |
| T2.4 | **Close the ~20% generic-scan overhead vs raw DataFusion** (Phase 2 ≤10% gate). Ship the decoded-batch cache promised in `DESIGN.md §7 Tier 1` (only footer metadata is cached today). | An agent loop re-reads the same immutable segments constantly; a decoded-batch LRU keyed by segment hash is trivially correct and likely the biggest remaining scan win. | Generic-scan overhead vs raw DataFusion on the same Parquet ≤10% (Part I open item); decoded-batch cache hit-rate reported in `--stats`. |

## Tier 3 — Operational polish (needed to *run* it in production)

| # | Item | Rationale | Acceptance criteria |
|---|------|-----------|---------------------|
| T3.1 | **High-N concurrency & soak tests.** N≫2 writer contention, long-running soak. | All current concurrency tests are 2-writer / single-reader-during-write; durability claims need stress evidence. | A soak test runs ≥N writers for a sustained period with 0 corruption and correct conflict accounting. |
| T3.2 | **Metrics export** (Prometheus/OpenTelemetry). Expose scan/prune/spill/conflict counters from the observability crate. | `tracing` init exists; production operators need scrapeable metrics. | Counters exposed on an opt-in endpoint; documented in `OPERATIONS.md`. |
| T3.3 | **Backup/restore for the object-store backend** (snapshot → export → import), documented and tested. | No documented DR story today. | Round-trip backup/restore test passes; documented procedure. |
| T3.4 | **Corruption *recovery*** (vs detection, which is strong). Rebuild-from-good-manifest, partial-write truncation recovery. | Corruption is well *detected* (`durability.rs:242/280`) but recovery is thin. | `verify`/repair reconstructs a usable head from the last good manifest without guessing; tested against injected partial writes. |

## Non-goals reaffirmed (do NOT pursue, per `DESIGN.md §9`)

Row-level MVCC / interactive multi-statement transactions; a cost-based
optimizer; a custom columnar format; distributed query; broad DuckDB-breadth
type coverage (nested/JSON/Union); MCP-in-core. Chasing these dilutes what
makes h5i-db distinctive.

---

# Part IV — QuestDB-inspired performance program (2026-07-23)

Sourced from a source-level study of `~/Ref/questdb` (Java engine + Rust/C++
native core), filtered to techniques that transfer to h5i-db's model
(immutable Parquet segments + DataFusion + manifest pruning).

**Principle.** Nearly every QuestDB advantage over a generic engine flows from
treating `symbol` as a first-class *interned + indexed* type — filters, GROUP
BY, and JOINs all run on `int` keys, and symbol bitmap indexes power its
crown-jewel fast paths (indexed ASOF, `LATEST ON`, `WHERE symbol = …`). That is
exactly h5i-db's target column and its current weak spot: per-file Parquet
dictionaries cannot be compared across segments, symbol pruning is capped at
the ≤128-value exact distinct set, and there is no symbol index. So the
highest-ROI borrows cluster there.

## Tier A — Symbol as a first-class type (the keystone)

| # | Item | Borrowed from | Acceptance criteria |
|---|------|---------------|---------------------|
| A1 | **Global symbol dictionary at the manifest level** (`symbol → u32`, stable table-global). Filters/GROUP BY/JOIN run on ints; dictionaries compare without materializing strings; ASOF maps dict→dict once (their `SymbolToSymbolJoinKeyMapping`). | `SymbolMapWriter`/`SymbolMapReaderImpl` | A symbol equality predicate prunes segments at any cardinality (not just ≤128); GROUP BY symbol runs on int keys; aggregate-state cache group-key eligibility no longer restricted to raw non-null Utf8. |
| A2 | **Per-segment symbol index sidecar** (postings `symbol → row-ranges`, or Parquet split-block bloom as the first tier). Subsumes 2.8b / T1.3. | `BitmapIndexWriter`, `SymbolColumnIndexer`; `parquet2` `bloom_filter/split_block.rs` | A symbol point query reads only row groups the index admits; sidecar is checksummed/immutable/fail-open like the existing predicate & aggregate caches; corruption → miss, never wrong result. |
| A3 | **Precompute "last row per symbol" per segment** in the manifest/sidecar; queries merge per-segment last-rows in manifest order. **Delivers the deferred `latest-per-key` rewrite** (honesty ledger: currently runs as a generic window plan). | `LatestByAllIndexedRecordCursor` (improved for immutability) | `LATEST ON symbol` / latest-per-key runs O(segments × symbols), not O(rows); reuses across append-only versions like the OHLCV aggregate-state cache; differential-tested vs the generic window plan. |

A1 is the keystone: A2 and A3 (and B1) build on the global dictionary.

## Tier B — Faster time-series operators (exploit sortedness you already have)

| # | Item | Borrowed from | Acceptance criteria |
|---|------|---------------|---------------------|
| B1 | **Indexed / short-circuited ASOF join.** `SymbolShortCircuit` — skip master rows whose symbol cannot match (cheap with A2); combine with T2.1's `by`-key repartition. | `SymbolShortCircuit`, `AsOfJoinIndexedRecordCursorFactory` | A by-keyed ASOF with a sparse match set scans fewer right rows than the current full-buffer path; measured row reduction reported. |
| B2 | **Out-of-order (O3) region-selective Parquet merge.** When a late batch overlaps existing segments, split prefix/merge/suffix and rewrite only touched row groups (16-byte `(ts,rowId)` merge index + radix sort). Ingest-side counterpart of T1.1. | `O3ParquetMergeStrategy`, `ooo_radix.h` | Out-of-order append no longer forces a full-table `write`; rewrite cost is proportional to overlapped row groups, not table size; row order and stats remain correct (property-tested, T0.2). |
| B3 | **Streaming SAMPLE BY fill variants.** Add `fill(prev/null/value/linear)` and dedicated first/last over the already-streaming `time_bucket` path. | `SampleByFill{Prev,Null,Value,Linear}`, `SampleByFirstLastRecordCursorFactory` | Fill variants stream in bounded memory; parity with DuckDB/QuestDB fill semantics (differential-tested). |

## Tier C — Scan & aggregation quality

| # | Item | Borrowed from | Acceptance criteria |
|---|------|---------------|---------------------|
| C1 | **Column byte-range sidecar** so the S3 backend prunes and range-reads without fetching the Parquet footer (eliminates the first-read footer round-trip the footer-metadata cache cannot). | `_pm` metadata (`qdb-parquet-meta`, `ParquetMetaFileReader`) | Cold S3 segment read issues no separate footer GET; byte-range GETs derived from the manifest; measured cold-read latency reduction. |
| C2 | **Compensated summation** (Kahan/Neumaier) in `vwap`/`wavg`/`ewma` accumulators. | `KSumDouble`, `NSumDouble` | Long-sum VWAP matches a high-precision reference within tolerance where naive f64 drifts; regression-tested on a full-mantissa dataset. |
| C3 | **HyperLogLog approx-distinct + parallel top-K** (lower priority). | `hyperloglog/`, `GroupByLongTopKJob` | `approx_count_distinct(symbol)` and top-N-by-volume available; opt-in. |

## Do NOT borrow (DataFusion covers it, or a §9 non-goal)

- **asmjit JIT filter compiler** (`jit/compiler.cpp`) — tied to raw pointers
  over mmapped memory; DataFusion's vectorized eval covers it; `DESIGN.md §7
  Tier 3` rules out replacing engine internals.
- **Zero-GC off-heap memory model** — a Java workaround irrelevant to
  Rust/Arrow.
- **Page-frame work-stealing, SwissTable `rosti`, in-place O3 rewrite** —
  DataFusion's parallel scan + repartitioned hash aggregation are the
  equivalents; do not rebuild them (only the *immutable-Parquet* O3 variant,
  B2, transfers).

## Cross-references between Parts III and IV

- A2 ⇄ T1.3 ⇄ 2.8b — symbol bloom/index is one investment described from three
  angles; build it once.
- B2 ⇄ T1.1 ⇄ 2.10 — out-of-order merge and small-write amplification share the
  manifest-delta / region-rewrite machinery.
- A3 delivers the `latest-per-key` rewrite the honesty ledger lists as
  undelivered.
- B1 ⇄ T2.1 — indexed short-circuit and `by`-key repartition are the same ASOF
  scale-up effort.
- T0.1's `sqllogictest-rs` is the same crate QuestDB uses (`qdb-sqllogictest`,
  63 `.test` files) and DataFusion uses — adopt, do not build.

## Part IV implementation status (2026-07-23, branch `improve-tests`)

Delivered incrementally, each additively (opt-in where it touches the hot path)
with serial tests and no regression to existing suites:

| Item | Status | Notes |
|------|--------|-------|
| C2 compensated summation | ✅ done | Neumaier in `vwap`/`wavg` + finance aggregate-state; state format/checksum unchanged (comp folded in at emit/seal). Full-mantissa test vs high-precision reference. |
| A2 symbol bloom filters | ✅ done | Opt-in `StorageOptions.bloom_filter_columns`; empty omitted from spec (byte-identical format, golden fixture passes). End-to-end test: bloom prunes row groups min/max cannot. Also fixed a latent bug — DF54 `PruningMetrics.as_usize()==0` had silently zeroed the reported `row_groups_pruned`. |
| C3 approx-distinct + top-K | ✅ done (DataFusion built-in) | `approx_distinct` (HLL) and `ORDER BY … LIMIT` TopK ship via default features; verified reachable + correct rather than reimplemented. |
| B3 SAMPLE BY fills | ✅ done | Added `value` constant fill + `prev`/`linear` aliases to gapfill/resample; first/last per bucket are DataFusion `first_value`/`last_value` over `time_bucket`. |
| B1 ASOF symbol short-circuit | ✅ done (structural) | Already realized by the keyed-run design (`RunIndex::Keyed` → O(1) probe miss for absent symbols), stronger than QuestDB's sorted-scan short-circuit; verified with an absent-symbol test. Parallel by-key repartition remains T2.1. |
| A3 last-row-per-symbol precompute | ✅ done | New `latest.rs` + `latest_on('t','by')` UDTF; per-segment "last row per group" cached as a checksummed Arrow-IPC sidecar (`cache/latest/v1`), merged in manifest order → O(segments × groups), append-only reuse. Additive (no existing path changed). Delivers the `latest-per-key` rewrite. |
| C1 column byte-range / metadata sidecar | 🔬 analyzed — staged with the read-path work | Its acceptance ("cold S3 read issues no footer GET; byte-range GETs derived from the manifest") is inherently **read-path-invasive**: it requires either a custom `ParquetFileReaderFactory` that serves `get_metadata` from a sidecar instead of the footer, or metadata embedded in the manifest and consumed by a custom reader. Neither is a safe additive change; the existing footer-metadata cache already covers warm/in-process reads. Best done opt-in (default off) with instrumented GET-count tests, as a focused follow-up with A1 — not rushed. |
| A1 global symbol dictionary | ⏳ pending | Format-level change to the manifest; large and format-breaking. Staged as dedicated work to honor the no-regression constraint. |
| B2 out-of-order (O3) Parquet merge | ⏳ pending | Ingest-path change (append is currently strict); large and higher-risk. Staged as dedicated work. |

Delivered (safe/additive, tested, committed): C2, A2, C3, B3, B1, A3 — six of
nine. The remaining three (C1, A1, B2) all change the read path, manifest
format, or ingest path; each is sequenced as focused, instrumented work so it
can be verified without regressing the benchmarked paths. C1 in particular was
implemented far enough to confirm its win cannot be realized additively — it
belongs with the read-path/format tier, not as a tail-of-session change.

---

# Part V — Agent-facing surfaces (2026-07-23)

Sourced from a survey of 2024–2026 top-conference papers on **AI-agent × database /
data-analysis** (VLDB, SIGMOD, CIDR, ICLR, NeurIPS, EuroSys, and preprints),
filtered by two hard constraints:

1. **No RAG, no vector search / embeddings, no local open-weight models.** A
   hosted API model (Claude) is allowed; every item below is embedding-free.
   Papers whose value depends on vector similarity (semantic search / sim-join /
   dedup, embedding schema-linking) or a local SLM are **excluded** — see *Do NOT
   borrow*.
2. **`DESIGN.md §8–§9`: agent features live in the CLI / Python / SKILL / sidecar
   layer, never the storage engine** ("anything agent-flavored must land in
   CLI/docs/skill"). Vector search is already a §9 non-goal, so the survey
   constraint and the project's own philosophy coincide.

**Framing — why this Part exists.** Parts I–IV harden the *engine*. Part V is the
orthogonal axis: the "for AI agents" layer the README already claims. The survey's
headline finding is convergence — four separate communities (text-to-SQL,
semantic-operator DBs, agent-systems infra, data-analysis agents) independently
concluded in 2024–26 that an agent-first data system needs **cheap branching,
rollback, provenance, point-in-time reads, and structured (non-vector) memory** as
baseline primitives (CIDR 2026, *Supporting Our AI Overlords*, Berkeley —
arXiv 2509.00997). **h5i-db already ships the storage half of all of them**:
immutable versioning = agent memory + rollback; O(1) time-travel = point-in-time
correctness; plan/apply = staged-then-commit transactions; the audit/diff UI =
provenance. The work below is thin agent-facing *surfaces* over primitives we
already own — not new engine machinery.

**The keystone substrate (build once, most tiers fall out).** Tiers A, C, and D
all reduce to one capability: **deterministic execution against a pinned immutable
version, with results cached/diffed by `(commit, query)`**. The engine is already
deterministic on a pinned version and already checksums segments; the missing piece
is a query-layer result cache keyed on `(version SHA, normalized query)` and a
CLI/Python surface to run "the same query across N pinned versions." Ship that
first; it is the common denominator.

**Provenance caveat.** Some finance / agent-systems items below are recent
preprints whose arXiv IDs the survey could not fully verify (several carried
implausible future-dated IDs). Those are marked `⚠`. The *mechanism* is what we
adopt; treat the citation as indicative, and re-verify venue/ID before citing
externally (same rule as Part II §Research basis). Venue-anchored citations are
marked `✓`.

## Tier V-A — Point-in-time / look-ahead-bias-free execution (flagship)

The single most differentiated surface, and the one that exploits our most
under-used primitive (time-travel). No other embedded DB markets time-travel for
look-ahead-bias control, which is *the* correctness bug in quant backtests. All
three items are quant-specific, embedding-free, and reuse existing snapshots +
query-local stats. **Scope honesty (state it in docs):** this addresses
*data-access* leakage, which a versioned store can prove; it does **not** address
the LLM's *pretraining* leakage, which no data layer can fix.

| # | Item | Source paper(s) | Acceptance criteria |
|---|------|-----------------|---------------------|
| V-A1 | **`leakage-delta` report** (lowest effort, highest demo value). Run any agent backtest twice — against `HEAD` and against `h5i('table', asof=decision_time)` — and diff. The "alpha that evaporates" quantifies decision-time data leakage; surface it as a new query-local stat next to scan-bytes/pruning. | ⚠ *When Alpha Disappears: A One-Switch Benchmark for Decision-Time Leakage* (preprint, 2026) — the leaking/non-leaking toggle. | A CLI/Python `backtest --leakage-check` runs both configurations via O(1) time-travel and reports a leakage-delta metric; a golden case with a deliberately-leaking feature shows non-zero delta and a clean feature shows ~0; the second run reuses cached states (no full recompute). |
| V-A2 | **`asof(t)`-scoped session mode.** Every scan in the session is provably bounded by an availability stamp; harden `ASOF JOIN` and `time_bucket` for availability-monotonicity + causal alignment, and add a static checker that flags any query whose data-availability effect exceeds its declared decision epoch. | ⚠ *Look-Ahead-Freedom as Temporal Non-Interference* (formal-methods preprint, 2026) — type-and-effect system, linear-time-checkable on the timestamp-only fragment (our case). | A session opened `--asof <t>` refuses (or flags in the audit trail) any scan reading data with availability `> t`; the checker is linear-time on timestamp-derived availability; differential-tested (T0.1) that `asof(t)` results equal a physically-truncated dataset at `t`. |
| V-A3 | **Point-in-time universe / fold snapshots** as addressable, reusable commit objects an agent references when building features (e.g. "tradable universe as-of date D"). | ⚠ *Standardized Benchmark of Look-Ahead Bias in Point-in-Time LLMs for Finance* (preprint, 2026) — point-in-time universes / data folds. | An agent can name and re-read a universe/fold by `(table, asof)` in O(1); reconstructing a past universe never reads post-`t` segments (verified against manifest pruning). |

## Tier V-B — Provenance-based data-safety policy

Upgrades the existing policy-gated plan/apply from a *procedural* gate into a
*provable* data-safety guarantee. DB-native, cheap, and directly validated as a
baseline agent-DB primitive by CIDR 2026.

| # | Item | Source paper(s) | Acceptance criteria |
|---|------|-----------------|---------------------|
| V-B1 | **Data Flow Control (DFC) policy over provenance.** A deterministic policy language — `SOURCE <t> SINK <t> / CONSTRAINT <bool expr> / ON FAIL {REMOVE \| KILL}` — enforced by **query rewriting** that carries source→sink lineage inline and evaluates the constraint during execution (not a separate provenance pass). `REQUIRED` forces every sink row to derive from a real source row (**anti-hallucination by construction**). Evaluate it in the **plan** phase; refuse **apply** on violation. | ✓ *Data Flow Control: Data Safety Guarantees for Agents* (Columbia DAPLab, CIDR 2026) — ~0.11% TPC-H overhead by piggybacking on execution; shows LLM-based guardrails hit only ~50% ("a non-starter"). | A DFC policy attached to a table causes `plan` to reject a mutation whose sink rows do not all derive from approved sources; the rewrite adds < a few % overhead on the benchmark workload; the rejection reason (which constraint, which rows) lands in the audit/diff UI; policy is deterministic and fail-closed (Part II invariant style). |
| V-B2 | **Partition / lineage type-tags** on datasets so a transformation that fits a transformer across a train/test or time boundary is rejected *before* it runs (leakage type-check as a plan-time policy). | ⚠ *A Grammar of Machine Learning Workflows: Rejecting Data Leakage at Call Time* (PL/ML preprint, 2026) — types encode train/val/test provenance. | Datasets carry partition/lineage tags; a preprocessing-before-split or temporal-order-violating mutation is rejected at `plan` with a precise message; clean pipelines pass unchanged; tested with a fit-across-split counterexample. |

## Tier V-C — Execution-guided SQL trust

Turns "an agent wrote SQL" into "SQL the DB validated." These text-to-SQL systems
hack around the *lack* of a deterministic sandbox with voting/replay — **h5i-db has
the sandbox for free** (a pinned immutable version). Surfaces live in the CLI/skill
layer and reuse plan/apply; the keystone `(commit, query)` cache makes candidate
execution cheap.

| # | Item | Source paper(s) | Acceptance criteria |
|---|------|-----------------|---------------------|
| V-C1 | **Execution-guided candidate selection.** Generate N candidate SQLs, execute each against a pinned snapshot, select by result-overlap (Minimum Bayes Risk over execution outputs); where execution is costly, approximate by comparing DataFusion `EXPLAIN` logical plans. | ⚠ *Query and Conquer: Execution-Guided SQL Generation* (preprint 2025) — MBR over execution outputs, `EXPLAIN`-plan approximation. ✓ *CHASE-SQL* (Google, ICLR 2025) — multi-candidate + selection (use a **hosted** pairwise judge, not the fine-tuned comparator). | A `query --candidates N` mode executes candidates against a pinned version deterministically and returns the MBR-selected result plus which candidates ran against which SHA (auditable); selection reduces error vs single-shot on a golden text-to-SQL set; no candidate mutates state (read-only against the snapshot). |
| V-C2 | **Query-fixer repair loop wired into plan/apply preview.** Run candidate → capture DataFusion error / empty-result → repair → re-preview; accept only on a result-shape/value assertion, not exact SQL match. | ✓ *CHASE-SQL* (ICLR 2025) — execution-error-driven fixer. ✓ *SWE-SQL / BIRD-CRITIC* (NeurIPS 2025) — reproduce→diagnose→fix→validate-against-tests. | A malformed agent query is repaired against a pinned version and only applied after passing a declared result assertion; the reproduce/fix/validate trace is recorded in the version-diff UI; differential-tested for equivalence to a hand-written correct query. |
| V-C3 | **Static constraint-verification pre-apply gate.** Cheap check of a generated query against declared schema / integrity constraints before execution; stacks in front of V-C1/V-C2. | ✓ *The Power of Constraints in NL-to-SQL Translation* (PVLDB Vol. 18, 2025). | A query violating a declared constraint is flagged/rejected at plan with the specific violation; false-positive rate measured on a golden set; composes with the execution gate (static → execute). |
| V-C4 | **Data-probing schema linking** for wide tick tables: discover relevant columns via bounded `SELECT DISTINCT … LIMIT` probes against a pinned snapshot instead of feeding the whole schema — embedding-free, cacheable per version. | ✓ *ReFoRCE* (Spider 2.0 agent, 2025) — column exploration by data-probing. ✓ *CHESS* (2024) — LSH entity matching (adopt the **non-vector** part; **drop** its column-description vector store). | On a wide table, an agent resolves the right columns from bounded probes (row/byte-capped) run against a pinned version; probe results cached by `(SHA, probe)`; no embeddings anywhere in the path. |

## Tier V-D — Reproducibility & statistical-validity guardrails

Monetizes O(1) time-travel + the keystone result cache: re-running an analysis many
times over data variants is cheap for us and expensive for everyone else.

| # | Item | Source paper(s) | Acceptance criteria |
|---|------|-----------------|---------------------|
| V-D1 | **Stability sweep (PCS sanity checks).** Materialize perturbed inputs as lightweight child commits, re-run the agent's full analysis on each, and report the *distribution* of conclusions to flag p-hacking / noise-chasing. | ⚠ *Sanity Checks for Agentic Data Science* (preprint 2026) — PCS perturbation checks; found 6/11 agentic conclusions unsupported despite a confident single run. | A `stability-sweep` primitive runs the analysis across K perturbations via child commits + cached sub-results and reports conclusion stability; shared sub-computations are deduped by `(commit, query)`; a known-unstable analysis is flagged, a robust one is not. |
| V-D2 | **Claim → re-computation verifier.** Decompose agent prose ("VWAP rose 3.2%") into atomic facts and re-derive each from the underlying commit via SQL; flag mismatches. Prefer code-based numeric verification over LLM-as-judge. | ⚠ *ChartFI: Faithfulness of Chart Descriptions* (preprint 2026) — atomic-fact code-based verification. ✓ *DABStep* (NeurIPS 2025 D&B) — deterministic auto-scorer (numeric tolerance, list-normalize) as the accept pattern. | Given an agent narrative + the source commit, the verifier re-derives each numeric claim from SQL and reports pass/fail with tolerance; a deliberately-wrong number is caught; scoring is deterministic, not LLM-judged. |

## Tier V-E — Agent memory & workspace surfaces

Mostly vocabulary + a thin API over primitives we already have; validates the "for
agents" positioning without engine changes.

| # | Item | Source paper(s) | Acceptance criteria |
|---|------|-----------------|---------------------|
| V-E1 | **Structured / temporal "memory-as-a-table"** over the versioned store: `revise` = versioned write, `temporal query` = time-travel read, `provenance recall` = commit-lineage walk, `graded forgetting` = retention policy. The explicitly **non-vector** agent-memory design. | ⚠ *Is Agent Memory a Database? (GEM / MemState)* (preprint 2026) — structured/temporal memory with per-field value histories + provenance, no vectors. | An agent stores/updates/recalls memory as a table where recall returns current values by default and prior values + provenance on temporal query; forgetting is a retention policy over old commits; zero embeddings. |
| V-E2 | **`fork → explore → commit/abort` + first-commit-wins** vocabulary on existing branches; Cordon-style multi-step agent transaction using a scratch branch, with the review UI as the verification gate. | ⚠ *Fork, Explore, Commit* (preprint 2026) — branch lifecycle + first-commit-wins; ⚠ *Cordon: Semantic Transactions for Tool-Using Agents* (EuroSys preprint) — stage-then-verify-then-commit-or-rollback. Validate churn against ⚠ *BranchBench* / *Branchable Databases Aren't Ready for Agentic Workloads* (Columbia DAPLab). | An agent forks a workspace branch per hypothesis, explores, and the first branch to pass review commits while siblings auto-invalidate; a multi-step trajectory commits atomically or rolls back via O(1) restore; documented as a CLI/skill workflow, not an engine concept. |

## Tier V-F — Semantic LLM operators (out-of-core, evidence-gated)

Interesting but in the most tension with our constraints: LLM UDFs sit near the
engine (against §9), and the no-vector rule bites hardest here (every
semantic-*join/dedup* speedup in the literature is embedding-powered). **Do not put
this in core.** If a concrete workload ever demands it, ship a **separate optional
`h5i-db-llm` package** (the way a hypothetical `h5i-db-mcp` was scoped), and only
the embedding-free subset.

| # | Item | Source paper(s) | Acceptance criteria |
|---|------|-----------------|---------------------|
| V-F1 | **`llm_filter` / `llm_map` / `llm_reduce` as DataFusion UDF/UDAF**, with **versioned `MODEL` / `PROMPT` objects** so a commit records which prompt+model produced a column (novel: nobody versions LLM outputs today). Cache keyed on `(segment hash, prompt ver, model ver)`. | ✓ *FlockMTL / Beyond Quacking* (VLDB 2025 demo) — LLM scalar/aggregate UDFs + `PROMPT`/`MODEL` as DDL objects (adopt; **drop** its RAG/hybrid-search half). | Lives in `h5i-db-llm`, off by default; LLM-op output is cached per immutable segment and reproducible given the pinned model/prompt version; provenance answers "which prompt+model produced this column at commit X." |
| V-F2 | **Predicate pushdown before LLM ops + model cascade.** Push cheap SQL/time/`time_bucket`/ASOF predicates ahead of any `llm_*` op so it sees far fewer rows; wrap ops in a Haiku→Sonnet→Opus confidence-escalation cascade. | ⚠ *PLOP: Placement of Semantic Operators* (preprint 2026) — cost-based LLM-op placement. ✓ *FrugalGPT* / model-cascade lineage (2024) — escalate on low confidence (here across hosted tiers only). | An `llm_*` query on a large table evaluates deterministic predicates first (measured row reduction before the LLM op); the cascade routes easy rows to the cheap tier and escalates uncertain ones; both are pure plan-rule/policy, no new operator semantics. |
| V-F3 | **Accuracy-guarantee sampling as an auditable quality annotation**; **`sem_join` via LLM key-extraction → native hash join** (the only no-vector join). | ✓ *LOTUS* (VLDB 2025) — proxy/gold operators with statistical accuracy bounds (adopt the bound-as-annotation; **skip** its embedding-accelerated join/search/dedup). ⚠ *Trummer, Implementing Semantic Joins Efficiently* (2025) — batching / LLM-blocking / key-extraction-to-equijoin. | A semantic column can carry a "computed by proxy, ≤ε error at 95% confidence vs gold" annotation in the version diff (sold as *statistical*, not correctness); `sem_join` runs as O(n) cacheable key-extraction + DataFusion hash join, never O(n²) vector similarity. |

## Do NOT borrow (forbidden technique or §9 non-goal)

- **Vector/embedding schema-linking** — CHESS's column-description vector store;
  use its LSH entity-matching + data-probing (V-C4) instead.
- **Semantic search / sim-join / dedup** — LOTUS `sem_search`/`sem_sim_join`/
  `sem_dedup`, and any "AI memory layer" with vector recall — embedding-native, no
  LLM-only variant. GEM/MemState (V-E1) is the clean structured alternative.
- **ELEET** (VLDB 2024) — its whole mechanism is a **local** pretrained SLM;
  forbidden. Its structured-extraction idea survives only as a hosted-model
  extraction UDF (= V-F3 key-extraction).
- **CrackSQL's cross-dialect embedding matcher** — if dialect translation is ever
  wanted (DuckDB/Postgres→DataFusion), use the rule-based *RISE* reduction with a
  hosted-LLM fallback, not embeddings.
- **Aryn/Sycamore's local DETR document parser**, distributed/Ray execution —
  clashes with the embedded single-node model; document data ≠ tick data.

## Cross-references (Part V ⇄ existing parts)

- **Keystone `(commit, query)` result cache** ⇄ Part II P2 predicate-cache / P3
  aggregate-state cache machinery — same "checksum-keyed, fail-open, corruption →
  miss" discipline (Part II invariants 2–3); build V's cache on that pattern.
- **V-C differential validation** ⇄ **T0.1** `sqllogictest-rs` harness — the same
  crate; the execution-guided gates ride on the differential-correctness substrate.
- **V-B1 DFC provenance** ⇄ existing **plan/apply + policy** (`policy.rs`,
  `plan.rs`) — DFC is the policy *language*; plan/apply is the enforcement point.
- **V-A time-travel hardening** ⇄ Part IV **B1 keyed-run ASOF** and `time_bucket`
  — the operators to make availability-monotonic.
- **V-D re-run economics** ⇄ Part I **O(1) version reads / restore** — the primitive
  that makes stability sweeps cheap.

## Evaluation targets (measure against, do not build)

Publishing fodder to prove the wins above, not features to implement:
- ✓ **DABStep** (NeurIPS 2025 D&B) — financial-analytics multi-step tasks; show a
  Claude agent on h5i beats raw-CSV baselines because `time_bucket`/ASOF/group-by are
  native (attacks the "non-idiomatic loop" failure mode).
- ✓ **CORE-Bench** (2024) — computational reproducibility; show `restore(sha)`
  replaces Docker+pinning archaeology.
- ✓ **Spider 2.0** (ICLR 2025) — wide-schema, multi-dialect realism yardstick for
  the V-C text-to-SQL layer.
- **KramaBench / InsightBench** — positioning only (multi-source data lakes, out of
  embedded scope); cite for the "agents produce plausible-but-broken pipelines"
  finding that motivates previewable, policy-gated mutations.

## Suggested build order

1. **Keystone substrate** — `(commit, query)` result cache + "run query across N
   pinned versions" CLI/Python surface (unblocks A/C/D).
2. **V-A1 `leakage-delta`** and **V-B1 DFC policy** — most differentiated, cleanest
   fit, lowest effort-to-impact, and exactly what a quant audience pays for.
3. **V-C1/V-C2** execution-guided selection + repair — turns agent SQL trustworthy.
4. **V-D1/V-D2** stability sweep + claim verifier — reproducibility moat.
5. **V-E1/V-E2** memory + fork/explore/commit vocabulary — mostly docs/skill/API.
6. **V-F** only if a real workload demands it, and only in a separate package.

## Part V implementation status (2026-07-23, branch `improve-tests`)

| Item | Status | Notes |
|------|--------|-------|
| V-A1 `leakage-delta` | ✅ done | `H5iSession::new_at` pins every table at a `ReadAt` (generalizes the latest-only registration); `leakage::check_leakage` runs a query at head vs an as-of point and diffs — numeric columns cast to f64 for per-cell delta, others string-compared, plus per-table withheld-version deltas. CLI `leakage-check <db> <sql> --as-of <ts\|version> [--tolerance]`. **Additive & default-path-neutral**: a new opt-in surface; the normal query path is untouched. Tests: 4 query integration (restatement delta, time-bounded no-leak, as-of-timestamp ≡ version pin, row-count change) + 1 CLI e2e. Confirmed the required primitive already existed — `ReadAt::AsOf` resolves by `committed_at_ns` (availability), exactly the look-ahead-free axis. |
| V-B1 DFC data-safety policy | ✅ done | Opt-in, per-table `DataPolicy` (sidecar `tables/<uuid>/DATA_POLICY.json`) with a small fuzz-safe typed grammar (NotNull / Compare / InSet + And/Or/Not, OnFail Reject\|Warn), evaluated over Arrow arrays in `core` (no DataFusion in the kernel). Enforced at every write path (`stage_write`/`stage_append`/`replace_range_impl`) **and at plan time** (`plan_write`/`plan_replace_range`) — a violating mutation is refused before it can be applied. Fail-closed (NULL never satisfies a comparison; corrupt policy errors). `Error::DataPolicyViolation` (exit 2, not retryable). CLI `data-policy get\|set\|clear` (JSON docs). **Opt-in / read-path-neutral**: a table without a policy pays only one metadata lookup on the *write* path; reads are never touched. Tests: 11 unit + 6 core integration + 1 CLI e2e. Row-dropping (DFC `REMOVE`) deliberately deferred. Scoped projection of DFC onto the explicit-row write path — full cross-table lineage rewriting remains future work. |

Delivered (additive, opt-in, tested, fmt + clippy `-D warnings` clean): **V-A1, V-B1**
— the two highest-ROI Part V items. Neither changes the default read path.
