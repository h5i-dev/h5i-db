# h5i-db Roadmap

Living roadmap. Last full update 2026-07-22 (branch `improve-performance`);
Parts III–IV added 2026-07-23 (branch `improve-tests`); Part V (agent-facing
surfaces, from a 2024–26 AI-agent×DB paper survey) added 2026-07-23.
Part IV addendum + Part VI (agent ergonomics & competitive positioning) added
2026-07-24 (branch `agentic-features`) from a codebase-grounded agent-UX
review, a three-track web survey of the 2025–26 "agentic database" landscape,
and an external cross-check of the performance program against production
engines and recent papers. Part VI's build order supersedes Part V's.

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
`docs/OPERATIONS.md` (3.10) · CI: Windows job, MSRV (1.89), supply-chain
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

## Part IV addendum — external cross-check (2026-07-24)

A web survey of production engines (QuestDB 2025–26 releases, ClickHouse
24.7+, DuckDB 2025 blogs, InfluxDB 3, kdb+) and 2023–26 papers, checked
against this codebase, **confirmed the program's direction** — several of the
industry's "top wins" are already shipped here: sorted-merge ASOF that skips
the sort DuckDB's planner cannot elide, streaming OHLCV over declared
`output_ordering`, the `latest_on` sidecar (same shape as InfluxDB 3's Last
Value Cache), and mergeable aggregate states (same shape as ClickHouse
projections). The measured losses map onto existing items: the ASOF
single-partition gap → **T2.1**; the ~2.3× time-range-scan gap vs ArcticDB →
**P4 layout + A1/A2** (the kdb+ sym-grouped, time-sorted-within-sym layout is
the 30-year-old validation of P4's direction); the ~1.5× full-aggregation gap
vs DuckDB and ~20% vs raw DataFusion → **T2.4**.

New items the cross-check surfaced (cheapest first; all subject to
invariant 7 — measure in `h5i-db-bench` before/after, never trust vendor
multipliers):

| # | Item | Source | Notes |
|---|------|--------|-------|
| D1 | ~~Verify/enable DataFusion TopK dynamic filters.~~ **Already active (verified 2026-07-24).** | DF 49–50 dynamic-filter work ("25×" on `ORDER BY ts LIMIT k`) | DF 54 defaults every dynamic-filter switch to `true` (`enable_topk_dynamic_filter_pushdown`, `enable_join_…`, `enable_aggregate_…`), and it engages with our provider: `EXPLAIN … ORDER BY ts DESC LIMIT 5` shows `DataSourceExec … predicate=DynamicFilter [ empty ], sort_order_for_reorder=[ts@0 DESC], reverse_row_groups=true`. No config change needed. |
| D2 | ~~ASOF `tolerance` as an early-exit bound.~~ **Already stronger than the borrow (verified 2026-07-24).** | QuestDB `TOLERANCE` clause | The probe is a binary search over per-key sorted runs (`asof.rs` `probe`), so there is no backward scan to terminate early — tolerance rejects the single candidate found, at O(log n). Beyond that, `tolerance` is *already* used to derive time bounds on the right-hand scan (`asof.rs`, filter derivation), which turns into manifest/row-group pruning. **Open refinement:** those bounds come from explicit left-side time predicates in the query; deriving them from the left table's manifest `time_range` when the query has no such predicate would extend the pruning to unfiltered joins. Plan-shape change, not a few-line fix — sequence with T2.1. |
| D3 | **Sort pushdown** (DataFusion `PushDownSort`, PR #17337). | DataFusion | `ORDER BY time` should never re-sort — segments are already time-sorted and ordering is declared; receive the preferred sort in the provider. |
| D4 | **`BYTE_STREAM_SPLIT` encoding for float price columns.** | arrow-rs (supported today) | One benchmark decides adoption; upstream notes the decode path is "largely unoptimized", so measure both ratio and scan speed. |
| D5 | **HORIZON JOIN** (asof at multiple future offsets = backtest label generation). | QuestDB 9.3.3 | More feature than perf: a natural `AsOfJoinExec` extension, and pairs with Tier V-A (agents generating labels inside the leakage-checked session). |
| D6 | **Rolling-window workload in the bench + segment-tree/streaming window evaluation.** | DuckDB "Flying Through Windows" (~4× via vectorized segment trees); FlatFIT / SlideSide papers | `rolling_*` is SQL sugar over a generic window plan today. Add the workload to `h5i-db-bench` and `compare_baselines.py` first; build a custom operator only if a loss is measured. |

**Compression watch (not build):** ALP (SIGMOD 2024) beats
Chimp/Gorilla/zstd on ratio *and* speed and is being standardized as a
Parquet encoding, but arrow-rs does not ship it — adopt when it lands
upstream. Pcodec is Rust-native but non-standard; breaking Parquet
compatibility is not worth it (§P5 evidence gate applies). No new as-of-join
algorithm papers appeared in 2024–26; production engines lead this area.

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

## Suggested build order (superseded by Part VI §build order, 2026-07-24)

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
| V-A1 `leakage-delta` | ✅ done | `H5iSession::new_at` pins every table at a `ReadAt` (generalizes the latest-only registration); `leakage::check_leakage` runs a query at head vs an as-of point and diffs (numeric columns cast to f64 for per-cell delta, others string-compared, plus per-table withheld-version deltas). CLI `leakage-check <db> <sql> --as-of <ts\|version> [--tolerance]`. **Additive & default-path-neutral**: a new opt-in surface; the normal query path is untouched. Tests: 4 query integration (restatement delta, time-bounded no-leak, as-of-timestamp ≡ version pin, row-count change) + 1 CLI e2e. Confirmed the required primitive already existed: `ReadAt::AsOf` resolves by `committed_at_ns` (availability), exactly the look-ahead-free axis. |
| V-B1 DFC data-safety policy | ✅ done | Opt-in, per-table `DataPolicy` (sidecar `tables/<uuid>/DATA_POLICY.json`) with a small fuzz-safe typed grammar (NotNull / Compare / InSet + And/Or/Not, OnFail Reject\|Warn), evaluated over Arrow arrays in `core` (no DataFusion in the kernel). Enforced at every write path (`stage_write`/`stage_append`/`replace_range_impl`) **and at plan time** (`plan_write`/`plan_replace_range`); a violating mutation is refused before it can be applied. Fail-closed (NULL never satisfies a comparison; corrupt policy errors). `Error::DataPolicyViolation` (exit 2, not retryable). CLI `data-policy get\|set\|clear` (JSON docs). **Opt-in / read-path-neutral**: a table without a policy pays only one metadata lookup on the *write* path; reads are never touched. Tests: 11 unit + 6 core integration + 1 CLI e2e. Row-dropping (DFC `REMOVE`) deliberately deferred. Scoped projection of DFC onto the explicit-row write path; full cross-table lineage rewriting remains future work. |

Delivered (additive, opt-in, tested, fmt + clippy `-D warnings` clean): **V-A1, V-B1**,
the two highest-ROI Part V items. Neither changes the default read path.

---

# Part VI — Agent ergonomics & competitive positioning (2026-07-24)

Sourced from three inputs, each grounded against the actual codebase rather
than taken at face value: (1) a herdr-inspired review of the agent-facing CLI
surface ("don't add AI to the DB; rebuild the DB's surface assuming the agent
is outside"); (2) a three-track web survey of the 2025–26 agentic-database
landscape (dev-side fork DBs, data-side git-for-data/quant, OLAP+MCP and
agent-native startups); (3) the perf cross-check folded into the Part IV
addendum. Part V is *capabilities* mined from papers; Part VI is *ergonomics*
and *positioning* — the friction an agent actually hits between those
capabilities, and where the market leaves room to stand.

## Competitive findings (2026-07-24 survey; re-verify dates before citing)

**Whitespace — confirmed unshipped by any product:**

1. **Enforced point-in-time sessions.** Every engine with as-of reads
   (ArcticDB, Dolt, Neon PITR) offers it opt-in per read; none can *jail* an
   agent session so it provably cannot read past a cutoff. The concept now has
   academic formalization (*Look-Ahead-Freedom as Temporal Non-Interference*,
   arXiv 2607.04958 — the same family as V-A2) but no implementation.
2. **Run ledger** — data version × code × metrics in one queryable link.
   Nearest misses: Bauplan run IDs, lakeFS commit-pinned reproducibility;
   neither reaches metrics.
3. **Token-aware output budgets in the engine.** The "MCP result came back as
   50K tokens" problem is widely acknowledged and solved only by middleware
   (token-budget proxies, spill-to-file patterns). No engine ships it
   first-party.

**Commoditized — do for hygiene, never as the flag:** MCP servers,
NL-to-SQL, skills distribution (MotherDuck, Tinybird, Supabase, QuestDB, and
Bauplan Skills 2026-02 all ship SKILL/skills packages), and
branch/fork-for-agents (Tiger Fluid Storage 2025-10, Neon/Databricks, Turso
ms-level branching, AgentDB's UUID-is-a-database).

**Watch list:** Dolt has gone all-in on "the database for AI agents" (MCP
2025-08, Agent Mode 2026-02, BranchBench 2026-06) — whoever defines the
benchmark defines the category; consider a quant-workload answer to
BranchBench. Turso and LanceDB are the nearest *embedded* competitors
(LanceDB already has versioning + time travel); neither is time-series/quant.
ArcticDB confirmed: BSL 1.1, commercial license required for any business
use, and **no agent surface at all** (no MCP, no skills).

**Positioning conclusion:** "agent-friendly analytics DB" as a banner is
taken. "**Embedded + versioned + time-series/quant + Apache-2.0**, where the
agent provably cannot see the future" remains unoccupied — and items 1–3
above are exactly V-A2, the keystone/run-ledger, and VI-A2 below.

## Tier VI-A — CLI ergonomics for agents

All items live in the CLI/skill/sidecar layer per `DESIGN.md §8–§9`; none
touch the storage engine. Ordered by effort-to-impact.

| # | Item | Rationale | Acceptance criteria |
|---|------|-----------|---------------------|
| VI-A1 | **`context` command — one-shot situational awareness.** All tables' schema, time range, row count, latest version + recent version note, active mutation/data policies, staged plans, in one deterministic call with `--budget <tokens>` truncation priorities. Everything needed is already in manifests + plan/policy sidecars. | Today an agent's first 30 seconds burn on a tables → schema → sample → versions walk, O(tables) round-trips. herdr's "zero-config rollup" translated to a DB. `--format json` also feeds any external fleet view (see do-not list). | One command returns the full picture within the budget; output is deterministic for a fixed DB state (cacheable in AGENTS.md); SKILL.md names it as the mandatory first move; e2e test parses it. |
| VI-A2 | **Output budgets via profile, not TTY sniffing.** `H5I_DB_PROFILE=agent` (env or per-DB config) defaults `--max-rows`/`--max-bytes`, head/tail + summary rendering, always-explicit `"truncated": true, "total_rows": N`, and spill of the full result to Parquet with a `full_result_path`. | Asking agents to remember `--max-rows` fails; one forgotten flag destroys the context window. Survey: no engine ships this — middleware-only today (a genuine first). Content must never change on non-TTY detection: pipes and CI must see identical bytes (git changes only *color* on non-TTY, for the same determinism reason). | With the profile set, no query can exceed the budget and the full result is recoverable from the spill path; without it, behavior is byte-identical to today; `limit_exceeded` envelope unchanged; documented as SKILL.md line 1. |
| VI-A3 | **`next_actions` + `did_you_mean` in the error envelope.** Extend `{code, message, retryable, hint}` with machine-executable `next_actions: [{cmd, why}]`, `did_you_mean` on identifier typos, and the referenced table's schema on SQL binder errors. | Hints are prose; agents want commands. All 25 variants live in one place (`error.rs`), so this is a single-site change that cuts 1–2 recovery round-trips per failure. | Envelope schema versioned; every mutation-ordering error (e.g. out-of-order append) carries at least one runnable `next_actions` entry (`replace-range --plan`, `ingest --mode write --plan`); CLI e2e tests parse and execute a suggested action; `hint` stays human-readable. |
| VI-A4 | **`demo` command + docs-as-tests.** `h5i-db demo` materializes a small synthetic tick dataset and prints a 30-second init→ingest→query→plan→apply→leakage-check tour. CI extracts and executes every code snippet in SKILL.md / `docs-src/` (extend `tools/build_docs.py`, which already parses them). | Agents execute documentation literally; one stale example flips them into guess-mode. Doc/binary drift is the top agent-trust bug class, and no snippet runs in CI today. | `demo` completes in <30 s on the reference machine; a CI job fails on any snippet whose command errors or whose output shape drifts; SKILL.md split into a ~400-token core + on-demand reference files. |
| VI-A5 | **`--idempotency-key` on mutations.** Key recorded in `VersionManifest.user_meta`; a retried mutation with the same key returns the original commit (no-op success) instead of double-appending. | Agents retry on ambiguous failures (timeout after commit); duplicated ticks are silent poison. Plans have CAS; direct appends have nothing. | Same-key retry after a successful commit returns the original version id and writes nothing; different key proceeds; property-tested (T0.2 style) under crash-mid-commit injection; documented in SKILL.md's retry guidance. |
| VI-A6 | **`plan apply --wait-for-approval --timeout <dur>`.** Park instead of fail: poll the staged plan until a human applies/discards via CLI or UI, then exit accordingly. | Turns policy violations from dead-ends into blocked-agent states a human can unblock from the UI; herdr's "blocked" concept transplanted. Rides existing plan storage + UI apply/discard routes unchanged. | Waiting process exits 0 on apply, distinct codes on discard/timeout/TTL-expiry; no busy-loop (bounded poll interval); e2e test covers apply-while-waiting. |
| VI-A7 | **Skill packaging & drift check.** `skill install --claude --codex` placing SKILL fragments, plus `skill check` warning on doc/binary version mismatch. | Commoditized (see findings) — hygiene, not differentiation. Do after VI-A4 gives the docs a tested core. | Installed skill references only CI-tested snippets; `skill check` flags a version mismatch; uninstall is clean. |

## research-mode: elevate V-A2 to a named flagship surface — **dual-axis**

The survey confirms V-A2 is the differentiator, and the codebase check
confirms the arrival half is nearly free: `ReadAt::{Version, AsOf, Snapshot}`
exists, and `leakage-check` already builds the exact primitive
(`H5iSession::new_at` pinning *every* table at a point).

**Design correction (2026-07-24, second pass): the arrival pin alone is not
enough.** Two blind spots make a version-pin-only research-mode overstate the
claim. First, the most common quant look-ahead bugs are *event-time* bugs
(windows overrunning into future rows, joins reading `T+1` data, full-sample
normalization) — rows that were always in the table, invisible on the arrival
axis. Second, on bulk-ingested history (one commit for ten years — the
typical cold start) every as-of resolves to the same version, so the arrival
jail is empty on day one. Therefore research-mode enforces **both axes**:

- **Arrival axis:** every table pinned at availability `t`
  (`ReadAt::AsOf` over `committed_at_ns`; per-commit granularity is
  sufficient — restatements arrive as new versions and are correctly
  excluded). Only *populated* under continuous ingestion or arrival replay
  (VI-B1).
- **Event-time axis:** an enforced predicate `time_column ≤ t − embargo`
  injected into every scan in the session — works from day one on bulk
  data and structurally blocks the window-overrun / future-join class the
  arrival axis cannot see.

Surface: `query --as-of <t> [--embargo <dur>]` plus a session/env pin so
every subsequent command inherits it. Backtests run **walk-forward** — one
pinned session per decision date — which is exactly the shape the keystone
`(commit, query)` cache makes cheap (same decision date re-runs warm). Keep
V-A2's scope-honesty line (data-access leakage only, not LLM pretraining
leakage). The one-sentence claim — *the only database that can show an agent
the past and nothing else* — is honest only with both axes shipped.

## Run ledger: concretize the keystone

`h5i-db run -- <cmd>` records, per run: every `(table, resolved version)`
read (all reads already resolve through `ReadAt`), git SHA, parameters, and
declared output metrics; `runs list` / `run diff A B` then answer "same code,
Sharpe 1.82 → 0.91, because `trades` v42→v43 restated 12,481 rows" — the
data-vs-code attribution no MLflow/W&B (code+metrics only) or
lakeFS/Nessie (data only) can make. **Design this together with the Part V
keystone `(commit, query)` result cache**: they share the substrate, and the
same cache that makes 40 nightly backtests re-read warm (perf) makes their
runs attributable (reproducibility). Design-first — schema before code.

## Tier VI-B — the arrival axis: replay, online ingestion, honesty (2026-07-24)

The arrival-axis features (leakage-check, restatement attribution, the run
ledger's data-vs-code answer) are only meaningful when the commit history
mirrors real data availability. There are two ways to get there — run the DB
as a continuous system-of-record (online), or *reconstruct* the history from
vendor publication timestamps (replay) — plus honesty tooling so a vacuous
zero-delta is never misread as a clean bill of health.

**Priority elevation:** T1.1 (small-write amplification) and B2 (out-of-order
merge) are hereby *prerequisites of the arrival-axis flag*, not just perf
items. If the claim is "the DB that remembers when data arrived", appending
every minute must be cheap (T1.1) and a late tick must not force a full-table
rewrite (B2). Implementation order unchanged; the justification is upgraded.

| # | Item | Rationale | Acceptance criteria |
|---|------|-----------|---------------------|
| VI-B1 | **Arrival replay: `ingest --arrival-column <col>`.** Split the input into commits ordered by a per-row publication/arrival timestamp, reconstructing the arrival history from a vendor point-in-time dataset in one bulk load. Requires a logical `available_at` on the manifest — back-dating `committed_at_ns` would break its wall-clock-monotonic invariant — with `ReadAt::AsOf` resolving against `available_at` and falling back to `committed_at_ns` when absent. | Kills the cold-start problem: converts every bulk-ingest user to the dual-axis story on day one. Survey: no engine ingests publication-stamped PIT data into a queryable commit chain. **Format-change tier** — sequence with A1/B2, not as a quick add. | Bulk-ingesting a PIT dataset with an arrival column yields N commits whose as-of reads reproduce each historical availability state; tables without `available_at` behave exactly as today (golden fixture); a restated row is visible at HEAD and absent at a pre-restatement as-of (e2e); `committed_at_ns` monotonicity untouched. |
| VI-B2 | **leakage-check hardening (3 fixes).** (1) Key-based row alignment (`--key <cols>`), or require a deterministic `ORDER BY` for multi-row results — comparison today is positional over the `min(rows)` overlap (`leakage.rs:247`), so one inserted row turns every subsequent per-row mismatch into noise. (2) Print "a zero delta does not prove absence of leakage" in the CLI/Python output — the doc comment says it; the output does not. (3) Vacuity detection: when `withheld_versions` is empty for every table, say so explicitly ("the arrival-axis check is vacuous on this database") and point at VI-B1. | The first bulk-ingest user who sees a silent zero-delta concludes the feature is broken; (3) prevents that structurally (the herdr move: the tool explains its own blind spot). (1) makes multi-row reports usable at all. | Multi-row diffs align on declared key columns; both notices asserted by CLI e2e; a single-commit DB produces the vacuity notice, and the same DB after arrival replay does not. |
| VI-B3 | **Data freshness in `context`** (extends VI-A1). Per-table last-commit age, lag against a declared expected cadence, and event-time gaps. | "Is this DB alive" is the rollup that matters most under continuous ingestion — for agents and for the humans supervising them. | `context` shows per-table freshness; a table past its declared cadence is flagged; zero cost when no cadence is declared. |
| VI-B4 | **`maintain` one-shot command.** compact + vacuum + verify under a time/space budget, policy-gated like today's `compact`. | The daemonless answer to "who does housekeeping during continuous ingestion": a cron entry or an agent runs one command. Daemon mode stays a §9 non-goal. | Bounded runtime honoring the budget; no-op cheap on an already-tidy DB; exit code distinguishes "done" from "budget exhausted, more remains". |
| VI-B5 | **Documented online-ingest loop pattern** (docs/SKILL, not code). Per-source watermarks, idempotency keys (VI-A5), writer-lock wait/retry etiquette. No `--follow` resident mode — that is daemonization by stealth. | The ingest loop is the product surface online users live in; an official pattern beats every user reinventing it wrong. | A docs-as-tests (VI-A4) covered walkthrough runs an idempotent, watermark-tracked loop end-to-end, including a simulated retry after an ambiguous failure. |

**Cadence honesty (scope):** the online story is minute-bar / EOD / vendor-file
cadence on a single writer — not sub-µs tick capture, which stays on the
"when NOT to use h5i-db" list. Blurring this invites a losing comparison with
kdb+; stating it buys trust.

## Do-not list (2026-07-24)

- **No TUI/fleet-view rewrite in core.** The axum UI (loopback + token +
  plan review) stays; VI-A1's `--format json` lets herdr or any external
  dashboard render fleet state. The DB's job is emitting legible state.
- **No TTY-based content switching** (VI-A2 rationale). Profiles and flags
  only.
- **No NL-to-SQL in the DB; MCP stays out of core** (§9 reaffirmed; the
  survey shows MCP is table stakes, not a moat — if ever shipped, a thin
  separate package for shell-less clients with correct
  readOnly/destructive hints).
- **Trading calendars / adjustments / symbol identity: staged, not now.**
  Only the cheap `DataPolicy` extensions land near-term —
  `monotonic(time)`, `no_gaps(max_gap)`, `outlier(z)` on the existing
  NotNull/Compare/InSet machinery. Calendar-aware bucketing
  (`XNYS`, half-days) means maintaining an external dataset forever;
  defer until a real workload demands it. Symbol *identity over time*
  relates to A1 (global symbol dictionary) — revisit when A1 lands.
- **README addition (cheap, trust-buying):** a "when NOT to use h5i-db"
  section (multi-TB distributed, OLTP, sub-µs capture) — also stops agents
  from mis-recommending it.

## Build order (supersedes Part V's; revised for dual-axis + VI-B)

1. **VI-A1 `context` (incl. VI-B3 freshness) + VI-A3 `next_actions` + VI-A2
   agent profile** — small, single-site changes with the largest per-line UX
   effect; VI-A2 is also a category first.
2. **Dual-axis research-mode + VI-B2 leakage-check hardening + VI-A5
   idempotency-key** — the flagship claim (honest only with both axes), its
   audit tool made unmisreadable, and the retry-safety agents need before
   being given write access.
3. **VI-A4 `demo` + docs-as-tests** — locks the trust layer before the
   surface grows further. The demo's leakage act must be scripted as a
   **restatement scenario** (a mid-history vendor correction commit): an
   event-time-style planted leak would show a zero delta and falsify the
   pitch.
4. **Keystone `(commit, query)` result cache designed jointly with the run
   ledger** — one substrate, two headline features (warm re-reads +
   attribution); do the schema design first. Walk-forward research-mode
   sessions are its first consumer.
5. **Format-change tier: VI-B1 arrival replay, sequenced with A1 (and the
   T1.1/B2 ingest work it elevates)** — populates the arrival axis for
   bulk-ingest users; the prerequisites of selling that axis.
6. **V-C1/V-C2, then V-D** — as in Part V.
7. **VI-A6 wait-for-approval, VI-A7 skill packaging, VI-B4 `maintain`,
   VI-B5 ingest-loop pattern, data-policy time-series extensions** —
   opportunistic, after the above.

Tier 0 (Part III) remains the standing precondition: every Part VI surface
multiplies trust already earned by the correctness harness, not the other way
around.

## Execution backlog — the next 20 tasks (2026-07-24)

The build order above sequences *phases*; this is the concrete pick list, in
two batches of ten. Sizes: S ≈ half a day, M ≈ 1–2 days, L ≈ several days.

**Batch 1 — surface & flag** (the minimal release bundle: items 1–5 of the
suggested start order — #9, #6, #4, #3, #2 — total ≈ 3 days — already form a
shippable story). Deliberately excluded: design-first items (run ledger,
arrival replay) and the format-change tier.

| # | Task | Size | Ref |
|---|------|------|-----|
| 1 | `context` command | M | VI-A1 |
| 2 | `H5I_DB_PROFILE=agent` output profile | M | VI-A2 |
| 3 | `next_actions` + `did_you_mean` in the error envelope | S–M | VI-A3 |
| 4 | research-mode v1: `query --as-of` (version pin; expose `new_at`) | S | V-A2 / Part VI |
| 5 | research-mode v2: `--embargo` event-time cutoff (scan-injected predicate; separate PR — the one-sentence claim is honest only after this lands) | M | V-A2 / Part VI |
| 6 | leakage-check quick fixes: vacuity notice + zero-is-not-innocence output line | S | VI-B2 (2)(3) |
| 7 | `--idempotency-key` on mutations | M | VI-A5 |
| 8 | `demo` + docs-as-tests (demo scripted as a restatement scenario) | M–L | VI-A4 |
| 9 | README rewrite: one-sentence "Why for agents" + "when NOT to use h5i-db" (incl. cadence honesty). Companion, same day: restructure SKILL.md — frontmatter, task-shaped core ≤60 lines (golden loop / decision rules / research loop), references/ split; thereafter every feature PR that obsoletes a workaround deletes its SKILL.md line | S (+S) | Part VI findings; VI-A7 prep |
| 10 | Two half-day verifications: D2 (does the ASOF probe use `tolerance` for early exit?) + D1 (TopK dynamic-filter config on DF 54) | S+S | Part IV addendum |

Suggested start order within batch 1: 9 → 6 → 4 → 3 → 2 → 1 → 10 → 5 → 7 → 8.

**Batch 2 — trust & substrate** (repays the Tier 0 debt the flag stands on,
and runs the two design tracks in parallel with it).

| # | Task | Size | Ref |
|---|------|------|-----|
| 11 | Differential correctness harness vs DuckDB (`sqllogictest-rs`; start with the supported subset + golden `.slt` for ASOF / `time_bucket` / time-travel — it is also the V-A2 acceptance check that `asof(t)` ≡ physically truncated data) | L | T0.1 |
| 12 | Re-enable fuzz smoke + harden the string SQL rewriters into a real parser (same PR series: the fuzz target hunts the mis-parses) | M | T0.3 + T0.4 |
| 13 | `proptest` storage invariants (≥8: append→scan multiset, compact preservation, delete-range exactness, time-travel roundtrip, …) | M | T0.2 |
| 14 | Run ledger × keystone `(commit, query)` cache: joint design doc, schema first (runs schema, cache key, metrics attachment; ledger implementation itself is batch 3+) | M | Part VI run ledger |
| 15 | Keystone `(commit, query)` result cache implementation per #14, following the P2/P3 checksum-keyed / fail-open discipline | L | Part V keystone |
| 16 | leakage-check key-based row alignment (`--key <cols>`) | M | VI-B2 (1) |
| 17 | **Unified format-change RFC** (design only, no code): `available_at` (VI-B1) + manifest deltas (T1.1) + O3 merge (B2) in one document — three items touching the manifest format must break it once, not three times | M | VI-B1 / T1.1 / B2 |
| 18 | Data-policy time-series extensions: `monotonic(time)` / `no_gaps(max_gap)` / `outlier(z)` — the one quant feature pulled forward; isomorphic extension of the existing DataPolicy machinery, no calendar swamp | M | Part VI do-not list carve-out |
| 19 | ASOF by-key repartition + spillable right buffer (the codebase's only `TODO(perf)`) | L | T2.1 / B1 |
| 20 | Online small bundle: freshness in `context` (if not in #1) + `maintain` one-shot | M | VI-B3 + VI-B4 |

Suggested start order within batch 2: 14 → 17 (both design tracks first) →
11 → 12 → 13 (trust repayment while designs settle) → 16 → 18 → 20 → 15 → 19.

Explicitly *not* in these batches: implementations of arrival replay / A1 /
T1.1 / B2 (blocked on RFC #17), V-C / V-D, HORIZON JOIN (D5), T2.4
decoded-batch cache, VI-A6 / VI-A7 / VI-B5. Leading batch-3 candidates: T2.4
and V-C1.
