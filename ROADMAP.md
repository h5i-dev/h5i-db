# h5i-db Roadmap

Eleven parts, added as the work that motivated them happened. Each carries its
own date and provenance in its heading, so this preamble no longer repeats
them.

**Read this file in two modes.** *Open now* below is the whole of what is not
done, and it is where to start. Everything after it is the reasoning: what was tried, what
was rejected and why, and the constraints a future change has to respect. That
reasoning is the point of keeping delivered parts around. The code says what
shipped; only this says what was deliberately not built, and those paragraphs
are the ones worth not re-deriving.

| Part | Subject | State |
|---|---|---|
| I | Production readiness | 6 open, listed below |
| II | Structural performance program | P0–P1 done, P2–P3 prototype, P4 planned, P5 partial |
| III | Gap analysis vs DuckDB | T3.4 (corruption *recovery*) open |
| IV | QuestDB-inspired performance | 6 of 9 done; C1 / A1 / B2 staged as format- or read-path work |
| V | Agent-facing surfaces | V-A1, V-B1 delivered. **Build order superseded by Part VI** |
| VI | Agent ergonomics & positioning | batch 1 delivered (all ten) |
| VII | Quant data-layer features | delivered |
| VIII | Lazy DataFrame builder (Python) | delivered |
| IX | Fork: multi-agent workspaces | IX-A1 … IX-A5 delivered |
| X | Fork at agentic scale | delivered |
| XI | Live execution without a backtest tax | **design only**, nothing built |
| XII | Optional 128-bit fixed point | not planned; one doc fix falls out of it |

## Open now

Everything not done, in one place. The Part column is where the reasoning
lives; this table is an index, not a replacement for it.

| Part | Item | Note |
|---|---|---|
| I | Bloom filters for high-cardinality entity columns (2.8b) | Only exact ≤128-distinct sets ship |
| I | Manifest deltas / compact encoding / WAL (2.10) | Every commit rewrites the full segment list |
| I | Generic-scan overhead ~20% vs raw DataFusion | Ledger goal was ≤10%; re-measure before optimizing |
| I | SQL-native `ASOF JOIN` syntax | UDTF + planner exist; bare-SQL parity unverified |
| I | Fuzz smoke in CI (3.11b) | Delivered, then deliberately disabled 2026-07-22; re-enable by uncommenting |
| I | Benchmark methodology debt | Bare-metal rerun, scaling curves |
| II | P2 graduation | Predicate cache is a prototype |
| II | P4 — `LayoutSpec`, layout health, previewable reclustering | Planned |
| II | P5 — ingest tiers, adaptive encoding | Partial, evidence-gated |
| III | T3.4 — corruption *recovery* | Detection is strong; recovery is thin |
| IV | C1 column byte-range / metadata sidecar | Analyzed: cannot be additive, belongs with the read-path tier |
| IV | A1 global symbol dictionary | Format-breaking |
| IV | B2 out-of-order Parquet merge | Ingest-path, higher risk |
| XI | Live execution | Design only; see the part before removing its seams |
| XII | `store.rs` overclaims f64 exactness | Doc says the whole `i64` range; the real bound is 9,007,199 units. Small, independent of the rest of XII |

Two more, measured 2026-07-31 and written up in
`benchmarks/backtest_compare/RESULTS.md` rather than given a part here,
because both are sweep features rather than optimizations: decode shared
across study trials (23% of a four-trial study) and trials run
concurrently across forks. They are the only remaining backtest-performance
items that are multiplier-class; the rest are percent-class and the machine's
own noise floor is now comparable to them.

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

Delivered incrementally, each additively, opt-in where it touches the hot path:

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
| D5 | **HORIZON JOIN** (asof at multiple future offsets = backtest label generation). | QuestDB 9.3.3 | More feature than perf: a natural `AsOfJoinExec` extension, and pairs with Tier V-A (agents generating labels inside the arrival-deltaed session). |
| D6 | **Rolling-window workload in the bench + segment-tree/streaming window evaluation.** | DuckDB "Flying Through Windows" (~4× via vectorized segment trees); FlatFIT / SlideSide papers | `rolling_*` is SQL sugar over a generic window plan today. Add the workload to `h5i-db-bench` and `compare_baselines.py` first; build a custom operator only if a loss is measured. |

**Compression watch (not build):** ALP (SIGMOD 2024) beats
Chimp/Gorilla/zstd on ratio *and* speed and is being standardized as a
Parquet encoding, but arrow-rs does not ship it — adopt when it lands
upstream. Pcodec is Rust-native but non-standard; breaking Parquet
compatibility is not worth it (§P5 evidence gate applies). No new as-of-join
algorithm papers appeared in 2024–26; production engines lead this area.

---

# Part V — Agent-facing surfaces (2026-07-23)

> **Build order superseded by Part VI.** V-A1 and V-B1 shipped; the rest of
> this part's sequencing was replaced by Part VI's, which was written against
> the codebase rather than against the paper survey. Read it for the item
> definitions and their paper attributions, not for what to do next.

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
| V-A1 | **`leakage-delta` report** (lowest effort, highest demo value). Run any agent backtest twice — against `HEAD` and against `h5i('table', asof=decision_time)` — and diff. The "alpha that evaporates" quantifies decision-time data leakage; surface it as a new query-local stat next to scan-bytes/pruning. | ⚠ *When Alpha Disappears: A One-Switch Benchmark for Decision-Time Leakage* (preprint, 2026) — the leaking/non-leaking toggle. | A CLI/Python `backtest --arrival-delta` runs both configurations via O(1) time-travel and reports a leakage-delta metric; a golden case with a deliberately-leaking feature shows non-zero delta and a clean feature shows ~0; the second run reuses cached states (no full recompute). |
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
| V-A1 `leakage-delta` | ✅ done | `H5iSession::new_at` pins every table at a `ReadAt` (generalizes the latest-only registration); `arrival::arrival_delta` runs a query at head vs an as-of point and diffs (numeric columns cast to f64 for per-cell delta, others string-compared, plus per-table withheld-version deltas). CLI `arrival-delta <db> <sql> --as-of <ts\|version> [--tolerance]`. **Additive & default-path-neutral**: a new opt-in surface; the normal query path is untouched. Confirmed the required primitive already existed: `ReadAt::AsOf` resolves by `committed_at_ns` (availability), exactly the look-ahead-free axis. |
| V-B1 DFC data-safety policy | ✅ done | Opt-in, per-table `DataPolicy` (sidecar `tables/<uuid>/DATA_POLICY.json`) with a small fuzz-safe typed grammar (NotNull / Compare / InSet + And/Or/Not, OnFail Reject\|Warn), evaluated over Arrow arrays in `core` (no DataFusion in the kernel). Enforced at every write path (`stage_write`/`stage_append`/`replace_range_impl`) **and at plan time** (`plan_write`/`plan_replace_range`); a violating mutation is refused before it can be applied. Fail-closed (NULL never satisfies a comparison; corrupt policy errors). `Error::DataPolicyViolation` (exit 2, not retryable). CLI `data-policy get\|set\|clear` (JSON docs). **Opt-in / read-path-neutral**: a table without a policy pays only one metadata lookup on the *write* path; reads are never touched. Row-dropping (DFC `REMOVE`) deliberately deferred. Scoped projection of DFC onto the explicit-row write path; full cross-table lineage rewriting remains future work. |

Delivered: **V-A1, V-B1**,
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

**Positioning correction (2026-07-25).** The line above is a whitespace
observation, not the product thesis, and later drafting over-read it as one.
The thesis stays the README identity: a fast, fully versioned, embedded
time-series database for quant research. "Provably cannot see the future"
overclaims what a data layer can own: most leakage happens after data leaves
the DB (splits, full-sample normalization, feature code, model pretraining),
so no DB-layer feature can certify a pipeline leak-free (the same scope
honesty already stated in V-A and in the `leakage-check` → `arrival-delta`
rename). The defensible claim is the README's own wording, *bounded at the
source*: what reached the client was bounded by version / arrival / event
time, and `arrival-delta` reports what a restatement changed. research-mode
and the arrival axis stay valuable audit features, one bullet among several,
not the banner; read the one-sentence claim in the research-mode section
below under this correction.

## Tier VI-A — CLI ergonomics for agents

All items live in the CLI/skill/sidecar layer per `DESIGN.md §8–§9`; none
touch the storage engine. Ordered by effort-to-impact.

| # | Item | Rationale | Acceptance criteria |
|---|------|-----------|---------------------|
| VI-A1 | **`context` command — one-shot situational awareness.** All tables' schema, time range, row count, latest version + recent version note, active mutation/data policies, staged plans, in one deterministic call with `--budget <tokens>` truncation priorities. Everything needed is already in manifests + plan/policy sidecars. | Today an agent's first 30 seconds burn on a tables → schema → sample → versions walk, O(tables) round-trips. herdr's "zero-config rollup" translated to a DB. `--format json` also feeds any external fleet view (see do-not list). | One command returns the full picture within the budget; output is deterministic for a fixed DB state (cacheable in AGENTS.md); SKILL.md names it as the mandatory first move; e2e test parses it. |
| VI-A2 | **Output budgets via profile, not TTY sniffing.** `H5I_DB_PROFILE=agent` (env or per-DB config) defaults `--max-rows`/`--max-bytes`, head/tail + summary rendering, always-explicit `"truncated": true, "total_rows": N`, and spill of the full result to Parquet with a `full_result_path`. | Asking agents to remember `--max-rows` fails; one forgotten flag destroys the context window. Survey: no engine ships this — middleware-only today (a genuine first). Content must never change on non-TTY detection: pipes and CI must see identical bytes (git changes only *color* on non-TTY, for the same determinism reason). | With the profile set, no query can exceed the budget and the full result is recoverable from the spill path; without it, behavior is byte-identical to today; `limit_exceeded` envelope unchanged; documented as SKILL.md line 1. |
| VI-A3 | **`next_actions` + `did_you_mean` in the error envelope.** Extend `{code, message, retryable, hint}` with machine-executable `next_actions: [{cmd, why}]`, `did_you_mean` on identifier typos, and the referenced table's schema on SQL binder errors. | Hints are prose; agents want commands. All 25 variants live in one place (`error.rs`), so this is a single-site change that cuts 1–2 recovery round-trips per failure. | Envelope schema versioned; every mutation-ordering error (e.g. out-of-order append) carries at least one runnable `next_actions` entry (`replace-range --plan`, `ingest --mode write --plan`); CLI e2e tests parse and execute a suggested action; `hint` stays human-readable. |
| VI-A4 | **`demo` command + docs-as-tests.** `h5i-db demo` materializes a small synthetic tick dataset and prints a 30-second init→ingest→query→plan→apply→arrival-delta tour. CI extracts and executes every code snippet in SKILL.md / `docs-src/` (extend `tools/build_docs.py`, which already parses them). | Agents execute documentation literally; one stale example flips them into guess-mode. Doc/binary drift is the top agent-trust bug class, and no snippet runs in CI today. | `demo` completes in <30 s on the reference machine; a CI job fails on any snippet whose command errors or whose output shape drifts; SKILL.md split into a ~400-token core + on-demand reference files. |
| VI-A5 | **`--idempotency-key` on mutations.** Key recorded in `VersionManifest.user_meta`; a retried mutation with the same key returns the original commit (no-op success) instead of double-appending. | Agents retry on ambiguous failures (timeout after commit); duplicated ticks are silent poison. Plans have CAS; direct appends have nothing. | Same-key retry after a successful commit returns the original version id and writes nothing; different key proceeds; property-tested (T0.2 style) under crash-mid-commit injection; documented in SKILL.md's retry guidance. |
| VI-A6 | **`plan apply --wait-for-approval --timeout <dur>`.** Park instead of fail: poll the staged plan until a human applies/discards via CLI or UI, then exit accordingly. | Turns policy violations from dead-ends into blocked-agent states a human can unblock from the UI; herdr's "blocked" concept transplanted. Rides existing plan storage + UI apply/discard routes unchanged. | Waiting process exits 0 on apply, distinct codes on discard/timeout/TTL-expiry; no busy-loop (bounded poll interval); e2e test covers apply-while-waiting. |
| VI-A7 | **Skill packaging & drift check.** `skill install --claude --codex` placing SKILL fragments, plus `skill check` warning on doc/binary version mismatch. | Commoditized (see findings) — hygiene, not differentiation. Do after VI-A4 gives the docs a tested core. | Installed skill references only CI-tested snippets; `skill check` flags a version mismatch; uninstall is clean. |

## research-mode: elevate V-A2 to a named flagship surface — **dual-axis**

The survey confirms V-A2 is the differentiator, and the codebase check
confirms the arrival half is nearly free: `ReadAt::{Version, AsOf, Snapshot}`
exists, and `arrival-delta` already builds the exact primitive
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

The arrival-axis features (arrival-delta, restatement attribution, the run
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
| VI-B2 | **arrival-delta hardening (3 fixes).** (1) Key-based row alignment (`--key <cols>`), or require a deterministic `ORDER BY` for multi-row results — comparison today is positional over the `min(rows)` overlap (`arrival.rs`), so one inserted row turns every subsequent per-row mismatch into noise. (2) Print "a zero delta does not prove absence of leakage" in the CLI/Python output — the doc comment says it; the output does not. (3) Vacuity detection: when `withheld_versions` is empty for every table, say so explicitly ("the arrival-axis check is vacuous on this database") and point at VI-B1. | The first bulk-ingest user who sees a silent zero-delta concludes the feature is broken; (3) prevents that structurally (the herdr move: the tool explains its own blind spot). (1) makes multi-row reports usable at all. | Multi-row diffs align on declared key columns; both notices asserted by CLI e2e; a single-commit DB produces the vacuity notice, and the same DB after arrival replay does not. |
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
  *(Update 2026-07-25: Part VII now carries concrete designs for
  adjustments (VII-A1) and symbol identity (VII-A3); both need no
  external calendar dataset, so only calendars stay deferred.)*
- **README addition (cheap, trust-buying):** a "when NOT to use h5i-db"
  section (multi-TB distributed, OLTP, sub-µs capture) — also stops agents
  from mis-recommending it.

## Build order (supersedes Part V's; revised for dual-axis + VI-B)

1. **VI-A1 `context` (incl. VI-B3 freshness) + VI-A3 `next_actions` + VI-A2
   agent profile** — small, single-site changes with the largest per-line UX
   effect; VI-A2 is also a category first.
2. **Dual-axis research-mode + VI-B2 arrival-delta hardening + VI-A5
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
| 6 | arrival-delta quick fixes: vacuity notice + zero-is-not-innocence output line | S | VI-B2 (2)(3) |
| 7 | `--idempotency-key` on mutations | M | VI-A5 |
| 8 | `demo` + docs-as-tests (demo scripted as a restatement scenario) | M–L | VI-A4 |
| 9 | README rewrite: one-sentence "Why for agents" + "when NOT to use h5i-db" (incl. cadence honesty). Companion, same day: restructure SKILL.md — frontmatter, task-shaped core ≤60 lines (golden loop / decision rules / research loop), references/ split; thereafter every feature PR that obsoletes a workaround deletes its SKILL.md line | S (+S) | Part VI findings; VI-A7 prep |
| 10 | Two half-day verifications: D2 (does the ASOF probe use `tolerance` for early exit?) + D1 (TopK dynamic-filter config on DF 54) | S+S | Part IV addendum |

Suggested start order within batch 1: 9 → 6 → 4 → 3 → 2 → 1 → 10 → 5 → 7 → 8.

### Batch 1 implementation status (2026-07-24, branch `agentic-features`)

**All ten delivered.** Built in the order above except that the docs (#9) and
docs-as-tests (#8) moved to the end, so they describe shipped reality and the
test validates the final text rather than an interim one.

| # | Status | Notes |
|---|--------|-------|
| 1 | ✅ | `context` + `--budget` + `--stale-after` (VI-B3 folded in). Budget shedding is monotone in the budget — the omission record is counted against it, and entries are fixed-size counts, because listing dropped names let a *tighter* budget produce a *larger* document. |
| 2 | ✅ | `H5I_DB_PROFILE=agent`: 1000 rows / 1 MiB, lazy Parquet spill (capped at 5M rows, reported when hit), stderr summary with an honest `total_rows`. Default profile byte-identical, incl. plan-level LIMIT pushdown. Explicit `--max-bytes` keeps its hard exit-4 contract. |
| 3 | ✅ | Envelope v2: `schema_version`, `next_actions[{cmd,why}]` with `<db>` substituted by the CLI, `did_you_mean` via case-insensitive edit distance. A CI test *executes* a suggested command. Python bindings expose both. |
| 4 | ✅ | `query --as-of` + `H5I_DB_AS_OF`; pins every table. Session construction now classifies its error, so a bad decision point is exit 2, not 5. |
| 5 | ✅ | `--decision-time` + `--embargo` + `H5I_DB_DECISION_TIME`, enforced by registering each table behind a view. **Design change:** the axes are independent, not one instant driving both — under a bulk ingest the commit and event clocks are years apart, and coupling them made the event-time axis unusable on exactly the databases it exists for. Fails closed on tables with no time column or a unitless integer one. |
| 6 | ✅ | `vacuous` + `notes` on every arrival-delta report. |
| 7 | ✅ | `--idempotency-key` on every direct mutation path; recorded in the manifest, matched by a bounded 64-commit walk back from head. |
| 8 | ✅ | `demo` (restatement scenario, ~0.25 s) + docs-as-tests + a CI job that also checks the skill stays `npx skills`-installable. |
| 9 | ✅ | README leads with the point-in-time claim and gains a when-NOT-to-use section; SKILL.md is task-shaped (65 lines) over `references/`. |
| 10 | ✅ | Both already delivered — see the Part IV addendum for the evidence. |

**Bug found and fixed after the batch (2026-07-24): the pin leaked through the
table functions.** `--as-of` / `--decision-time` bound a session by swapping
what the *catalog* returns, but `h5i()`, `asof_join()`, `gapfill()`,
`resample()`, `tail()` and `latest_on()` resolve their tables straight from the
`Database`, so they read past the pin: `FROM h5i('trades')` returned the head
under both axes. The tests only ever exercised plain table references, which is
why item 5 passed while the guarantee did not hold. Closed in `pin.rs`: the
arrival axis is applied to every table function (so `--as-of` holds and the
operators stay usable), and the event-time axis refuses them, since they
consume their tables internally rather than exposing something the cutoff can
be pushed into. **Lesson for the remaining tiers: a guarantee stated over
"every query" must be tested through every path that reaches storage, not just
the one the feature was written against.**

**Renamed after the fact (2026-07-24): `leakage-check` → `arrival-delta`**
(CLI verb, `Database.arrival_delta`, `arrival::arrival_delta`,
`ArrivalDeltaReport`, and the `leakage_detected` field → `changed`). The old
name promised a verdict the tool cannot deliver: look-ahead comes in many
shapes and this sees one of them, so a clean result never meant a clean query.
That is the same overclaim the README carried, and the `vacuous`/`notes`
fields existed largely to walk the name back. `diff_version` was considered
and rejected because `VersionDiff` already names a different thing (the
data-level diff in `incremental.rs`); `arrival-delta` also coheres with the
arrival/event-time vocabulary the rest of Part VI uses, and limits its own
scope by name.

**Not yet done, and deliberately so:** the release-profile benchmark gate
(`benchmarks/run_performance_workload.py`) has not been re-run. Nothing in
this batch touches the scan or plan layer — the default query path keeps its
LIMIT pushdown and early exit, the extra work is one `getenv` and one `Option`
compare per batch, and the catalog listing added for `did_you_mean` is on the
error path only — but that reasoning should be confirmed against the gate on
a reference machine before merge (Part II invariant 7).

**Batch 2 — trust & substrate** (repays the Tier 0 debt the flag stands on,
and runs the two design tracks in parallel with it).

| # | Task | Size | Ref |
|---|------|------|-----|
| 11 | Differential correctness harness vs DuckDB (`sqllogictest-rs`; start with the supported subset + golden `.slt` for ASOF / `time_bucket` / time-travel — it is also the V-A2 acceptance check that `asof(t)` ≡ physically truncated data) | L | T0.1 |
| 12 | Re-enable fuzz smoke + harden the string SQL rewriters into a real parser (same PR series: the fuzz target hunts the mis-parses) | M | T0.3 + T0.4 |
| 13 | `proptest` storage invariants (≥8: append→scan multiset, compact preservation, delete-range exactness, time-travel roundtrip, …) | M | T0.2 |
| 14 | Run ledger × keystone `(commit, query)` cache: joint design doc, schema first (runs schema, cache key, metrics attachment; ledger implementation itself is batch 3+) | M | Part VI run ledger |
| 15 | Keystone `(commit, query)` result cache implementation per #14, following the P2/P3 checksum-keyed / fail-open discipline | L | Part V keystone |
| 16 | arrival-delta key-based row alignment (`--key <cols>`) | M | VI-B2 (1) |
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

---

# Part VII — Quant data-layer features (zipline/qlib source study, 2026-07-25)

Sourced from a source-level study of `~/Ref/zipline` (final Quantopian
master, `014f1fc3`) and `~/Ref/qlib` (`79633dd9`), on branch
`quant-features`, filtered by the same scope rule as Parts III–IV: h5i-db
borrows data-layer and analytics-layer mechanisms, never the backtester,
order simulation, ML platform, or live-trading surface. Findings that refine
an *existing* item (run ledger #14, RFC #17, T0.1, D6, gapfill) are folded
into Tier VII-C rather than duplicated as new items. File:line references
are to the studied checkouts above. Tier VII-D (companion analytics layer)
was added the same day from an ecosystem check of the maintained metric
libraries (`quantstats` active; `empyrical` unmaintained since ~2020, fork
`empyrical-reloaded`; `skfolio` / `Riskfolio-Lib` for optimization;
`pypbo` for backtest-overfitting probability) — re-verify maintenance status
before depending on any of them. VII-B7 was added 2026-07-26 from a source
study of `~/Ref/Riskfolio-Lib` (v7.3.0), which confirmed the buy-don't-build
boundary for optimization/shrinkage while surfacing the small
order-statistic slice that belongs in the engine.

**Framing.** Both engines converge on the same lesson from opposite
directions: their most valuable machinery exists to fake, in application
code, what a versioned timestamp-native database provides structurally.
zipline's bundle system copies the entire dataset into a timestamped
directory per ingest to get arrival-axis time travel
(`data/bundles/core.py:374-491`; cite in docs as the motivating
anti-pattern for h5i-db manifests). qlib's PIT store hand-rolls fact-level
bitemporality as linked lists of byte offsets, read whole-file per query,
because no engine sat underneath (`qlib/data/data.py:748-830`, self-labelled
"not multi-threading-safe"). And both share one architectural regret worth
recording as a warning: positional calendar indexing (integer offsets into a
global calendar array) hardcodes bars-per-day assumptions
(`high_freq.py:86`: 240), silently loses precision past 16.7M bars (a
float32 row index, `file_storage.py:336`), and structurally rules out
crypto/irregular grids (`domain.py:200-207` makes "date not on the calendar"
a hard error). h5i-db's timestamp-native model with calendars kept at the
edge as data is confirmed, not challenged, by both studies.

**Layering rule (decided 2026-07-25; governs every item in this Part).**
Rust/engine if the feature *reads data as part of its semantics* or must
hold under a research-mode pin; Python companion if it *orchestrates
queries or renders artifacts*. Two facts settle most cases. (1) There is no
"export to Python" step for SQL-reachable features: UDFs/UDAFs/UDTFs are
registered at one site (`crates/h5i-db-query/src/session.rs:238-269`) and
are immediately callable from `db.sql(...)`, the CLI, and the UI. (2) The
batch-1 pin bug is the cautionary case — a feature that resolves tables
outside the catalog escapes the pin, so `adjust()` / `pit()` / universe
resolution in Python would sit permanently outside the only guarantee the
project claims ("bounded at the source"). Conversely, do **not** move
companion math to Rust for speed alone: returns-series metrics run over
thousands of rows where numpy already operates at memory bandwidth and the
PyO3/Arrow boundary crossing costs the same order as the computation, so the
speedup is a wash while the wheel/MSRV/iteration costs are real (§P5's
evidence gate and invariant 7 apply — profile first). Row count decides:
scan-path work (20 M rows) is Rust, post-query work (10³–10⁵ rows) is
Python. The one legitimate non-speed reason to move a companion item into
Rust is **CLI reachability** — an agent driving the binary cannot call a
Python-only helper (see VII-D4).

## Tier VII-A — Corporate actions, PIT fundamentals, identity

The new data-layer features. All three exploit versioning/ASOF machinery
that already exists; none needs an external calendar dataset.

| # | Item | Source | Acceptance criteria |
|---|------|--------|---------------------|
| VII-A1 | **Adjustment layer with read-time ratios.** A versioned `adjustments` table `(entity, effective_date, kind: mul\|add, value)` plus an `adjust()` surface (table function or rewrite) that restates a price window into the basis known at decision time t by composing ratios as a suffix product over effective dates ≤ t. Dividend ratios (`1 - amount/prev_close`) are computed **lazily at read time against the pinned version's closes**, fixing zipline's write-time baking (`data/adjustments.py:456-530`), which silently desynchronizes ratios from restated bars. The `add` kind covers futures roll splicing, which zipline models as the *same* mechanism (`data/history_loader.py:189-205`): one feature covers splits, dividends, and continuous futures. The perspective flag (`is_perspective_after`, `history_loader.py:471-545`) maps onto the existing decision-time axis. | zipline `lib/adjusted_array.py:181-318`, `lib/_windowtemplate.pxi:118-137` (apply adjustments known on-or-before the anchor row), `lib/adjustment.pyx:280-412`, `pipeline/loaders/frame.py:50-59` (apply-date vs effective-date split) | Golden cases reproduce zipline's worked split example (0.5 split, `history_loader.py:471-545`); the same adjusted query at two DB versions with a restated close yields correspondingly different dividend ratios (the write-time-baking bug is untestable in zipline, provable here); a futures roll-splice case via `kind=add`; volume uses 1/ratio for splits only; differential-tested against a hand-written pandas reference. |
| VII-A2 | **Point-in-time fundamentals pattern.** A `(instrument, field, period, arrival_date, value)` table shape plus a `pit()` read surface: last announced value per period as of decision time (ASOF on `arrival_date`), a `P()`-style collapse that evaluates a period-axis expression per calendar bar (`P(Mean($$roewa_q, 2))` = 2-period mean of the quarters *known at t*), and qlib's hard refusal to reference future periods (`pit.py:32-36` raises). Two zipline rules land in the same feature: effective visibility of an event is `max(event_date, arrival_date)` (`pipeline/loaders/utils.py:123`, the ASOF-with-embargo rule in one line), and *forward-looking* rows (next earnings date) are visible only in `[learned_at, happens_at)` (`utils.py:25-79`). Fact-level arrival is a **column**, which commit-level versioning cannot express: this independently validates `available_at` in RFC #17. | qlib `pit.py:24-72`, `data.py:748-830`, 20-byte record + linked-list format (`config.py:247-258`, `docs/advanced/PIT.rst`); zipline `pipeline/loaders/utils.py:25-138` | The qlib golden restatement case reproduces (2019Q2 `roewa` reads 0.0 on 07-15..17 and 0.175322 from 07-18, `tests/test_pit.py:96-104`); a future-period reference errors; reads prune on `arrival_date` via the manifest (no per-bar loop, verified with `--stats`); a `[learned_at, happens_at)` visibility case for a forward-looking event. |
| VII-A3 | **Symbol identity and universe membership spans.** A `symbol_mappings` pattern with half-open ownership periods where each period's end is *recomputed as the successor's start* (gap elimination makes as-of resolution total, zipline `assets.py:104-138`); ambiguity-as-error lookup semantics (no as-of date + multiple owners must fail, never guess: `MultipleSymbolsFound`, `assets.py:819-867`); universe membership as `(instrument, valid_from, valid_to)` spans applied as a **post-filter after window computation** (qlib threads spans down and masks after per-instrument evaluation, `data.py:559-628`), so index entry never truncates a lookback window. | zipline `assets/assets.py:101-189, 745-867`, `asset_db_schema.py` (sid + time-ranged symbol rows + EAV side-table); qlib `file_storage.py:193-218`, `data.py:691-723` | A ticker-reuse fixture resolves to different entities by as-of date; an ambiguous no-date lookup errors with the candidates listed; a rolling factor over a universe-filtered query equals compute-then-filter (window not truncated at membership start); an expression-defined universe (qlib `ExpressionDFilter`, `filter.py:312-370`) compiles to spans that enter the result-cache key. |

## Tier VII-B — Factor/expression surface

| # | Item | Source | Acceptance criteria |
|---|------|--------|---------------------|
| VII-B1 | **Close the rolling-operator gap.** The study's headline quantification: ~20 operators cover the *entire* published qlib factor zoo (Alpha158 + Alpha360), all expressible as window functions or mergeable UDAFs. Missing in h5i-db today: windowed OLS `slope`/`rsquare`/`resi` (regression of x on within-window position), `idxmax`/`idxmin` (1-based argmax position), time-series `rank` (percentile of current value in trailing window), rolling `corr`/`cov` (with the zero-variance NaN mask, `ops.py:1494-1497`), `mad`, rolling `quantile`. qlib's incremental accumulators (`_libs/rolling.pyx`: ring buffer carrying `x_sum, x2_sum, y_sum, xy_sum…`, O(1) per step) are literally mergeable aggregate states, so these are P3-cache-eligible from day one. | qlib `ops.py:713-1524` (full vocabulary), `_libs/rolling.pyx:48-134`; zipline `factors/statistical.py:484-572` (`vectorized_beta` with an explicit `allowed_missing` NaN budget) | Parity with pandas/qlib reference output on a full-mantissa dataset (the P3 float lesson applies); merge-of-states ≡ recompute property test per operator; `Skew` rejects N<3 and `Kurt` N<4 (qlib semantics); zero-variance windows yield NULL, not garbage, for corr/rsquare. |
| VII-B2 | **Cross-sectional window functions.** `cs_rank`, `cs_zscore`, `cs_demean`, `cs_winsorize` over `PARTITION BY <time bucket>`. Neither reference engine has this in its expression layer: qlib has *zero* cross-sectional operators (cross-section lives in a separate pandas processor stage, `processor.py:300-371`), and zipline implements them as a triple-nested Python loop (`lib/normalize.py`). One SQL statement replacing a two-stage factor pipeline is the cheapest differentiated win in this Part. Borrow zipline's NaN discipline: winsorize cutoffs count non-NaN only (`factor.py:1855-1889`); masked entities are excluded from the statistic but NaN-filled in the output. | zipline `factors/factor.py:540-1086`, `lib/normalize.py`; qlib `processor.py:300-371` (`CSRankNorm`, incl. the `(rank_pct-0.5)*3.46` unit-std rescale) | A one-statement SQL factor reproduces qlib's `CSRankNorm` pipeline on a golden dataset; NaN/mask semantics match zipline's row funcs; works composed over `time_bucket` output (minute and daily). |
| VII-B3 | **Alpha158/360 conformance corpus.** The ~518 expression strings (`contrib/data/loader.py:61-310, 4-58`) are a ready-made test suite for the factor surface: they exercise exactly the VII-B1/B2 vocabulary plus Ref/Mean/Std/Abs/Log/Greater/Less and nothing else. Compile each to SQL, run against a pinned fixture, compare to recorded qlib output. This is a quant-flavored extension of the T0.1 differential harness, not a separate mechanism. | qlib `contrib/data/loader.py` | Every Alpha158 K-bar/price/rolling family and Alpha360 column compiles and matches recorded qlib reference values within tolerance on a golden Parquet fixture; corpus runs in CI (subset per-PR, full nightly); failures name the expression string. |
| VII-B4 | **Lookback widening + `window_safe`.** Two plan-time properties. (1) Widen-then-trim: derive each rolling expression's required lookback statically and extend the scanned time range by the union lookback, trimming before emit, so a time-filtered rolling query is correct at the range edge (qlib `get_extended_window_size`, `base.py:222-235`, returns an exact `(left, right)` pad and represents *future* refs as a right pad; zipline computes union-`extra_rows` per leaf and per-consumer `offset` truncation, `graph.py:302-457`). This is the D6 planning rule. (2) `window_safe`: an expression-level flag making adjustment-correctness a type property; rolling windows over `adjust()`-ed prices are rejected unless the input is adjustment-invariant (returns, ratios), because composing windows over restated levels is silently wrong (zipline raises `NonWindowSafeInput`, `term.py:610-614`, `errors.py:517-528`). Depends on VII-A1. | qlib `base.py:222-235`, `ops.py:764-824`; zipline `pipeline/graph.py:302-457`, `term.py:95, 610-614` | A rolling query over `WHERE ts >= t0` equals the unfiltered query sliced to `>= t0` (no silent warmup truncation, property-tested); N overlapping windows over one table plan a single widened scan (verified via `--stats` bytes); a windowed aggregate over a non-invariant adjusted level errors with an explanation, and over returns passes. |
| VII-B5 | **Volatility & liquidity estimators (SQL-side).** Two families of per-bucket estimators that need bar/tick shape rather than a returns series, and that no SQL engine ships. (1) OHLC volatility: Parkinson, Garman-Klass, Rogers-Satchell, Yang-Zhang — pure per-bucket OHLC arithmetic, so they extend the existing OHLCV rollup and are mergeable-aggregate-state eligible exactly like `vwap`. (2) Realized-variance family (realized variance, bipower variation, a noise-robust two-scale variant) and liquidity/microstructure measures (Amihud illiquidity, Roll effective spread, Kyle's lambda, order-flow imbalance, VPIN) — all windowed aggregations over trades/quotes. These are the "metrics" most often hand-rolled in pandas per notebook, and they belong beside `vwap`, not in the companion layer. | Ecosystem gap (no maintained SQL/engine implementation); standard estimator literature | Each estimator matches a reference implementation on a golden OHLCV/tick fixture; the OHLC family registers as P3-cache-eligible aggregate states and a warm re-query reuses states; documented tick-data preconditions (which need trades vs quotes) so a wrong-input call errors rather than returning a plausible number. |
| VII-B6 | **Label generation & stationarity transforms (SQL-side).** Triple-barrier first-touch labeling (profit target / stop / time horizon, whichever is hit first) expressed over the horizon-join machinery rather than a pandas loop — this is the concrete consumer that justifies **D5 HORIZON JOIN**. Plus fractional differentiation as a fixed-width weighted window (a stationarity transform, hence a window function). Both are numerical methods, not metrics, and both are per-row over full tables, so they are engine work by the layering rule. | López de Prado labeling/transform lineage; D5 (QuestDB 9.3.3 horizon join) | Triple-barrier labels match a reference pandas implementation including simultaneous-touch tie-breaking (documented and deterministic); labeling a large table streams in bounded memory; fracdiff weights are computed once per (d, width) and the transform matches reference output; both refuse to run under a `--decision-time` pin without an explicit opt-in, since they read forward in time by construction. |
| VII-B7 | **Tail-risk and drawdown aggregates (SQL-side, 2026-07-26).** Order-statistic risk measures as mergeable UDAFs with rolling UDWF forms: `var_hist(x, alpha)` and `cvar_hist(x, alpha)` (historical VaR / expected shortfall), `semi_deviation(x)`, `lpm(x, mar, p)` (lower partial moments: the Sortino/Omega denominators), plus the drawdown family (`max_drawdown`, `avg_drawdown`, `ulcer_index`) as ordered-pass window functions over a cumulative-return level (the `ewma` execution shape). Pure sorting/cumsum math, no linear algebra. The engine-tier justification is form, not speed: the value is `GROUP BY`/`PARTITION BY` reachability (per-symbol, per-regime CVaR in one statement; a rolling 252-bar CVaR as a window function) over scan-scale tables, which a post-query returns-series library structurally cannot express. The single-series scalar versions stay delegated per VII-D2. `var_hist` subsumes the rolling `quantile` gap named in VII-B1. Explicitly excluded (buy, don't build): EVaR/RLVaR/EDaR/RLDaR (solver-bound: exponential/power cones), the range variants (VRG/CVRG/TGRG…), and L-moment CRMs. | Riskfolio-Lib `riskfolio/src/RiskFunctions.py` source study (v7.3.0, 2026-07-26): ~35 of its 44 risk measures are pure numpy order statistics with no cvxpy dependency; its `tests/test_portfolio.py` doubles as a numerical oracle | Each aggregate matches the Riskfolio-Lib reference on a full-mantissa golden returns fixture (the P3 float lesson); rolling forms match a pandas `rolling().apply()` reference; the quantile interpolation rule is pinned and documented, not "whatever the sort gives"; minimum-observation rules error rather than emit garbage (CVaR requires at least one tail observation, `ceil(alpha·n) ≥ 1`); drawdown functions document their input contract (level vs. return series) and require an explicit ordering; cookbook page `03_risk_and_production/01_var_expected_shortfall` is rewritten on top of the new functions instead of hand-rolled SQL. |

## Tier VII-C — Refinements folded into existing items

No new item numbers; these amend the named designs.

| Folds into | Refinement | Source |
|---|---|---|
| Run ledger (#14) | Record the **uncommitted** `git diff` / `git status` / `git diff --cached` alongside the commit SHA (research runs are dirty-tree runs); split immutable **params** from mutable **tags** (qlib's online/offline model state is a tag query, not a param); artifacts form a declared DAG with existence-checked prerequisites and idempotent regeneration (`depend_cls` + `check()` + `skip_existing`), which is what makes a ledger *replayable* rather than merely recorded. | qlib `workflow/recorder.py:362-378`, `record_temp.py:34-159, 212-246`, `workflow/online/utils.py:19-180` |
| Research-mode / walk-forward | A walk-forward **span planner** as a pure API: `(template spans, step, expanding\|sliding, embargo) → [(train, embargo, test)…]`, calendar-aligned, generated before any compute; qlib's `RollingGen` (+ `trunc_days` leak truncation and the `MultiHorizonGenBase` label-leak accounting) is the field-tested shape. First consumer of the keystone `(commit, query)` cache. | qlib `workflow/task/gen.py:126-137, 140-301, 304-348` |
| Research-mode embargo | The embargo should eventually be **data, not a constant**: zipline expresses per-session availability as a cutoff timestamp table (default: 45 minutes *before* the open, `data_query_offset`), which handles half-days and per-market conventions without a calendar dependency in the engine. | zipline `pipeline/domain.py:60-75, 169-209` |
| Gapfill/LOCF | Two load-bearing rules: (1) never forward-fill past an entity's end-of-life (zipline restores NaNs after `asset.end_date` because ffill will happily fabricate prices for delisted symbols); (2) seeding a leading gap requires a backward last-traded lookup whose value is restated into the window's perspective (composes with VII-A1). `LAST_VALUE(… IGNORE NULLS)` reintroduces bug (1) unless spans (VII-A3) bound the fill. | zipline `data/data_portal.py:988-1032` |
| Keystone cache / P2-P3 | Confirmations, not changes: canonicalize-then-hash keys (sort/dedupe/strip *before* md5; range deliberately excluded from the key; cache the full series, slice at read) and per-node memoization making CSE free. Key the keystone cache on `(version, canonical_expr, freq)`. | qlib `utils/__init__.py:271-274, 350-372`, `cache.py:502-557`, `base.py:184-203` |
| `context` / data health (VI-A1/B3) | A `describe`-style data-health surface has a field-tested metric list: per-column null count/ratio, inf count, distinct count, mean/std/skew/kurt, lag-1 autocorrelation, per time bucket. All trivially SQL; a natural aggregate-state-cache consumer. | qlib `contrib/report/data/ana.py:28-216`, `scripts/check_data_health.py` |

## Tier VII-D — Companion analytics layer (Python, 2026-07-25)

Pure-Python, post-query, over frames the engine already returns. Packaged
as an optional extra or separate distribution (the `h5i-db-llm` precedent in
`DESIGN.md §9`) so `pip install h5i-db` stays pyarrow-only and pandas-scale
dependencies never enter the base wheel. No Rust core: every item here runs
over 10³–10⁵ rows (see §Layering rule).

**Selection principle.** Single-run performance ratios are commodity —
wrap, never reimplement (zipline itself delegates to `empyrical` rather than
writing its own Sharpe). What is *not* commodity is anything whose input is
**many runs** or **raw microstructure**, because both are things only a
versioned store can hand you and a returns-series library structurally
cannot see. VII-D1 is the differentiated item; VII-B5/B6 are the
microstructure half and VII-B7 the grouped/rolling risk half, deliberately
placed in the engine tier.

| # | Item | Rationale | Acceptance criteria |
|---|------|-----------|---------------------|
| VII-D1 | **Overfitting statistics wired to the run ledger** (the flagship of this tier). Deflated Sharpe Ratio, minimum track record length, Probability of Backtest Overfitting (CSCV), effective number of independent trials, and multiple-testing corrections (White's Reality Check, Hansen's SPA). | Every one of these requires the **number of independent trials** that produced the winning strategy. In practice that number is guessed or omitted, because nothing records how many backtests were run — the run ledger (#14) records exactly it, and V-D1's stability sweep produces the trial × time-slice matrix CSCV consumes. This is a statistic nobody else can compute *honestly*, which is a stronger claim than computing it faster. The math is thin (`pypbo` is a single-purpose research repo); the value is the wiring. | DSR/MinTRL match published reference values on the source papers' worked examples; the trial count is read from the ledger rather than passed by hand, and a run not in the ledger is refused rather than silently counted as one trial; CSCV consumes the sweep matrix with shared sub-results deduped by `(commit, query)`; a deliberately overfit strategy set yields DSR ≈ 0 / high PBO and a robust one does not. |
| VII-D2 | **Version-attributed tearsheet.** Standard ratios (Sharpe, Sortino, Calmar, Omega, CAGR, volatility, drawdown table, VaR/CVaR, tail ratio, win rate, profit factor, capture ratios, Kelly, ulcer index, alpha/beta, rolling variants) **delegated** to `quantstats` / `empyrical-reloaded` (single-series scalar form only; grouped/rolling SQL forms of the tail-risk subset are engine work, VII-B7); statistical tests (ADF, KPSS, Hurst, variance ratio, Ljung-Box) delegated to `statsmodels` / `arch`. h5i-db's contribution is the header: the data version SHA, the arrival/decision-time pins, and the embargo the numbers were computed under. | "A Sharpe you can cite" — plain quantstats cannot say which data produced its number. Wrapping keeps the metric surface at zero maintenance while the provenance line is the part only this project can write. | Report header carries version SHA + both pin axes + embargo, and regenerating from the same SHA reproduces byte-identical numbers; ratio values match the wrapped library exactly (we are not a second implementation); the wrapper is generic over `f(returns) -> scalar` so a new metric needs no plumbing (zipline's `ReturnsStatistic` shape). |
| VII-D3 | **Factor evaluation report.** Information coefficient and rank IC per date, ICIR, IC decay across horizons, quantile-bucket forward returns and the top-minus-bottom spread, factor autocorrelation, and turnover. Computation is cross-sectional SQL (VII-B2) plus horizon joins (D5); this item is the summarization and rendering. | The alphalens/qlib overlap, and the natural consumer of VII-B2 — the IC decay curve is one query rather than a per-horizon loop. Both reference projects ship this and both compute it in pandas; we can push the heavy half into SQL. | IC/rank-IC match qlib's `SigAnaRecord` output on a golden fixture; the decay curve is produced by a single horizon-join query (verified in the plan, not a Python loop); quantile spreads reproduce alphalens semantics incl. NaN/mask handling. |
| VII-D4 | **Validation splitters.** Purged K-fold cross-validation with embargo and combinatorial purged CV (CPCV) — index arithmetic with no data access. **Must share code and embargo semantics with the walk-forward span planner** (VII-C), not duplicate them. | Numerical methods rather than metrics, and the natural companion to walk-forward: both answer "which spans may this fold see". Duplicating embargo logic in two places is how the two drift apart. | Purged folds contain no observation whose label horizon overlaps a test fold (property-tested); CPCV path count matches the reference combinatorics; a leaking split is rejected with the offending indices named; splitter and span planner share one embargo implementation. **Interface decision (defer to #14):** if the run ledger exposes walk-forward as a CLI verb, the shared span/embargo core moves to Rust for agent reachability and Python keeps only the sklearn-shaped wrapper. |

**Out of scope for this tier** (buy, don't build): portfolio optimization
and covariance shrinkage/denoising (`skfolio`, `Riskfolio-Lib`,
`PyPortfolioOpt` own this), execution algorithms and market-impact models,
Monte-Carlo strategy simulators, and any plotting framework beyond the
single tearsheet. Reimplementing a standard ratio is a defect, not a
feature.

## Do NOT borrow (confirmed by source study)

- **All storage machinery**: bcolz ragged ctables, `.bin` flat files with a
  float32 header index, HDF5 dataset caches with hand-built row-offset
  indexes, Redis reader/writer locks, `.meta` pickle sidecars. Parquet +
  manifest + DataFusion supersedes every one (row-group stats replace the
  hand-built indexes; MVCC manifests replace the locks; commit versions
  replace mtime-free cache invalidation).
- **Bundle-style time travel** (full copy per ingest, directory-listing
  resolution): cite as the anti-pattern h5i-db's manifests exist to kill.
- **Positional calendar indexing** in any form; reaffirms the Part VI
  do-not entry for calendars. Date spines are input relations.
- **numexpr fusion** (zipline `pipeline/expression.py`) and **LabelArray**
  (hand-built dictionary encoding): DataFusion's planner and Arrow
  dictionary arrays are the modern equivalents. One requirement survives:
  dictionary-encoded columns carrying overwrite-style adjustments must remap
  adjustment payloads alongside the baseline (`adjusted_array.py:340-357`).
- **Backtest machinery**: MetricsTracker/Ledger/PositionTracker, simulation
  clock gating, MLflow/MongoDB infrastructure. And note zipline itself
  delegates risk statistics to `empyrical` rather than reimplementing them:
  Sharpe-class portfolio metrics belong in the Python companion layer over
  exported returns (VII-D2), not in SQL aggregates.
- **Reimplementing any commodity performance ratio.** See VII-D2: wrapping a
  maintained library is the feature; a second implementation is a defect.

## Cross-references (Part VII ⇄ existing parts)

- VII-A1 amends the Part VI do-not list (adjustments pulled forward with a
  concrete calendar-free design); `window_safe` (VII-B4) is its guard.
- VII-A2 ⇄ **RFC #17 `available_at`**: fact-level arrival is the same axis;
  design them together. Also ⇄ D5 HORIZON JOIN (label generation reads the
  other direction along the same event axis).
- VII-A3 ⇄ **A1 global symbol dictionary**: entity identity and dictionary
  interning are one design conversation.
- VII-B1 ⇄ **P3 aggregate states** (mergeable accumulators) and **D6**
  (rolling workload in the bench first; custom operators only on a measured
  loss).
- VII-B3 ⇄ **T0.1** differential harness: same substrate, quant corpus.
- VII-B5 ⇄ **P3 aggregate states** (the OHLC volatility family is the second
  mergeable-state family after OHLCV/VWAP, and its best graduation evidence).
- VII-B6 ⇄ **D5 HORIZON JOIN**: triple-barrier labeling is the concrete
  workload that justifies building it.
- VII-C run-ledger rows ⇄ **#14** joint design doc; span planner ⇄ the
  keystone cache's walk-forward consumer.
- **VII-D1 ⇄ #14 run ledger ⇄ V-D1 stability sweep** — the tightest coupling
  in this Part: the ledger supplies the trial count, the sweep supplies the
  trial × slice matrix, and VII-D1 is the statistic they exist to enable.
  None of the three is fully valuable alone; consider them one story when
  sequencing.
- VII-D2 ⇄ **VI-A2 output profile** (a tearsheet is a spill-sized artifact,
  not a context-window payload) and ⇄ the run ledger's declared metrics: the
  companion layer is the *producer* of the metrics the ledger attributes.
- VII-D3 ⇄ **VII-B2 + D5**: it is the reporting half of those two.
- VII-D4 ⇄ **VII-C span planner** (shared embargo core) and ⇄ **#14** for the
  Rust-vs-Python interface decision.

## Build order (relative to the batch 2 list)

1. **VII-B2 cross-sectional functions + VII-B1 rolling UDAFs** — small,
   additive, immediately testable; start VII-B3's corpus with whatever
   subset compiles and grow it as operators land (it doubles as the T0.1
   quant extension, batch 2 #11).
2. **VII-B4** widen-then-trim in the planner (with D6's bench workload) and
   the `window_safe` flag stub.
3. **VII-A1 adjustment layer** — the flagship of this Part; ships with
   `window_safe` enforcement and the roll-splice case.
4. **VII-A3 identity/universe spans** — sequence with A1 (shared entity-key
   design).
5. **VII-A2 PIT fundamentals** — sequence with RFC #17; the table shape
   works today via ASOF, so docs/cookbook can precede the dedicated surface.
6. **VII-C rows** land opportunistically inside their host items.

The companion tier runs on its own track, gated on the run ledger rather
than on the engine items above:

- **VII-D2 tearsheet first** — it is mostly wrapping, it is the cheapest
  demonstration that provenance-attributed metrics are worth having, and it
  produces the "declared output metrics" the ledger design (#14) needs a
  concrete consumer for. Do it while #14 is being designed, not after.
- **VII-B5** alongside VII-B1 (same UDAF machinery, and the OHLC volatility
  family is P3's graduation evidence).
- **VII-D3** once VII-B2 lands; **VII-B6 + D5** as a pair when a labeling
  workload appears.
- **VII-D4** with the walk-forward span planner, sharing one embargo core.
- **VII-D1 last of the tier, and only after the run ledger exists** — it is
  the differentiated item, but a DSR computed from a hand-passed trial count
  is exactly the overclaim the Part VI positioning correction warns about.
  The statistic is only honest when the ledger supplies the count.

Tier 0 (Part III) remains the standing precondition, and the Part VI
positioning correction applies to how this Part is marketed: these are
features of a versioned time-series database for quant research, not
components of a leakage-proof pipeline.

---

# Part VIII — Lazy DataFrame builder for the Python API (2026-07-25)

**Decision.** Build a polars-shaped lazy query builder in the Python
package that **compiles to the existing SQL surface**. This is the unbuilt
half of a standing commitment, not a new direction: `DESIGN.md §1` names
the primary API as "SQL **and** DataFrame", `DESIGN.md §6` claims parity
between them, and `DESIGN.md §6.6` already slates `rolling(mean, '30m')`
sugar for "the DataFrame/Python API, not a new engine". Today none of that
exists in Python: the surface is `db.sql(string)` plus `db.read(...)` (a
fixed keyword-argument scan, not composable), and `QueryResult` calls
itself lazy while wrapping an already-materialized table.

**Why a builder, precisely.** For a one-off interactive query, SQL is fine
and often clearer; the builder is not a replacement surface. It wins in
three cases, and the first is the one this project is now committed to:
(1) *programmatic composition*: the VII-B factor workload means generating
hundreds of expressions in loops over windows and columns, which f-string
SQL does with quoting hell and injection risk (qlib grew a bespoke
expression DSL for exactly this; zipline's Pipeline *is* this builder
pattern); (2) *tooling*: autocomplete, type checks, and build-time errors
instead of a parse error out of a 40-line string; (3) *reusable handles*:
a lazy pipeline that can be extended with another `.filter(...)` and
executed against N pinned versions (the Part V "same query across N
versions" surface).

**The decided interface** (vocabulary follows polars, which the target
user already knows; explicitly *not* ArcticDB's `q[q["x"] > 1]` style):

```python
(db.table("trades", as_of="2026-07-01")     # lowers to the h5i() UDTF, so the pin holds
   .filter(col("symbol").is_in(syms))
   .group_by("symbol")
   .agg(col("price").mean().alias("px"))
   .collect())                               # or .to_pandas() / .to_polars()
```

## Lowering rules (the design's load-bearing wall)

1. **The builder is a compiler, never an evaluator** (the Part VII
   layering rule governs). Every verb lowers to SQL text executed through
   the same native `sql()` path, against the same session with the same
   registered UDTFs/UDFs/UDAFs/UDWFs
   (`crates/h5i-db-query/src/session.rs:238-275`). No Python-side kernel,
   ever: a verb that cannot lower to SQL does not ship (it goes to the
   engine, or to Tier VII-D if it is post-query analytics).
2. **Version resolution stays in the catalog.** `db.table(name,
   version=…| as_of=…| snapshot=…)` lowers to `h5i('name', …)` (plain
   table name when unpinned), so research-mode pins see the query exactly
   as they see hand-written SQL. The batch-1 pin bug is the cautionary
   case: any Python-side table/version resolution would sit permanently
   outside "bounded at the source".
3. **The generated SQL is a first-class artifact.** `.sql()` returns it,
   `.explain(analyze=…)` proxies EXPLAIN; formatting is deterministic so
   it can be logged, diffed, snapshot-tested, and recorded by the run
   ledger (#14) with no new record shape. This is also the graduation
   path: a user who outgrows the builder copies its SQL and keeps going.
4. **Injection safety is structural, not disciplined.** All identifiers
   and literals pass through one central quoting/escaping formatter;
   user strings never concatenate into SQL anywhere else.
5. **Escape hatch from day one.** `sql_expr("…")` embeds a raw SQL
   expression as an `Expr`. Full SQL coverage through builder verbs is an
   explicit non-goal; the escape hatch is the pressure valve that keeps
   the verb set closed.
6. **Zero new pyo3 surface in v1.** The builder is a pure-Python module
   inside `h5i_db` with no new dependencies (the base wheel stays
   pyarrow-only, the VII-D packaging rule untouched). Crossing a
   `LogicalPlan`/Substrait over the boundary is deferred until SQL-text
   generation is *measured* as limiting (inexpressible plan or
   double-parse cost visible in the bench), per the §P5 evidence gate.

## Tier VIII-A — Builder core

| # | Item | Acceptance criteria |
|---|------|---------------------|
| VIII-A1 | **Expression core + lowering.** `col()`/`lit()`; arithmetic, comparison, boolean ops; `is_in`/`is_null`/`between`; `alias`, `cast`. Verbs: `filter`, `select`, `with_columns`, `sort`, `limit`. `.sql()` and `.explain()`. The central identifier/literal formatter (rule 4). | Differential test: every documented example's `.collect()` equals `db.sql()` of hand-written golden SQL (Arrow-level equality). Property test over adversarial identifiers and string literals (quotes, unicode, reserved words) round-trips correctly. Generated SQL is byte-stable across runs (snapshot-tested). |
| VIII-A2 | **Time-travel entry point.** `db.table(name, version=\|as_of=\|snapshot=)` lowering per rule 2; `.collect(memory_limit=, timeout=, max_rows=)` passes through to the existing `sql()` limits; terminal methods return the existing `QueryResult`. | A pinned builder query behaves identically to the equivalent `h5i()` SQL under a research-mode pin (tested with a stale-version fixture). Conflicting pin kwargs error the same way `db.read` does. Unpinned `db.table('t')` lowers to the plain name (latest, re-resolved per query). |
| VIII-A3 | **Aggregation + the registered operator surface.** `group_by`/`agg` with standard aggregates plus `vwap`/`wavg`; `.over()` windows; `ewma`, the VII-B1 rolling UDWFs (`ts_rank`, `ts_corr`, `mad`, …) and VII-B2 cross-sectional (`cs_rank`, `cs_winsorize`) as expression methods; the `DESIGN.md §6.6` rolling sugar (`col('price').rolling_mean('30m')`) lowering to the documented `RANGE INTERVAL` window pattern. | Each operator reachable from the builder is differential-tested against its documented SQL form in `docs-src/manual/sql.md`. Rolling sugar generates exactly the documented window-frame SQL. An unknown aggregate/operator raises at build time (not engine parse time) with a did-you-mean, reusing the envelope-v2 edit-distance approach. |
| VIII-A4 | **Joins.** `.join(other, on=, how='inner'\|'left')` between two builder pipelines (lowered via subqueries/CTEs) and `.join_asof(other, on=, by=, direction=, tolerance=)` lowering to the `asof_join` UDTF. | `join_asof` matches the documented UDTF semantics; the raw-time-unit tolerance caveat from the SQL manual is surfaced in the docstring. A worked example joins the same table at two pinned versions (the Part V N-versions surface) and is documented. |
| VIII-A5 | **Escape hatch, docs, drift protection.** `sql_expr()`; a manual page with executable examples; `skills/h5i-db/references/python.md` update; fix the `QueryResult` docstring (it claims lazy; the builder is the lazy handle, `collect()` materializes). | The docs page passes the executable-docs test; every builder example shows its `.sql()` output so the docs teach the lowering, not just the verbs. The skill reference lists the verb set. The base wheel gains no new dependency (checked in CI metadata). |

## Do NOT build

- **No eager mode, no Python-side compute.** The moment one verb executes
  in Python, the pin guarantee and the layering rule are both broken for
  every pipeline containing it.
- **No ArcticDB-style `__getitem__` DSL.** One vocabulary (polars), one
  way to spell each verb.
- **No `LogicalPlan`/Substrait boundary crossing in v1** (rule 6; revisit
  only on measured evidence).
- **No full-SQL-coverage ambition.** New verbs are accepted only with a
  programmatic-composition use case; one-off queries belong in SQL, and
  `sql_expr()` covers the gap meanwhile.
- **No new capability.** The builder must add ergonomics only: anything it
  can express is reachable via `db.sql()` and the CLI, so agent
  reachability (the layering rule's CLI clause) holds by construction.

## Cross-references (Part VIII ⇄ existing parts)

- Part VIII **is** the concrete design for the DataFrame half of
  `DESIGN.md §1/§6`; the §6 claim "they share plans, so feature parity is
  free" is made true in Python by sharing *SQL text* rather than plan
  objects.
- VIII-A2 ⇄ **research-mode pin** (Part VI, `pin.rs`): the entry point
  must route through `h5i()` so the pin sees every builder query.
- VIII-A3 ⇄ **VII-B1/B2**: the builder is the second frontend to those
  operators; the **VII-B3** Alpha158 corpus can later compile qlib
  expression strings through the builder, making the corpus double as
  builder conformance.
- VIII-A4 ⇄ **D5 / `asof.rs`**: `join_asof` is the DataFrame verb
  `DESIGN.md §6.4` promised ("operator-first: DataFrame `join_asof` …").
- VIII-A5 ⇄ **VI-A2 / DESIGN.md §8**: agents keep driving SQL/CLI; the
  builder is the human-notebook surface (`DESIGN.md`: "notebook usability
  is how a DataFrame store earns adoption").
- `.sql()` ⇄ **run ledger #14**: a builder pipeline is recorded by its
  generated SQL; the ledger needs no new record shape.

## Build order

1. **VIII-A1 + VIII-A2 together** — expression core and pin-correct entry
   point are one correctness story; neither is shippable alone.
2. **VIII-A3** — the factor workload is the motivating user; sequence
   after VII-B1/B2 land (they define the operator surface being wrapped).
3. **VIII-A4**, then **VIII-A5** finishing touches — though docs land in
   lockstep with each item (the drift test forces this anyway), so
   VIII-A5 is really a running obligation plus the final sweep.

This Part is additive and must not block Part VII engine work: the
builder wraps whatever operator surface exists at the time, and grows
with it.

---

# Part IX — Fork: multi-agent writable workspaces (2026-07-26)

**Decision.** Build `fork` as **catalog aliasing plus pins**, not as
branched table histories. A fork never writes to a base table: it gets a
fork-scoped catalog, a GC pin on the base versions it was created from,
and, on first write to a base name, a **copy-on-write shadow table** (a
new `table_id` whose first manifest is a copy of the pinned base
manifest: O(#segments) of JSON, zero data movement). The branch pointer
is the catalog; the branch itself is an ordinary table, so every
existing codepath (commit, flock, vacuum, retention, time-travel,
compaction) applies to it unchanged. Verbs: `fork create / diff /
promote / drop`. **No merge**: promote is a table-granularity
compare-and-swap into main. Fork-and-discard is the dominant agent path
and stays O(1).

**Positioning.** The "embedded × analytical × local × branch"
intersection is empty. *BranchBench* (arXiv 2604.17180, Columbia DAPLab;
verified 2026-07-26, lifting the Part V ⚠ on existence) defines the
workload (agentic speculative branching: fork per hypothesis, discard
losers, promote winners) and benchmarked only server systems (Neon,
DoltgreSQL, TigerData, Xata, Postgres baselines); no embedded entrant
exists. DuckLake chose OCC over branching (⚠ open feature request,
Discussion #194, unverified). h5i-db's answer is neither `cp -r`
(O(data), no shared GC, no diff) nor OCC (conflicts *during* work):
workspaces are disjoint by construction, so agents conflict only at
promote, and explicitly.

## Design rules (the load-bearing wall)

1. **A branch is a table; the catalog is the branch pointer.** `HEAD`
   stays the *only* mutable object per table (`layout.rs:8`). No
   fork-scoped HEAD namespaces, ever: a second mutable object per table
   dir would make every manifest-listing codepath branch-aware.
2. **The fork object is a `Snapshot` superset and a GC root.** Layout:
   `forks/<hash>.json` (pins `{table_id → sequence + manifest_checksum}`
   plus a freeform `user_meta` blob) and
   `catalog/forks/<hash>/tables/<hash>.json` for fork-scoped entries.
   Retention (`retention.rs` floor checks) and `drop_table`
   (`database.rs:507`) consult forks exactly as they consult snapshots.
3. **Refinement invariant.** A shadow manifest may reference only (a)
   segments in its own dir, or (b) segments listed in the pinned base
   manifest. (b) is GC-safe because the pin holds the base retention
   floor, so those segments stay inside base vacuum's referenced set for
   the fork's lifetime. Enforced at the only two points where a
   cross-table path can enter a manifest — shadow materialization and
   promote — and re-derived by `verify`. *(Corrected during
   implementation: the original plan said "enforced at commit time",
   which would have cost two extra object reads on every fork write to
   re-check a property that holds inductively. Later commits can only
   add own-prefix segments or carry forward already-checked ones.)*
4. **FORMAT bump is a fence, not a migration.** `FORMAT` goes to 2 on
   first fork creation so pre-fork binaries refuse to open rather than
   raising retention floors past pins they cannot see. No existing
   object is rewritten.
5. **Conflict unit is the table; policy is CAS, first-commit-wins.**
   Promote commits the shadow head into main with `expected_version` =
   the fork's pinned base sequence (the field already exists:
   `database.rs:108`, checked at `database.rs:1018`). The loser gets a
   clean version conflict: drop, or re-fork and re-run. This is the
   V-E2 vocabulary made structural.
6. **Fork-mode compact filters to `created_by_sequence > fork base`.**
   "Compact never copies base bytes" becomes a rule, not an emergent
   property of the small-segment threshold.
7. **Reads never touch HEAD.** Fork reads resolve name → pin → manifest
   by direct path, so they cannot observe (or contend with) concurrent
   main writers.

## Tier IX-A — Fork core

| # | Item | Acceptance criteria |
|---|------|---------------------|
| IX-A1 | **Fork object + pin integration.** `fork create <name> [--as-of TS]` (pins via `as_of_sequence`, `database.rs:696`); read path through the fork (two-level catalog, pinned manifests). A read-only fork is already a named, frozen, multi-table as-of view. | Retention floor refuses to rise above a fork pin; `drop_table` refuses on fork-pinned tables; `vacuum` after a main compact deletes nothing a live fork reads (fixture: fork, compact main, vacuum, fork still scans byte-identical results). A format-1 binary refuses to open a DB containing a fork. `put_if_absent` create: racing same-name creators get one winner, one clean error. |
| IX-A2 | **Fork-scoped catalog + `drop` + `list`.** Tables created in a fork live under the fork catalog; `fork drop` deletes fork-owned tables first, fork object (the pin) last; `fork list` shows age, pinned bytes, tables created/shadowed. | Fork-created tables are invisible to the main catalog and to main `vacuum`'s table listing. Kill mid-drop: the fork stays pinned and re-running `fork drop` completes (idempotent, resumable). Pinned-bytes accounting matches `du` on a fixture. |
| IX-A3 | **COW shadow-on-write + invariant enforcement.** First write to a base name creates the shadow (manifest copy at `sequence: 0`, `parent: None`, provenance under a namespaced `user_meta` key — **no new manifest field was needed**); later commits are ordinary. | Zero parquet bytes copied at shadow creation (asserted on bytes written). N concurrent forks writing the same logical table produce zero writer-lock contention (each holds its own `table_id` flock). Any manifest violating the refinement invariant is rejected where the path is introduced; `verify` detects a violating fixture. |
| IX-A4 | **`fork diff`.** Catalog diff (created / shadowed / dropped names) plus per-shadow segment-set diff via `segments_by_checksum()`, rows/bytes deltas, schema-revision changes. | Diff reads manifests only (no segment I/O, asserted). Output stable and machine-readable (JSON), so an agent can gate promote on it. |
| IX-A5 | **`fork promote --table T`.** Hardlink (local FS) or backend copy of the shadow's own-dir segments into the base dir, then commit the shadow head manifest as base's next version, `OpKind::Promote`, `expected_version` = pinned base sequence. | First promote wins; second gets a version conflict, not a partial merge. Main never contains cross-dir segment refs (fsck after promote + fork drop). When every intervening base commit is `OpKind::Compact`, the error names it and suggests rebase-over-compaction (the rebase itself may land as a fast-follow). Hardlink path does zero byte copies on one filesystem. |

## Costs accepted, stated honestly

- **Deferred reclamation.** Live forks hold retention floors down, so a
  compacted main temporarily stores old + new bytes until forks drop
  (same tradeoff as snapshots today; `fork list` pinned-bytes is the
  visibility valve).
- **Promote is O(#segments) metadata ops**, not pure metadata (hardlinks
  locally, server-side copy on object stores). Promote is the rare
  verb; keeping vacuum fork-ignorant is worth it.
- **Shadow history restarts at sequence 0**; time-travel across the fork
  point is a two-hop walk through the recorded origin. Within a fork,
  time travel *below* the pin works normally (the base's history is still
  the base's history), and reads above it are `VersionNotFound` — a fork
  can look back but never forward.
- **Fork-per-agent is the assumed mapping.** Two agents sharing one fork
  fall back to today's single-writer-per-table semantics.

## Do NOT build

- **No 3-way merge, no row-level conflict resolution.** That is Dolt's
  decade. The conflict unit is the table and the policy is CAS; write
  that down instead of pretending promote is not a merge.
- **No `--embargo` on forks.** A sliding "now" ceiling cannot be
  enforced once a fork writes derived tables (the guarantee would have
  to police every write path forever). Look-ahead protection stays with
  research-mode's read-only pins; revisit only on observed demand.
- **No global content-addressed segment pool as a fork prerequisite.**
  It is a layout migration that makes GC reachability global and taxes
  databases that never fork. Revisit for cross-table dedup on its own
  evidence, not for this.
- **No fork-of-fork in v1.** Base = main only; nesting multiplies the
  promote/GC paths before the primitive has users. *(Superseded by Part X
  X-C1 on BranchBench's evidence: MCTS explores a deep narrow tree and
  cannot be expressed as a flat star. Delivered 2026-07-27.)*
- **No run-ledger coupling.** Neither feature exists yet; the
  `user_meta` blob is the loose join point, and correlation can live in
  the ledger later.

## Cross-references (Part IX ⇄ existing parts)

- IX-A1 ⇄ **snapshot machinery** (`snapshot.rs`, `retention.rs`): the
  fork object is a `Snapshot` superset; pins reuse the floor-refusal and
  drop-refusal checks verbatim.
- IX-A5 ⇄ **`CommitOptions.expected_version`** (`database.rs:108`): the
  promote CAS is an existing field, not new machinery.
- Part IX ⇄ **V-E2**: supersedes V-E2's "CLI/skill workflow, not an
  engine concept" for the fork lifecycle; fork → explore →
  commit/abort + first-commit-wins is now engine substrate, and the
  review-UI verification gate sits on top of `fork diff`.
- Part IX ⇄ **research-mode (Part VI dual-axis)**: `fork --as-of` is the
  writable counterpart of the read-only pin ("a workspace over a frozen
  base you can write into"); the pin machinery is shared.
- IX-A3 ⇄ **VII-B factor workload**: the shadow table is where an agent
  materializes per-hypothesis features; N hypotheses share one base with
  zero copies and zero contention.

## Build order

1. **IX-A1** — pins + read path; ships a named multi-table as-of view
   on its own.
2. **IX-A2** — writable workspace via fork-created tables, before any
   COW exists.
3. **IX-A3** — COW shadows + the invariant (the correctness core).
4. **IX-A4** — diff (agents gate on it; drop is already covered by A2).
5. **IX-A5** — promote, last: rarest verb, only one touching main.

Each step is independently shippable; nothing in Part IX blocks or is
blocked by Parts VII/VIII (different subsystem: catalog/GC, not query
surface).

## Part IX implementation status (2026-07-26, branch `support-fork-quant`)

**Delivered: IX-A1 … IX-A5 complete**, plus CLI and Python surfaces.
`crates/h5i-db-core/src/fork.rs` (types, storage, operations),
fork-awareness in `database.rs` (`open_fork`, `entry`, `list_tables`,
`effective_head`, `entry_for_write`, `materialize_shadow`,
`drop_table`, `verify`), pin checks in `retention.rs`, `h5i-db fork
create|list|show|diff|promote|drop` plus a global `--fork` flag, and
`Database.create_fork/fork/forks/fork_diff/promote/drop_fork` in Python.
Tests: 16 unit + 43 core integration + 10 CLI e2e + 12 Python.

**Three things the design got wrong, found by building it:**

1. **"Vacuum's code does not change" was false, and dangerously so.**
   Vacuum's orphaned-table-directory sweep (`database.rs`, the
   `table.is_none()` branch) collects any `tables/<uuid>/` not in the
   *global* catalog. Fork-owned tables are deliberately absent from it —
   that is what makes them invisible to main — so the sweep deleted every
   fork's shadow and scratch tables outright. The reachability roots had
   to grow to cover every fork catalog. Verified by mutation: reverting
   that fix makes `vacuum_never_collects_a_forks_own_tables` fail with a
   missing HEAD. The lesson generalizes: *"which catalog"* was an
   unstated assumption in every reachability computation, not just the
   one the design predicted.
2. **The FORMAT bump had to be a new axis, not a version increment.**
   Raising `FORMAT_VERSION` would have stamped every *manifest* as v2 and
   locked v1 readers out of fork-free databases. The fence needed its own
   constant (`READER_VERSION` / `FORK_MIN_READER_VERSION`) applied only to
   the database-level `min_reader_version`, leaving manifests at v1.
3. **The refinement invariant did not belong at commit time** (see design
   rule 3).

**Two bugs the tests caught in adjacent code**, both in the Python
bindings: `promote_conflict` fell through to the base `H5iError` instead
of `ConflictError`, and `as_of` via `datetime` lost ~hundreds of ns to
float64 rounding (`dt.timestamp() * 1e9` needs 61 mantissa bits), enough
to land a cutoff on the wrong side of a commit.

**Deferred as designed:** `--embargo` on forks, fork-of-fork, run-ledger
coupling, global content-addressed segment pool, row-level merge. Also
not built: dropping a *base* table from inside a fork (would need a
tombstone in the fork catalog; refused with a clear message instead), and
rebase-over-compaction for a promote blocked only by layout changes — the
condition is detected and named in the error, but the caller still
re-forks by hand.

## Part VIII implementation status (2026-07-25, branch `data-frame-lazy-run`)

| # | State | Notes |
|---|---|---|
| VIII-A1 | ✅ | `h5i_db/dataframe.py`: `Expr` with real precedence rendering (no defensive parentheses), `col`/`lit`/`sql_expr`/`when`, verbs `filter`/`select`/`with_columns`/`sort`/`limit`/`head`/`unique`/`pipe`, `.sql()`/`.explain()`/`.schema()`. One quoting site; identifiers **always** quoted so Arrow's case survives (bare SQL folds to lowercase). |
| VIII-A2 | ✅ | `Database.table(name, version=\|as_of=\|snapshot=)` → `h5i()`; unpinned → bare name, which is snapshot-bound per query so two references to one table agree. Conflicting pins raise `InvalidInputError` with `code`/`hint` set like the native layer. |
| VIII-A3 | ✅ | `group_by().agg()`/`.count()`, `.over()`, rolling sugar (row-count **or** duration frames), the VII-B1 UDWFs, the VII-B2 cross-sectional pair plus `cs_demean`/`cs_zscore` as generated SQL, `ewma`, `vwap`/`wavg`, `time_bucket`. Build-time validation for alpha range, winsorize cutoffs, durations and cast types. |
| VIII-A4 | ✅ | `.join()` (inner/left/right/full/cross/semi/anti) with `l`/`r` as contract aliases; `.join_asof()` lowering to the `asof_join` UDTF. **It refuses a pinned or already-operated-on side** rather than silently reading latest — the table function's blind spot, surfaced instead of inherited. |
| VIII-A5 | ✅ | `sql_expr()`; `docs-src/api/dataframe.md`; skill reference updated; the `QueryResult` "lazy" docstring corrected (it was never lazy). Base wheel gains no dependency — asserted by a test that AST-parses the module's imports. Mistyped operators raise at build time naming the nearest real method (`.groupby` → `.group_by`), the edit-distance approach the CLI envelope already uses. |

**Verification.** 127 Python tests (`crates/h5i-db-python/python/tests/`),
at **100% line and branch coverage** of `dataframe.py`,
run in CI for the first time — the job previously built the wheel and ran an
inline smoke script, so `test_bindings.py` was never executed. Coverage:
differential tests against hand-written golden SQL (Arrow-level equality) for
aggregation, OHLCV, every window/rolling/cross-sectional operator, scalar
functions, joins and ASOF; adversarial round-trips for nine identifier shapes
(reserved words, embedded quotes, unicode, dots, `--`) and eight literal
shapes, including `'; DROP TABLE trades; --` as a value; pin tests proving a
pinned pipeline differs from the same pipeline at head; wrap-rule tests
pinning the flat-vs-subquery decisions; and byte-stability of generated SQL.

Two test techniques earned their keep and are worth reusing. Precedence is
checked **numerically** — every expression is computed twice, once as
generated SQL and once in Python, so a misplaced parenthesis surfaces as a
wrong number instead of passing a string comparison nobody reads closely.
And the wrap rules are checked by **executing** a matrix of verb orderings
rather than reasoning about them; that matrix is what found all four
wrap-rule defects, including `.limit(n).group_by(...)` applying `LIMIT`
after the grouping, which planned cleanly and returned a wrong answer. The
lesson generalises past this Part: a query builder's dangerous bugs are the
ones that produce *valid* SQL meaning something else, and only execution
finds them.

Writing the numeric cases also settled a semantics question: integer/integer
division truncates, because the builder must not mean something different
from the same expression in `db.sql()`. Pinned by test, called out in docs.

**Ten defects, none found by reading.** Four wrap-rule bugs came from the
verb-ordering matrix; an adversarial review pass (fuzzing arithmetic trees
against a Python evaluator, and sweeping verb × verb combinations) found six
more, two of them silent wrong answers: `a * (b / c)` rendering as
`a * b / c` (only AND/OR may be flattened — every other operator is
left-associative, and integer division makes the re-association observable),
and a filter after a window-function projection folding into the same level,
where SQL's WHERE-before-SELECT order recomputed the window over the
surviving rows. The other four were wrong at the boundary: a projection
aliasing the pending sort key silently re-pointed `ORDER BY`; `with_columns`
documented a replace it never implemented; `.over()` on a compound
expression bound to the last operand only; and `join(on=[])` dropped the
`ON` clause and cross-joined. All are fixed and pinned; `with_columns` now
overwrites via an explicit `replace=` lowering to `* EXCEPT (…)`, which is
the schema-free way to do it without breaking laziness.

The pattern across all ten: **not one was a crash.** Every defect either
produced valid SQL meaning something else, or a planner error far from its
cause. A builder that emits a string the engine accepts has no failing
edge to trip over, so correctness has to come from executing the
combinations and comparing values — budget review effort accordingly.

**Generated tests** (`test_dataframe_matrix.py`) then made both techniques
permanent rather than one-off investigations: seeded fuzzing of arithmetic
and boolean operator trees against a Python reference implementing *SQL*
semantics (truncating division, sign-follows-dividend modulo), and the full
7×7 and 7×7×7 verb matrix, each pipeline executed and checked against
invariants — a row limit anywhere still bounds the result, a filter still
holds at the end, a window column is never recomputed downstream. Driving
the remaining gaps with `coverage --branch` was worth it twice over: it
found `.sign()` lowering to a function DataFusion does not have (dead API
that no test had ever called) and a `when(...).then(...)` chain being
unnameable without `.otherwise()`. The chain is now an `Expr` in its own
right, since a `CASE` with no `ELSE` is already a complete expression.

Two lessons for the next surface of this kind. Coverage of a *generated*
API is not busywork: an unexercised method is one that may not lower to
anything real, and only calling it finds out. And the fuzzer's reference
must model the target's semantics, not Python's — the first draft used
Python's flooring division and reported a false positive within seconds.

**Then a vacuity audit, which is the finding worth carrying forward.**
Asked whether the suite was actually comprehensive, the answer was no, and
for a reason coverage cannot show: the fixture generated **one row per
timestamp**, so every `PARTITION BY ts` bucket held a single row. On that
data `cs_rank` is always 1.0, `cs_demean` always 0.0, `cs_zscore` always
NULL and `cs_winsorize` an identity. The Tier VII-B2 operators had passing
differential tests that would have passed against a badly broken
implementation, because both sides computed the same degenerate answer.
Ranking, tie-averaging, percentile normalisation and NULL exclusion were
never observed at all. The same audit found **no NULL anywhere in any
fixture**, leaving the documented NULL discipline of those operators
untested.

`test_dataframe_semantics.py` fixes both with a panel fixture (5
timestamps × 6 symbols, including a tie, an outlier and a zero-variance
bucket) and a nullable column whose NULL pattern ranges from none to all,
with expectations from references written in plain Python **from the
specification** rather than from the generated SQL. It pins one trap worth
naming: SQL's three-valued logic means `filter(p)` and `filter(~p)` do not
partition the rows, since `NOT NULL` is NULL — surprising in an API shaped
like polars. Mutation-checked: making the cross-section global instead of
per-bucket fails seven tests, five of them the new ones.

The generalisation, for the next operator family: **a differential test on
degenerate data proves only that both sides are degenerate.** Any operator
defined over a group needs a fixture where the group has more than one
member, and the fixture should assert its own non-degeneracy — which
`test_the_panel_fixture_is_actually_a_cross_section` now does, so the
guard cannot rot back into vacuity unnoticed.

**Type breadth and scale** (`test_dataframe_types_and_scale.py`) close the
last two gaps the audit named. Types: timezone-aware timestamps (aware,
naive and offset literals; `time_bucket` with a timezone; RANGE interval
frames, the combination most likely to break on an aware column), ns/ms
units, `date32`, boolean columns used as predicates in their own right,
`decimal128` precision surviving `sum`/`mean`, and the int32 → int64 /
float32 → double promotions, pinned because the result type is not the
input type. Scale: 16k rows over 8 appends, asserting from `EXPLAIN
ANALYZE` that a time-range filter opens fewer segment groups than a full
scan and an unsatisfiable predicate opens none, that a memory budget turns
an overrun into a typed `LimitError`, and that a deadline cancels a
quadratic query. This is the first Python-side check of the pruning claim
`manual/sql.md` makes; it was previously only reachable via the CLI's
`--stats`.

One of those was a genuine open question rather than a formality: **does
the builder's subquery wrapping defeat predicate pushdown?** It does not —
a wrapped pipeline opens exactly the same segment groups as the flat one.
Worth keeping as a test, because a regression there would cost a full scan
on every wrapped query while every correctness test still passed.

`test_docs_are_executable.py` closes a gap the Rust
`docs_are_executable` test leaves: that test only runs lines starting
`h5i-db`, so Python fences were never checked. The new one executes every
runnable Python fence on the builder page **and** asserts each following
sql fence is what the example actually compiles to — a claimed lowering
is now a tested one (mutation-checked: corrupting a documented frame
fails it).

**Deferred, as designed.** No `LogicalPlan`/Substrait crossing (rule 6 —
SQL-text generation has not yet been measured as limiting). Join output
columns are not deduplicated, so a `SELECT *` over two tables sharing a name
yields both; documented rather than solved, since fixing it needs schema
knowledge the builder deliberately does not fetch at build time.

---

# Part X — Fork at agentic scale (2026-07-26)

**Decision.** Evolve Part IX's fork from "a handful of long-lived agent
workspaces" to "thousands of ephemeral hypotheses". One new structure,
the **fork visibility index** (X-A1), funds the three capabilities that
matter: cross-fork aggregation (X-A2/A3), constant-time GC guards at
high fork counts (X-A1), and, later, fork-of-fork (X-C1). Everything
else in this Part is lifecycle plumbing (batch create/drop, an
ephemeral tier) and the benchmark that proves the claims.

**Positioning.** *BranchBench* [1] (the paper that lifted Part IX's
existence ⚠, now read in full) benchmarked Neon, DoltgreSQL, TigerData,
Xata and Postgres baselines against five agentic workloads (software
engineering, failure reproduction, data cleaning, MCTS, Monte Carlo
simulation) and found (i) a fundamental tension: content-addressed
designs fork instantly but reads degrade 5-4000x as branches and
mutations accumulate, while page-server designs keep reads fast but pay
25-1500x on branch creation and cap at ~20 live branches; and (ii) **no
system supports cross-branch aggregation**, which every
compare-outcomes workflow ends with. h5i-db dodges (i) by construction:
shadow manifests reference segment paths directly, so there is no chain
replay and no tree traversal, and read cost is independent of how the
fork came to be. What we lack, measured against their workloads: the
simulation topology (1000 flat forks) hits our O(#forks) guard scans;
MCTS (deep narrow trees, depth ~25) hits "no fork-of-fork"; data
cleaning and simulation both end in a cross-fork aggregation we cannot
push below the application. Closing those three gaps occupies the
design point BranchBench measured as empty: instant forks, flat reads,
cross-branch aggregation, GC that survives branch churn.

## Design rules (the load-bearing wall)

1. **The index is a cache, never a source of truth.** Pin JSONs remain
   the authoritative, self-checksummed GC roots (IX design rule 2). The
   visibility index is generation-stamped and rebuilt from the fork
   objects on any mismatch; a corrupt or missing index costs a rebuild,
   never data. No new crash-consistency obligations.
2. **Sharing granularity is the segment; visibility is metadata.**
   "Which forks see this tuple" is constant within a segment, so branch
   visibility is a per-segment fork bitmap, not a per-tuple annotation.
   This deliberately takes the metadata-level shortcut past
   tuple-versioned designs (Decibel's membership bitmaps [2], provenance
   semirings [9]): at our granularity the bitmap join captures the
   runtime win with none of the machinery.
3. **Aggregates decompose over immutable segments.** `agg(segment)`
   cached by (path, checksum) is correct forever; a fork's answer is
   the combine of shared-segment partials plus its own deltas.
   Count/min/max come free from Parquet footers; sums are cheap;
   quantiles use mergeable sketches (t-digest
   [6], KLL [7]) and are approximate, labeled as such.
4. **Ephemeral means ephemeral.** The ephemeral fork tier is
   process-local state: no file, no GC root, no durability, lost on
   crash by design ("branches die young", the weak generational
   hypothesis [8] applied to forks). Promotion to a durable fork is one
   JSON write, the same object IX-A1 already defines.
5. **Fork-of-fork must not create read chains.** The refinement
   invariant generalizes to "own segments, or segments pinned by any
   ancestor"; manifests keep direct paths, so depth-25 reads cost the
   same as depth-1. If an implementation choice would make read cost a
   function of fork depth, it is the wrong choice (that is the failure
   mode BranchBench measured in Dolt).

## Tier X-A — Visibility index + cross-fork reads

| # | Item | Acceptance criteria |
|---|------|---------------------|
| X-A1 | **Fork visibility index.** One maintained structure answering "which forks pin this table/segment, at what sequence" in O(1) reads: per-table pin map plus per-segment fork bitmaps (EWAH-style). Consumers: retention floor check (`retention.rs`), `drop_table` guard, vacuum's orphan sweep and root walk (`database.rs`), X-A2's scan planning. | The three guard paths do constant index reads instead of listing every fork (asserted on object-read counts). Differential test: index answers equal the slow full-scan answers on randomized fork populations. Delete or corrupt the index: next consumer rebuilds it and results are byte-identical. 1000-fork fixture: vacuum and `set_retention` wall time flat vs fork count. Index write is not on the fork-read path (readers never require it). |
| X-A2 | **Cross-fork union scan.** `scan_forks(table, forks=…)` in the query layer: the table unioned across N forks with a `__fork` column, planned so shared base segments are scanned once and per-fork delta segments appended. Python surface returns a lazy DataFrame (Part VIII builder verb). | Results equal N independent per-fork scans stacked (differential test). Shared segments opened exactly once regardless of N (asserted from `EXPLAIN ANALYZE` / stats, the VIII-A5 technique). Works on base + forks that shadowed, forks that did not, and fork-created tables absent from some forks (absent forks contribute zero rows, not errors). |
| X-A3 | **Per-segment aggregate cache + data-level diff.** Partial aggregates cached by (segment path, checksum); `sum`/`count`/`min`/`max` exact, quantiles via mergeable sketches; `fork diff --data` reports row/byte deltas from segment set difference without scanning shared segments. | 1000-fork aggregation over a mostly-shared table scans shared segments once and reads cached partials thereafter (asserted on bytes read, second run). Cache entries invalidate by checksum only (mutation test: edited segment fixture is detected). Sketch-based quantiles within documented error on the panel fixture. `fork diff --data` does no segment I/O for unchanged segments (asserted). |

## Tier X-B — Lifecycle at agentic scale

| # | Item | Acceptance criteria |
|---|------|---------------------|
| X-B1 | **Batch fork verbs.** `fork_many(n, prefix)` resolves the pin set once and writes N fork objects; batch drop for mass pruning. | Star-1000 fixture: `fork_many(1000)` does one HEAD read pass + 1000 object writes (asserted on read counts), and is >10x faster than a create loop. Batch drop of k forks leaves the index and GC roots consistent (verify passes); kill mid-batch: re-run completes, no fork half-dropped (per-fork ordering from IX-A2 preserved). |
| X-B2 | **Ephemeral fork tier.** In-process fork: pin set in memory, fork-created/shadow tables under a scratch prefix, no fork object written. `persist()` promotes to a durable fork (writes the IX fork object); process exit or crash abandons it. | Ephemeral create/drop round-trip writes zero objects under `forks/` (asserted). `persist()` then crash: the durable fork is complete and pinned. Crash before `persist()`: next vacuum reclaims the scratch tables (they are orphans by design, and the sweep must see them as such). Read/write semantics identical to durable forks (shared test suite runs in both modes). |
| X-B3 | **Rebase-over-compaction on promote.** Finish the case IX-A5 already detects: when every intervening base commit is `OpKind::Compact`, promote rebases automatically instead of telling the caller to re-fork (logical content unchanged, only layout moved). | A promote blocked only by compaction succeeds and the result equals re-fork-and-replay (differential fixture). Any non-Compact intervening commit still yields the clean CAS conflict. `verify` passes after rebase; no cross-dir refs in main. |

## Tier X-C — Fork-of-fork

| # | Item | Acceptance criteria |
|---|------|---------------------|
| X-C1 | **Nested forks** (supersedes IX "not in v1" on BranchBench's evidence: the MCTS topology is deep narrow trees, and MCTS is the flagship agentic pattern). Pins form a DAG; generalized refinement invariant (design rule 5); promote targets the parent (fork or base) with the same CAS semantics; subtree drop; configurable depth cap. | Read latency flat vs fork depth (measured to depth 32, the claim that differentiates us). Dropping a parent with live children is refused (or subtree-drop is explicit); vacuum with a 3-level tree deletes nothing any live descendant reads (mutation-check the root walk, the IX lesson). Kill mid-subtree-drop: resumable, never an unpinned fork with live tables. Promote fork→parent-fork and fork→base both CAS-conflict cleanly against concurrent parent commits. |

## Tier X-D — Benchmarks + docs

| # | Item | Acceptance criteria |
|---|------|---------------------|
| X-D1 | **`fork-bench`**: BranchBench-shaped microbenchmarks [1] ported to the embedded setting: n-th fork creation latency, read/write throughput vs live fork count, storage amplification, reclamation speed after mass drop, branching overhead ratio; topologies star-1000 and wide-shallow now, deep-narrow after X-C1. | Runs in CI at reduced scale (serial, memory-bounded); full scale documented and reproducible. Numbers land in docs, not just CI logs: the README-level claim ("instant forks, flat reads, cross-fork aggregation") cites this benchmark. |
| X-D2 | **Docs debt.** `docs/OPERATIONS.md` gains a fork section (vacuum interaction, pinned-bytes math, backup implications: the runbook currently predates forks entirely); `DESIGN.md` gains the fork design (the note at §"restore to fork forward" is stale); remove dead `fork::pins_by_table`, re-export `ForkSummary`. | The OPERATIONS vacuum and disk-usage sections answer "why is disk not shrinking" for a fork-pinned base without leaving the page. Docs-are-executable coverage extends to fork CLI examples. Dead code gone (`cargo +nightly udeps`-clean or equivalent grep test). |

## Costs accepted, stated honestly

- **Index maintenance is write amplification**: one extra object write
  per fork create/drop (amortized to ~zero inside `fork_many`). Bounded
  by design rule 1: losing the index costs a rebuild scan, which is
  exactly today's behavior.
- **The aggregate cache trades storage for scan time.** Entries are
  immortal-correct (immutability) but not free; the cache is evictable
  and bounded, and X-A3 must publish its size the way `fork list`
  publishes `bytes_pinned`.
- **Ephemeral forks are lost on crash.** That is the contract; the cost
  is that "my exploration disappeared" is a support question with a
  one-line answer (`persist()`).
- **Fork-of-fork multiplies the promote and GC paths** (the reason IX
  deferred it). The depth cap and subtree-drop keep the DAG tame; the
  X-C1 crash fixtures are the price of admission, not optional.
- **Quantiles over cross-fork scans are approximate** when served from
  sketches. Exact remains available by scanning; the API must say which
  it did.

## Do NOT build

- **Row-level merge / 3-way merge.** Unchanged from Part IX, and now
  with workload evidence: BranchBench's MCTS and simulation workloads
  discard branches, they do not merge them; table-granularity promote
  CAS covers the software-engineering case.
- **Per-tuple provenance annotation / K-relation single-pass
  evaluation** [9]. Design rule 2: at segment granularity the bitmap
  join subsumes it. The semiring formulation is a paper for
  tuple-versioned systems, not a feature for this one.
- **Content-hash transposition detection** ("same state reached twice
  collapses to one branch"). State identity is not storage identity in
  a non-canonical store: row order, segment layout and compaction
  timing all break the hash. Only canonical content-addressed designs
  (Dolt's prolly trees) can even attempt it.
- **Speculative pre-fork / warm pools** (Catalyzer [10], REAP [11]
  territory). Those hide compute-provisioning latency that a
  page-server architecture pays per branch. Our fork create is one
  metadata write; there is nothing to hide.
- **Policy-aware GC** (evict by search value, not LRU). In an embedded
  model the search framework owns pruning and calls `drop_fork`
  itself; teaching the DB the agent's value function is coupling
  without a customer.
- **Delta-chain recreation/storage optimization** (the
  Bhattacherjee/OrpheusDB/ForkBase lineage [3][4][5]). A real research
  line, aimed at systems with version chains; we have none. Our
  nearest analog is compaction policy for accumulations of small
  fork-local segments, which the existing compactor already owns.

## Cross-references (Part X ⇄ existing parts)

- X-A1 ⇄ **IX design rule 2**: the index is a derived view over the
  fork objects that rule made GC roots; it changes their *lookup* cost,
  never their authority.
- X-A2/A3 ⇄ **Part VIII builder**: `scan_forks` is a builder entry
  point like `Database.table`; the `__fork` column and lazy-DataFrame
  surface reuse VIII-A2's pinning discipline, and the "assert segment
  opens from EXPLAIN ANALYZE" technique is VIII-A5's scale test.
- X-B2 ⇄ **IX-A2 drop ordering**: an abandoned ephemeral fork must look
  like the crash states IX-A2 already recovers from (orphan scratch
  tables), not a new failure mode.
- X-B3 ⇄ **IX-A5**: closes the "detected, named, deferred" rebase.
- X-C1 ⇄ **IX "Do NOT build: fork-of-fork in v1"**: superseded on
  evidence, with IX's own lesson #1 (every reachability computation has
  an unstated "which catalog" assumption) as the test-design guide.
- X-D1 ⇄ **Part II**: the benchmark discipline (measure, then claim)
  applied to the fork subsystem.

## Build order

1. **X-A1** — the foundation; everything else consumes it. Ships alone
   as "GC guards no longer scale with fork count".
2. **X-A2, then X-A3** — the headline capability. After these two,
   the claim "instant forks with cross-fork aggregation over shared
   segments" holds, and no benchmarked system can make it [1].
3. **X-B1 + X-B2** — cheap, parallel to X-A work after A1; B3 rides
   any promote-adjacent change.
4. **X-C1** — the big rock, deliberately after the index exists and
   deliberately last among features: it multiplies paths that must
   already be boringly solid.
5. **X-D1/D2** — wrap around everything; D2's OPERATIONS debt should
   not wait for D1.

If only two steps get funded, fund 1 and 2.

## References

1. E. Ang, I. K. Kim, S. Weldon, K. Durand, K. Kaffes, E. Wu.
   *BranchBench: Aligning Database Branching with Agentic Demands.*
   arXiv:2604.17180, 2026. Benchmark code:
   https://github.com/ElaineAng/db-fork. Companion posts: DAPLab,
   "Branchable Databases Aren't Ready for Agentic Workloads" (2026);
   DoltHub, "An Overview of BranchBench" (2026).
2. M. Maddox, D. Goehring, A. J. Elmore, S. Madden, A. Parameswaran,
   A. Deshpande. *Decibel: The Relational Dataset Branching System.*
   PVLDB 9(9), 2016.
3. S. Bhattacherjee, A. Chavan, S. Huang, A. Deshpande,
   A. Parameswaran. *Principles of Dataset Versioning: Exploring the
   Recreation/Storage Tradeoff.* PVLDB 8(12), 2015.
4. S. Huang, L. Xu, J. Liu, A. J. Elmore, A. Parameswaran.
   *OrpheusDB: Bolt-on Versioning for Relational Databases.*
   PVLDB 10(10), 2017.
5. S. Wang, T. T. A. Dinh, Q. Lin, Z. Xie, M. Zhang, Q. Cai, G. Chen,
   B. C. Ooi, P. Ruan. *ForkBase: An Efficient Storage Engine for
   Blockchain and Forkable Applications.* PVLDB 11(10), 2018.
6. T. Dunning, O. Ertl. *Computing Extremely Accurate Quantiles Using
   t-Digests.* arXiv:1902.04023, 2019.
7. Z. Karnin, K. Lang, E. Liberty. *Optimal Quantile Approximation in
   Streams.* FOCS 2016.
8. D. Ungar. *Generation Scavenging: A Non-disruptive High Performance
   Storage Reclamation Algorithm.* ACM SIGSOFT/SIGPLAN Software
   Engineering Symposium on Practical Software Development
   Environments, 1984. (The weak generational hypothesis behind X-B2.)
9. T. J. Green, G. Karvounarakis, V. Tannen. *Provenance Semirings.*
   PODS 2007.
10. D. Du et al. *Catalyzer: Sub-millisecond Startup for Serverless
    Computing with Initialization-less Booting.* ASPLOS 2020.
11. D. Ustiugov et al. *Benchmarking, Analysis, and Optimization of
    Serverless Function Snapshots.* ASPLOS 2021. (REAP.)

Non-paper design sources: Git packfile delta-depth limits, reachability
bitmaps (EWAH) and commit-graph generation numbers; Mercurial revlog
snapshot-insertion heuristics; SVN cheap copies; ZFS/Btrfs deadlists
and reference counting. These informed rules 2 and 5 and the X-A1
bitmap representation; none is a dependency.

## Part X implementation status (2026-07-27, branch `survery-fork-system`)

**X-A1: delivered.** `crates/h5i-db-core/src/fork_index.rs`, consumed by
`retention.rs` (floor check), `database.rs` (`drop_table` guard, vacuum
roots). Measured: the
drop guard reads 2 objects at 6 forks where the full scan read 7, and
the count no longer grows with fork count.

*Design corrected while building it.* The plan called for "per-table pin
map plus per-segment fork bitmaps". The per-segment half was dropped:
segment membership changes on every fork *write*, so maintaining it in a
persisted index would put an index write on the fork write path, which
design rule 1 forbids and which buys nothing, because X-A2 computes
segment visibility at scan time from manifests it must read anyway. What
shipped indexes pins and owned tables, which change only on fork
create/drop/materialize.

The bigger correction is to *how* it stays correct. The plan implied a
maintained index (update on every fork mutation). What shipped is
maintenance-free: nothing writes the index except the reader that finds
it stale, and staleness is detected by listing `forks/` and
`catalog/forks/` and reusing only entries whose object is still present
at the same size. Two listings replace N reads, a rebuild costs one read
per *changed* fork, and there is no update site to forget. That
matters because the forgettable-update failure mode is exactly the one
that loses data.

**X-A2: delivered.** `crates/h5i-db-query/src/fork_scan.rs` registers
`forks('table' [, 'a,b'])`; `Database::fork_names()` and Python
`db.fork_scan()` alongside. 13 integration tests.

The plan's "shared base segments scanned once" needed a specific plan
shape: group segments by *which forks reference them*, scan each group
once, cross-join against that group's fork labels, union the groups.
Single-fork groups use a literal projection instead of a join. That is not only
cheaper, but the only shape that works when a group is empty, since a
scan over zero files reports `UnknownPartitioning(0)` and fails
`CrossJoinExec`'s `SinglePartition` requirement. Measured: 10 forks over
3 shared segments scan 3.

**X-A3: mostly already built; scope corrected to a regression test.**
Both halves of the plan dissolved on contact:

1. *"Data-level `fork diff` from segment set difference"* was **already
   implemented in Part IX**. `fork::diff_one` reports `rows_base` /
   `rows_fork`, `bytes_base` / `bytes_fork` and
   `segments_added`/`removed`/`shared`, all from manifests. The Part X
   claim that "today's diff is metadata-level" was simply wrong. What was
   missing was the *test*: Part IX listed "diff reads manifests
   only (no segment I/O)" as an acceptance criterion and never asserted
   it. `tests/fork_diff_io.rs` now does, on both the add and delete
   paths, by recording every object read and failing on any `.parquet`.
2. *"Per-segment partial aggregate cache"* is **the manifest**, and it
   already exists. Segments carry per-column min/max/null-count;
   `provider.rs::manifest_statistics` surfaces them as exact
   `Statistics`; DataFusion answers `count`/`min`/`max` from them with
   zero segment reads. Being keyed by an immutable segment, it needs no
   eviction policy and no invalidation, which is the cost the plan budgeted for.

   Extending it to `sum` was investigated and rejected: in DataFusion
   54.1 only `count`, `min` and `max` implement `value_from_stats`
   (`datafusion-functions-aggregate/src/{count,min_max}.rs`), so a
   recorded sum (in the manifest or in a sidecar) would not remove one
   segment read without also writing a custom optimizer rule and
   physical operator. Float sums would additionally disagree with
   scan-derived sums in the last ulp, so two queries that should agree
   would not. Quantile sketches inherit the same blocker.

   The lesson generalises past this item: *"add a cache" is only a
   design if something downstream can consume it.* The consumer here is
   an engine rule that does not exist, so the work would have been
   storage and invalidation machinery serving nothing.

**X-B1: delivered.** `create_forks` / `fork_many` / `drop_forks` in
`fork.rs`, CLI `fork create --count N` and `fork drop <names…>`, Python
`create_forks` / `fork_many` / `drop_forks` / `fork_names`. 11 core
integration tests + 2 CLI e2e.

Two things fell out of building it. The single-fork verbs are now the
batch verbs with one name, which removes the risk of the two drifting;
and the durability barrier turned out to matter more than the puts:
`create_many` takes one fsync pass for the whole batch rather than one
per fork, which is what dominates at a few hundred branches. The CLI
output shapes for one fork are deliberately unchanged (one object, and
`dropped` still a scalar), because agents parse them.

**X-B3: delivered.** A promote blocked *only* by compaction is now
replayed onto the new layout instead of refused; `PromoteResult` gains
`rebased_from`, and the manifest note and `h5i.forked_from` record it.

The rebase is deliberately partial and refuses rather than guessing. It
is sound only while the fork still holds every row it inherited: a fork
that *deleted* inherited rows cannot be replayed from metadata, because
a compacted segment may merge rows it dropped with rows it kept and
nothing short of reading the data says which. That case still conflicts,
with a message saying why. The Part IX test asserting this case fails
was updated to assert it rebases, because the behaviour change is the point.

**X-C1: delivered.** Nested forks, superseding Part IX's "no
fork-of-fork in v1". `Fork` gains `parent` and `depth`, `ForkPin` gains
`spec_revision` and `created_at_ns`, `MAX_FORK_DEPTH = 32`,
`drop_fork_tree`, and a drop guard. 15 integration tests in
`tests/fork_nested.rs`.

The design turned out to be smaller than the plan feared, for one
reason: **a pin is keyed by table id and does not care whose table it
is.** A child pinning its parent's tables is the same mechanism pointed
one level down, so the write path, the commit protocol, compaction and
the refinement invariant all generalised untouched: a child's shadow
references paths listed in the manifest it shadows, which is exactly
what the invariant already said.

What did have to change was **name resolution**, and it is the one part
that could have been silently wrong. `entry_opt` and `list_tables`
resolved a name through the *global catalog*, filtered by the pin set.
Inside a nested fork that answers with the base table a parent had
already shadowed. They now resolve through the pin set by name, which is
the only structure that knows what a name meant at fork time, and it
is also read-free, so the fix made flat forks cheaper too. Carrying
`spec_revision`/`created_at_ns` on the pin is what made that possible at
zero cost: the catalog entry is already in hand when the pin is written.

Both added-field sets use `Option` + `skip_serializing_if` rather than
`#[serde(default)]` alone. That distinction is what keeps old forks
readable, and it is not a style preference: a fork
object is self-checksummed, so a field that serialises when absent
changes the bytes and turns every fork in every existing database into a
corruption error. Pinned by `a_fork_written_before_nesting_still_verifies`.

The GC guard is stated in terms of **dependency, not lineage**: a drop
is refused when another fork pins a table this one owns. A child that
forked before its parent wrote anything holds nothing of the parent's,
so dropping the parent is allowed. That is correct, and is what a
parent-name check would have got wrong. `drop_fork_tree` discovers the
subtree by the same relation and drops it deepest-first.

Measured: resolving a table at depth 24 costs **exactly** as many object
reads as at depth 2 (asserted with `assert_eq!`, not a bound). That is
the property BranchBench found every system missing (Dolt's reads
degrade 5-4000x with depth), and it holds here because a shadow manifest
names its segments by path, so there is no chain to walk.

---

# Part XI. Live execution without a backtest tax (2026-07-31)

Live execution is a stated non-goal today (`README.md`: "the backtester never
routes a real order"). This part does not change that. It records what the
2026-07-31 measurement work established about *whether the goal could be
adopted later without giving up the kernel*, so that the answer is written
down rather than re-derived, and so the seams that already exist are not
removed by someone who does not know why they are there.

## The finding that motivates it

Splitting NautilusTrader the same way we split ourselves decomposed the gap.
Their control arm is a strategy that never subscribes: the engine streams
every quote through its message bus and venue, and never calls Python.

| | engine, no Python | strategy boundary | total |
|---|---:|---:|---:|
| h5i | **0.329 µs/event** | ~0.77 | ~1.10 |
| NautilusTrader | **3.593 µs/event** | 0.771 | 4.364 |

The engine term is 10.9×, and it is the whole of our path-dependent
advantage: our boundary is no better than theirs and possibly worse. The
composition inverts. Nautilus spends 82% of a run in the engine and 18%
crossing into Python; we spend roughly a quarter and three quarters.

**The two arms were not compiled with the same integer width, and the
10.9× is not decomposed until they are.** Nautilus switches its fixed-point
raw type with a `high-precision` Cargo feature, defaulting to `i64` in the
Rust crate but to `i128` in the Python package (`build.py` sets
`HIGH_PRECISION=True` unless the environment says otherwise, and forces it
off only on Windows). The 1.230.0 aarch64 wheel these numbers came from
reports `HIGH_PRECISION 1`, `PRECISION_BYTES 16`, so it was doing 128-bit
arithmetic at a scale of 1e-16 while our arm was doing 64-bit arithmetic at
1e-9. An `i128` add spans two registers where an `i64` add is one
instruction, so some unmeasured share of the gap is width, not dispatch.

The message-bus explanation below is still the one the architecture
supports, but it is now an explanation for an *undecomposed* number. The
separation is a measurement, not an argument: build one wheel with
`HIGH_PRECISION=false` and re-run the no-subscription arm. Until that is
done, quote the 10.9× as an engine-to-engine total and not as a bus cost.

The 3.593 µs is what their engine costs with zero callbacks attached.
That is not waste. It is the price of a message bus: events are published to
a topic, components subscribe handlers at runtime, and dispatch is a lookup
plus a handler walk plus dynamic dispatch, on every event. It buys the thing
we do not have: the same strategy code running live and in backtest, with
components addable without touching the engine.

So our 10.9× says we are solving a smaller job, not that we are faster at the
same one.

## Why the tax is avoidable

Because live is not throughput-bound.

```text
  h5i kernel                      3,000,000 events/s
  Nautilus engine (bus, no Python)  278,000 events/s
  a real venue feed, all symbols     10,000 – 100,000 events/s
```

At 100k events/s a 3.6 µs dispatch is a third of one core. The bus is cheap
in live and expensive only in backtest, where the point is to replay 200k
events as fast as the machine allows. The cost is therefore not inherent to
having live execution; it is inherent to using **one dispatch mechanism for
both modes**, which is the choice Nautilus made in exchange for code
identity.

The alternative is to share everything except dispatch.

```text
                backtest                          live
                ────────                          ────
  inbound  Parquet → decode → RecordSource   socket → adapter → RecordSource
                          │                                   │
                          └─────────────┬─────────────────────┘
                                        ▼
                    ┌────────────────────────────────────┐
                    │  Replay (k-way merge)              │  shared
                    ├────────────────────────────────────┤
                    │  Engine::step()                    │  shared
                    │  books · order lifecycle · risk    │
                    │  · portfolio · settlement          │
                    ├────────────────────────────────────┤
                    │  strategy.on_event()               │  shared, direct call
                    └────────────────┬───────────────────┘
                                     ▼ intent only
                    ┌────────────────────────────────────┐
                    │  ExecutionClient                   │  the seam
                    └───────┬────────────────────┬───────┘
                            ▼                    ▼
                  SimulatedExecution        LiveExecution → venue
                  fills made by engine      fills return from venue
```

## Seams that already exist

Two of the three edges are in the codebase, and both were built for this.

**Outbound.** `ExecutionClient` (`execution.rs`) is documented as the live
seam: "The simulator implements it; a live adapter would implement the same
one … if simulation and production reach the venue through different code,
they diverge, and nothing detects it because there is no shared surface to
compare." It carries *intent* (submit, cancel, amend) and deliberately never
a fill, because in production fills can only come the other way.

**Inbound.** `RecordSource` is `Box<dyn Iterator<Item = Result<Record>>>`. A
channel receiver is an `Iterator`, so a live feed is a `RecordSource` with no
engine change at all. Ordering also survives: the merge keys on `ts_init`,
"when this system could first have known it", which for a live receiver is
arrival order and therefore monotonic by construction.

## What is actually hard, none of it performance

1. **Fills reverse direction.** The engine produces fills today, in
   `match_resting`. Live must accept them from the venue instead. This is the
   large change: a mode distinction in the middle of the engine rather than at
   its edges. A seam existing for the outbound half does not make it small.
2. **`step()` is synchronous.** A strategy that takes 10 ms blocks the loop
   for 10 ms. Backtest does not care; live needs a stated backpressure policy
   (drop, queue, or block). A design decision, not an optimization.
3. **Throughput and tail latency are different objectives.** The kernel is
   tuned for the former. Anything that batches to help throughput can hurt
   p99, so the hot path needs auditing against a latency budget rather than
   an events/s number.
4. **Single-threaded stays correct, and is normal.** One thread owning state,
   fed by queues, is how most execution systems are built. Determinism in
   backtest comes from the total order key, which live has no need to
   enforce.

## The divergence risk, and the test that answers it

Sharing everything is precisely why Nautilus put both modes on one bus. Two
dispatch paths can drift, and a bug in one that is absent in the other is the
worst kind: it appears only in production.

Three mitigations, in order of strength. The engine, order lifecycle, risk,
portfolio and settlement stay one implementation. Only dispatch differs, and
dispatch is small enough to review as a unit. `ExecutionClient` keeps a
shared surface at the venue edge, which is what it was for. And the
divergence becomes testable: record a live session into the canonical
tables, replay it through the backtest path, and assert the strategy's
decisions are identical. That is the shape `result.verify()` already has.

A live session recorded this way is a fork, so live-to-backtest
reproducibility falls out of the storage design rather than needing to be
built.

## Status

Design only. Nothing here is implemented or measured, and the 0.329 µs figure
holds under the assumption that there is no live path. Whether it survives one
is not knowable until there is one to measure. What is established is that a
design exists in which backtest pays nothing for live's flexibility, and that
half of its seams already carry weight in the current code.

Do not remove `ExecutionClient`'s intent-only contract, or narrow
`RecordSource` to a concrete type, without reading this part first.

---

# Part XII. Optional 128-bit fixed point (2026-07-31)

`Price`, `Qty` and `Money` are `i64` scaled by `1e-9` (`types.rs:25`). This
part records what that costs, what NautilusTrader does about the same problem,
and why copying their fix would not be enough here.

> **Implemented on branch `feature-i128`.** The survey below is kept as
> written, because it is what the design was argued from. What was built,
> and where it deliberately departs from the survey's own recommendation,
> is in *Part XII implementation status* at the end.

## There are two limits, and they are 1024x apart

The one that gets quoted is arithmetic overflow at `i64::MAX / 1e9`:

```text
  9,223,372,036.854775807   largest representable value
              0.000000001   smallest tick
```

Overflow is refused rather than wrapped. `checked_add`, `checked_sub`,
`from_f64` and `notional` all return `Result`, so a run stops with
`BacktestError::Overflow { what }` naming the operation. A silently negative
balance is not a failure mode this has.

The limit that binds first is exactness in storage. Canonical tables hold
`f64` columns and `store.rs` converts at the boundary, so the usable range is
capped by the 53-bit mantissa, not by `i64`:

```text
  2^53 / 1e9  =      9,007,199.25   above this, raw values stop surviving
  i64  / 1e9  =  9,223,372,036.85   overflow
```

Above roughly nine million units, half of adjacent raw values do not survive a
write-then-read cycle. Sampling 100 consecutive raws either side of 1e16
(about 1e7 units), 50 come back changed.

### The module doc overclaims this

`store.rs:10` says the conversion "is exact for everything the fixed-point
type can represent" and cites `float_storage_round_trips_exactly` as proof.
The test does not reach that far: its largest case is `9_999_999.999999999`,
and it passes for a reason that does not generalise. The test starts from a
float literal, which is already a value `f64` can hold, so the trip back is
trivially identity. Arbitrary raw values at the same magnitude fail half the
time.

The claim is true for every price a prediction market or an equity quotes, and
`round_trip_holds_across_the_whole_tick_grid` covers that grid exhaustively.
It is not true to the top of the type's range, and the doc should say nine
million rather than imply nine billion. Fixing the sentence is smaller than
fixing the range and should not wait for it.

## Where the limits are actually reached

Denomination first. JPY 9,223,372,036 is about USD 60M, so a yen-denominated
book hits the arithmetic ceiling two orders of magnitude sooner than a dollar
one, and the exactness ceiling at JPY 9,007,199 (about USD 60k) sooner still.

Quantity second. Meme-coin units run to the trillions; 9.2e9 is reached by a
position of ten billion tokens, which is a few hundred dollars of SHIB.
Hyperliquid already works around this by quoting `kPEPE` in thousands, and the
instrument tests carry `KPEPE-PERP`, so the venues most exposed to it have
often scaled the unit themselves.

Price is not a realistic case. Nine billion is four orders above the most
expensive instrument anyone quotes.

## How Nautilus does it

A `cfg`-switched type alias, with every use site naming the alias:

```rust
#[cfg(feature = "high-precision")]     pub type PriceRaw = i128;
#[cfg(not(feature = "high-precision"))] pub type PriceRaw = i64;

pub struct Price { pub raw: PriceRaw, pub precision: u8 }
```

`Quantity` uses `u128`/`u64`, unsigned because quantities do not go negative.
Five constants move together: `FIXED_PRECISION` (9 or 16), `FIXED_SCALAR` (1e9
or 1e16), `PRECISION_BYTES` (8 or 16), the Arrow schema string
`FixedSizeBinary(8)` or `(16)`, and `PRICE_MAX`. The feature is off by default
(`default = []`), and `defi` turns it on because 18-decimal wei needs it.

Two details are worth copying regardless of whether the rest is.

They export the build mode as a symbol:

```rust
#[unsafe(no_mangle)]
pub static HIGH_PRECISION_MODE: u8 = cfg!(feature = "high-precision") as u8;
```

A compile-time choice is invisible across an FFI boundary otherwise, and
Python reading 8 bytes where the binary wrote 16 is a silent corruption rather
than an error.

And they carry a scar from the migration, documented at `fixed.rs:34`.
Catalogs written before December 2025 used `int(value * FIXED_SCALAR)` instead
of `round(value * 10^precision) * scale`, so the decode path now runs
correction functions on every read, with a note that the overhead will become
opt-in "once catalogs have been repaired or migrated". The hazard in widening
a fixed-point type is the scale conversion, not the integer.

## Why the alias alone would not work here

Nautilus stores raw integers as `FixedSizeBinary`, so widening the integer
widens the column and the value survives. We store `f64` and convert at the
boundary. Widening `i64` to `i128` while the column stays `f64` moves the
overflow ceiling and leaves the exactness ceiling at 9,007,199 units, which is
the one that binds. Worse, raising `SCALE` to `1e16` to buy precision would
drop the exactness ceiling to `2^53 / 1e16`, which is 0.9 units.

So the change is not "swap the integer". It is:

1. A `cfg`-switched raw type and a scale that moves with it, following the
   Nautilus shape, including the exported mode symbol.
2. A storage representation that can hold what the wider type represents.
   `Decimal128` is the natural Arrow answer and Parquet supports it.
3. A read path that keeps existing `f64` segments readable. Segments are
   immutable and cannot be rewritten in place, so both encodings have to
   coexist behind the schema, which is the same problem the fork work solved
   for shadowed tables.
4. A rounding contract that is pinned by test before anything is written with
   it, given what the conversion cost Nautilus.

Item 3 is the reason this is not a small change. Items 1 and 2 are mechanical.

## Status and recommendation

Not planned. The default `i64` path should stay the default whatever happens,
because it is what the 0.329 µs/event kernel is measured on, and `i128` does
not fit a register.

The one thing that should happen soon is independent of all of it: correct the
`store.rs` doc claim, and extend `float_storage_round_trips_exactly` to assert
the boundary at 9,007,199 rather than imply there is not one. A test that
passes because its input was already a float is not testing what its name
says.

## Part XII implementation status (2026-07-31, branch `feature-i128`)

Built, in four commits: `1ddd4188` (switchable raw type), `f589fcf0` (the
wide build, green), `59416110` (the decimal encoding layer), `bbea27bd`
(per-table encoding wired through schema and store).

| | |
|---|---|
| `--features wide` | `Raw` becomes `i128`; default stays `i64` |
| `FixedEncoding::{Float, Decimal}` | per table, chosen at creation |
| tests | 338 pass on `i64`, 338 pass on `i128` |
| lint | `clippy --all-targets` clean in both modes |

### Two deliberate departures from the survey above

**The scale does not move.** The survey's item 1 says "a `cfg`-switched raw
type *and a scale that moves with it*, following the Nautilus shape". That
was wrong, and the survey says why three paragraphs earlier: raising `SCALE`
to `1e16` would drop the f64 exactness ceiling to 0.9 units. Nautilus moves
the scale because 18-decimal wei needs the decimal places. Our binding limit
is range, not decimal places, so `SCALE` stays `1e-9` in both modes. The tick
grid, the f64 conversion and every value already written are untouched, and
there is nothing to migrate -- which is the cost Nautilus is still paying at
`fixed.rs:34`.

**The storage encoding is not the build's to choose.** A compile-time
encoding is what makes a Nautilus catalog unreadable by the other build.
Here the encoding is a property of the table, recorded in its schema:
`create_market_data_tables_with(db, FixedEncoding::Decimal)` decides it once,
every writer reads it back with `encoding_of`, and every reader dispatches on
the array it is handed. Both builds read both encodings. A narrow build
meeting a decimal value too large for its `Raw` refuses it through the same
checked `narrow()` as every other conversion, rather than truncating.

`Decimal128(38, 9)` fits by construction: its backing value is an `i128` at
scale `1e-9`, which is exactly what a `Raw` is, so encode and decode are
integer moves with no float in the path.

### What the audit found that the compiler would not have

**Nine silent-wrap narrowings.** Sites that promoted to `i128`, did
arithmetic, then wrote `as i64` -- which wraps rather than errors. In a
fixed-point type that is how a position becomes its own negation. All nine
now go through a checked `narrow()`. This is a fix to the shipping `i64`
build, not something the feature introduced.

**A test asserting the wrong thing.** `checked_arithmetic_reports_overflow`
hardcoded `i64::MAX`; under `wide` it would have passed while testing
nothing. It now asserts against `Raw::MAX`.

**What must not widen.** Nanosecond timestamps, tick counts, outcome counts
and slice counts stay `i64`. Only raw fixed-point values follow `Raw`.
Widening those too would have compiled and been wrong.

### The correction this Part asked for, made

The survey's closing recommendation -- fix the `store.rs` doc claim and make
the round-trip test assert the boundary -- is done.
`float_storage_round_trips_exactly` is now
`float_storage_is_exact_up_to_the_mantissa_and_not_past_it`, and it starts
from raw integers rather than float literals, which is the flaw the survey
identified. The `store.rs` and `schema.rs` module docs now say nine million
and name the decimal encoding as the way past it.

### The mode symbol is not needed here

Nautilus exports `HIGH_PRECISION_MODE` with `no_mangle` because Python reads
their raw integers across FFI, where a build mismatch is a silent
corruption. Our Python boundary (`crates/h5i-db-python`) passes `f64` in and
out and never sees a raw, so a wide build is invisible from Python and cannot
corrupt anything by being unnoticed. The symbol would be ceremony.

### What the wide build costs, measured

`benches/replay_path.rs` now reports `build.raw_bits`, because the two builds
produce otherwise identical JSON and comparing the wrong pair is the easiest
mistake to make with a compile-time switch. It also takes
`--encoding float|decimal|both`, which measures the storage dimension in one
process since that half is a runtime choice.

Method: the two binaries alternated **ABBA**, ten pairs, 200k book events and
40k trades, after a warm-up run. Order alternation matters here -- a first
attempt ran the arms in a fixed order and every phase came out "slower",
which is what monotonic drift looks like when it all lands on the second arm.

`i128` versus `i64`, same source:

| phase | i64 median | i128 median | i128 min vs i64 min | i128 slower in |
|---|---:|---:|---:|---:|
| kernel (replay + engine) | 75.8 ms | 95.0 ms | **+53%** | 10/10 |
| decode (arrow -> Record) | 63.5 ms | 71.4 ms | +11% | 9/10 |
| seed (write) | 381.5 ms | 353.9 ms | -2% | 3/10 |

The kernel result is the one to quote: 10/10 pairs and a 53% gap between the
two arms' best times, well clear of this machine's noise. It is also the
expected shape -- an `i128` add spans two registers. Wide mode buys range and
pays for it in the arithmetic, which is why it is not the default.

**The i64 path is not measurably changed, but I cannot claim it is free.**
Against the pre-feature commit (`914fe0cc`), same ten-pair ABBA method, the
kernel differs by +14% median and +12% min with today's build slower in 8/10
pairs -- and the same arm's own samples range from 58 to 106 ms, so the noise
floor at n=10 is wider than the effect being looked for. What can be said is
that no phase moved by more than the run-to-run spread. What cannot be said
is "zero cost", and one candidate is real: `notional` is on the fill path and
now ends in a checked `narrow()` where it used to end in `as i64`. That is a
compare and a branch bought in exchange for not wrapping a position into its
negation, which is not a trade worth undoing. Resolving an effect that small
needs a microbenchmark of `notional` itself, which this phase-split bench
cannot provide.

Storage encoding, same process, both builds: decimal decode runs at **0.85 to
0.89x** the float decode. Faster, not slower -- decoding a decimal is an
integer move, decoding a float is a multiply, a round and a range check.
The cost of the decimal encoding is bytes on disk and the pruning gap below,
not decode time.

### Still open

Core's `Decimal128` support is the T1.2 row in Part III and is *not* done: a
decimal column gets no min/max zone-map pruning and no aggregate-state
acceleration. Pruning fails open, so a scan over a decimal table is correct
and prunes by time only. That is the standing cost of choosing the encoding
today, and closing T1.2 removes it.
