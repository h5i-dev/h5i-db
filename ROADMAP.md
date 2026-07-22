# h5i-db Roadmap

Living roadmap. Last full update 2026-07-22 (branch `improve-performance`).

This document merges the former `ROADMAP_PERFORMANCE.md` into the
production-readiness roadmap; the separate file is gone. Part I tracks
production readiness, Part II the structural performance program. Statuses in
this update were re-verified against the source (grep/tests/benchmarks), not
carried forward from earlier revisions.

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
