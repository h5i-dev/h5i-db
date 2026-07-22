# h5i-db structural performance roadmap

Status: living roadmap, updated 2026-07-22 against source checkpoint `0e07680`.
This document is intentionally separate from `ROADMAP.md`: that file tracks
production-readiness work, while this one describes performance features
inspired by recent database research.

Status legend: **done** means implemented with focused tests; **partial** means
useful infrastructure exists but the phase's exit gate is unmet; **planned**
means no production implementation was found in the checkpoint audit.

## 1. Decision

h5i-db should optimize the amount of work performed before it invests in a
custom file format or lower-level kernels. Its most useful performance identity
is:

> A workload- and version-aware analytical database that skips data and reuses
> work across immutable versions.

The structural program is:

1. **partial:** query-local telemetry with reproducible benchmarks;
2. **done:** planner statistics, metadata-only `COUNT(*)`, and exact-set pruning;
3. **planned:** an immutable-segment predicate cache;
4. **planned:** version-aware, mergeable aggregate states;
5. **planned:** workload-aware, previewable partial reclustering.

These fit the current architecture: versions are immutable manifests, segments
have stable checksums, segment statistics already exist, scans already use
DataFusion pruning, and destructive rewrites already have a plan/apply review
flow. A cache miss can therefore cost time but must never affect correctness.

The next implementation slice is therefore P0 query-local metrics and workload
telemetry, followed by a small P2 exact-match predicate-cache prototype. P1 no
longer blocks either item. FastLanes/LeCo-style storage, mixed hot/cold formats,
and selective late materialization remain experiments, but only after profiles
show that bytes decoded or decoder CPU dominate after P2-P4 land.

## 2. Cost model and success criteria

Use the following accounting model instead of a single wall-clock number:

```text
query latency
  = planning
  + metadata I/O
  + data bytes read
  + rows/values decoded
  + sort and shuffle
  + join/aggregation
  + result materialization
```

Every optimization in this roadmap must identify the term it removes. Report
both cold and warm runs; a warm filesystem cache is not a predicate-cache hit.
Use median and p95 over at least five measured runs after warm-up, and always
report input rows, segments, row groups, bytes, output rows, and peak memory.

No paper's best-case speedup is a project target. For example, changing one of
50 segments can reduce aggregate *scan work* toward 1/50, and uniform data over
64 symbol partitions can reduce a single-symbol scan toward 1/64. Neither
implies a 50x or 64x end-to-end speedup.

Required benchmark shapes:

- one symbol plus a narrow and a wide time range;
- all-symbol cross-sectional queries over the same ranges;
- repeated filters, including slightly changed literals and projections;
- repeated OHLCV/VWAP queries after appending 1 of N segments;
- the same aggregates after compaction, overwrite-range, restore, and schema
  evolution;
- ASOF joins with selective left/right time ranges;
- 10, 100, 1,000, and 10,000 segments, including small-file workloads;
- cold cache, warm cache, predicate-cache hit, and aggregate-state hit cases.

## 3. Current foundation and gaps

The 2026-07-22 checkpoint audit found these performance foundations delivered:

- immutable Parquet segments referenced by immutable version manifests;
- stable segment checksums, row/byte counts, schema revision, time range, and
  per-column min/max/null counts in `SegmentMeta`;
- exact low-cardinality string distinct sets (up to 128 values) written into
  manifest statistics and used by `PruningStatistics::contained`;
- exact planner row/null/min/max statistics where representable, post-pruning
  scan statistics, and a tested metadata-only `COUNT(*)` path;
- `sort_key`, target segment/row-group size, codec options, pairwise O(n)
  sortedness checks, bounded sorted-batch merging, and streaming core scans;
- ASOF time/projection/limit pushdown, declared ordering, and memory-pool
  accounting, with focused performance-path tests;
- streaming CLI and Python query output plus retractable VWAP window state;
- append-only version diffs, scans of only added segments, and a tail stream;
- preview/apply mutation plans and copy-on-write compaction.
- query-local execution IDs and performance reports with DataFusion physical
  scan bytes, rows, pruning, operator timing, sort, and spill attribution;
- bounded opt-in telemetry containing query fingerprints rather than SQL, plus
  an explicit disposable sidecar flush;
- a no-link repeated-query runner with result checksums, environment metadata,
  and baseline regression gates.

The remaining structural gaps are:

- physical-byte reporting uses DataFusion's scan-range metric; it does not yet
  split metadata, requested, compressed, and decompressed bytes;
- high-cardinality entity columns have no probabilistic membership summary;
- there is no persistent predicate or aggregate-state cache;
- layout remains a static sort key; `core/src/layout.rs` describes object paths,
  not a query-aware physical `LayoutSpec`;
- no cost model selects a subset of segments for reclustering;
- generic-scan overhead versus raw DataFusion remains about 20% in the current
  design ledger, above its original 10% goal.

The implementation may move while this roadmap is active. Each phase starts
with a short source audit and updates this list instead of assuming these facts
remain true.

Verification for this update: the existing `h5i-db-query` `query_misc` target
passed the concurrent query-local reporting test, the lightweight observability
crate passed 3 tests, the no-link workload gate passed 3 Python tests, and the
public CLI report checker observed a 274-byte physical scan against the golden
database fixture. Earlier `query_misc`/`asof_perf` and `roadmap_features`
checkpoints passed 19 and 6 tests respectively.

## 4. Phase P0: make saved work observable

**Status: done for the P2/P3 foundation.** Reports and telemetry are
query-local, bounded, privacy-aware, and exercised through the public CLI. The
remaining byte-category and Python-surface refinements are additive and do not
block cache attribution.

Do this before an adaptive feature. Otherwise the database cannot decide what
to optimize and benchmarks cannot explain a win.

### P0.1 Per-query performance report

Give each execution a query-local metrics collector and expose a stable JSON
report through Rust, CLI, and eventually Python:

```text
query fingerprint and snapshot sequence
planning/execution/output time
segments: total, zone-map pruned, cache-pruned, opened
row groups: total, pruned, selected
bytes: metadata, requested, read, decompressed
rows: decoded, predicate-qualified, output
sorts/shuffles: count, input rows, spill bytes
predicate cache: lookups, hits, rows/ranges reused
aggregate cache: states requested, reused, built, merged
```

Instrument actual object reads as well as planned file sizes. DataFusion plan
metrics can supply operator rows and spill data, but an instrumented object
store/read adapter is needed for physical bytes. Concurrent queries must never
share counters.

**Delivered.** `H5iSession::sql_reported` assigns a query ID during physical
planning and returns a stream whose final report is isolated to that execution.
It combines manifest scan attribution with DataFusion physical-plan metrics for
scan ranges, scan output rows, row-group/page pruning, operator timing, sorts,
and spills. Rust, CLI `query --stats`, and the UI backend expose the stable
serializable report. A concurrent query test verifies that scan attribution
does not cross execution boundaries. Separating metadata reads and
decompressed bytes remains a later refinement.

### P0.2 Workload telemetry

Store a bounded, opt-in workload log containing:

- normalized logical predicate fingerprints;
- referenced columns and operators;
- snapshot sequence and chosen layout revision;
- predicate selectivity and the scan report above;
- time-range buckets and frequency/recency weights when layout tuning is
  enabled.

Literal values can be sensitive. Hash exact normalized expressions by default;
range boundaries or cleartext values require an explicit telemetry setting.
Apply a size cap and retention period. Telemetry is advisory and disposable,
not versioned table state.

**Delivered.** `SessionOptions::telemetry_capacity` enables an in-memory
bounded ring; zero disables it. Entries contain the query fingerprint and
performance report, never SQL text or literals. Explicit flush writes a
versioned, disposable workload envelope outside table manifests, so queries do
not introduce hidden writes.

### P0.3 Benchmark gates

Add a checked-in data generator and result schema. A performance change passes
only if it preserves result checksums and does not regress the cross-sectional
or ingest control workload beyond an agreed threshold. Keep benchmark results
out of correctness tests, but retain machine, compiler, dataset, and command
metadata so runs are comparable.

**Delivered.** `benchmarks/run_performance_workload.py` drives an already-built
CLI and therefore adds no Rust/DataFusion link target. The checked-in workload
runs warm and repeated cross-sectional and symbol/time cases, rejects changing
result checksums, records binary/workload hashes and machine/compiler metadata,
and can fail a baseline comparison above a configurable median-query threshold.
The existing Rust benchmark remains the ingest control and dataset generator.

## 5. Phase P1: finish cheap pruning before adding caches

**Status: done for the exact-statistics path.** The functional work below is in
the source and covered by query tests. Probabilistic blooms remain an optional
extension rather than a blocker for P2.

### P1.1 Manifest statistics in planning

**Delivered.** `H5iTableProvider::statistics()` folds manifest values into
DataFusion `Statistics`, and the scan builder receives statistics for surviving
segments. Row counts and eligible column statistics use exact precision; total
compressed bytes remain conservatively inexact. Tests cover provider statistics
and metadata-only `COUNT(*)`.

Future aggregate rewrites must still require complete, untruncated statistics
whose type semantics are exact. Row counts can be exact when a column bound is
unknown. Preserve tests for null-only columns, truncated strings, NaN, schema
revisions, and filtered scans as coverage expands beyond `COUNT(*)`.

### P1.2 Equality pruning for entity columns

**Delivered for exact low-cardinality sets.** Segment writing records up to 128
exact distinct string values and drops the set when the threshold is exceeded.
`PruningStatistics::contained` uses those values, and a query test demonstrates
that a symbol predicate skips an unrelated segment.

The remaining optional tier for configured columns such as `symbol`,
`exchange`, and `venue` is:

- a bloom filter with format version, hash seed, bit count, and hash count when
  the exact-set threshold is exceeded;
- no summary when construction is not beneficial.

False positives are allowed; false negatives are correctness bugs. The normal
DataFusion filter must remain in the plan and recheck every returned row.
Benchmark manifest growth as well as bytes skipped before adding blooms.
Parquet-native bloom/page indexes can be evaluated independently when the
Arrow/Parquet reader consumes them effectively.

### P1.3 Remove known avoidable work

**Delivered.** ASOF projection/time/limit pushdown and bounded memory, streaming
scan/query output, bounded sorted writes, O(n) sortedness checks, retractable
VWAP/window state, and ordering/statistics declarations are present with focused
tests.

Functional exit gate: passed for planner statistics, metadata-only `COUNT(*)`,
exact-set pruning, ASOF paths, sortedness, and VWAP retraction. Physical scan
attribution is now supplied by the P0 report.

## 6. Phase P2: immutable predicate cache

**Status: row-group prototype delivered.** Exact deterministic conjunctions can
now reuse checksum-keyed row-group selections. Fine-grained qualifying row
ranges and workload-based admission remain future extensions.

[Predicate Caching](https://www.amazon.science/publications/predicate-caching-query-driven-secondary-indexing-for-cloud-data-warehouses)
stores qualifying tuple ranges from repeated base scans. h5i-db has an unusually
good invalidation model for this idea: a segment checksum never changes, and a
new version normally reuses most old segment checksums.

### P2.1 Exact-match cache first

Start with deterministic base-table predicates and exact fingerprint matches.
Do not begin with predicate implication or a learned selector.

```text
PredicateCacheKey {
    segment_checksum,
    schema_revision,
    normalized_predicate_hash,
    expression_semantics_version,
}

PredicateCacheValue {
    qualifying_row_ranges,   // or row-group/page selections in the first PoC
    source_row_count,
    qualified_row_count,
    checksum,
}
```

Normalization must preserve casts, null semantics, timestamp timezone, string
comparison/collation, function versions, and decimal types. Only immutable,
deterministic expressions are eligible. Never cache `now()`, random functions,
session-dependent UDFs, or expressions with unsupported semantics.

The first implementation may cache selected row groups because it integrates
more easily with Parquet. The useful target is compressed qualifying row ranges
fed to the Parquet reader's row-selection facility, with the original predicate
still applied. Run-length/delta encoding should be enough initially; choose a
bitmap only after measuring density.

**Delivered.** The first eligibility contract accepts only conjunctions of
typed column/literal equality and range comparisons, requires an equality term,
and rejects casts, functions, null predicates, and other semantics it cannot
fingerprint exactly. The key hashes normalized typed terms together with the
segment checksum, schema revision, and expression-semantics version. Cache hits
attach a `ParquetAccessPlan`; DataFusion still evaluates the original predicate.

### P2.2 Sidecar lifecycle

Keep cache objects outside manifests, for example under a database-local
`cache/predicates/v1/` namespace. They are not part of snapshot checksums,
replication, backup correctness, or the commit protocol.

- publish with create-new or temp + atomic rename;
- checksum values; on corruption, delete/ignore and rebuild;
- allow concurrent duplicate builders, with one winning publication;
- bound by bytes with LRU/clock eviction and optional TTL;
- garbage-collect entries whose segment no longer exists, but never require
  eager invalidation for correctness;
- expose hit, rejected, build, and eviction reasons in metrics.

Compaction creates new segment checksums and therefore causes misses, not stale
hits. Old versions continue to hit entries for their old segments until normal
cache eviction.

**Delivered for the prototype.** Versioned JSON entries live under
`cache/predicates/v1/`, contain no literal values, carry an envelope checksum,
and publish with create-if-absent. Missing, corrupt, rewritten, or new segments
degrade to misses. Corrupt entries are discarded and rebuilt. Oldest entries
are evicted after successful publication to keep the namespace under a 256 MiB
default bound. The CLI requires explicit `--predicate-cache`, so a normal
read-only query session introduces no hidden sidecar writes.

### P2.3 Safe extensions

After exact-match wins are demonstrated:

1. cache conjuncts independently and intersect their row selections;
2. share a predicate result across queries with different projections;
3. use frequency, build cost, selectivity, and saved bytes to decide admission;
4. consider range subsumption only with a proved expression algebra.

Exit gate: on a repeated selective query, warm predicate-cache execution reads
materially fewer data bytes than a warm filesystem-cache execution; a forced
cache corruption, schema revision, and segment rewrite all degrade to a miss
without changing results.

Prototype exit gate: passed for a correlated two-column predicate that ordinary
per-column Parquet statistics cannot eliminate. The test asserts lower physical
scan bytes on the warm hit, result stability after forced sidecar corruption,
reuse across an append-only version, and a clean miss after compaction rewrites
the segment checksum. Schema revision is part of every key; a dedicated schema
evolution case remains before declaring the full P2 phase production-complete.

## 7. Phase P3: version-aware mergeable aggregate states

**Status: explicit finance-state prototype delivered.** A persistent
`AggregateStateStore` now reuses per-segment OHLCV/VWAP states across arbitrary
manifests. SQL optimizer substitution and general aggregate-plan recognition
remain intentionally unimplemented.

[OpenIVM](https://ir.cwi.nl/pub/34276) shows that incremental view maintenance
can be expressed using an existing SQL engine. h5i-db should initially take a
narrower route that exploits immutable segments rather than compiling arbitrary
SQL deltas.

### P3.1 Segment-state store

Persist one mergeable state per segment and normalized aggregate fragment:

```text
AggregateStateKey {
    segment_checksum,
    schema_revision,
    aggregate_plan_hash,
    expression_semantics_version,
}
```

The plan hash includes filters, group expressions, aggregate arguments, casts,
null behavior, timezone/calendar rules, and UDF/function versions. Encode state
in a versioned Arrow IPC or similarly self-describing envelope with a checksum.
Use the same disposable sidecar rules as the predicate cache.

**Delivered for the first registered specification.**
`FinanceAggregateSpec` registers timestamp, float64 price, float64/int64 volume,
and an optional non-null string grouping column. Its typed plan fingerprint,
segment checksum, schema revision, and semantics version address a checksummed
JSON state under `cache/aggregates/v1/`. The sidecar shares P2's bounded
oldest-first eviction helper and create-if-absent publication. The API is
explicitly opt-in because grouped state contains result values such as symbols.

Eligible initial states:

- `count`, `sum`, `min`, and `max`;
- average as `(sum, count)`;
- VWAP as `(sum(price * volume), sum(volume))`;
- OHLCV as ordered first/last pairs, high, low, and volume;
- variance/covariance and linear regression sufficient statistics;
- fixed time-bucket and bounded-cardinality symbol groupings.

First/last requires a deterministic total tie-breaker, not timestamp alone.
Integer overflow, decimal precision, floating-point merge order, NaN, and nulls
must match the uncached DataFusion result contract. If exact equivalence cannot
be guaranteed, the aggregate is ineligible rather than approximately reused.

The delivered finance contract is narrower than SQL equivalence: required
columns must be non-null, price/volume and their products/sums must remain
finite, and int64 volumes must be exactly representable as float64. Open/close
use `(timestamp, segment checksum, row offset)` as a deterministic total key.
Unsupported types and unsafe numeric states fail instead of being cached.

### P3.2 Reuse across any manifest, optimize append specially

For a requested snapshot:

1. resolve its exact manifest;
2. look up a state for every referenced segment;
3. scan only missing segments and publish their states;
4. merge states and produce the final result.

This is correct across arbitrary versions because the current manifest, not a
version delta, defines the input. Append-only versions get the ideal path: reuse
all old segment states and scan only new segments. Compaction or overwrite may
create new checksums and require recomputation for rewritten segments, while
unchanged segments still hit. The existing append-only `diff` API is a useful
fast path but must not be the correctness foundation.

**Delivered.** `finance_rollup` resolves the requested manifest on every call,
looks up every referenced segment independently, scans only misses, merges in
manifest order, and returns requested/reused/built/scan/byte/corruption/eviction
metrics. Historical versions therefore reuse the same immutable states without
depending on a version-delta chain.

Optionally persist a small composition index from `(table id, sequence,
aggregate plan hash)` to the ordered state keys. It is an acceleration index,
not a materialized result that bypasses manifest validation.

### P3.3 Integration order

1. internal `AggregateStateStore` plus property tests comparing scan-and-merge
   with full recomputation;
2. an explicit Rust API for registered aggregate specifications;
3. built-in OHLCV and VWAP incremental rollups;
4. an optimizer rewrite only when it proves an exact match;
5. broader SQL-to-delta compilation only if real workloads demand it.

Do not support holistic aggregates such as exact median/quantile or arbitrary
window functions in the initial mergeable-state path.

Integration steps 1 and 2 are delivered for the fixed finance specification;
step 3 is delivered as an unbucketed optional-symbol OHLCV/VWAP rollup. Fixed
time buckets, optimizer rewrites, and arbitrary SQL aggregates remain later
work. The existing `h5i-db-bench` binary now records cold and warm state-store
runs, avoiding another DataFusion-linked benchmark target.

Exit gate: after appending 1 of N equal segments, a repeated eligible aggregate
scans only the new segment; compaction, overwrite, restore, schema evolution,
and cache corruption produce the same result as a forced full recomputation.

Prototype exit gate: passed for warm zero-scan reuse, a 1-of-3 append scan,
historical-version reuse, forced sidecar corruption, and a compaction rewrite,
each compared with the explicit forced-recompute mode. Restore, overwrite, and
schema-evolution cases remain required before a future SQL optimizer rewrite.

## 8. Phase P4: workload-aware, previewable reclustering

**Status: planned.** Existing compaction and mutation plan/apply provide the
safe rewrite substrate, but there is no physical `LayoutSpec`, layout health,
workload cost model, or boundary-segment selector. The existing Rust module
named `layout` is the on-disk object-path layout and should not be mistaken for
this feature.

[Workload-Aware Incremental Reclustering (WAIR)](https://arxiv.org/abs/2602.23289)
observes that partitions crossing frequently used query boundaries dominate
pruning quality. [MDDL](https://www.amazon.science/publications/automated-multidimensional-data-layouts-in-amazon-redshift)
and [Pando](https://www.vldb.org/pvldb/vol16/p2316-sudhir.pdf) show that repeated
predicates and predicate correlation can guide better layouts than a single
static column sort.

h5i-db should adopt the policy idea, not reproduce a cloud warehouse's complete
automatic physical designer.

### P4.1 Make layout explicit

Evolve the table spec with a format-versioned `LayoutSpec` that separates:

- partitioning: e.g. trading date, symbol range, or stable hash bucket;
- ordering within a partition: e.g. `(symbol, ts)`;
- segment and row-group targets;
- the layout revision that produced each segment.

Do not overload `time_column` or claim global time ordering for files ordered by
`(symbol, ts)`. DataFusion output-order declarations, append validation, ASOF
planning, and compaction must all understand the distinction.

Partial reclustering initially preserves a table's partition/order keys and
only improves clustering under those keys. Changing keys is a full layout
migration unless the planner explicitly supports mixed per-segment revisions;
during any mixed state it may advertise only the weakest ordering shared by all
segments. Never infer table-wide ordering from a newly rewritten subset.

For finance, evaluate at least:

```text
time-major:    partition trading_day(ts), order (ts, symbol)
entity-major:  partition hash(symbol),    order (symbol, ts)
```

Choose one canonical layout per table initially. Maintaining both is a
secondary index/materialized copy with substantial write and storage cost, and
must be a later explicit feature rather than an automatic default.

### P4.2 Layout health

Compute health from manifests and the bounded workload log:

- bytes and segments scanned per query family;
- zone-map overlap and symbol-membership density;
- frequency-weighted boundary heat per segment;
- sort coverage and partition skew;
- small-file/fragmentation ratio;
- estimated rewrite bytes and expected bytes saved per future query.

The score should be understandable, for example:

```text
expected benefit
  = sum(query_frequency * estimated_bytes_avoided)
    - rewrite_bytes * configured_write_cost
```

Use recent and long-term windows so a brief workload shift does not rewrite the
whole table. Require a minimum benefit, minimum candidate size, cooldown, and
maximum rewrite budget.

### P4.3 `optimize --plan` before `--apply`

Extend the existing preview/apply machinery. A plan records:

- telemetry window and query-family summary;
- exact input segment checksums and source head;
- proposed partition/order/layout revision;
- only the boundary/overlapping segments selected for rewrite;
- expected old/new segment count and temporary disk amplification;
- estimated rewrite bytes, bytes saved per workload cycle, and break-even query
  count;
- correctness checks and reasons any segment was excluded.

Apply must reject a stale source head or re-plan explicitly; it must never
silently widen the rewrite. Old versions continue referencing old immutable
segments, so the storage estimate must include retention until vacuum is safe.

Start with user-chosen keys and WAIR-like segment selection. Automatic key
selection comes later, limited to simple columns, hash/range buckets, and at
most two keys. General predicate-derived and correlation-aware layouts are a
research track only after the simpler designer has benchmark evidence.

Exit gate: the plan's predicted and observed bytes saved are calibrated on held
out queries; partial reclustering beats full-table rewrite in total
read-plus-rewrite cost and never harms snapshot results.

## 9. Phase P5: ingest tiers and adaptive encoding, only if justified

**Status: partial at the single-format layer.** Streaming query output, core
scan streams, bounded sorted-batch merging, target-sized Parquet segments, and
automatic compaction foundations have landed. Mixed Arrow/Parquet segments and
adaptive per-column encoding have not.

### P5.1 Write/read tiers

[Vortex](https://research.google/pubs/vortex-a-stream-oriented-storage-engine-for-big-data-analytics/)
motivates a layer serving both streaming and batch analytics. This embedded
project has implemented the simpler first step: bounded Parquet segments,
streaming scan/output paths, and compaction machinery. Measure that path under
small appends before adding another physical format.

Only if measured ingest latency remains unacceptable should manifests support
mixed segment formats:

```text
hot:  small Arrow IPC segments for low-latency ingestion
cold: sorted, clustered, compressed Parquet segments
```

Mixed formats require a manifest format tag, crash-safe promotion, dedup rules,
two scan adapters, compaction idempotence, and tests proving no duplicate or
missing rows after every fault point. It is not a shortcut.

### P5.2 Exhaust Parquet first

Benchmark per-column Parquet choices before inventing a format: dictionary,
delta encodings, byte-stream split, codec/level, page size, row-group size,
page indexes, and bloom filters. A small sample-based writer policy may choose
among supported encodings if its CPU cost pays back in end-to-end workloads.

[The FastLanes File Format](https://vldb.org/pvldb/vol18/p4629-afroozeh.pdf)
and [LeCo](https://arxiv.org/abs/2306.15374) are useful references for fine-grain
SIMD-friendly decoding, sampling-based encoding selection, cross-column
correlation, and learned serial correlation. They require a new reader/writer
and tighter DataFusion integration, so prototype them only when profiles show
decoder CPU or post-pruning bytes as a top-two cost. Require a material
end-to-end win, not only a compression microbenchmark, and retain a Parquet
export/interoperability path.

## 10. Ideas deliberately deferred

### Selective late materialization

[Selective Late Materialization](https://www.vldb.org/pvldb/vol18/p4616-liu.pdf)
selects a materialization point per attribute and reports gains in a modified
DuckDB. In h5i-db this crosses DataFusion optimizer, physical-plan, and Parquet
reader boundaries. First take projection pushdown, row filters, ASOF payload
deferral, and row selection. Do not fork DataFusion until profiles identify
payload materialization as a dominant residual cost and an upstream extension
point is unavailable.

### Active storage and self-describing formats

[Active Data Lakes](https://www.vldb.org/pvldb/vol19/p1372-ginter.pdf)
argues for an active storage layer that can expose secondary access paths and
virtual formats. The manifest/cache concepts here preserve the useful physical
independence, but an always-on storage service conflicts with h5i-db's embedded
scope. Keep segment format tags and access indexes extensible; do not add a
service solely to follow the architecture.

### Full predicate-derived layouts

MDDL/Pando-like derived predicates and correlated logical partitions can beat
column layouts, but introduce expression-versioning, telemetry privacy,
overfitting, and expensive rewrites. Implement explicit finance-friendly
partition/order keys and boundary reclustering first.

### Arbitrary incremental SQL

General joins, non-monotone operations, holistic aggregates, and windows need
proper differential semantics and potentially retractions. Fixed mergeable
states cover the high-value finance cases with a much smaller correctness
surface. Revisit full IVM after those cases demonstrate sustained demand.

## 11. Delivery order

| Priority | Status | Deliverable | Primary work removed | Dependency |
|---|---|---|---|---|
| P0 | **done** | Query-local metrics, workload log, benchmark matrix | uncertainty | existing scan metrics |
| P1 | **done** | Planner stats, exact-set pruning, existing pushdowns | bytes and rows | maintenance only |
| P2 | **row-group prototype done** | Immutable predicate cache | repeated scan and decode | P0 attribution; P1 pruning |
| P3 | **finance prototype done** | Segment aggregate-state store, OHLCV/VWAP | recomputation | P0; reuse P2 sidecar lifecycle |
| P4 | planned | `LayoutSpec`, health, partial optimize plan/apply | future scan and sort | P0 telemetry; existing plan/apply |
| P5 | partial | Parquet adaptation, then optional hot tier/custom encoding | ingest/decode residuals | evidence from P0-P4 |

Recommended near-term sequence:

1. replace the session-global metrics drain with an execution/query ID and an
   execution-local report; instrument actual object reads and decoded rows;
2. add the bounded, privacy-aware workload log and checked-in repeated-query
   benchmark;
3. prototype exact-match predicate-cache entries at row-group granularity for
   one deterministic symbol/time predicate shape;
4. reuse the proven sidecar/checksum/eviction machinery for segment aggregate
   states, starting with `count`, OHLCV, and VWAP;
5. build layout health and read-only optimize recommendations before any new
   rewrite policy.

P2 and the state-store internals of P3 can proceed independently after the P0
identity/metrics interface stabilizes. P4 should consume telemetry but must
never mutate layout without an inspectable plan. P5 has no calendar commitment
until its benchmark gate is met.

## 12. Correctness and operability invariants

All performance work must preserve these rules:

1. A version manifest remains the sole authority for a snapshot's rows.
2. Cache absence, eviction, corruption, or version mismatch causes a miss, not
   a query error or a different result.
3. Every persistent cache key includes segment identity and expression/engine
   semantics; schema revision alone is insufficient.
4. Optimizer rewrites are exact and testable against a forced uncached plan.
5. Layout optimization uses plan/apply, exact input checksums, rewrite budgets,
   and temporary-space estimates.
6. Old snapshots remain readable after optimization until retention/vacuum
   policy explicitly removes them.
7. Performance claims include end-to-end latency, resource usage, and controls;
   theoretical scan reductions and paper results are labeled as such.

## 13. Research basis and interpretation

The sources motivate mechanisms, not promised outcomes:

| Work | Mechanism used here | Decision |
|---|---|---|
| [Predicate Caching, SIGMOD 2024](https://www.amazon.science/publications/predicate-caching-query-driven-secondary-indexing-for-cloud-data-warehouses) | cache qualifying ranges for repeated scans | adopt after telemetry/pruning |
| [WAIR, SIGMOD 2026](https://arxiv.org/abs/2602.23289) | prioritize query-boundary partitions and price rewrite cost | adopt in previewable partial reclustering |
| [MDDL, SIGMOD 2024](https://www.amazon.science/publications/automated-multidimensional-data-layouts-in-amazon-redshift) | learn layout from repeated predicates | adopt only a simple key recommender first |
| [Pando, VLDB 2023](https://www.vldb.org/pvldb/vol16/p2316-sudhir.pdf) | exploit predicate correlation for skipping | defer general logical partitions |
| [OpenIVM, SIGMOD 2024 demo](https://ir.cwi.nl/pub/34276) | maintain results from deltas using an existing engine | adopt fixed mergeable states first |
| [Selective Late Materialization, VLDB 2025](https://www.vldb.org/pvldb/vol18/p4616-liu.pdf) | choose a materialization point per attribute | defer engine-level work |
| [FastLanes, VLDB 2025](https://vldb.org/pvldb/vol18/p4629-afroozeh.pdf) | fine-grain adaptive expression encoding | benchmark-gated research track |
| [LeCo, SIGMOD 2024](https://arxiv.org/abs/2306.15374) | exploit serial correlation in columns | benchmark-gated research track |
| [Vortex, SIGMOD 2024](https://research.google/pubs/vortex-a-stream-oriented-storage-engine-for-big-data-analytics/) | support streaming and batch storage paths | try single-format micro-segments first |
| [Active Data Lakes, VLDB 2026](https://www.vldb.org/pvldb/vol19/p1372-ginter.pdf) | external access paths and physical independence | preserve extensibility; no service now |

The publish venue/status and measurements above should be rechecked when used
in external project claims. This roadmap's implementation decisions do not
depend on a paper's headline speedup.
