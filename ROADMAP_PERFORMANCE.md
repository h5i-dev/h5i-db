# h5i-db structural performance roadmap

Status: proposal, 2026-07-22. This document is intentionally separate from
`ROADMAP.md`: that file tracks production-readiness work, while this one
describes performance features inspired by recent database research.

## 1. Decision

h5i-db should optimize the amount of work performed before it invests in a
custom file format or lower-level kernels. Its most useful performance identity
is:

> A workload- and version-aware analytical database that skips data and reuses
> work across immutable versions.

The first research-driven features should be:

1. query and scan telemetry with reproducible benchmarks;
2. complete metadata pruning and planner statistics;
3. an immutable-segment predicate cache;
4. version-aware, mergeable aggregate states;
5. workload-aware, previewable partial reclustering.

These fit the current architecture: versions are immutable manifests, segments
have stable checksums, segment statistics already exist, scans already use
DataFusion pruning, and destructive rewrites already have a plan/apply review
flow. A cache miss can therefore cost time but must never affect correctness.

FastLanes/LeCo-style storage, mixed hot/cold formats, and selective late
materialization remain worthwhile experiments, but only after profiles show
that bytes decoded or decoder CPU dominate after the first five items land.

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

As inspected on 2026-07-22, h5i-db already has:

- immutable Parquet segments referenced by immutable version manifests;
- a stable segment checksum, row/byte counts, schema revision, time range, and
  per-column min/max/null counts in `SegmentMeta`;
- `sort_key`, target segment size, target row-group size, and codec options;
- manifest-based DataFusion pruning that fails open;
- append-only version diffs and scanning only added segments;
- preview/apply mutation plans and copy-on-write compaction.

The important gaps are:

- `PruningStatistics::contained` has no bloom/distinct-set implementation;
- the provider does not yet expose the manifest's exact statistics to the
  optimizer;
- metrics do not cleanly attribute bytes and rows to one concurrent query;
- there is no persistent predicate or aggregate-state cache;
- layout is a static sort key rather than a measured physical-design decision;
- no cost model selects a subset of segments for reclustering.

The implementation may move while this roadmap is active. Each phase starts
with a short source audit and updates this list instead of assuming these facts
remain true.

## 4. Phase P0: make saved work observable

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

### P0.3 Benchmark gates

Add a checked-in data generator and result schema. A performance change passes
only if it preserves result checksums and does not regress the cross-sectional
or ingest control workload beyond an agreed threshold. Keep benchmark results
out of correctness tests, but retain machine, compiler, dataset, and command
metadata so runs are comparable.

## 5. Phase P1: finish cheap pruning before adding caches

This phase is mostly conventional engineering, but it is a prerequisite for
meaningful research-derived features.

### P1.1 Manifest statistics in planning

Fold manifest row/byte/min/max values into DataFusion `Statistics`, and attach
post-pruning statistics to the scan where the API permits it. This enables join
ordering and metadata-only `COUNT(*)`, `MIN`, and `MAX` only where every segment
has complete, untruncated statistics whose type semantics are exact. Row counts
can be exact even when a column bound is unknown. Test null-only columns,
truncated strings, NaN, schema revisions, and filtered scans.

### P1.2 Equality pruning for entity columns

Add a versioned membership summary to each segment for configured low-cardinal
columns such as `symbol`, `exchange`, and `venue`:

- exact sorted distinct values below a cardinality/byte threshold;
- otherwise a bloom filter with format version, hash seed, bit count, and hash
  count;
- no summary when construction is not beneficial.

Wire this into `PruningStatistics::contained`. False positives are allowed;
false negatives are correctness bugs. The normal DataFusion filter remains in
the plan and rechecks every returned row. Benchmark manifest growth as well as
bytes skipped. Parquet-native bloom/page indexes can be added independently
when the Arrow/Parquet reader consumes them effectively.

### P1.3 Remove known avoidable work

Complete the existing `ROADMAP.md` items that otherwise obscure structural
wins: ASOF projection/filter pushdown and bounded memory, streaming ingest and
output, O(n) sortedness checks, retractable VWAP/window state, and correct
ordering/statistics declarations. These are higher confidence than a new cache.

Exit gate: the performance report accounts for at least 95% of planned segment
bytes as read or pruned, and the entity/time benchmark demonstrates that
membership summaries skip irrelevant segments without changing results.

## 6. Phase P2: immutable predicate cache

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

## 7. Phase P3: version-aware mergeable aggregate states

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

Exit gate: after appending 1 of N equal segments, a repeated eligible aggregate
scans only the new segment; compaction, overwrite, restore, schema evolution,
and cache corruption produce the same result as a forced full recomputation.

## 8. Phase P4: workload-aware, previewable reclustering

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

### P5.1 Write/read tiers

[Vortex](https://research.google/pubs/vortex-a-stream-oriented-storage-engine-for-big-data-analytics/)
motivates a layer serving both streaming and batch analytics. For this embedded
project, first try the simpler solution: stream writes, create bounded Parquet
micro-segments, and compact them by policy or group commit.

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

| Priority | Deliverable | Primary work removed | Dependency |
|---|---|---|---|
| P0 | Per-query metrics, workload log, benchmark matrix | uncertainty | none |
| P1 | Planner stats, membership pruning, existing pushdowns | bytes and rows | P0 metrics |
| P2 | Immutable predicate cache | repeated scan and decode | P1 pruning |
| P3 | Segment aggregate-state store, OHLCV/VWAP | recomputation | P0; P2 lifecycle patterns |
| P4 | `LayoutSpec`, health, partial optimize plan/apply | future scan and sort | P0 telemetry; P1 stats |
| P5 | Parquet adaptation, then optional hot tier/custom encoding | ingest/decode residuals | evidence from P0-P4 |

P2 and the state-store internals of P3 can proceed independently once P0/P1
interfaces stabilize. P4 should consume telemetry but must never mutate layout
without an inspectable plan. P5 has no calendar commitment until its benchmark
gate is met.

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
