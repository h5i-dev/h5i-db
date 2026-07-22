# h5i-db design and roadmap

Status: proposed design, 2026-07-21

## Executive decision

h5i-db should be a high-performance, embedded, versioned DataFrame and time-series database written in Rust. It should not be a Rust reimplementation of DuckDB, an agent-memory product, or a distributed database.

The design is:

```text
Rust API / Python (PyArrow) / non-interactive CLI
                         |
           snapshot and table APIs
                         |
      +------------------+------------------+
      |                                     |
DataFusion adapter                    direct scan API
(optional SQL feature)              (small core build)
      |                                     |
      +---------- Arrow batches ------------+
                         |
       versioned table/storage kernel
                         |
 immutable manifests + immutable Parquet segments
                         |
        local filesystem first; S3 later
```

The important boundary is that the versioned storage kernel does **not** depend on DataFusion. DataFusion is the default query-engine adapter and is enabled in the CLI, but users who only need versioned Arrow reads and writes can build a materially smaller library.

The initial consistency model is multiple readers and one successful writer per table commit. Writers may race, but updating a table head is compare-and-swap (CAS): one succeeds and the others receive an explicit conflict. There is no last-writer-wins behavior.

MCP, agents, worktrees, seats, review state, and agent memory are not database concepts. Agents use the same CLI, Rust API, or Python API as every other client. If an MCP adapter becomes useful, it belongs in a separate package after the database interface is stable.

## Product contract

### One-sentence definition

> An embedded Rust database for immutable, versioned Arrow tables, optimized for time-range reads, append-heavy workloads, point-in-time reproducibility, and analytical queries.

### Target workloads

- Market data, telemetry, experiment results, and other append-heavy time series.
- DataFrames with tens to hundreds of columns and many millions to billions of rows.
- Repeated narrow time-range and column-subset reads.
- Historical corrections that replace a bounded time range without copying unaffected data.
- Reproducible research and backtests that pin exact versions of several tables.
- Embedded analytical SQL, including joins, group-bys, windows, and bounded-memory execution.

### Explicit non-goals through the first beta

- OLTP, row-at-a-time updates, secondary B-tree indexes, foreign keys, or high-contention writes.
- Distributed query execution, replication, consensus, or multi-region availability.
- Atomic transactions spanning multiple tables.
- A server process, wire protocol, RBAC, or user management.
- Reimplementing a SQL parser, optimizer, or general-purpose execution engine.
- ArcticDB API or storage-format compatibility.
- A custom columnar data format before Parquet is shown to be the bottleneck.
- MCP or any h5i/agent-specific data model.

## What the reference implementations imply

### ArcticDB: copy the problem boundary, not the implementation

ArcticDB is the closest product model. Its reference layer points to a version chain; versions point through an index layer to immutable, compressed data segments. Unchanged segments are shared between versions. The source also makes the costs visible:

- The version layer is a linked-list-like structure, so old-version reads can require many object reads.
- Small appends create many small immutable objects and require explicit data compaction.
- Normal concurrent writes to one symbol are unsupported; the documented outcome can be last-writer-wins unless staged writes are used.
- The current group-by API accepts one grouping column and five aggregate operations, although newer LazyDataFrame APIs cover more filtering, projection, grouping, and resampling cases.
- Row and column tiling helps wide-frame slicing, but introduces a custom format and more objects.

h5i-db should retain immutable segment sharing and user-visible time travel, while using conflict detection, logarithmic version navigation, and Parquet first. It should row-partition segments initially and rely on Parquet column projection instead of implementing ArcticDB-style column tiles.

### DuckDB: use as the SQL and benchmark oracle

DuckDB is a complete DBMS: parser, binder, optimizer, vectorized execution, MVCC, storage, WAL/checkpointing, buffer management, spill, indexes, and extensions. The inspected source uses 2,048-row standard execution vectors and a default 122,880-row storage row group. Its ASOF join alone has a dedicated planner and physical operator with multiple execution strategies.

That scope is evidence against building a query engine. DuckDB should instead be:

- A correctness oracle for SQL and ASOF semantics.
- A performance baseline for scans, aggregates, joins, windows, and Parquet access.
- A source of design lessons such as vectorized batches, zone-map pruning, bounded memory, and separate logical/physical planning.

Embedding DuckDB as h5i-db's engine is not the preferred design. Its C++ build, extension boundary, internal storage model, and Arrow bridge make snapshot-manifest integration less natural than a Rust-native DataFusion `TableProvider`. It remains a fallback worth revisiting if DataFusion cannot meet the measured query targets.

### DataFusion: use behind a narrow adapter

The inspected DataFusion tree is version 54.1.0. Its `TableProvider` interface directly accepts projection, filters, and limit when producing an execution plan. Its Parquet implementation already has row-group statistics, bloom-filter and page pruning, async object-store reads, streaming Arrow output, memory pools, disk limits, and spilling.

The fit is strong, but it is not free:

- Default features include many expression families, compression codecs, Parquet, recursive protection, and SQL. h5i-db should disable defaults and enable only tested features.
- DataFusion is a large and fast-moving dependency. All DataFusion types must stay inside one adapter crate.
- No native ASOF join implementation was present in the inspected 54.1.0 tree. ASOF requires an h5i-db logical/physical extension or a later upstream capability.
- Time-series functions such as configurable time buckets and OHLC convenience need small UDF/UDAF additions even though ordinary windows and ordered `first_value`/`last_value` already exist.

### TimescaleDB: adopt partitioning and invalidation lessons selectively

TimescaleDB demonstrates the value of separating a time partition key, segment/group keys, and ordering keys. Its columnstore records min/max, first/last, and bloom metadata, and its continuous aggregates track invalidated time ranges rather than rebuilding everything.

For h5i-db this implies:

- Make the time column and sort key explicit table metadata.
- Keep segment-level min/max statistics in the h5i-db manifest so pruning does not first fetch every Parquet footer.
- Add optional equality-key bloom filters only after measurements justify their cost.
- Model future materialized rollups as versioned derived tables plus invalidated ranges, not as a special mutable cache.

Continuous aggregates are post-beta. TimescaleDB code under `tsl/` and current ArcticDB code have source-available licenses rather than the repository's Apache-2.0 license, so they are architectural references only; implementation must be original and based on public behavior and formats.

## Data model

```text
Database
|-- catalog
|-- Table (stable UUID, mutable name reference)
|   |-- TableSpec
|   |-- HEAD -> VersionRef
|   |-- Version 0
|   |-- Version 1
|   `-- Version N
|       `-- SegmentIndex -> immutable Parquet segments
`-- Snapshot (name -> {table UUID: VersionRef})
```

### TableSpec

Every table has:

- An Arrow schema, including field and schema metadata.
- An optional `time_column`.
- A `sort_key`, initially empty or beginning with the time column.
- Future-compatible `partition_key` metadata; partitioned writes need not be implemented in the first release.
- User metadata with a documented size limit.
- Storage options such as target segment size and compression codec.
- A monotonically increasing schema revision.

The time column must be an Arrow timestamp or integer epoch. Timestamp precision through nanoseconds is preserved. Internally, comparisons normalize timestamp values to UTC; original timezone metadata remains in the Arrow schema. Null time values are rejected for a table that declares a time column.

A time column is an ordering/indexing hint, not a uniqueness constraint. Duplicate timestamps are legal. Operations that need deterministic tie-breaking use the full sort key and then stable input order.

### VersionRef and VersionManifest

A committed version has both a human-friendly sequence and a collision-resistant commit ID:

```text
VersionRef {
    sequence: u64,
    commit_id: UUID,
    manifest_key: string,
    manifest_checksum: bytes,
}
```

The immutable version manifest contains:

- Format version, table UUID, `VersionRef`, parent, and binary-lifting ancestor pointers.
- Monotonic `committed_at`, writer-provided metadata, and operation kind.
- Arrow schema and schema revision.
- Row count, byte count, global time bounds, and sortedness.
- A segment index, initially an inline ordered list.
- Checksums for every referenced object.

Ancestor pointers cover distances 1, 2, 4, 8, and so on. Reading sequence `N` or resolving `as_of(time)` therefore takes `O(log V)` manifest reads instead of walking every intervening version. `committed_at` is `max(wall_clock, parent.committed_at + 1ns)`, ensuring the committed chain is monotonic even with imperfect client clocks.

The first implementation uses an inline segment list with warnings at 1,024 segments and a configurable hard guard at 4,096. The manifest schema reserves an alternative persistent segment-tree root. If compaction cannot keep manifests bounded in real workloads, a copy-on-write B-tree ordered by time/sort bounds replaces the inline list without changing the higher layers.

### Segment metadata

Each immutable segment records:

- Object key, encoded byte size, row count, and checksum.
- Arrow schema revision.
- Min/max and null count for the time column and explicitly configured pruning columns.
- Sort bounds and whether they are exact.
- Parquet row-group statistics summary.
- Creation version and optional encryption metadata reserved for later.

The initial writer targets approximately 128 MiB of Arrow input per Parquet object and 16-64 MiB per row group. These are tuning defaults, not format constants. Zstd is the default codec; uncompressed and LZ4/Snappy are optional only when benchmarked and enabled.

## On-disk layout

The local layout is directory-based and deliberately inspectable:

```text
FORMAT
catalog/tables/<hash-of-name>.json
tables/<table-uuid>/spec/00000001.json
tables/<table-uuid>/HEAD
tables/<table-uuid>/versions/<sequence>-<commit-id>.json
tables/<table-uuid>/segments/<segment-id>.parquet
snapshots/refs/<hash-of-name>.json
snapshots/manifests/<snapshot-id>.json
tmp/<writer-id>/...
```

Names are stored in the referenced JSON and hashed for paths; raw user strings never become filesystem paths. `FORMAT` identifies database and minimum reader versions. JSON is acceptable for small control metadata in the first format because it is inspectable and easy to migrate. Parquet holds all bulk data. Canonical serialization plus a checksum detects torn or corrupted metadata.

Changing JSON to a compact binary encoding is not a priority unless metadata profiles show it matters. Format readers must be versioned from the first commit.

## Commit protocol and consistency

### Invariants

1. Segments and version manifests are immutable after publication.
2. A reader resolves each table's head once and never observes it changing mid-scan.
3. `HEAD` is the only mutable object in a table's version path.
4. `HEAD` never points to an object that is not durable and checksum-valid.
5. A table commit is visible entirely or not at all.
6. A writer never silently overwrites a commit based on a different parent.
7. Uncommitted objects are unreachable and may be vacuumed after a grace period.

### Local commit

1. Resolve `HEAD`; retain its generation and `VersionRef` as the expected parent.
2. Acquire the table writer lock, then revalidate the expected parent.
3. Write new segments under a unique temporary writer directory.
4. Flush and `fsync` segment files; atomically rename them into immutable locations.
5. Write, flush, and atomically publish the immutable version manifest.
6. Write and `fsync` a new `HEAD` file, then atomically rename it over the old head.
7. `fsync` affected directories before reporting success.

The lock serializes local writers, while expected-parent validation preserves explicit optimistic-concurrency semantics. A caller can request `expected_version`; a stale value produces `Conflict`, not an implicit retry.

### Object-store commit

S3 is introduced only after local semantics pass fault injection. Segments and the manifest are uploaded first. `HEAD` is then updated with an ETag/version conditional PUT. A backend without conditional update support is read-only unless the user explicitly selects a documented single-writer mode; h5i-db must never pretend it offers conflict detection when the backend cannot provide it.

### Read snapshots

A query resolves all referenced table heads before planning and pins those `VersionRef`s for its lifetime. This makes the statement repeatable, but without cross-table transactions the heads may have been committed at slightly different moments.

A named database snapshot is an immutable map from table UUIDs to exact `VersionRef`s. Creating one captures a reproducible set, not a claim of a globally atomic write. Callers that require a known set can supply expected versions while creating the snapshot.

## Write and version semantics

The operations must be few and unambiguous:

| Operation | Meaning | First implementation |
|---|---|---|
| `write` | Replace the entire logical table with the input | New version; no old data copied |
| `append` | Add rows after the current ordered tail | Exact schema; nondecreasing sort key; conflict checked |
| `replace_range` | Replace rows in an inclusive/exclusive time interval | Rewrite overlapping segments, reuse untouched ones |
| `restore` | Make historical contents current | New version referencing old segments; history is not rewound |
| `compact` | Rewrite small adjacent segments into target-sized segments | New logically equivalent version |
| `expire_before` | Set a history floor, respecting named snapshots | Post-MVP |
| `vacuum` | Delete unreachable temporary/orphaned objects after a grace period | Dry-run by default |

`append` is deliberately strict. Its input schema must match exactly, its declared sort order must be valid, and its minimum sort key must not precede the current maximum. Unsorted ingestion uses `write` initially and a staged/sort-and-commit API later.

`replace_range` is the first correction primitive. It reads and rewrites only boundary/overlapping segments, then builds a manifest that shares all unaffected segments. This avoids a permanent merge-on-read delta layer. Arbitrary predicate updates and deletes are deferred; they are a poor fit for the initial append-oriented product.

The MVP has no atomic multi-table `write_batch`. A batch helper may execute independent commits and return a result per table, but its name and documentation must state that it is not atomic.

### Schema evolution

The MVP uses static schemas for `append` and `replace_range`. `write` may establish a new schema because it replaces all data. A later schema-evolution milestone permits:

- Adding nullable columns.
- Widening explicitly approved numeric types.
- Changing metadata without changing physical values.

Dropping, narrowing, renaming, or changing timestamp interpretation requires `write` into a new version or table. Readers materialize missing newly added columns as null. No implicit Pandas-style type coercion occurs in the storage kernel.

## Read and query path

### Direct scan API

The storage kernel exposes an async stream of Arrow `RecordBatch` values with:

- `ReadAt::Latest`, `Version(sequence)`, `VersionRef`, `AsOf(time)`, or named snapshot.
- Projection.
- Time range with explicit bound inclusivity.
- A small typed predicate subset used only for segment pruning.
- Batch-size and output-row limits.

The scan resolves a version, prunes segments from manifest statistics, then relies on Parquet row-group/page pruning for finer filtering. Projection is pushed into the Parquet reader. Results stream; collecting an entire table is an API convenience, not the engine default.

### DataFusion adapter

`h5i-db-datafusion` implements a snapshot-bound `TableProvider`:

1. The provider owns an exact `VersionRef`, never a mutable table name alone.
2. `scan` translates projection, supported filter expressions, and limit into an h5i-db scan plan.
3. Manifest statistics eliminate whole Parquet objects.
4. DataFusion's Parquet source handles row-group/page pruning and produces the physical stream.
5. Unsupported or inexact filters remain in DataFusion's plan; the provider reports a filter as `Exact` only when it really evaluates it.

No DataFusion types leak into storage manifests or the public core traits. The adapter is the only crate upgraded when DataFusion APIs change.

The CLI SQL build should start from something like `default-features = false` plus `sql`, `parquet`, `datetime_expressions`, and the few expression families actually exposed. Dependency versions are pinned and updated intentionally, not accepted through a broad semver range.

### Query feature boundary

Available from DataFusion in the SQL milestone:

- Projection and predicates.
- Multi-column group-by and normal aggregates.
- Sort, limit, union, CTEs, subqueries, and ordinary joins.
- Window functions and ordered first/last aggregates.
- Streaming results, parallel scans, bounded memory, and spill-to-disk.

h5i-db additions:

- `time_bucket(interval, timestamp, origin, offset)` with nanosecond-safe semantics.
- OHLC helpers expressed using ordered first/last, min/max, and optional volume sum.
- `asof_join` with equality `by` keys, one ordered inequality, direction, tolerance, and inner/left modes.
- Scan metrics: manifests/segments/row groups considered and pruned, bytes read, spill bytes, and peak memory.

ASOF must not be disguised as a trivial UDF. The first correct interface is a Rust/Python relational API and a CLI subcommand backed by a sorted streaming physical operator. SQL `ASOF JOIN` syntax follows only when it can be implemented through a maintained planner extension or upstream DataFusion support. DuckDB defines the semantic test oracle.

## Public interfaces

### Rust sketch

```rust
let db = Database::open("market.h5db").await?;
let prices = db.table("prices").await?;

let commit = prices
    .append(input_batches, AppendOptions::default().expected(head))
    .await?;

let stream = prices
    .scan(ReadAt::Version(commit.sequence))
    .project(["symbol", "ts", "price"])
    .time_range(start..end)
    .execute()
    .await?;

let result = db
    .sql_at(ReadSnapshot::Named("backtest-2026-07-01"), sql)
    .await?;
```

Concrete Rust APIs will evolve, but streaming Arrow input/output, explicit read versions, explicit bounds, and explicit expected heads are fixed design requirements.

### Python

Python accepts and returns PyArrow tables or record-batch readers through Arrow's C Data/C Stream interfaces to avoid copies. Pandas and Polars integration is conversion sugar:

```python
db.write("prices", pyarrow_table)
db.append("prices", new_rows, expected_version=41)
reader = db.read("prices", as_of="2026-07-01T00:00:00Z", streaming=True)
reader = db.sql("SELECT symbol, avg(price) FROM prices GROUP BY symbol")
```

Pandas is not a required runtime dependency of the core Python wheel. `to_pandas()` and Polars construction happen through PyArrow.

### CLI

The CLI is non-interactive by default and supports SQL from an argument, file, or stdin:

```text
h5i-db init market.h5db
h5i-db write market.h5db prices --input prices.parquet
h5i-db append market.h5db prices --input - --format arrow-ipc --expected-version 41
h5i-db query market.h5db --sql "SELECT ..." --format jsonl --read-only
h5i-db versions market.h5db prices --format json
h5i-db snapshot create market.h5db backtest-2026-07-01
h5i-db compact market.h5db prices
h5i-db vacuum market.h5db --dry-run --format json
```

Output formats are Arrow IPC stream, Parquet, JSON, JSONL, CSV, and a human table. Machine-readable errors go to stderr and contain `code`, `message`, `retryable`, and structured context. Stable exit categories are success, usage, not-found/schema, conflict, resource-limit/cancelled, and corruption/internal.

Resource controls are first-class CLI/query options:

- `--read-only`
- `--max-output-rows` and `--max-output-bytes`
- `--memory-limit`
- `--spill-dir` and `--spill-limit`
- `--timeout`
- `--threads`

An output limit is not represented as a query memory limit. Cancellation propagates through DataFusion and storage reads. A read-only query never performs opportunistic compaction or metadata repair.

## Compaction, retention, and repair

Compaction is a normal versioned write, not an in-place mutation. The planner selects adjacent small segments with compatible schema and ordering, reads them, writes target-sized replacements, and CAS-commits a logically equivalent manifest. Concurrent user writes cause a conflict and leave only vacuumable orphans.

Initial triggers:

- Explicit `compact` only in the first vertical slice.
- Recommend compaction when small-segment count or manifest count crosses a measured threshold.
- Add opt-in automatic compaction between CLI operations only after its latency is predictable; never run it during read-only queries.

Vacuum uses mark-and-sweep from table heads, retained version ancestry, and named snapshots. It has a grace period so an in-flight writer's objects are not removed. The MVP vacuum only removes abandoned temporary/orphaned objects and always offers a dry-run. History expiration and deletion of committed manifests arrive with `expire_before` after reachability tests are exhaustive.

Repair commands verify head, manifest, segment existence, sizes, and checksums. Repair does not guess a new head automatically; it reports candidates and requires an explicit user action.

## Crate and dependency boundaries

Proposed workspace:

```text
crates/h5i-db-types       Arrow-facing IDs, schemas, manifests, errors
crates/h5i-db-storage     storage trait and local filesystem backend
crates/h5i-db-core        tables, versions, commits, scans, compaction
crates/h5i-db-datafusion  TableProvider, SQL, UDF/UDAF, query metrics
crates/h5i-db-cli         non-interactive binary
crates/h5i-db-python      PyO3 + Arrow C stream bridge
crates/h5i-db-s3          optional object-store backend
```

Dependency rules:

- Core uses focused Arrow crates and Parquet, not the DataFusion umbrella.
- DataFusion, SQL parser, Tokio runtime configuration, and spill machinery stay in the adapter/CLI side.
- The local backend uses `std::fs`; S3/object-store dependencies are optional.
- Compression features are explicit. Do not enable every DataFusion default codec or expression package.
- Public core traits do not expose DataFusion, S3 SDK, PyO3, or CLI types.
- `cargo tree`, cold build time, release binary size, and duplicate Arrow/Parquet versions are recorded in CI.

This is “fewer dependencies” as an architectural property, not as a contest to avoid a well-tested engine. Replacing DataFusion's optimizer, joins, windows, memory accounting, and spill would create far more code and risk than the dependency saves.

## Concrete roadmap

Each phase has an exit gate; dates should be assigned only after Phase 0 measurements.

### Phase 0 — feasibility and contracts

Deliverables:

- Workspace and crate boundaries above.
- Arrow schema/manifest prototypes with golden compatibility fixtures.
- Minimal Parquet segment writer/reader.
- DataFusion `TableProvider` proof of concept with projection and time-filter pruning.
- Dependency, compile-time, binary-size, and raw-Parquet benchmark baselines.
- Fault-injection test harness for each local commit step.
- ADRs fixing Parquet-first, CAS heads, strict append schemas, and feature-gated DataFusion.

Exit gate: one million generated rows can be written, versioned, and scanned through both the direct API and SQL; killing the writer at every commit boundary yields either the old or new head, never a corrupt visible table.

### Phase 1 — local versioned storage preview

Deliverables:

- Local database/catalog and table creation.
- `write`, strict ordered `append`, latest/version/as-of read, list versions, and `restore`.
- Projection and time-range segment pruning.
- Immutable JSON manifests, Parquet segments, checksums, locks, and head CAS.
- Rust streaming API and basic CLI using Arrow IPC/Parquet.
- Snapshot create/read/list for exact table-version maps.
- Orphan discovery and dry-run vacuum.

Exit gate: property tests cover version sharing and range bounds; two racing writers produce one commit and one conflict; 10,000 versions remain logarithmic to resolve by sequence/as-of; corruption is reported with the exact object key.

### Phase 2 — analytical SQL alpha

Deliverables:

- Pinned DataFusion adapter and SQL CLI.
- Projection/filter/limit pushdown plus manifest and Parquet pruning metrics.
- Aggregates, multi-column group-by, normal joins, sorts, and windows.
- Memory, disk-spill, thread, timeout, and output limits.
- JSON/JSONL/CSV/human streaming renderers and stable error schema.
- `EXPLAIN` and `EXPLAIN ANALYZE` with h5i-db scan metrics.

Exit gate: SQL differential tests pass against DuckDB for the supported subset; a forced-spill query succeeds within its memory limit; cancellation releases file handles and temporary spill files; wrapper overhead on a raw Parquet scan is within 10% of direct DataFusion on the same files.

### Phase 3 — time-series MVP

Deliverables:

- `replace_range` with unchanged-segment reuse.
- Explicit sort verification and better staged/sort-and-commit ingestion.
- `time_bucket`, resampling helpers, OHLC recipes, and ordered first/last.
- Streaming ASOF physical operator and Rust/Python/CLI relational API.
- Explicit compaction planner/executor and fragmentation diagnostics.
- Python wheel with zero-copy PyArrow streams.

Exit gate: ASOF results, duplicates, nulls, directions, and tolerance match DuckDB golden cases; corrections rewrite only overlapping segments; 10,000 tiny appends followed by compaction produce the same logical rows with a bounded segment count; PyArrow round trips preserve schema metadata, decimals, timestamps, and nulls.

This is the first release called **MVP**. It is useful locally from Rust, Python, and shell without pretending that object-store consistency is solved.

### Phase 4 — object storage and schema evolution

Deliverables:

- Storage capability contract and S3 backend with conditional head updates.
- Retry/idempotency behavior, multipart cleanup, request metrics, and credential pass-through.
- Nullable-column additions and approved numeric widening.
- Equality pruning metadata/bloom filters if benchmarks justify them.
- History floor, retention policy, and reachability-based committed-data vacuum.
- Optional persistent segment index if inline manifests fail the threshold.

Exit gate: S3-compatible fault/concurrency tests prove no lost updates; request-count benchmarks show old-version lookup is logarithmic and range scans avoid unrelated objects; vacuum cannot remove data pinned by any named snapshot.

### Phase 5 — beta hardening

Deliverables:

- Format migration policy and two-version backward-read compatibility tests.
- Fuzzing for manifests, Parquet boundaries, predicates, and crash recovery.
- Full benchmark dashboard against ArcticDB, DuckDB, and raw DataFusion/Parquet.
- Packaging for major platforms, observability, documentation, and support matrix.
- Decision on partition keys and copy-on-write segment index based on production-shaped data.

Exit gate: defined durability, compatibility, and performance SLOs pass in CI; no known silent-corruption or lost-update path remains.

### Later, only with evidence

- Continuous/materialized time-bucket aggregates using invalidation ranges.
- Partitioning by symbol/device plus ordered clustering.
- SQL syntax for ASOF if the planner integration is maintainable.
- Encryption, remote cache, or a read-only server gateway.
- Custom segment encoding or column tiling.
- A separately maintained `h5i-db-mcp` adapter.

## Benchmark and acceptance plan

Use generated and captured-shape datasets, with all seeds and schemas checked in. Report rows/s, p50/p95 latency, peak RSS, bytes and objects read/written, spill bytes, and total storage.

Required workloads:

1. Full write of narrow and 100-column frames.
2. One large append and 10,000 tiny appends.
3. Four-column reads over 0.01%, 1%, and 100% of the time range.
4. Equality plus time predicates at different selectivities.
5. Group-by, ordered window, regular join, and ASOF join.
6. Latest, version 1, and as-of lookup after 10,000 commits.
7. Range correction touching one boundary segment and 25% of a table.
8. Compaction before/after read latency and object count.
9. Forced memory pressure and spill.
10. Writer crash/fault at every durable commit step.

Comparators:

- Raw DataFusion over the identical Parquet files isolates h5i-db metadata overhead.
- DuckDB over the identical Parquet files is the general SQL/ASOF baseline.
- ArcticDB on equivalent DataFrames is the versioned append/range-read baseline.

Initial measurable targets:

- Version and as-of resolution scale with `O(log V)` manifest reads.
- An ordered time-range scan reads no segment whose exact bounds are disjoint.
- Append writes only new data, metadata, and head; it never rewrites unaffected segments.
- h5i-db SQL scan overhead is at most 10% over direct DataFusion on the same files.
- A bounded query either stays within configured memory plus documented allocator slack, spills within its disk limit, or fails with a resource-limit error.
- Every acknowledged local commit survives process termination and reopening.
- Compaction of tiny appends materially reduces object count without changing Arrow results or old versions.

Absolute throughput targets should be set from Phase 0 on named hardware. Inventing hardware-independent rows/second numbers would not guide engineering.

## Decision triggers and risks

### When to reconsider DataFusion

Keep DataFusion if the adapter meets scan-overhead, memory, correctness, and upgrade-cost gates. Reconsider DuckDB embedding or a narrower custom execution layer only if two consecutive pinned DataFusion releases fail a critical workload and an isolated prototype demonstrates a clear improvement. Do not switch based on binary size alone while ignoring the code that would need to replace joins, windows, and spill.

### When to invent a storage format

Stay on Parquet until profiles show a repeatable bottleneck. A custom format/column tiling investigation starts only if, after correct pruning and compaction:

- Narrow time-and-column slices remain at least 2x slower than the ArcticDB baseline, or
- Parquet metadata/object reads dominate latency, or
- Required decimal/timestamp semantics cannot be represented correctly.

Any new format must beat Parquet on target workloads, retain Arrow streaming, and include a migration/compatibility story.

### Principal risks

- **Tiny append fragmentation:** mitigate with diagnostics, staged ingestion, and explicit compaction before background policy.
- **DataFusion churn and dependency size:** isolate, pin, minimize features, and continuously record build metrics.
- **Object-store consistency:** require CAS capability and test request-level fault cases; never fall back silently.
- **Metadata growth:** inline-list limits now, reserved persistent-tree representation later.
- **Schema semantic mismatch:** test Arrow/PyArrow nulls, decimals, dictionaries, timezone metadata, and nested types explicitly; publish a support matrix.
- **ASOF complexity:** ship a tested relational API before custom SQL syntax and use DuckDB differential cases.
- **Cross-table expectations:** state clearly that snapshots are reproducible maps, not multi-table write transactions.
- **Licensing contamination:** do not copy BSL ArcticDB or Timescale-licensed implementation code into this Apache-2.0 repository.

## MCP and agent integration

There is no MCP milestone in the core roadmap. A shell-capable agent can discover schema and operate the database with stable CLI commands and structured output. A Python-capable agent can use the Python package. Skills can document those operations without changing the database.

An optional MCP adapter is justified only if there is demonstrated demand from clients without shell/Python access, or if a remote deployment needs centralized authentication and schema discovery. If built, it is a thin, separate consumer of the public API:

```text
h5i-db          database crates and CLI
h5i-db-python   Python package
h5i-db-mcp      optional adapter, separate release
```

The adapter must not introduce MCP types, agent identity, or worktree semantics into manifests, storage, query planning, or consistency rules.

## Source notes

Local references were inspected at these commits on 2026-07-21:

- ArcticDB `dd6edd5e48f1dc1f4974f6dba200effad85faa47`: `docs/mkdocs/docs/technical/on_disk_storage.md`, `cpp/arcticdb/version/version_map_entry.hpp`, `cpp/arcticdb/pipeline/slicing.hpp`, and Python processing/library APIs.
- DuckDB `21aca0424f1faf78b593b1e6fbfdd4846624c987`: `src/include/duckdb/common/vector_size.hpp`, `src/include/duckdb/storage/storage_info.hpp`, optimizer zone-map paths, and `src/execution/physical_plan/plan_asof_join.cpp`.
- DataFusion `5de7f1db95191f81ce6472361785db8f63ac2db1` (workspace 54.1.0): `datafusion/catalog/src/table.rs`, `datafusion/datasource-parquet`, execution memory/disk management, aggregate first/last, and feature definitions.
- TimescaleDB `c3dc5b2546e77341e19153d585b7e690d6d8c7b4`: hypertable/chunk structures, `tsl/src/compression/README.md`, compression metadata builders, and `tsl/src/continuous_aggs/README.md`.

The local clones were under `/home/koukyosyumei/Ref`; the `../ref` path named in the prompt was not present in this checkout.
