# h5i-db — Design & Roadmap

> A high-performance, embedded, **versioned DataFrame / time-series database** for
> quantitative-finance-style workloads, written in Rust, with a UX designed so AI
> agents can drive it as an ordinary tool.
>
> This document records the concrete design decisions and roadmap. It is informed
> by a source-level study of four reference systems checked out under
> `~/Ref/`: **ArcticDB**, **DuckDB**, **Apache DataFusion** (v54.1, Arrow 59), and
> **TimescaleDB**. File references below point into those checkouts.

---

## 1. Positioning — what we are building and why

One sentence:

> **ArcticDB's storage model, redesigned, with DuckDB-class queries — as one
> embedded Rust library.**

| | DuckDB | ArcticDB | **h5i-db** |
|---|---|---|---|
| Primary API | SQL | Python / pandas | SQL **and** DataFrame, Rust/Python/CLI |
| Query engine | full optimizer/executor (~580k LOC C++) | limited clause pipeline (filter/project/groupby/resample/concat only) | DataFusion (reused, not rebuilt) |
| User-facing versioning | none on native storage (`AT (VERSION =>…)` is syntax-only, delegated to Iceberg/Delta extensions — `catalog.cpp:880`) | first-class (time travel, snapshots) | first-class, redesigned (no O(N) version-chain walk) |
| Unit of data | table in one DB file | library / symbol / version | database dir → table → version |
| Concurrency | MVCC, transient versions, GC'd after commit | hard single-writer-per-symbol assumption | optimistic CAS commit, explicit conflict error; staged parallel ingest later |
| Time-series ops | ASOF JOIN, time_bucket, IEJoin | date_range, resample, limited groupby | ASOF join exploiting sorted storage, time_bucket, resample, gapfill |

The gap in the market is real: DuckDB deliberately treats versions as ephemeral
(internal MVCC artifacts, garbage-collected at commit — `cleanup_state.cpp`),
and ArcticDB has a strong versioned-storage model but a weak query engine and a
fragile concurrency story. Lance proves the "versioned columnar format +
DataFusion" combination works; GreptimeDB proves "time-series engine on
DataFusion" works. Nobody combines **user-facing versioning + time-series query
power + embedded simplicity** in one Rust library.

### Decisions carried over from the discussion (settled, not revisited)

- **No MCP server in the core.** Agents drive the DB through the CLI / Python
  API / SQL like any other tool. If an MCP adapter is ever wanted (shell-less
  clients, centralized auth), it becomes a separate `h5i-db-mcp` package. The
  DB must never depend on it.
- **No agent/worktree/seat concepts inside the DB.** Isolation between agents =
  separate database directories, which is already natural for an embedded DB.
  h5i's design does not constrain this project.
- **"For AI agents" is a UX property, not a storage-model property**: headless
  CLI, machine-readable output and errors, resource limits, trivial install.
  (§8)
- **Fewer dependencies is better, but not at the price of rebuilding a SQL
  engine.** We take DataFusion (trimmed via feature flags) and Arrow/Parquet;
  we do not take a server, an ORM, or a framework.

---

## 2. What the reference systems taught us

### ArcticDB (what we inherit, and the five defects we fix)

ArcticDB is an immutable, content-addressed key tree over a blob store:
`VERSION_REF → VERSION (linked list) → TABLE_INDEX (slice map) → TABLE_DATA`
(`cpp/arcticdb/entity/key.hpp`). Every mutation writes new keys and swaps one
ref. Update/delete_range rewrite **only the boundary segments** intersecting
the affected time range and re-reference the rest (`version_core.cpp:
compute_update_ranges`). Tiling defaults: 100k rows × 127 columns per segment;
per-block LZ4; per-column min/max/unique stats. This copy-on-write core is
sound and we keep its spirit.

Defects we explicitly fix:

1. **O(N) version-chain walk.** Version history is a singly-linked list on
   storage; reading version N-k costs k sequential round-trips
   (`version_map.hpp:follow_version_chain`), papered over with ref-key caching
   and compaction at depth 5. → We store the version log as a flat,
   directly-addressable manifest log (§4): any version is O(1).
2. **Unsafe concurrent writes.** "We don't support parallel writes to the same
   symbol" is a hard assumption (`version_map_entry.hpp:239`); two writers can
   silently clobber the ref key. Their `StorageLock` is documented as
   non-atomic. → We make the head swap an **atomic compare-and-swap** and
   return an explicit conflict error (§5).
3. **No relational joins.** The clause pipeline has concat/merge only. → Full
   SQL joins come free with DataFusion; as-of join is our custom operator.
4. **Limited pushdown.** Time-range pruning plus an opt-in column-stats index;
   no bloom filters anywhere in the codebase. → Segment-level zone maps + bloom
   filters wired into DataFusion's pruning framework (§6).
5. **Fragmentation requires manual compaction discipline.** → Compaction is a
   first-class, policy-drivable operation from day one (§5).

Worth copying as-is: the staged-write pattern (parallel writers stage
independent segments, a single `finalize` publishes them — their `APPEND_REF`/
`compact_incomplete_impl` design), content-hash **dedup** of identical segments,
and snapshots as extra GC roots pinning a set of version manifests.

### DataFusion (what we get for free, verified in source at v54.1)

- **Pruning is a reusable, format-agnostic crate.** `PruningPredicate` +
  the `PruningStatistics` trait (`datafusion/pruning`,
  `common/src/pruning.rs:63`) work over *any* statistics source — we implement
  `PruningStatistics` over our version manifest and get
  min/max + bloom (`contained()`) segment pruning without touching Parquet
  internals.
- **`TableProvider`** gives projection, filter (`Exact`/`Inexact`), and limit
  pushdown (`catalog/src/table.rs`). Async `SchemaProvider::table()` means
  "resolve table@version from the manifest store" can do I/O naturally. Each
  table version = one immutable `TableProvider` instance.
- **Time travel needs no grammar hacks**: a table function (UDTF,
  `register_udtf`) supports `SELECT … FROM tbl('trades', version => 3)` /
  `(as_of => TIMESTAMP '…')` today; DuckDB-style `AT (VERSION => …)` syntax can
  come later via `RelationPlanner` (template:
  `datafusion-examples/relation_planner/table_sample.rs`).
- **Sorted-by-time data is rewarded**: ordered/streaming aggregation
  (`InputOrderMode::Sorted`) gives bounded-memory, incremental GROUP BY when
  input order matches group keys — exactly our `date_bin` rollup case.
- **The one real gap is the as-of join.** No temporal/asof operator exists
  in-tree (issue #318 open since 2021). `PiecewiseMergeJoinExec` covers single
  inequality range joins but not "latest row ≤ t per key". All extension seams
  exist (custom logical node + `ExecutionPlan` + `ExtensionPlanner`); this is a
  focused effort, not a research project.
- **Dependency weight is controllable**: default features can be dropped
  (`parquet`, `sql`, regex/crypto/unicode expression packs are all optional),
  and the granular crates can be used instead of the umbrella crate.

### DuckDB (design lessons, not code)

- ~582k LOC; parser+planner+optimizer alone ~163k. Confirms: **do not rebuild
  the SQL frontend** — that is exactly what DataFusion replaces.
- **ASOF join algorithm** (`physical_asof_join.cpp`): split conditions into
  equality partition keys + one inequality time key; partition, sort each side
  by time, then per left row **exponential search + binary search** on the
  sorted right side; encode sort keys as memcmp-able byte strings. We copy
  this — with one improvement DuckDB cannot make: our segments are already
  sorted by time within a partition, so the sort phase can often be skipped
  entirely.
- Storage substrate numbers worth adopting: ~120k-row row groups, 2048-row
  vectors, per-column-segment min/max zone maps, per-segment auto-chosen codec.
- Their MVCC core is small (~4k LOC): snapshot reads + optimistic write-write
  conflict detection + undo buffer. Reassurance that our much simpler
  table-level optimistic CAS is a legitimate starting point, and a blueprint if
  we ever need row-level concurrency.
- `time_bucket` semantics (origin 2000-01-03 for sub-month widths, month-based
  path for calendar widths) are a good spec to copy verbatim.

### TimescaleDB (time-series design patterns, minus Postgres baggage)

- **Chunk exclusion**: compute the exact set of matching time chunks from query
  predicates *before* scanning, via range exclusion over a chunk catalog
  (`hypertable_restrict_info.c`). In our design "chunk" = segment, and this is
  the manifest + PruningStatistics path. Retention = drop whole segments
  (their `drop_chunks`), O(#segments) not O(rows).
- **Type-directed codecs**: delta-of-delta for timestamps/ints, Gorilla for
  floats, dictionary for low-cardinality, RLE bitmaps for bools
  (`tsl/src/compression/`). Parquet gives us DELTA_BINARY_PACKED, dictionary,
  byte-stream-split + zstd/lz4 — close enough for v1; the Gorilla-class codecs
  are the benchmark-gated custom-format escape hatch (§10).
- **segment_by / order_by** as the two user-visible layout knobs (partition
  column + sort order within segments), with auto-default heuristics.
- **Sparse per-batch metadata** (min/max + bloom + first/last) as the scan
  accelerator — validates our manifest-statistics design.
- **Continuous aggregates**: store *partial* aggregate state per time bucket;
  answer queries as materialized-head UNION live-tail; track staleness with an
  invalidation log + threshold watermark. In a versioned store this gets
  simpler: a rollup records the version it materialized from, and the diff
  between that version and head is the invalidation. Deferred to Phase 5.
- Baggage to drop: chunks as real child tables, trigger/view machinery,
  background-worker processes — an embedded engine hooks its own write path
  and uses in-process async tasks.

---

## 3. Architecture

```
        Python (pyo3, zero-copy Arrow FFI)   CLI (h5i-db)      Rust API
                     └──────────────┬──────────────┴───────────────┘
                                    ▼
                      DataFusion  (SQL + DataFrame plans)
              custom bits: AsOfJoinExec, gapfill, table@version UDTF
                                    ▼
                    Versioned table layer  (h5i-db core)
        catalog · version manifests · commit protocol (CAS) · GC/vacuum
                                    ▼
                     Immutable columnar segments (Parquet)
                      sorted by time, zone maps + bloom
                                    ▼
                object_store:  local filesystem  |  S3  (| memory)
```

Crate layout (workspace):

| crate | contents | depends on |
|---|---|---|
| `h5i-db-core` | catalog, manifest format, commit protocol, segment writer/reader, statistics, GC, compaction | `arrow`, `parquet`, `object_store` (no DataFusion) |
| `h5i-db-query` | `TableProvider`/`PruningStatistics` impls, table@version UDTF, `AsOfJoinExec`, gapfill, session wrapper | `h5i-db-core`, `datafusion` (trimmed features) |
| `h5i-db-cli` | `h5i-db` binary | `h5i-db-query` |
| `h5i-db-python` | pyo3 bindings, pandas/polars interop | `h5i-db-query` |

`h5i-db-core` compiles without DataFusion — the storage layer stays
independently testable and the door stays open to swapping engines. All I/O
goes through the `object_store` crate so local-FS and S3 are the same code
path (with one backend-specific primitive: the atomic head swap, §5).

---

## 4. Data model and on-disk layout

```
Database (a directory / S3 prefix)
  └── Table
        ├── declared schema (Arrow), optional time-index column, segment_by / order_by
        └── Versions v0, v1, v2, …  (immutable, linear history)
              └── Manifest → list of segments (+ stats) constituting that version
```

```
mydb/
  CATALOG                          # table registry (name → table dir, schema, options)
  tables/trades/
    HEAD                           # tiny file: current version number + manifest hash  ← the ONLY mutable object
    manifests/
      000000000000.mf             # one manifest per version, addressed by version number → O(1) time travel
      000000000001.mf
    segments/
      <uuid>.parquet              # immutable; referenced by ≥1 manifests
    snapshots/
      eod-2026-07-18              # named pin → version number (extra GC root)
```

**Manifest** (one per version; the unit of commit):

- version number, parent version, created-at, committer note, user metadata
- schema (+ schema-evolution lineage)
- operation kind (write / append / update / delete_range / compact — for audit)
- segment list; per segment: path, row count, byte size, **per-column min/max,
  null count**, time range, sort order flag, content hash, optional bloom
  filter offsets
- table-level rollups: total rows, global time range

Design deltas vs ArcticDB, stated once: manifests are **directly addressed by
version number** (flat log, not a linked list) so `read(version=k)` is one
GET; the manifest embeds the slice map (their separate `TABLE_INDEX` layer
collapses into it); deletes need no tombstone machinery — a version's manifest
simply doesn't reference removed data, and `vacuum` deletes segments referenced
by no live manifest and no snapshot. Content-hash dedup (skip writing a
segment whose hash already exists) is kept.

Format notes: manifests are a compact self-describing binary (postcard or
flatbuffers — decide at implementation; JSON sidecar dump for debuggability).
When version count grows large, a periodic `MANIFEST_LOG` summary file (Delta
checkpoint-style) keeps `list_versions()` from listing thousands of objects —
Phase 5 concern, the format reserves room for it now.

**Types.** Arrow types throughout. First-class for finance: `Timestamp(ns, tz)`
as the time index, `Decimal128`, dictionary-encoded strings. Schema evolution
(add column, widen type) recorded in the manifest lineage; reads reconcile
old segments against the current schema with null backfill (ArcticDB's dynamic
schema, minus its column-hash-bucketing complexity).

---

## 5. Write path and concurrency

API surface (Rust shown; Python/CLI mirror it):

```rust
db.create_table("trades", schema, TableOptions { time_index: Some("ts"),
                                                 segment_by: vec!["symbol"],  // optional
                                                 target_segment_rows: 120_000 })?;
db.write("trades", batches)?;                 // replace → new version
db.append("trades", batches)?;                // append rows → new version
db.update("trades", range, batches)?;         // copy-on-write boundary segments (ArcticDB-style)
db.delete_range("trades", start, end)?;       // ditto, no new data
db.read("trades")                             // latest
db.read_at("trades", Version(42))             // time travel, O(1)
db.read_as_of("trades", ts)                   // resolve ts → version, then O(1)
db.snapshot("eod-2026-07-18", tables)?;       // named pin across tables
db.list_versions("trades")?; db.restore("trades", Version(42))?;   // restore = new head pointing at old manifest
db.compact("trades")?; db.vacuum("trades", keep)?;
```

**Commit protocol — multiple readers, single successful writer per table:**

1. Writer reads `HEAD` (version *n*), writes new segments (invisible until
   published), writes manifest *n+1*.
2. Writer atomically swaps `HEAD` from *n* to *n+1*:
   local FS → write-temp + `rename` with an O_EXCL lock file;
   S3 → conditional `If-Match`/`If-None-Match` PUT (the primitive ArcticDB's
   `ReliableStorageLock` uses; supported by `object_store::put_opts`).
3. CAS failure → **explicit `VersionConflict` error** (never last-writer-wins,
   never a silent clobber). The orphaned segments are cleaned by vacuum.
4. Client-level auto-retry: for pure **appends**, rebase is trivial (re-read
   head, re-point parent, re-CAS — segments need no rewrite) and is offered as
   `append_with_retry`. Overlapping updates always surface the conflict.

Readers never block and never see partial state: they resolve `HEAD` (or an
explicit version) once and read only immutable objects after that.

**Parallel ingest** (Phase 5): ArcticDB's staged-write pattern — N workers
`stage()` segments into a staging area concurrently (no version created), one
`finalize()` sorts/merges/dedups and publishes a single commit.

**Compaction** rewrites many small segments into target-size segments as a
normal commit (op kind `compact`, data-identical). Triggerable manually from
day one, policy-driven (small-segment-count threshold) later. **Retention** =
`delete_range` + `vacuum`; because segments are time-partitioned, expiring old
data drops whole segments (TimescaleDB's `drop_chunks` insight).

---

## 6. Query layer

**Engine**: DataFusion session preconfigured by `h5i-db-query`; both SQL and
the DataFrame API are exposed (they share plans, so feature parity is free).

**What we implement on top:**

1. `TableProvider` per (table, version): `Exact` filter pushdown for
   time-range and segment_by-column predicates; projection/limit pushdown;
   `statistics()` from the manifest.
2. `PruningStatistics` over manifest stats → segment pruning via
   `PruningPredicate` before any I/O (min/max now, `contained()`/bloom in
   Phase 3). Time-range queries touch only overlapping segments — the
   TimescaleDB chunk-exclusion behavior.
3. **Table functions for time travel**: `tbl('trades')`,
   `tbl('trades', version => 42)`, `tbl('trades', as_of => TIMESTAMP '…')`.
   `AT (VERSION => …)` sugar later.
4. **`AsOfJoinExec`** — our flagship operator (Phase 4): DuckDB's algorithm
   (partition by keys, order by time, exponential+binary search probe,
   memcmp-able sort keys), plus the optimization DuckDB structurally can't
   have: when scan order is already (partition, time) — which our storage
   guarantees via segment_by/order_by — skip the sort and stream the merge.
   Exposed in SQL (`ASOF JOIN … ON trades.sym = quotes.sym AND
   trades.ts >= quotes.ts`) and DataFrame (`join_asof`).
5. **Time-series functions**: `date_bin`/`date_trunc` are already in
   DataFusion; add `time_bucket` (DuckDB/Timescale semantics incl. calendar
   months + origin/offset), `first`/`last` bookend aggregates (mergeable,
   TimescaleDB `agg_bookend.c` spec), OHLC as a convenience over them; gapfill
   + `locf()` + `interpolate()` as a post-aggregation operator (Timescale's
   gapfill-node design) in Phase 4.
6. Rolling/window: DataFusion window functions already cover
   `avg OVER (PARTITION BY sym ORDER BY ts RANGE INTERVAL …)`; we add sugar
   (`rolling(mean, '30m')`) in the DataFrame/Python API, not a new engine.

Sorted-by-time storage + `InputOrderMode::Sorted` gives streaming GROUP BY
time-bucket without hashing; the `TableProvider` declares output ordering so
the optimizer can use it.

**Dependency trimming**: disable DataFusion default features we don't need
(crypto/encoding/unicode expression packs, avro); keep `parquet`, `sql`,
datetime, regex. Revisit umbrella-vs-granular crates once the build exists.

---

## 7. Optimization strategy — how far to customize DataFusion

DataFusion yes, "as it is" no. The optimizer work that pays off for this
workload is mostly *not* a classical query optimizer: quant queries are
shallow (scan → filter → bucket → aggregate, occasionally one as-of join), so
there is no deep join-order search space, and DataFusion is already in
DuckDB's ballpark on scan/aggregation benchmarks (ClickBench). The leverage
is elsewhere. Three tiers, in strictly decreasing return-on-effort:

**Tier 1 — optimize below the engine (mandatory; Phase 2).**
For time-series, the decisive factor is how little data gets read, not how
cleverly the plan is rearranged.

- Segment pruning from manifest stats + exact filter/projection/limit
  pushdown (§6) — a narrow time-range query over years of data must touch
  only overlapping segments. No plan optimization competes with reading 1%
  of the data.
- **Metadata-only answers**: `COUNT(*)`, `MIN/MAX(ts)` served from the
  manifest with zero scan. DataFusion's `AggregateStatistics` physical rule
  performs this rewrite for free *if* our `TableProvider.statistics()`
  reports honest exact statistics — a requirement on the provider, not new
  optimizer code.
- **Decoded-batch cache**: segments are immutable and content-addressed, so
  caching decoded Arrow batches keyed by segment hash is trivially correct.
  An embedded DB driven by an agent loop (describe → sample → query →
  refine, repeatedly over the same table) re-reads the same segments
  constantly; this cache likely buys more wall-clock than any optimizer
  rule. LRU with a byte budget wired into `--memory-limit`.

**Tier 2 — targeted custom rules injecting domain knowledge (planned;
Phases 2 & 4).** DataFusion's rule-based optimizer is general-purpose and
does not know our data is stored sorted by `(segment_by, time)`. A small set
of custom `PhysicalOptimizerRule`s closes the gap — the same seam InfluxDB
3.0 (IOx) and GreptimeDB spend their custom-rule budget on, which is good
precedent that this is where domain engines win:

- **Order preservation** (Phase 2): declare output ordering from the
  `TableProvider`; ensure plans keep it — `SortPreservingMerge` across
  segments, streaming (`InputOrderMode::Sorted`) aggregation for
  `time_bucket` rollups, and no order-destroying round-robin repartition on
  time-ordered scans. Streaming through a year of ticks in bounded memory
  vs hash-aggregating it is the single biggest plan-level difference.
- **Quant-idiom rewrites** (Phase 4): "latest row per key" (window top-1 /
  `MAX(ts)` self-join patterns) → manifest-guided reverse scan that stops at
  the first hit per key; recognizable rollup patterns → sorted-input
  aggregation. Each rule is a few hundred lines against stable extension
  APIs.
- **Custom operators where DataFusion has none** (Phase 4): `AsOfJoinExec`
  and gapfill (§6). Joins are DataFusion's weakest area relative to DuckDB;
  our flagship join being custom sidesteps their weakest component rather
  than inheriting it.

**Tier 3 — replacing DataFusion internals (don't, without a benchmark that
forces it).** Own aggregation executor, own vectorized kernels, a
cost-based optimizer, or a fork: this is where maintenance cost explodes —
DataFusion moves fast and forked internals turn every upgrade into a merge
project. A CBO in particular buys almost nothing on shallow plans. The
precedent systems all stopped at Tier 2; so do we. Any Tier-3 proposal needs
a written benchmark case that Tiers 1–2 demonstrably cannot meet (same bar
as the custom file format, §10).

---

## 8. Agent-facing UX (the actual "for AI agents" layer)

No MCP, no protocol — a disciplined CLI and Python API that any shell-capable
agent (Claude Code, Codex, scripts, CI) can drive.

```bash
h5i-db query  market.db "SELECT symbol, avg(price) FROM trades GROUP BY symbol" \
              --format json --max-rows 1000 --timeout 30s --read-only
h5i-db tables market.db                      # list tables + row counts + time ranges
h5i-db schema market.db trades               # Arrow schema as JSON
h5i-db sample market.db trades -n 20         # peek rows
h5i-db versions market.db trades             # version log with op kinds + notes
h5i-db ingest market.db trades data.parquet --mode append   # also csv, arrow IPC on stdin
h5i-db snapshot / restore / compact / vacuum …
```

Contract, enforced from the first release:

- **Output**: `--format table|json|jsonl|csv|arrow` (json = `{schema, rows,
  stats}` envelope; arrow = IPC stream on stdout for lossless piping).
- **Errors**: machine-readable on stderr — `{code, message, hint}`; stable
  exit codes (0 ok / 2 user error / 3 conflict / 4 limit exceeded / 5 internal).
  Error messages state the fix ("version 42 not found; latest is 57;
  run `h5i-db versions …`") because the consumer replans from the text.
- **Limits as flags**: `--max-rows`, `--max-bytes`, `--timeout`,
  `--memory-limit`, `--read-only`. An agent supervisor can hard-cap any call.
- **Non-interactive always**: no prompts, no pager, no TTY assumptions; SQL
  via arg or stdin.
- **Introspection is first-class** (`tables`/`schema`/`sample`/`describe`)
  because schema discovery is the first thing every agent does.
- **Distribution**: single static binary + `pip install h5i-db`.
- A `SKILL.md` teaching agents the CLI (schema discovery → query → limits →
  error handling) ships in-repo; that, not a protocol server, is the
  integration story. Multi-agent isolation = one database directory per agent;
  nothing in the engine knows what an agent is.

---

## 9. Roadmap

Phases are cumulative; each ends with something demonstrable. Rough sizing
assumes one focused developer + AI agents; phases 0–2 are the proof of value.

**Phase 0 — Walking skeleton.**
Workspace scaffolding; local-FS `object_store`; CATALOG + HEAD + manifest v0
(write/read); Parquet segment writer with per-column min/max collected into
the manifest; `create_table`/`write`/`append`/`read`; naive `TableProvider`
(no pruning); CLI `query/tables/schema/ingest` with `--format json`.
*Exit: `ingest` a CSV, `query` it via SQL, from a fresh clone in one command.*

**Phase 1 — Versioning correct end-to-end.**
Atomic CAS commit + `VersionConflict` + append rebase-retry; `read_at`/
`read_as_of`; `update`/`delete_range` with boundary-segment copy-on-write;
snapshots; `restore`; `list_versions`; `compact`; `vacuum` (snapshot-aware);
content-hash dedup; crash-safety property tests (kill mid-commit → old head
intact, orphans vacuumable). Concurrency stress test: N writers × M readers.
*Exit: the versioning semantics ArcticDB has, without its footguns.*

**Phase 2 — Query performance.**
`PruningStatistics` over manifests + `Exact` pushdown (benchmark: narrow
time-range query over years of data touches only overlapping segments);
declared output ordering → order-preservation rules + streaming ordered
aggregation (§7 Tier 2); metadata-only `COUNT`/`MIN`/`MAX` via exact
provider statistics; decoded-batch cache keyed by segment hash (§7 Tier 1);
`time_bucket` + `first`/`last` UDFs; DataFusion feature-flag trim; first public benchmark
vs DuckDB-over-Parquet and ArcticDB (ingest rate, time-range scan, bucketed
aggregation, cold version read).
*Exit: pruning + ordered aggregation demonstrably working; honest numbers.*

**Phase 3 — Python.**
pyo3 bindings, zero-copy Arrow FFI; pandas/polars/pyarrow in-out;
`db.sql()` → DataFrame; wheels on PyPI; optional per-segment bloom filters
for high-cardinality equality predicates (`contained()` hook).
*Exit: `pip install h5i-db; db.sql("…").to_pandas()` in a notebook.*

**Phase 4 — Time-series operators (the differentiator).**
`AsOfJoinExec` (sorted-merge fast path + fallback sort path; SQL `ASOF JOIN`
via RelationPlanner + DataFrame `join_asof`); quant-idiom rewrite rules
(latest-row-per-key → manifest-guided reverse scan, §7 Tier 2); `resample`
sugar; gapfill + locf/interpolate operator; OHLC helper; benchmark as-of
join vs DuckDB and pandas `merge_asof`.
*Exit: quote/trade as-of join + resample pipeline, faster than pandas,
competitive with DuckDB, with time travel underneath.*

**Phase 5 — Scale-out of storage (not compute).**
S3 backend via conditional-PUT CAS + real-S3 integration tests; staged
parallel ingest (`stage`/`finalize`); manifest-log checkpoint for
1000s-of-versions tables; background policies (compaction, retention) as
library calls + CLI cron verbs, no daemon; optional versioned continuous
aggregates (partial-state rollups whose invalidation = version diff — the
clean reformulation of Timescale's cagg design).
*Exit: shared S3 database, many concurrent staging writers, bounded metadata.*

**Explicit non-goals (revisit only with evidence):** MCP server (separate
package if ever), server/daemon mode, distributed query, multi-master writes,
row-level MVCC transactions, vector search, RBAC, custom SQL dialect beyond
listed extensions, ArcticDB API compatibility, custom columnar file format
(see §10), agent/worktree/memory concepts in the engine.

---

## 10. Risks and pre-committed fallbacks

- **Parquet may underperform on tiny time-range reads or ultra-wide tables**
  (ArcticDB tiles columns at 127 per segment for a reason). *Mitigation:*
  benchmark first (Phase 2); if real, add column-group tiling *within the
  manifest* (segment = row-range × column-group, still Parquet) before ever
  designing a custom format. A bespoke format is the last resort and gated on
  a written benchmark case Parquet cannot meet (the Gorilla/delta-delta
  codecs from Timescale/DuckDB are the reference designs if we get there).
- **DataFusion API churn** (fast-moving project). *Mitigation:* pin the
  version; confine DataFusion types to `h5i-db-query`; `h5i-db-core` stays
  engine-free.
- **S3 CAS semantics vary across S3-compatible stores.** *Mitigation:*
  capability probe at open; refuse multi-writer mode (single-writer still
  fine) on stores without conditional PUT, loudly.
- **As-of join correctness edge cases** (ties, NULLs, strict vs non-strict,
  backward vs forward). *Mitigation:* adopt DuckDB's semantics and port its
  test corpus (`benchmark/micro/join/asof_join*`, plus its SQL tests) before
  optimizing.
- **Scope creep toward "agent platform".** *Mitigation:* §9 non-goals list;
  anything agent-flavored must land in CLI/docs/skill, never in the engine.

---

## 11. Open questions (fine to defer, recorded so they aren't lost)

1. Manifest encoding: postcard vs flatbuffers (decide in Phase 0 with a
   micro-benchmark; both keep the JSON debug dump).
2. Multi-table atomic snapshots are in (cheap: one snapshot file pinning many
   table versions); multi-table atomic *commits* are not — is that ever needed
   for the finance use case? Revisit after Phase 1 usage.
3. Version history is **linear** by design (branch = copy the database dir or
   `restore` to fork forward). If real demand for branching appears, the
   manifest's parent pointer already supports a DAG — but branching a DB like
   git is exactly the "worktree in a DB" idea we decided smells wrong, so the
   bar is high.
4. Name of the CLI binary: `h5i-db` (current) vs something shorter.
