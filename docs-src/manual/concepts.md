---
title: Core concepts
description: Versions, segments, manifests, snapshots, previewable mutation plans, and the mutation policy — the mental model behind h5i-db.
order: 3
---

# Core concepts

h5i-db has a small set of load-bearing ideas. Once they click, every command
and API method is predictable.

## The database is a directory

A database is one directory on disk; there is no server. Inside it, each table
owns immutable Parquet **segments** (the data), immutable JSON **manifests**
(one per version), and a single small mutable file, `HEAD`, that names the
current version:

```text
market.db/
  FORMAT
  catalog/  snapshots/
  tables/<table-uuid>/
    HEAD                       # the only mutable file per table
    manifests/<seq>.json       # one immutable manifest per version
    segments/<uuid>.parquet    # immutable, time-sorted data
```

Everything except `HEAD` is write-once. That single fact is what makes
backups a file copy, crash recovery trivial, and old versions permanently
readable — see the [Operations guide](operations.html).

## Versions: every write is a commit

Every write — append, full write, range replace/delete, restore, compact —
produces a new **version**: a manifest listing exactly which segments make up
the table at that point, plus statistics, a commit timestamp, and your
optional `--note`. Publishing a version is an atomic compare-and-swap on
`HEAD`:

- **Readers never block** and never see partial writes; a reader holds a
  manifest, and everything a manifest references is immutable.
- **Racing writers never interleave.** The loser of the swap gets an explicit
  `version_conflict` error (retryable). Pass `--expected-version` /
  `expected_version=` to demand the head hasn't moved — optimistic locking.
- **History is a hash chain.** Each manifest records its parent's blake3
  checksum; `verify` walks the chain.

Reading old versions is O(1) — a version *is* a manifest read:

```sql
SELECT * FROM h5i('trades', 42);                        -- exact version
SELECT * FROM h5i('trades', '2026-07-01T00:00:00Z');    -- as of commit time
```

`restore` makes an old version current by committing a *new* version with the
old contents — history only moves forward, nothing is erased.

## The time axis

Declaring `--time-column` at table creation is the single highest-leverage
schema decision:

- Storage is **sorted by the time column** (plus any additional `--sort-key`
  columns). Appends must respect that order — out-of-order appends are
  rejected, which is what keeps bucketed aggregations streaming instead of
  sorting.
- Each segment's manifest entry records its **time range and column min/max**.
  A query for a narrow time window prunes non-overlapping segments *before any
  I/O* — run `query --stats` to watch it happen.
- The time-series operators (`asof_join`, `gapfill`, `tail`, time-range plans)
  are keyed off the declared time column.

!!! warning "Raw units"
    APIs that take numeric time arguments — plan ranges, `gapfill` step, ASOF
    tolerance, `read(time_start=…)` — use **raw integers in the time column's
    unit**. For the common `timestamp[us]` column, that is microseconds:
    `60_000_000` is one minute. The CLI's `--start`/`--end` accept RFC3339
    strings and convert for you.

## Snapshots

A **snapshot** pins the current version of one or more tables under a name:

```console
$ h5i-db snapshot create market.db eod-2026-07-18
```

```sql
SELECT * FROM h5i('trades', 'eod-2026-07-18');
```

A snapshot is a tiny checksummed JSON map `{table → version}` — O(1) to
create, free to keep. Snapshots make backtests reproducible ("run against
`eod-2026-07-18`, forever") and answer audit questions ("what did we know at
close on date X?"). A table pinned by a snapshot cannot be dropped.

## Previewable mutations: plan / apply

Destructive operations (`delete-range`, `replace-range`) can run in two modes:

- **Direct**: commit immediately, like any write.
- **Planned** (`--plan` / `plan_delete_range()`): run the *full* write path —
  affected rows computed, new segments staged on disk — but stop before
  publishing. You get a plan id, a machine-readable summary (rows affected,
  segments rewritten vs. reused), and before/after row samples.

```console
$ h5i-db delete-range market.db trades --start … --end … --plan
$ h5i-db plan show market.db trades <plan-id>
$ h5i-db plan apply market.db trades <plan-id>     # metadata-only swap
```

`apply` is cheap and atomic; it fails with a conflict if the table head moved
after the plan was made (re-plan, don't retry). `discard` drops the staged
segments; abandoned plans expire after 7 days. Every manifest records whether
it was committed directly or via a reviewed plan (`execution_mode` and the
plan hash) — an audit trail, not just a safety net.

## The mutation policy

The **policy** is a per-database set of boolean gates deciding which
operations may commit *without* a reviewed plan:

```console
$ h5i-db policy set market.db direct_delete=false direct_write=false
```

Flags: `direct_append`, `direct_write`, `direct_replace`, `direct_delete`,
`direct_restore`, `direct_compact`. With a flag off, the direct form of that
operation fails with a `policy_violation`; the plan/apply flow still works.
This is the recommended guardrail when [agents](agents.html) or shared
pipelines write to the database: the write path is identical, only the
review requirement changes.

## Queries and sessions

SQL runs on DataFusion with h5i-db's storage underneath:

- **Plain table names** (`FROM trades`) are snapshot-bound when the session
  opens — every query in a session sees a consistent set of versions.
- **`h5i('trades')`** re-resolves to the latest version at each query.
- Segment pruning, projection pushdown, and (for unchanged versions) cached
  per-segment aggregate states make repeat analytics fast.

Resource guards are first-class: row caps, timeouts, and memory budgets with
disk spilling are available as CLI flags and `sql()` keyword arguments — they
raise clean, typed errors instead of truncating silently.

## Maintenance in one paragraph

`verify` re-checks structural integrity (checksums, object existence;
`--deep` re-reads every segment). `compact` rewrites accumulations of small
segments into target-sized ones — a query-speed tool, not a space reclaimer.
`vacuum` deletes *unreachable* objects (crashed-writer debris, discarded
plans) — dry run by default. Committed history is never deleted. Cadence and
disk-usage math: [Operations guide](operations.html).

## Read more

- [SQL reference](sql.html) — the full function library with signatures.
- [CLI reference](cli.html) — every command and flag.
- Cookbook deep dives:
  [time travel](../cookbook/00_fundamentals/05_time_travel_and_versioning.html),
  [previewable mutations](../cookbook/00_fundamentals/06_previewable_mutations.html),
  [maintenance](../cookbook/00_fundamentals/08_maintenance.html).
