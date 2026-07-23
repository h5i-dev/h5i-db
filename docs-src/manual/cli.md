---
title: CLI reference
description: Every command, flag, output format, and exit code of the h5i-db command-line tool.
order: 4
---

# CLI reference

```console
$ h5i-db <command> <db> [args…] [--format table|json|jsonl|csv|arrow]
```

The `h5i-db` binary is non-interactive by design: no prompts, no pager, SQL
from an argument or stdin, results on stdout, diagnostics on stderr. The
database path is always the first positional argument of a command (there is
no global `--db` flag).

## Global behavior

### Output formats

`--format` is global and defaults to `table`.

| Format | Behavior |
|---|---|
| `table` | Human-readable aligned columns (buffered; metadata commands render pretty JSON) |
| `json` | One JSON array of row objects; explicit nulls; empty result is `[]` |
| `jsonl` | One compact JSON object per row per line |
| `csv` | With header row; empty result still emits the header |
| `arrow` | Arrow IPC stream on stdout — lossless, pipe into other tools |

### Errors and exit codes

Errors are a single JSON envelope on **stderr**:

```json
{"code": "version_conflict", "message": "…", "retryable": true, "hint": "…"}
```

Exit codes are stable and branchable:

| Code | Meaning |
|---|---|
| `0` | Success (including broken pipe from `… \| head`) |
| `2` | User error — bad arguments, bad SQL, missing table |
| `3` | Conflict — another writer moved the head; usually retryable |
| `4` | Limit exceeded — `--max-rows`, `--max-bytes`, memory budget, timeout |
| `5` | Internal error |

Diagnostics volume is controlled with `RUST_LOG` (default `warn`).

### Shared write flags

Commands that commit a version (`ingest`, `restore`, `replace-range`,
`delete-range`, `compact`) accept:

| Flag | Meaning |
|---|---|
| `--expected-version <N>` | Require the table head to be exactly version N (optimistic guard); mismatch exits 3 |
| `--note <text>` | Free-text note recorded in the version manifest |

---

## Database & tables

### `h5i-db init`

Create a new database directory.

```console
$ h5i-db init market.db
```

### `h5i-db create-table`

Create a table. The schema comes from `--schema` JSON **or** `--like` a data
file (exactly one required).

```console
$ h5i-db create-table market.db trades --like ticks.parquet --time-column ts
$ h5i-db create-table market.db bars \
    --schema '[{"name":"ts","type":"timestamp_us","nullable":false},
               {"name":"symbol","type":"utf8"},{"name":"close","type":"float64"}]' \
    --time-column ts --sort-key ts,symbol
```

| Flag | Meaning |
|---|---|
| `--schema <json>` | Explicit schema. Types: `int8..int64`, `uint8..uint64`, `float32/float64`, `utf8`, `bool`, `timestamp_s/ms/us/ns` (UTC), `date32`, `date64`. Aliases accepted: `int`→int32, `long`/`bigint`→int64, `float`→float32, `double`→float64, `string`/`str`/`text`→utf8, `boolean`→bool, `date`→date32, `timestamp`→timestamp_ns. `nullable` defaults to `true`. |
| `--like <file>` | Infer the schema from a Parquet/CSV/Arrow file |
| `--time-column <col>` | Time index column — strongly recommended for time-series tables; forced non-nullable |
| `--sort-key <cols>` | Comma-separated sort key (defaults to the time column) |
| `--target-segment-mb <N>` | Target segment size in MiB of in-memory data (default 128) |

### `h5i-db tables`

List tables with row counts and time ranges. Columns: `table`, `version`,
`rows`, `bytes`, `segments`, `time_range`, `time_column`.

### `h5i-db schema`

Show a table's schema and options (`schema_revision`, `time_column`,
`sort_key`, fields with types and nullability).

### `h5i-db sample`

Show the first rows of a table.

| Flag | Meaning |
|---|---|
| `-n, --rows <N>` | Row count (default 10) |
| `--version <N>` | Read at a specific version |

### `h5i-db rename`

Rename a table — a catalog edit, no data moves.

```console
$ h5i-db rename market.db trades trades_raw
```

### `h5i-db drop-table`

Drop a table and its data. Refuses if the table is pinned by a snapshot, and
requires `--yes`:

```console
$ h5i-db drop-table market.db scratch --yes
```

!!! danger "Irreversible"
    `drop-table` permanently deletes data — it is the one command that
    bypasses versioning. Snapshots protect tables from it; use them.

---

## Reading & querying

### `h5i-db query`

Run SQL. The query comes from the argument, or stdin when `-`.

```console
$ h5i-db query market.db "SELECT symbol, vwap(price, size) FROM trades GROUP BY symbol" \
    --format json --max-rows 1000 --timeout 30s
$ cat report.sql | h5i-db query market.db - --format csv > out.csv
```

| Flag | Meaning |
|---|---|
| `--max-rows <N>` | Abort after N produced rows |
| `--max-bytes <N>` | Stop after N output bytes (checked at batch boundaries); truncation exits 4 with a `limit_exceeded` envelope |
| `--timeout <dur>` | Query timeout, e.g. `30s`, `5m` |
| `--memory-limit-mb <N>` | Memory budget in MiB; enables disk spilling under pressure |
| `--spill-dir <path>` | Spill directory (with `--memory-limit-mb`) |
| `--threads <N>` | Number of threads / partitions |
| `--stats` | Print scan/pruning statistics to stderr after the query |
| `--predicate-cache` | Read and build immutable predicate-cache sidecars |

See the [SQL reference](sql.html) for `h5i()` time travel, `asof_join`, and
the time-series function library.

### `h5i-db versions`

List a table's committed versions: `version`, `op`, `committed_at`, `rows`,
`bytes`, `segments`, `note`.

---

## Writing data

### `h5i-db ingest`

Ingest Parquet/CSV/Arrow into a table — from a file, or stdin with `-`.

```console
$ h5i-db ingest market.db trades ticks.parquet
$ curl -s https://…/ticks.csv | h5i-db ingest market.db trades - --input-format csv
```

| Flag | Meaning |
|---|---|
| `--input-format <fmt>` | `auto` (default) \| `parquet` \| `csv` \| `arrow`. Auto uses the file extension, or sniffs leading bytes on stdin |
| `--mode <mode>` | `append` (default) — strict ordered append; `write` — replace table contents |
| `--retries <N>` | Retry appends on version conflicts (default 5; safe for pure appends) |

Plus the [shared write flags](#shared-write-flags). Input batches are
schema-aligned against the table (purely representational casts like
timezone-less CSV timestamps are applied automatically). CSV assumes a header
row.

!!! note "Arrow over stdin"
    Pipe an Arrow IPC **stream**, not an IPC *file* — a file's random-access
    footer can't be consumed from a pipe. h5i-db detects the difference and
    tells you.

### `h5i-db restore`

Make a historical version current. History moves forward — restore commits a
*new* version with the old contents.

```console
$ h5i-db restore market.db trades 42 --note "roll back bad load"
```

### `h5i-db replace-range`

Replace all rows in `[start, end)` of the time column with the given input.

```console
$ h5i-db replace-range market.db trades \
    --start 2026-07-01T09:30:00Z --end 2026-07-01T16:00:00Z \
    --input corrected.parquet --plan
```

| Flag | Meaning |
|---|---|
| `--start <t>` | Range start, **inclusive** — RFC3339, or a raw integer in the column's unit |
| `--end <t>` | Range end, **exclusive** |
| `--input <file>` | Replacement data (or `-` for stdin); **omit to delete the range** |
| `--input-format <fmt>` | As in `ingest` |
| `--plan` | Stage a previewable plan instead of committing immediately |

### `h5i-db delete-range`

Delete all rows in `[start, end)` — shorthand for `replace-range` with no
input. Same `--start`/`--end`/`--plan` flags.

```console
$ h5i-db delete-range market.db trades --start 09:30… --end 09:31… --plan
{"plan_id": "5c41…", "summary": {"rows_affected": 12481, "segments_reused": 127}}
```

---

## Plans, policy & snapshots

### `h5i-db plan`

Manage previewable-mutation plans (created by `--plan` above).

| Subcommand | Meaning |
|---|---|
| `plan list <db> <table>` | Pending plans: `plan_id`, `op`, `base_version`, `created_at`, `expired`, summary |
| `plan show <db> <table> <plan-id>` | Full plan JSON on stdout; before/after row samples on stderr |
| `plan apply <db> <table> <plan-id>` | Publish the plan — fails with a conflict if the head moved |
| `plan discard <db> <table> <plan-id>` | Drop the plan; staged segments become vacuumable |

Plans expire after 7 days; see
[plan hygiene](operations.html#mutation-plan-hygiene).

### `h5i-db policy`

The mutation policy decides which operations may commit **without** a
reviewed plan.

```console
$ h5i-db policy show market.db
$ h5i-db policy set market.db direct_delete=false direct_write=false
```

Keys: `direct_append`, `direct_write`, `direct_replace`, `direct_delete`,
`direct_restore`, `direct_compact` — each `true`/`false`. The update is an
atomic read-modify-write.

### `h5i-db snapshot`

Pin table versions under a name.

| Subcommand | Meaning |
|---|---|
| `snapshot create <db> <name> [tables…]` | Pin current versions (all tables when omitted); `--note` supported |
| `snapshot list <db>` | List snapshots |
| `snapshot delete <db> <name>` | Delete a snapshot (the versions it pinned remain readable by number) |

---

## Maintenance & tools

### `h5i-db compact`

Rewrite small segments into target-sized ones, as a new version. Row count is
verified preserved.

| Flag | Meaning |
|---|---|
| `--target-mb <N>` | Override the target segment size (MiB of in-memory data) |

### `h5i-db vacuum`

Remove unreachable objects — dry run unless `--apply`.

```console
$ h5i-db vacuum market.db                    # inspect candidates
$ h5i-db vacuum market.db --apply            # actually delete
```

| Flag | Meaning |
|---|---|
| `[table]` | Restrict to one table (optional positional) |
| `--grace-seconds <N>` | Never touch objects newer than this (default 3600) |
| `--apply` | Actually delete |

Read [the cadence guidance](operations.html#vacuum) before scripting this.

### `h5i-db verify`

Check structural integrity: checksums, hash chain, object existence.

```console
$ h5i-db verify market.db trades --deep
```

`--deep` additionally re-reads every segment and verifies content checksums.
Problems are reported in the chosen format and the command exits non-zero.

### `h5i-db ui`

Launch the local review UI — loopback only.

| Flag | Meaning |
|---|---|
| `--port <N>` | Port (default 7351) |
| `--allow-mutations` | Enable plan apply/discard from the UI (default: read-only) |

The UI shows pending plans with previews, the version timeline with audit
badges, version diffs, and an SQL scratchpad that reports pruning per query.
