---
title: Database
description: "h5i_db.Database reference: lifecycle, tables, writing, reading and SQL, time travel, mutation plans, policy, and maintenance."
order: 1
---

# `Database`

An h5i-db database directory — the top-level handle everything hangs off. A
database is a plain directory on disk; there is no server. The handle is a
context manager, so the idiomatic form is:

```python
import h5i_db

with h5i_db.Database("market.db", create=True) as db:
    ...
```

Many methods return plain `dict`s decoded from the engine — commit results
carry keys like `version`, `rows`, `bytes`, `segments`; they are made to be
logged.

## Constructor

### `h5i_db.Database`

```python
Database(path, create=False, read_only=False)
```

Open (or create) a database directory.

**Parameters**

`path` (`str`)
:   Filesystem path to the database directory.

`create` (`bool`, default `False`)
:   Open-or-create — create the directory if it does not exist.

`read_only` (`bool`, default `False`)
:   Reject every write at the handle level; write calls raise
    [`PolicyError`](exceptions.html).

**Raises**

`NotFoundError`
:   The directory does not exist and `create` is `False`.

## Lifecycle

### `Database.close`

```python
close() -> None
```

Release the native handle. Idempotent, and also called by `__exit__`.
In-flight operations on other threads finish normally; later calls on this
object raise `H5iError` with `code == "closed"`.

### `Database.closed`

```python
closed -> bool
```

Property — whether the handle has been closed.

### `Database.path`

```python
path -> str
```

The directory this handle was opened on.

## Tables

### `Database.create_table`

```python
create_table(name, schema, time_column=None, sort_key=None) -> dict
```

Create a table from an Arrow schema.

**Parameters**

`name` (`str`)
:   Table name, unique within the database.

`schema` (`pyarrow.Schema`)
:   The Arrow schema. Field order is preserved.

`time_column` (`str`, optional)
:   The time-axis column — strongly recommended for time-series tables. It
    enables segment pruning, ASOF joins, range plans, and `tail`, and is
    forced non-nullable.

`sort_key` (`Iterable[str]`, optional)
:   Columns the table is sorted by on disk. Defaults to `[time_column]`.

**Returns**

`dict` — creation metadata (table id, schema revision).

```python
db.create_table("trades", schema, time_column="ts", sort_key=["ts", "symbol"])
```

### `Database.tables`

```python
tables() -> list[str]
```

Names of all tables in the database.

### `Database.schema`

```python
schema(name, version=None, as_of=None, snapshot=None) -> pyarrow.Schema
```

Schema of a table at a read point (latest by default).

**Parameters**

`name` (`str`)
:   Table name.

`version` (`int`, optional)
:   Read the schema as of this exact version.

`as_of` (`str`, optional)
:   RFC3339 timestamp — the schema as of the latest commit at or before it.

`snapshot` (`str`, optional)
:   Named snapshot to resolve the version from.

!!! note "One read point"
    Pass at most one of `version` / `as_of` / `snapshot`; more than one raises
    [`InvalidInputError`](exceptions.html). This rule holds for every
    read-point method below.

### `Database.versions`

```python
versions(name) -> list[dict]
```

Committed versions, oldest first — one dict per version with the version
number, operation, commit time, and row / byte / segment counts, plus any
`note`.

### `Database.drop_table`

```python
drop_table(name) -> None
```

Permanently drop the table and its data.

**Raises**

`ConflictError`
:   A snapshot pins the table — delete the snapshot first.

## Writing

Every write is one atomic, durable commit that produces a new version.

### `Database.append`

```python
append(name, data, *, expected_version=None, note=None) -> dict
```

Strict ordered append.

**Parameters**

`name` (`str`)
:   Table name.

`data` (`TableLike`)
:   A `pyarrow.Table`, `RecordBatch`, or sequence of batches. Rows must
    respect the table's sort order.

`expected_version` (`int`, optional)
:   Optimistic guard — commit only if the head is exactly this version, else
    [`ConflictError`](exceptions.html). Use it when the append depends on
    what you last read.

`note` (`str`, optional)
:   Free-text note recorded in the version manifest.

**Returns**

`dict` — commit metadata (`version`, `rows`, `bytes`, `segments`).

**Raises**

`InvalidInputError`
:   `sort_order_violation` if rows are out of order, or `schema_mismatch`.

`ConflictError`
:   Another writer moved the head; retryable. Pure appends are retried
    internally (up to 5 times) before this surfaces.

### `Database.write`

```python
write(name, data, *, expected_version=None, note=None) -> dict
```

Replace the table's contents in one commit — a restatement, not an
overwrite: the previous state stays readable as its version. Parameters match
[`append`](#databaseappend).

### `Database.restore`

```python
restore(name, version) -> dict
```

Make a historical version current by committing a new version with its
contents. History only moves forward — nothing is erased.

**Parameters**

`name` (`str`)
:   Table name.

`version` (`int`)
:   The version to restore.

## Reading & SQL

### `Database.sql`

```python
sql(query, memory_limit=None, timeout=None, max_rows=None) -> QueryResult
```

Run SQL — full DataFusion plus the [h5i extensions](../manual/sql.html).
Returns a [`QueryResult`](results-and-plans.html#queryresult).

**Parameters**

`query` (`str`)
:   The SQL text.

`memory_limit` (`int`, optional)
:   Query memory budget in **bytes**; enables disk spilling under pressure.

`timeout` (`float`, optional)
:   Deadline in seconds. On expiry, raises
    [`TimeoutError`](exceptions.html) and cancels execution.

`max_rows` (`int`, optional)
:   Raise [`LimitError`](exceptions.html) as soon as the result exceeds this —
    execution stops early rather than truncating silently.

**Returns**

`QueryResult` — with `.to_arrow()`, `.to_pandas()`, `.to_polars()`, `len()`.

```python
df = db.sql(
    "SELECT * FROM h5i('trades', 42)", timeout=30, max_rows=1_000_000
).to_pandas()
```

### `Database.read`

```python
read(name, version=None, as_of=None, snapshot=None, columns=None,
     time_start=None, time_end=None, limit=None, timeout=None) -> pyarrow.Table
```

Direct scan of one table version — no SQL layer, minimal overhead.

**Parameters**

`name` (`str`)
:   Table name.

`version` / `as_of` / `snapshot`
:   Read point (latest by default); at most one. `as_of` is an RFC3339 string.

`columns` (`list[str]`, optional)
:   Project to these columns.

`time_start` (`int`, optional)
:   Inclusive lower time bound, in **raw time units** (µs for `timestamp[us]`).
    Prunes segments before I/O.

`time_end` (`int`, optional)
:   Exclusive upper time bound, same units.

`limit` (`int`, optional)
:   Stop after this many rows.

`timeout` (`float`, optional)
:   Deadline in seconds.

**Returns**

`pyarrow.Table`

```python
window = db.read("trades", columns=["ts", "price"],
                 time_start=t0_us, time_end=t1_us)
```

### `Database.leakage_check`

```python
leakage_check(query, version=None, as_of=None, snapshot=None,
              tolerance=None) -> dict
```

Look-ahead-bias diagnostic (the Python surface of the CLI
[`leakage-check`](../manual/cli.html#h5i-db-leakage-check)). Runs `query` twice
— against the current head (*leaking*: every commit, including rows that only
became available after the decision instant) and against a decision read point
(*non-leaking*) — and returns the delta between the two results.

**Parameters**

`query` (`str`)
:   The SQL to evaluate under both read points.

`version` / `as_of` / `snapshot`
:   The decision point; **exactly one is required**. `as_of` is an RFC3339
    string matched by commit *availability* time.

`tolerance` (`float`, optional)
:   Per-cell numeric noise floor below which a difference is ignored
    (default `1e-9`).

**Returns**

`dict` — the leakage report: `leakage_detected`, per-column
`head → asof (delta)`, `max_abs_delta`, `row_count_differs`, and
`withheld_versions` (per table, the head-vs-as-of version gap).

**Raises**

`InvalidInputError`
:   No decision point was given, or more than one.

```python
report = db.leakage_check(
    "SELECT symbol, vwap(price, size) AS vwap FROM trades GROUP BY symbol",
    as_of="2026-07-01T16:00:00Z",
)
if report["leakage_detected"]:
    print("alpha that evaporates:", report["max_abs_delta"])
```

!!! note "Scope"
    A non-zero delta proves *availability* leakage (late-arriving or restated
    rows across commits). A zero delta does not prove its absence, and this
    does not detect look-ahead *inside* a single snapshot.

## Snapshots

### `Database.snapshot`

```python
snapshot(name, tables=None, note=None) -> dict
```

Pin current table versions under a name. Address it later from SQL as
`h5i('t', 'name')` or `read(snapshot=…)`.

**Parameters**

`name` (`str`)
:   Snapshot name.

`tables` (`list[str]`, optional)
:   Tables to pin. Defaults to **all** tables.

`note` (`str`, optional)
:   Free-text note.

## Mutation plans

The previewable plan/apply flow. These return a
[`MutationPlan`](results-and-plans.html#mutationplan) — the staged segments
already exist on disk; publishing is a metadata-only swap.

### `Database.plan_replace_range`

```python
plan_replace_range(name, start, end, data=None, note=None) -> MutationPlan
```

Stage a previewable replacement of the half-open time range `[start, end)`.

**Parameters**

`name` (`str`)
:   Table name.

`start` (`int`)
:   Inclusive range start, in **raw time units** (µs for `timestamp[us]`).

`end` (`int`)
:   Exclusive range end, same units.

`data` (`TableLike`, optional)
:   Replacement rows. Omit (or `None`) to **delete** the range.

`note` (`str`, optional)
:   Free-text note carried onto the resulting version.

**Returns**

`MutationPlan` — inspect `.summary` / `.before_sample`, then `.apply()`.

### `Database.plan_delete_range`

```python
plan_delete_range(name, start, end, note=None) -> MutationPlan
```

Sugar for `plan_replace_range(name, start, end, None, note)` — stage a
range deletion.

### `Database.list_plans`

```python
list_plans(name) -> list[MutationPlan]
```

Pending (not yet applied or discarded) plans for a table.

## Policy

### `Database.policy`

```python
policy() -> dict
```

The [mutation policy](../manual/concepts.html#the-mutation-policy) as a dict
of boolean flags: `direct_append`, `direct_write`, `direct_replace`,
`direct_delete`, `direct_restore`, `direct_compact`.

### `Database.set_policy`

```python
set_policy(policy=None, **flags) -> dict
```

Update the mutation policy; unspecified flags keep their value. The merge is
atomic (read-modify-write under the metadata lock).

**Parameters**

`policy` (`dict`, optional)
:   Flags to set, as a dict.

`**flags` (`bool`)
:   Flags to set, as keyword arguments — `db.set_policy(direct_delete=False)`.

**Returns**

`dict` — the merged policy that was stored.

**Raises**

`InvalidInputError`
:   An unknown flag name.

## Data-safety policy

Where the [mutation policy](#databasepolicy) gates *who* may write directly, a
per-table **data policy** gates *what data* may be written — typed constraints
checked fail-closed on every write and at plan time
([CLI reference](../manual/cli.html#h5i-db-data-policy)). A table with no policy
is unconstrained and pays no read-path cost.

### `Database.data_policy`

```python
data_policy(table) -> dict | None
```

The table's data-safety policy as a dict, or `None` when unset.

### `Database.set_data_policy`

```python
set_data_policy(table, policy) -> dict
```

Install (overwrite) a table's data-safety policy. Returns the stored policy.

**Parameters**

`table` (`str`)
:   Table name.

`policy` (`dict`)
:   A typed policy document. Predicates compose `not_null`, `compare`, and
    `in_set` with `and` / `or` / `not`; each constraint's `on_fail` is
    `"reject"` (fail the write) or `"warn"`.

```python
db.set_data_policy("trades", {"constraints": [
    {"name": "positive_price",
     "predicate": {"compare": {"column": "price", "op": "gt",
                               "value": {"float": 0.0}}},
     "on_fail": "reject"}]})
```

**Raises**

`InvalidInputError`
:   A malformed policy document, or — when a later write breaks a constraint —
    a `data_policy_violation` (the write is refused before it lands).

### `Database.clear_data_policy`

```python
clear_data_policy(table) -> None
```

Remove a table's data-safety policy (writes become unconstrained).

## Maintenance

### `Database.compact`

```python
compact(name, note=None) -> dict
```

Rewrite small segments into target-sized ones as a new version — a query-speed
tool; old segments stay pinned by history.

### `Database.vacuum`

```python
vacuum(table=None, grace_seconds=3600, apply=False) -> dict
```

Remove unreachable objects (crashed-writer debris, discarded plans). Committed
history is never touched.

**Parameters**

`table` (`str`, optional)
:   Restrict to one table. Defaults to the whole database.

`grace_seconds` (`int`, default `3600`)
:   Never touch objects newer than this — keep it above your longest ingest.

`apply` (`bool`, default `False`)
:   Actually delete. The default is a dry run.

**Returns**

`dict` — the candidate (or deleted) object list.

### `Database.verify`

```python
verify(name, deep=False) -> dict
```

Structural integrity check: checksum chain and object existence.

**Parameters**

`name` (`str`)
:   Table name.

`deep` (`bool`, default `False`)
:   Also re-read every segment and verify content checksums.

**Returns**

`dict` — a report; problems are listed in it rather than raised.
