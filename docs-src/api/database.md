---
title: Database
description: "h5i_db.Database reference: lifecycle, tables, writing, reading and SQL, time travel, mutation plans, policy, and maintenance."
order: 1
---

# `Database`

```python
h5i_db.Database(path: str, create: bool = False, read_only: bool = False)
```

An h5i-db database directory. `create=True` opens or creates;
`read_only=True` rejects every write at the handle level (raises
[`PolicyError`](exceptions.html)). The handle is a context manager.

```python
with h5i_db.Database("market.db", create=True) as db:
    ...
```

Many methods return plain `dict`s decoded from the engine's JSON — commit
results carry keys like `version`, `rows`, `bytes`; inspect them, they are
made to be logged.

## Lifecycle

### `Database.close`

```python
close() -> None
```

Release the native handle (idempotent). Later operations on this object raise
`H5iError` with `code == "closed"`. In-flight operations on other threads
finish normally. Also called by `__exit__`.

### `Database.closed`

```python
closed: bool     # property
```

### `Database.path`

```python
path: str        # the directory this handle was opened on
```

## Tables

### `Database.create_table`

```python
create_table(name: str, schema: pyarrow.Schema,
             time_column: str | None = None,
             sort_key: Iterable[str] | None = None) -> dict
```

Create a table from an Arrow schema. `time_column` declares the time axis —
strongly recommended for time-series tables (it enables pruning, ASOF joins,
range plans, `tail`); the column is forced non-nullable. `sort_key` defaults
to the time column.

```python
db.create_table("trades", schema, time_column="ts", sort_key=["ts", "symbol"])
```

### `Database.tables`

```python
tables() -> list[str]
```

### `Database.schema`

```python
schema(name: str, version: int | None = None,
       as_of: str | None = None, snapshot: str | None = None) -> pyarrow.Schema
```

Schema of a table at a read point (latest by default). At most one of
`version` / `as_of` (RFC3339 string) / `snapshot` may be given.

### `Database.versions`

```python
versions(name: str) -> list[dict]
```

Committed versions, one dict per version: version number, operation, commit
time, row/byte/segment counts, note.

### `Database.drop_table`

```python
drop_table(name: str) -> None
```

Permanently drops the table and its data. Refuses (with `ConflictError`) if a
snapshot pins it.

## Writing

### `Database.append`

```python
append(name: str, data: TableLike, *,
       expected_version: int | None = None, note: str | None = None) -> dict
```

Strict ordered append — one atomic, durable commit. `data` is a
`pyarrow.Table`, `RecordBatch`, or sequence of batches. Rows must respect the
table's sort order; violations raise `InvalidInputError`
(`sort_order_violation`). Version conflicts with concurrent writers are
retried internally (up to 5 times) — safe for pure appends; pass
`expected_version` for optimistic locking when the append depends on what you
last read. `note` lands in the version manifest.

### `Database.write`

```python
write(name: str, data: TableLike, *,
      expected_version: int | None = None, note: str | None = None) -> dict
```

Replace the table's contents in one commit. The previous state remains
readable as its version — a restatement, not an overwrite.

### `Database.restore`

```python
restore(name: str, version: int) -> dict
```

Make a historical version current by committing a new version with its
contents. History only moves forward.

## Reading & SQL

### `Database.sql`

```python
sql(query: str, memory_limit: int | None = None,
    timeout: float | None = None, max_rows: int | None = None) -> QueryResult
```

Run SQL — full DataFusion plus the [h5i extensions](../manual/sql.html).
`timeout` is a deadline in seconds (raises
[`TimeoutError`](exceptions.html) and cancels execution). `max_rows` raises
[`LimitError`](exceptions.html) as soon as the result exceeds it — execution
stops early rather than silently truncating. `memory_limit` (bytes) enables
disk spilling under pressure.

```python
res = db.sql("SELECT * FROM h5i('trades', 42)", timeout=30, max_rows=1_000_000)
df = res.to_pandas()
```

### `Database.read`

```python
read(name: str, version: int | None = None, as_of: str | None = None,
     snapshot: str | None = None, columns: list[str] | None = None,
     time_start: int | None = None, time_end: int | None = None,
     limit: int | None = None, timeout: float | None = None) -> pyarrow.Table
```

Direct scan of one table version — no SQL layer, minimal overhead. At most
one of `version` / `as_of` / `snapshot`. `columns` projects;
`time_start`/`time_end` are a half-open `[start, end)` range in **raw time
units** (µs for `timestamp[us]` columns) and prune segments before I/O.

```python
window = db.read("trades", columns=["ts", "price"],
                 time_start=t0_us, time_end=t1_us)
```

## Snapshots

### `Database.snapshot`

```python
snapshot(name: str, tables: list[str] | None = None,
         note: str | None = None) -> dict
```

Pin current table versions under a name (all tables when `tables` is
omitted). Address it from SQL as `h5i('t', 'name')` or `read(snapshot=…)`.

## Mutation plans

See [MutationPlan](results-and-plans.html#mutationplan) for the object these
return.

### `Database.plan_replace_range`

```python
plan_replace_range(name: str, start: int, end: int,
                   data: TableLike | None = None,
                   note: str | None = None) -> MutationPlan
```

Stage a previewable replacement of `[start, end)` (raw time units) with
`data` — or a deletion when `data` is `None`. The full write path runs and
new segments are staged, but nothing is published until
[`plan.apply()`](results-and-plans.html#mutationplanapply).

### `Database.plan_delete_range`

```python
plan_delete_range(name: str, start: int, end: int,
                  note: str | None = None) -> MutationPlan
```

Sugar for `plan_replace_range(name, start, end, None, note)`.

### `Database.list_plans`

```python
list_plans(name: str) -> list[MutationPlan]
```

Pending (not yet applied/discarded) plans for a table.

## Policy

### `Database.policy`

```python
policy() -> dict
```

The mutation policy as a dict of boolean flags:
`{"direct_append": True, "direct_write": True, "direct_replace": True,
"direct_delete": True, "direct_restore": True, "direct_compact": True}`.

### `Database.set_policy`

```python
set_policy(policy: dict | None = None, **flags: bool) -> dict
```

Update the mutation policy; unspecified flags keep their value. The merge is
atomic (read-modify-write under the database metadata lock). Unknown flags
raise `InvalidInputError`. Returns the merged policy that was stored.

```python
db.set_policy(direct_delete=False, direct_write=False)
```

## Maintenance

### `Database.compact`

```python
compact(name: str, note: str | None = None) -> dict
```

Rewrite small segments into target-sized ones as a new version. A query-speed
tool — old segments stay pinned by history.

### `Database.vacuum`

```python
vacuum(table: str | None = None, grace_seconds: int = 3600,
       apply: bool = False) -> dict
```

Remove unreachable objects. Dry run unless `apply=True`; `grace_seconds`
protects recent objects (keep it above your longest ingest). Returns the
candidate/deleted list.

### `Database.verify`

```python
verify(name: str, deep: bool = False) -> dict
```

Structural integrity check: checksum chain, object existence; `deep=True`
re-reads every segment and verifies content checksums. Returns a report dict;
problems are listed rather than raised.
