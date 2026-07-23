---
title: QueryResult & MutationPlan
description: Converting query results to Arrow, pandas, and Polars; previewing and applying mutation plans.
order: 2
---

# `QueryResult` & `MutationPlan`

## QueryResult

Returned by [`Database.sql()`](database.html#databasesql) — a lazy holder of
an Arrow result with convenience converters.

```python
res = db.sql("SELECT symbol, vwap(price, size) AS v FROM trades GROUP BY symbol")

res.to_arrow()      # pyarrow.Table (zero-copy)
res.to_pandas()     # pandas.DataFrame
res.to_polars()     # polars.DataFrame (Polars is an optional dependency)
len(res)            # row count
```

### `QueryResult.to_arrow`

```python
to_arrow() -> pyarrow.Table
```

The underlying Arrow table, as produced by the engine — types preserved
exactly (timestamps keep unit and timezone).

### `QueryResult.to_pandas`

```python
to_pandas() -> pandas.DataFrame
```

### `QueryResult.to_polars`

```python
to_polars() -> polars.DataFrame
```

Imports `polars` on first use; raises `ImportError` if it isn't installed.

### Dunder support

`len(res)` is the row count; `repr(res)` shows the Arrow table.

---

## MutationPlan

A previewable, not-yet-published mutation — created by
[`plan_replace_range` / `plan_delete_range`](database.html#databaseplan_replace_range),
listed by [`list_plans`](database.html#databaselist_plans). The staged
segments already exist on disk; publishing is a metadata-only atomic swap.

```python
plan = db.plan_delete_range("trades", t0_us, t1_us, note="strip bad ticks")

plan.summary            # {"rows_affected": 12481, "segments_reused": 127, …}
plan.before_sample      # pyarrow.Table — rows as they are now
plan.after_sample       # pyarrow.Table — rows as they would become

plan.apply()            # publish — or:
plan.discard()          # drop; staged segments become vacuumable
```

### Fields

| Field | Type | Meaning |
|---|---|---|
| `table` | `str` | Table the plan targets |
| `plan_id` | `str` | UUID — also usable with the CLI (`h5i-db plan apply …`) and the review UI |
| `summary` | `dict` | Machine-readable impact: affected rows, segments rewritten vs. reused |
| `raw` | `dict` | The full plan document as stored |

### `MutationPlan.before_sample` / `after_sample`

```python
before_sample: pyarrow.Table | None    # property
after_sample:  pyarrow.Table | None   # property
```

Row samples of the affected range before and after the mutation; `None` when
the plan carries no sample (e.g. nothing affected).

### `MutationPlan.apply`

```python
apply() -> dict
```

Publish the plan as a new version. Raises
[`ConflictError`](exceptions.html) if the table head moved since the plan was
made — **re-plan instead of retrying**: the plan was computed against a base
version that no longer reflects reality.

### `MutationPlan.discard`

```python
discard() -> None
```

Drop the plan; its staged segments become vacuum candidates immediately.
Abandoned plans (neither applied nor discarded) expire after 7 days — see
[plan hygiene](../manual/operations.html#mutation-plan-hygiene).

!!! tip "Policy interaction"
    With the [mutation policy](../manual/concepts.html#the-mutation-policy)
    gating direct deletes/writes, the plan flow is the *only* way to mutate —
    which is exactly the point: every destructive change gets a previewed,
    auditable checkpoint.
