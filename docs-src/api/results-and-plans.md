---
title: QueryResult & MutationPlan
description: Converting query results to Arrow, pandas, and Polars; previewing and applying mutation plans.
order: 2
---

# `QueryResult` & `MutationPlan`

Two small result objects returned by [`Database`](database.html): the lazy
holder a query produces, and the staged handle a mutation plan produces.

## QueryResult

Returned by [`Database.sql()`](database.html#databasesql). A lazy holder of an
Arrow result with convenience converters — data stays in Arrow until you ask
for a specific frame type.

```python
res = db.sql("SELECT symbol, vwap(price, size) AS v FROM trades GROUP BY symbol")
res.to_pandas()     # -> pandas.DataFrame
len(res)            # -> row count
```

### `QueryResult.to_arrow`

```python
to_arrow() -> pyarrow.Table
```

The underlying Arrow table, types preserved exactly (timestamps keep unit and
timezone). Zero-copy.

### `QueryResult.to_pandas`

```python
to_pandas() -> pandas.DataFrame
```

Convert to a pandas DataFrame.

### `QueryResult.to_polars`

```python
to_polars() -> polars.DataFrame
```

Convert to a Polars DataFrame.

**Raises**

`ImportError`
:   Polars is not installed (it is an optional dependency).

### Dunder methods

```python
len(res)     # row count
repr(res)    # repr of the underlying Arrow table
```

## MutationPlan

A previewable, not-yet-published mutation — returned by
[`plan_replace_range` / `plan_delete_range`](database.html#databaseplan_replace_range)
and [`list_plans`](database.html#databaselist_plans). The staged segments
already exist on disk; publishing is a metadata-only atomic swap.

```python
plan = db.plan_delete_range("trades", t0_us, t1_us, note="strip bad ticks")

plan.summary            # {"rows_affected": 12481, "segments_reused": 127, …}
plan.before_sample      # pyarrow.Table — rows as they are now
plan.after_sample       # pyarrow.Table — rows as they would become

plan.apply()            # publish — or plan.discard()
```

### Attributes

`table` (`str`)
:   Table the plan targets.

`plan_id` (`str`)
:   UUID — also usable from the CLI (`h5i-db plan apply …`) and the review UI.

`summary` (`dict`)
:   Machine-readable impact: affected rows, segments rewritten vs. reused.

`raw` (`dict`)
:   The full plan document as stored.

### `MutationPlan.before_sample`

```python
before_sample -> pyarrow.Table | None
```

Property — a sample of the affected rows **before** the mutation, or `None`
when the plan carries no sample.

### `MutationPlan.after_sample`

```python
after_sample -> pyarrow.Table | None
```

Property — the same rows **after** the mutation would apply.

### `MutationPlan.apply`

```python
apply() -> dict
```

Publish the plan as a new version.

**Returns**

`dict` — commit metadata for the new version.

**Raises**

`ConflictError`
:   The table head moved since the plan was made. **Re-plan instead of
    retrying** — the plan was computed against a base version that no longer
    reflects reality.

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
    which is the point: every destructive change gets a previewed, auditable
    checkpoint.
