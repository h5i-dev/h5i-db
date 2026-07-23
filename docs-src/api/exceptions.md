---
title: Exceptions
description: "The typed error hierarchy: every h5i-db error carries a stable code, an actionable hint, and a retryable flag."
order: 3
---

# Exceptions

Every h5i-db failure raises a subclass of `h5i_db.H5iError`, and every
instance carries the same structured envelope the CLI prints on stderr:

```python
try:
    db.read("nope")
except h5i_db.NotFoundError as e:
    e.code        # "table_not_found"   — stable, branchable identifier
    e.hint        # actionable suggestion (e.g. how to list tables)
    e.retryable   # False               — retrying without change won't help
```

Messages are formatted `"[{code}] {message} (hint: {hint})"`.

## Hierarchy

Everything subclasses `H5iError`, which subclasses `Exception`:

| Exception | Meaning | Typical codes |
|---|---|---|
| `H5iError` | Base class for all h5i-db errors (attributes: `code`, `hint`, `retryable`) | `closed`, `query` |
| `NotFoundError` | Database, table, version or snapshot does not exist | `database_not_found`, `table_not_found`, `version_not_found`, `snapshot_not_found` |
| `ConflictError` | Concurrent-writer conflict or already-exists collision; usually retryable | `version_conflict`, `table_exists`, `database_exists`, `lock_timeout` |
| `InvalidInputError` | Bad argument, schema mismatch, sort-order violation or unsupported operation | `invalid_input`, `schema_mismatch`, `sort_order_violation`, `unsupported` |
| `PolicyError` | Operation forbidden by the mutation policy or a read-only handle | `policy_violation`, `read_only` |
| `CorruptionError` | Checksum/format verification failed; data may be damaged or written by a newer h5i-db | `corruption`, `format_too_new` |
| `LimitError` | A configured limit (memory, max_rows, segment count) was exceeded | `limit_exceeded` |
| `TimeoutError` | The operation exceeded its deadline | `timeout` |
| `StorageError` | Underlying storage / IO / encoding failure | `storage`, `io`, `arrow`, `parquet`, `metadata` |

!!! note "`h5i_db.TimeoutError` shadows the builtin"
    It subclasses `H5iError`, **not** Python's builtin `TimeoutError` — catch
    `h5i_db.TimeoutError` (or `H5iError`) specifically.

## Patterns

**Branch on type for control flow, on `.code` for precision:**

```python
try:
    db.append("trades", batch, expected_version=v)
except h5i_db.ConflictError:
    v = db.versions("trades")[-1]["version"]     # re-read, re-derive, retry
except h5i_db.InvalidInputError as e:
    if e.code == "sort_order_violation":
        batch = batch.sort_by("ts")              # fix and retry once
    else:
        raise
```

**Respect `retryable`:** it encodes whether backing off can help.
`ConflictError` and `TimeoutError` generally can be retried;
`InvalidInputError` and `PolicyError` cannot — fix the call (or get the plan
reviewed) instead.

**Surface `hint`:** hints are written to be shown — to a user, a log, or an
LLM agent deciding its next step. Don't swallow them.

**Catch-all:** `except h5i_db.H5iError` catches every h5i-db failure while
letting genuine bugs (`TypeError`, …) propagate. A closed handle raises
`H5iError` with `code == "closed"`.
