---
name: h5i-db
description: Use when working with an h5i-db database — an embedded, versioned time-series database for quant research, driven by the `h5i-db` CLI or the `h5i_db` Python package. Covers orienting in a database, querying it under a context budget, running look-ahead-free backtests, ingesting safely, and previewing destructive changes before they land.
---

# Driving h5i-db

Every write is an immutable commit. Nothing you do destroys history short of
`vacuum --apply` after deleting snapshots, so the failure mode to actually
worry about is not losing data — it is writing *wrong* data, or reading data
you should not have been able to see yet.

## Start every session with these two lines

```bash
export H5I_DB_PROFILE=agent      # caps every result; withheld rows spill to Parquet
h5i-db context market.db --format json
```

`context` answers in one call what `tables`, `schema`, `sample` and `versions`
answer in a dozen: every table's columns, size, time range and head version,
which operations policy requires a plan for, and any plan already staged and
waiting for review. Add `--budget 2000` on a wide catalog; it sheds detail in a
fixed order and tells you what it dropped.

## Then work in this loop

1. **Query.** `h5i-db query market.db "<sql>" --format json`. Under the agent
   profile a stderr summary reports `total_rows` and, when the result was
   truncated, a `full_result_path` holding all of it.
2. **Read failures, don't guess.** stderr is always
   `{code, message, retryable, hint, did_you_mean, next_actions}`.
   `next_actions[].cmd` are runnable commands — prefer them to inventing a fix.
   Retry only when `retryable` is true. Exit codes: 0 ok · 2 your input · 3
   version conflict · 4 limit/timeout · 5 corruption (stop and report).
3. **Preview before you destroy.** Any `delete-range` / `replace-range` takes
   `--plan`, which stages the change and shows exactly what it touches; a human
   or a rule applies it. A `policy_violation` is the signal to use that flow,
   never to look for a way around it. → [references/mutations.md](references/mutations.md)
4. **Make writes retry-safe.** Pass `--idempotency-key <token>` on any ingest
   you might repeat. A retry after an ambiguous failure then returns the commit
   that already happened instead of appending the rows twice.

## Backtesting: let the database withhold the future

Do not filter the future out in SQL — you will forget once, and a leaked
backtest looks like a good one. Pin the session instead, so no query in it can
reach past the decision instant:

```bash
h5i-db query market.db "<sql>" \
  --decision-time 2026-07-01T00:00:00Z \   # rows stamped later are unreadable
  --embargo 1d                             # extra safety gap
```

`--as-of <version|snapshot|timestamp>` pins the other axis — which *commits*
are visible — so a restatement that arrived later stays invisible too.
→ [references/research-mode.md](references/research-mode.md)

## Reference

[SQL extensions](references/sql.md) (time travel, ASOF joins, `time_bucket`,
`vwap`, `ewma`) · [mutations and safety net](references/mutations.md) ·
[research mode and leakage](references/research-mode.md) ·
[Python](references/python.md)
