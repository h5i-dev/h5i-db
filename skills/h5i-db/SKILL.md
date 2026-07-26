---
name: h5i-db
description: Use when working with an h5i-db database — an embedded, versioned time-series database for quant research, driven by the `h5i-db` CLI or the `h5i_db` Python package. Covers creating a database and loading Parquet or CSV tick data, orienting in an existing one, querying it (SQL, ASOF joins, OHLCV/VWAP rollups, time travel) under a context budget, running look-ahead-free backtests, and previewing destructive changes before they land.
---

# Driving h5i-db

Every write is an immutable commit. Nothing you do destroys history short of
`vacuum --apply` after deleting snapshots, so the failure mode to actually
worry about is not losing data — it is writing *wrong* data, or reading data
you should not have been able to see yet.

`h5i-db <command> --help` is the authoritative flag reference and cannot go
stale. Reach for it before guessing at a flag.

## Starting from nothing

A database is one directory; there is no server.

```bash
h5i-db init market.db
h5i-db create-table market.db trades --like ticks.parquet --time-column ts
h5i-db ingest market.db trades ticks.parquet --idempotency-key load-1
```

`--like` infers the schema from a Parquet/CSV/Arrow file. The time column must
be named at creation: without one you lose segment pruning, the ASOF join, and
research mode. → [references/setup.md](references/setup.md)

To see the whole thing work before touching real data, `h5i-db demo` builds a
small database and walks ingest → query → restatement → leakage in seconds.

## Starting from an existing database

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

## Trying something you might throw away

Work that may not survive belongs in a fork, not in the base database. A fork
is a writable workspace pinned to the current state; creating one copies no
data, and several can run against the same dataset at once without contending.

```bash
h5i-db fork create market.db agent-01
h5i-db ingest market.db features out.parquet --fork agent-01   # base untouched
h5i-db fork drop market.db agent-01                            # or promote it
```

Every data command takes `--fork <name>`. Keep it if it worked
(`fork diff`, then `fork promote --table <t>`, which is first-commit-wins and
must not be retried on conflict); drop it if it did not.
→ [references/forks.md](references/forks.md)

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

[Setup and guardrails](references/setup.md) · [SQL extensions](references/sql.md)
(time travel, ASOF joins, `time_bucket`, `vwap`, `ewma`) ·
[mutations and safety net](references/mutations.md) ·
[research mode and leakage](references/research-mode.md) ·
[forks and parallel work](references/forks.md) ·
[Python](references/python.md)

Everything needed for the work above is in this directory and in
`h5i-db <command> --help`. If you have network access and want to go deeper,
<https://db.h5i.dev/llms.txt> indexes the full manual and Python API; treat it
as optional, since it describes the published version rather than the binary
you have.
