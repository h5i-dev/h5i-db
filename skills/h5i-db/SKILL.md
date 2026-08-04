---
name: h5i-db
description: Use when working with an h5i-db database — an embedded, versioned time-series database and event-driven backtesting engine for quant research, driven by the `h5i-db` CLI or the `h5i_db` Python package. Covers creating a database and loading Parquet or CSV tick data, orienting in an existing one, querying it (SQL, ASOF joins, OHLCV/VWAP rollups, time travel) under a context budget, look-ahead-free reads, exploring in a notebook whose kernel survives between commands, running event-driven backtests with settlement and fee models, scoring them (factor stats, tearsheets, deflated Sharpe), importing vendor market-data archives, and previewing destructive changes before they land.
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

## Exploring: don't pay the load twice

When answering a question means running code more than once against the same
state, use the notebook. Its kernel persists between commands, so a 40-second
load is paid once instead of once per idea, and the `.ipynb` is what a human
opens afterwards to see what you actually tried.

```bash
h5i-db nb exec research.ipynb --code "df = load(); df.shape"
h5i-db nb exec research.ipynb --code "df.groupby('venue').size()"   # df is still there
```

`%%sql` cells query a database with no Python in the way, `--detach` covers
runs too long to wait for, and `nb watch <file> --split right` puts a live,
read-only view in a pane beside the human without touching your session.
→ [references/notebook.md](references/notebook.md)

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

Forks nest (`fork create … --fork <parent>`), so a search tree of hypotheses
is expressible directly; `forks('t')` queries a table across every fork at
once with a `__fork` column; and `fork create --count N` makes a fanout in one
command. → [references/forks.md](references/forks.md)

## Look-ahead-free reads: let the database withhold the future

Do not filter the future out in SQL — you will forget once, and a leaked
result looks like a good one. Pin the session instead, so no query in it can
reach past the decision instant:

```bash
h5i-db query market.db "<sql>" \
  --decision-time 2026-07-01T00:00:00Z \   # rows stamped later are unreadable
  --embargo 1d                             # extra safety gap
```

`--as-of <version|snapshot|timestamp>` pins the other axis — which *commits*
are visible — so a restatement that arrived later stays invisible too.
→ [references/research-mode.md](references/research-mode.md)

## Simulating a strategy

The backtester is a separate layer with no CLI verb: drive it from Python, or
from `python -m h5i_db.backtest` with a JSON config.

```python
from h5i_db import backtest

config = backtest.BacktestConfig(
    run_id="momentum-001",
    data=backtest.DataConfig(signals="signals", snapshot="2024-q1"),
    portfolio=backtest.PortfolioConfig(starting_cash=100_000.0),
)
backtest.inspect(db, config).raise_for_errors()   # refuse unsupported claims
result = backtest.execute(db, config)             # runs inside its own fork
```

Three things decide whether the number it returns means anything, and each has a
way of going wrong quietly:

- **Stamp a signal a microsecond after the quote it was decided from.** An order
  sharing a timestamp with a book event may match the previous snapshot.
- **Read `settlement_pnl` beside `realized_pnl`.** A book held to resolution
  reads zero in the second, and settlement only applies when the replay reached
  the instant the result became observable.
- **Search honestly.** `WalkForward` plus `TopK` keeps a holdout a holdout, and
  `quant.deflated_sharpe` discounts whatever the search picked.

→ [references/backtest.md](references/backtest.md) ·
[references/quant.md](references/quant.md)

## Loading somebody else's market data

`h5i_db.venues` normalises vendor Parquet already on disk into the canonical
tables. It does not fetch; a vendor dialect is an `ArchiveLayout` literal rather
than a code path, re-running an import replays instead of duplicating, and
requested versus loaded coverage comes back as separate facts.

```bash
python -m h5i_db.venues markets market.db specs.json
python -m h5i_db.venues ingest  market.db specs.json --root /mnt/mirror --min-coverage 0.95
```

→ [references/data-onramp.md](references/data-onramp.md)

## Reference

[Setup and guardrails](references/setup.md) · [SQL extensions](references/sql.md)
(time travel, ASOF joins, `time_bucket`, `vwap`, `ewma`) ·
[mutations and safety net](references/mutations.md) ·
[research mode and leakage](references/research-mode.md) ·
[forks and parallel work](references/forks.md) ·
[notebooks](references/notebook.md) (persistent kernels, `%%sql`, detached cells, watch panes) ·
[Python](references/python.md) ·
[backtesting](references/backtest.md) (configs, preflight, settlement, studies) ·
[quant analytics](references/quant.md) (factors, tearsheets, deflated Sharpe, PBO) ·
[vendor data on-ramp](references/data-onramp.md) (archives, layouts, ledger replay)

Everything needed for the work above is in this directory and in
`h5i-db <command> --help`. If you have network access and want to go deeper,
<https://db.h5i.dev/llms.txt> indexes the full manual and Python API; treat it
as optional, since it describes the published version rather than the binary
you have.
