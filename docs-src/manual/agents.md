---
title: Agents & automation
description: "The machine contract: structured output and errors, stable exit codes, resource limits as flags, and policy-gated review for AI agents and pipelines."
order: 6
---

# Agents & automation

h5i-db needs no MCP server or custom protocol to be driven by AI agents,
schedulers, or CI: agents use the same CLI and Python API as everyone else.
What makes that safe is a deliberate machine contract: structured output,
structured errors, hard resource limits, and a write path that policy can
gate behind human review.

```console
$ h5i-db query market.db "SELECT symbol, vwap(price, size) FROM trades GROUP BY symbol" \
    --format json --max-rows 1000 --timeout 30s        # machine formats + hard limits

$ h5i-db delete-range market.db trades --start 09:30… --end 09:31… --plan
{"plan_id": "5c41…", "summary": {"rows_affected": 12481, "segments_reused": 127}}

$ h5i-db policy set market.db direct_delete=false      # agents must preview; humans can too
```

## Machine-readable everything

- **Output**: `--format json | jsonl | csv | arrow` on every command
  ([formats](cli.html#output-formats)). `jsonl` is the natural choice for
  streaming consumers.
- **Errors**: a single JSON envelope on stderr,
  `{"code", "message", "retryable", "hint"}`. `code` is a stable identifier
  (`version_conflict`, `table_not_found`, `limit_exceeded`, …), `retryable`
  says whether backing off and retrying can help, and `hint` names the next
  thing to try.
- **Exit codes** are stable and branchable: `0` ok, `2` user error,
  `3` conflict, `4` limit exceeded, `5` internal
  ([details](cli.html#errors-and-exit-codes)).

In Python the same envelope arrives as a
[typed exception hierarchy](../api/exceptions.html): every `H5iError` carries
`.code`, `.hint`, and `.retryable`.

```python
try:
    db.append("trades", batch)
except h5i_db.ConflictError:
    ...                      # retryable: another writer won; re-read and retry
except h5i_db.InvalidInputError as e:
    print(e.hint)            # not retryable: fix the call
```

## Resource limits as flags

A supervisor can hard-cap any call without touching the database:

| CLI | Python | Effect |
|---|---|---|
| `--max-rows N` | `sql(max_rows=N)` | Stop as soon as the result exceeds N rows, with a clean `limit_exceeded` error instead of silent truncation |
| `--timeout 30s` | `sql(timeout=30)` | Deadline; cancels execution on expiry |
| `--memory-limit-mb N` | `sql(memory_limit=N)` | Memory budget with disk spilling under pressure |
| `--max-bytes N` | — | Cap output bytes at batch boundaries |
| (open read-only) | `Database(path, read_only=True)` | Reject every write at the handle level |

### The agent output profile

Passing a limit on every call only works if the caller remembers, and one
forgotten flag can end a session. `H5I_DB_PROFILE=agent` moves the budget into
the environment instead:

```console
$ export H5I_DB_PROFILE=agent
$ h5i-db query market.db "SELECT * FROM trades" --format jsonl
{"ts":"2026-07-01T09:30:00Z","symbol":"AAPL","price":210.5}
… 1000 rows …
```

stdout stops at 1000 rows or 1 MiB, and a JSON summary on stderr reports what
was withheld:

```json
{"profile":"agent","truncated":true,"total_rows":2841193,"returned_rows":1000,
 "full_result_path":"/tmp/h5i-db-results/result-….parquet","full_result_rows":2841193}
```

Nothing is lost, only withheld: the rows that did not fit are in that Parquet
file. `H5I_DB_RESULT_DIR` moves where those land. Two properties hold in this
mode. Output content never depends on whether stdout is a terminal, so a piped
run produces the same bytes as an interactive one. And an explicit
`--max-bytes` you pass yourself stays a hard `limit_exceeded` error rather
than a soft truncation.

## Policy-gated review

The [mutation policy](concepts.html#the-mutation-policy) forces chosen
operations through the previewable plan/apply flow:

```console
$ h5i-db policy set market.db direct_delete=false direct_write=false direct_replace=false
```

An agent that then tries a direct delete gets a `policy_violation` error whose
hint points at `--plan`. The staged plan carries exact affected-row counts and
before/after samples; a human reviews it in the
[UI](cli.html#h5i-db-ui) (`h5i-db ui market.db`) or via `plan show`, and
applies or discards it. Every committed manifest records its
`execution_mode` and plan hash, so the audit trail distinguishes reviewed
from direct writes forever.

Where the *mutation* policy gates who may write directly, a per-table
[data policy](cli.html#h5i-db-data-policy) gates *what data may be written*:
typed constraints (`not_null`, `compare`, `in_set`, composed with
`and`/`or`/`not`) checked fail-closed on every write and at plan time. A
violating batch is refused with `data_policy_violation` before it can land, so
an agent can't quietly commit malformed rows.

## Patterns that work

- **Idempotent retries:** appends racing another writer raise
  `version_conflict` (exit 3 / `ConflictError`, `retryable: true`). The CLI
  retries pure appends itself (`ingest --retries`, default 5); in Python,
  `append()` retries internally as well. For read-modify-write flows, pass
  `--expected-version` and re-derive on conflict rather than blindly retrying.
- **Pin what you read:** have agents record the version they computed from
  (`versions`, or read via `h5i('t', v)`), so every downstream artifact is
  attributable to an exact input state. The cookbook works this through in
  [reproducible backtests](../cookbook/03_risk_and_production/02_reproducible_backtests.html)
  and the [paper-trading loop](../cookbook/03_risk_and_production/05_live_paper_trading_loop.html).
- **Bound the pull, not the loop:** a cutoff applied when the data leaves the
  database survives the trip into pandas, where nothing else can enforce it.
  [`query --decision-time <ts>`](cli.html#point-in-time-reads) hides rows
  stamped later and `--as-of` pins which commits exist; set
  `H5I_DB_DECISION_TIME` to bound a whole session.
- **Measure what the data did afterwards.** No cutoff can prevent a vendor
  restating history, so measure it instead:
  [`arrival-delta … --as-of <decision-time>`](cli.html#h5i-db-arrival-delta)
  re-runs a query at both read points and reports how much the answer moved.
  Read `vacuous` before the number: on a database with no arrival history the
  zero is arithmetic rather than evidence.
- **Orient in one call:** [`context`](cli.html#h5i-db-context) answers what
  `tables`, `schema`, `sample` and `versions` answer, in one deterministic
  document that also names any staged plan and the operations policy gates.
- **Notes are for provenance:** `--note` / `note=` lands in the version
  manifest; make agents write *why* ("re-mark after vendor restatement,
  ticket DX-142"), and `versions` becomes your change log.
- **Incremental consumers use `tail`:** strictly ordered commits mean
  "give me exactly the rows since version N" is
  [`tail('t', N)`](sql.html#tail), with no timestamp cursors.
