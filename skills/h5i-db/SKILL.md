---
name: h5i-db
description: Use when working with an h5i-db database — an embedded, versioned time-series DB driven by the `h5i-db` CLI or the `h5i_db` Python package. Covers discovering tables/schemas, SQL queries (ASOF joins, time_bucket, time travel), safe ingestion, and previewable plan/apply mutations.
---

# Using h5i-db (for AI agents)

h5i-db is an embedded, versioned time-series database. You drive it with the
`h5i-db` CLI (or the `h5i_db` Python package). Every write produces an
immutable version; nothing you do can destroy history short of `vacuum
--apply` after deleting snapshots.

## Golden rules

1. **Discover before you act**: `tables` → `schema` → `sample`, then query.
2. **Prefer `--format json`** (or `jsonl` for row streams). Parse stderr on
   failure: it is always `{code, message, retryable, hint}`. If
   `retryable: true` (conflicts, lock timeouts), retry; otherwise follow the
   `hint` — do not retry blindly.
3. **Exit codes**: 0 ok · 2 your input was wrong · 3 version conflict
   (someone else committed; re-read and retry) · 4 resource limit/timeout ·
   5 corruption/internal (stop and report).
4. **Mutations that remove or change data should be planned first**, and the
   database policy may force this. `--plan` costs one extra command and gives
   you (and the human reviewing you) an exact preview — see
   [references/mutations.md](references/mutations.md).
5. **Cap yourself**: pass `--max-rows`, `--timeout`, `--memory-limit-mb` on
   queries; the harness may kill you, but the flags fail cleanly.

## Discovery

```bash
h5i-db tables market.db --format json          # names, row counts, time ranges
h5i-db schema market.db trades --format json   # columns, types, time column, sort key
h5i-db sample market.db trades -n 20           # peek rows
h5i-db versions market.db trades --format json # commit history with ops + notes
```

## Query (read-only, safe)

```bash
h5i-db query market.db "SELECT symbol, avg(price) FROM trades GROUP BY symbol" \
  --format json --max-rows 1000 --timeout 30s
```

Time-series SQL extensions (time travel via `h5i()`, `asof_join`,
`time_bucket`, `vwap`, `ewma`, …) are catalogued in
[references/sql.md](references/sql.md).

## Ingest

```bash
h5i-db ingest market.db trades new_ticks.parquet                 # append (default, auto-retries conflicts)
h5i-db ingest market.db trades snapshot.csv --mode write         # replace the whole table
```

Appends are strict: input must be time-sorted and start at/after the table's
max timestamp. Out-of-order data → use `replace-range` or `--mode write`.
CSV/Parquet/Arrow accepted; `-` reads stdin.

## Going further

- [references/sql.md](references/sql.md) — the SQL extension catalogue
  (time travel, ASOF joins, bucketing, finance aggregates) and query stats.
- [references/mutations.md](references/mutations.md) — the plan/apply
  preview flow for destructive changes, snapshots, restore, verify, vacuum.
- [references/python.md](references/python.md) — the `h5i_db` Python API in
  four lines.
