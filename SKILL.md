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
   you (and the human reviewing you) an exact preview.
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

SQL extensions available:

| Function | Purpose |
|---|---|
| `h5i('trades')`, `h5i('trades', 42)`, `h5i('trades', '2026-07-01T00:00:00Z')`, `h5i('trades', 'snapname')` | time travel: latest / version / as-of / snapshot |
| `asof_join('trades','quotes','ts','ts','symbol'[,'backward'\|'forward'[,tolerance]])` | most-recent-quote-per-trade joins |
| `time_bucket('1m', ts)` | bucketing (also '5s', '1h', '1d', '1mo'…) |
| `vwap(price, size)` / `wavg(w, x)` | weighted aggregates |
| `ewma(x, alpha) OVER (PARTITION BY sym ORDER BY ts)` | exponential smoothing |
| `first_value/last_value(price ORDER BY ts)` | OHLC open/close |

Add `--stats` to see pruning (segments skipped) on stderr.

## Ingest

```bash
h5i-db ingest market.db trades new_ticks.parquet                 # append (default, auto-retries conflicts)
h5i-db ingest market.db trades snapshot.csv --mode write         # replace the whole table
```

Appends are strict: input must be time-sorted and start at/after the table's
max timestamp. Out-of-order data → use `replace-range` or `--mode write`.
CSV/Parquet/Arrow accepted; `-` reads stdin.

## Mutations — plan first

```bash
# 1. preview (writes staged segments, changes nothing visible)
h5i-db delete-range market.db trades --start 2026-07-01T09:30:00Z \
    --end 2026-07-01T09:31:00Z --plan --format json
# → {"plan_id": "...", "summary": {"rows_affected": 12481, ...}}

# 2. a human can inspect it in the UI (h5i-db ui market.db), or you show them
h5i-db plan show market.db trades <plan_id>

# 3. publish (fails with exit 3 if the table head moved since planning)
h5i-db plan apply market.db trades <plan_id>
# or abandon:
h5i-db plan discard market.db trades <plan_id>
```

`replace-range --input fix.parquet --plan` works the same for corrections.
If policy forbids direct mutations you'll get `policy_violation` — that is
your cue to use the plan flow, not to look for a workaround.

## Versioning safety net

```bash
h5i-db snapshot create market.db pre-experiment    # pin before risky work
h5i-db restore market.db trades 42                 # roll contents back (history kept)
h5i-db verify market.db trades --deep              # checksums + object existence
h5i-db vacuum market.db                            # dry-run of garbage collection
```

## Python

```python
import h5i_db
db = h5i_db.Database("market.db")            # read_only=True for analysis-only
df = db.sql("SELECT * FROM h5i('trades', 42)").to_pandas()
plan = db.plan_delete_range("trades", t0, t1); plan.summary; plan.apply()
```
