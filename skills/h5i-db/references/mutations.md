# Mutations — plan first

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

## Retry safety

Any mutation takes `--idempotency-key <token>`. A retry after an ambiguous
failure — a timeout that may or may not have committed — carries the same key,
finds the commit it already produced, and returns it with `segments_added: 0`
instead of writing the rows a second time. Duplicated ticks are silent poison:
nothing errors, the data is simply wrong from then on.

```bash
h5i-db ingest market.db trades day.parquet --idempotency-key load-2026-07-01
```

Retries are deduplicated against the last 64 commits, which covers a retry
loop; it is not a general-purpose exactly-once ledger.
