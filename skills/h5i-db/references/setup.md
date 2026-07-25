# Creating a database, and the guardrails worth setting up front

## Tables

```bash
h5i-db init market.db
```

A database is a directory. `init` creates it; everything else lives inside.

Two ways to give a table its schema:

```bash
# Infer it from a data file (Parquet, CSV or Arrow)
h5i-db create-table market.db trades --like ticks.parquet --time-column ts

# Or state it explicitly
h5i-db create-table market.db trades --time-column ts --schema \
  '[{"name":"ts","type":"timestamp_ns","nullable":false},
    {"name":"symbol","type":"utf8"},
    {"name":"price","type":"float64"},
    {"name":"size","type":"int64"}]'
```

Types: `int8`…`int64`, `uint8`…`uint64`, `float32`/`float64`, `utf8`, `bool`,
`date32`/`date64`, `timestamp_s`/`_ms`/`_us`/`_ns`.

`--time-column` is not optional in practice. Without one the table loses
segment pruning, the ASOF join, `time_bucket`, and research mode — a table with
no time column refuses an event-time cutoff outright rather than being silently
exempt from it. Inferred schemas mark every column nullable; the time column is
tightened to non-null automatically.

`--sort-key` defaults to the time column and must start with it. `--target-segment-mb`
(default 128) sets how large segments grow before a new one starts.

## Loading data

```bash
h5i-db ingest market.db trades ticks.parquet --idempotency-key load-1
h5i-db ingest market.db trades - --input-format csv            # stdin
h5i-db ingest market.db trades snapshot.parquet --mode write   # replace contents
```

`append` (the default) is strict: input must be sorted by the time column and
start at or after the table's current maximum. Out-of-order data is a
`sort_order_violation`, and its `next_actions` name the two real escapes —
`replace-range --plan` for a bounded correction, `--mode write` for a full
restatement. Do not work around it by sorting the table's history away.

## Data-safety policy (opt-in, per table)

Typed constraints checked on every write *and* at plan time, so malformed rows
are refused before they can be committed rather than found later:

```bash
h5i-db data-policy set market.db trades '{
  "constraints": [
    {"name": "positive_price",
     "predicate": {"compare": {"column": "price", "op": "gt", "value": {"float": 0.0}}},
     "on_fail": "reject"}
  ]}'

h5i-db data-policy get market.db trades      # null when unset
h5i-db data-policy clear market.db trades
```

Predicates compose with `and` / `or` / `not` over `not_null`, `compare`
(`eq`/`ne`/`lt`/`lte`/`gt`/`gte`) and `in_set`. Fail-closed: NULL never
satisfies a comparison. `on_fail` is `reject` or `warn`.

## Mutation policy (per database)

Which operations may commit without a reviewed plan:

```bash
h5i-db policy show market.db
h5i-db policy set market.db direct_delete=false direct_replace=false
```

Keys: `direct_append`, `direct_write`, `direct_replace`, `direct_delete`,
`direct_restore`, `direct_compact`. Turning one off is how you make the
plan/apply gate mandatory for an agent rather than advisory.

## Housekeeping

```bash
h5i-db compact market.db trades      # rewrite small segments into target-sized ones
h5i-db vacuum market.db              # dry run; --apply to actually delete
h5i-db verify market.db trades --deep
```

## Seeing it work first

```bash
h5i-db demo            # --keep to inspect the database it builds
```

Builds a small database and walks the whole arc — ingest, the metric a strategy
would have traded on, a vendor restatement previewed through plan/apply, the
leakage that correction reveals, and a session that cannot read past its
decision instant. Takes about a second, and every number printed comes from the
database it just built.
