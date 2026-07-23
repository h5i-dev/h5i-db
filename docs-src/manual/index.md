---
title: Overview
description: What h5i-db is, what it is for, and how the documentation is organized.
order: 0
---

# h5i-db documentation

<p class="doc-lede">h5i-db is a high-performance analytical database for quantitative
finance and time-series workloads: an embedded, versioned store with DataFusion SQL,
native ASOF joins, and previewable mutations — written in Rust, driven from the CLI
or Python, designed to be safe in the hands of AI agents.</p>

<div class="doc-divider"></div>

A database is a **single directory on disk** — like SQLite or DuckDB, there is no
server process. Data lives in immutable, time-sorted Parquet segments; every write is
an atomic commit that produces a new **version**, and any past version stays readable
forever:

```console
$ h5i-db init market.db
$ h5i-db ingest market.db trades ticks.parquet
$ h5i-db query market.db "SELECT symbol, vwap(price, size) FROM trades GROUP BY symbol"
```

```python
import h5i_db

db = h5i_db.Database("market.db")
df = db.sql("SELECT * FROM h5i('trades', 42)").to_pandas()   # time travel
```

## What makes it different

- **Time-series SQL, natively.** Full SQL through DataFusion, plus `asof_join`,
  `time_bucket`, `vwap`, `ewma`, `gapfill`, and friends. Storage is time-sorted
  and declares it, so bucketed aggregations stream instead of sorting.
- **Every write is a version.** Immutable segments and per-version manifests make
  version reads O(1) and `as_of` lookups O(log V). Named snapshots pin exact
  versions across tables, keeping backtests reproducible forever.
- **Previewable mutations.** Deletes and range replacements can be staged as
  **plans**: exact affected-row counts and before/after samples first, a
  metadata-only `apply` second. A mutation policy can *require* this flow.
- **Crash-safe by construction.** fsync-before-swap, checksums on every object,
  and a manifest hash chain. The old head survives a crash at any step.
- **Agent-ready by contract.** Machine-readable output formats, structured
  errors with a `retryable` flag, stable exit codes, and resource limits as
  flags — the same CLI and API humans use, safe to hand to automation.

## Finding your way around

<div class="card-grid">
  <a class="card" href="quickstart.html">
    <span class="card-no">MANUAL</span>
    <span class="card-title">Quickstart</span>
    <span class="card-desc">A working database in five commands — CLI and Python side by side.</span>
  </a>
  <a class="card" href="concepts.html">
    <span class="card-no">MANUAL</span>
    <span class="card-title">Core concepts</span>
    <span class="card-desc">Versions, segments, manifests, snapshots, plans, and the mutation policy.</span>
  </a>
  <a class="card" href="cli.html">
    <span class="card-no">REFERENCE</span>
    <span class="card-title">CLI reference</span>
    <span class="card-desc">Every command, flag, output format, and exit code of the <code>h5i-db</code> binary.</span>
  </a>
  <a class="card" href="sql.html">
    <span class="card-no">REFERENCE</span>
    <span class="card-title">SQL reference</span>
    <span class="card-desc">Time travel, ASOF joins, and the time-series function library beyond stock DataFusion.</span>
  </a>
  <a class="card" href="../api/">
    <span class="card-no">REFERENCE</span>
    <span class="card-title">Python API</span>
    <span class="card-desc"><code>h5i_db.Database</code>, query results, mutation plans, and typed exceptions.</span>
  </a>
  <a class="card" href="../cookbook/">
    <span class="card-no">TUTORIALS</span>
    <span class="card-title">Cookbook</span>
    <span class="card-desc">36 executed notebooks: fundamentals, market data engineering, alpha research, risk &amp; production.</span>
  </a>
</div>

## Where to go next

- Never used h5i-db? Start with [Installation](installation.html), then the
  [Quickstart](quickstart.html).
- Coming from pandas/Polars research code? The
  [Cookbook fundamentals](../cookbook/#00_fundamentals) teach the database
  concepts through market-data examples.
- Running it in production? Read the [Operations guide](operations.html) —
  backup, vacuum, compaction, and the recovery runbook.
- Wiring it into an agent or pipeline? See
  [Agents & automation](agents.html) for the machine contract.
