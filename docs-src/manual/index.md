---
title: Overview
description: What h5i-db is, what it is for, and how the documentation is organized.
order: 0
seo_title: "h5i-db manual: versioned time-series database"
---

# h5i-db documentation

<p class="doc-lede">h5i-db is a fast analytical database for quantitative
finance and time-series workloads: an embedded, versioned store with DataFusion SQL,
native ASOF joins, and previewable mutations. It is written in Rust, driven from the
CLI or Python, and built to be safe in the hands of AI agents.</p>

<div class="doc-divider"></div>

A database is a **single directory on disk**: like SQLite or DuckDB, there is no
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

- **Time-series SQL, natively:** full SQL through DataFusion, plus `asof_join`,
  `time_bucket`, `vwap`, `ewma`, and `gapfill`. Storage is time-sorted and
  declares it, so bucketed aggregations stream instead of sorting.
- **Every write is a version:** immutable segments and per-version manifests make
  version reads O(1) and `as_of` lookups O(log V). Named snapshots pin exact
  versions across tables, so a backtest stays reproducible.
- **Previewable mutations:** deletes and range replacements can be staged as
  **plans**. You get exact affected-row counts and before/after samples first,
  then a metadata-only `apply`. A mutation policy can *require* this flow.
- **Crash-safe by construction:** fsync-before-swap, checksums on every object,
  and a manifest hash chain. The old head survives a crash at any step.
- **Agent-ready by contract:** machine-readable output formats, structured
  errors with a `retryable` flag, stable exit codes, and resource limits as
  flags. It is the same CLI and API humans use, safe to hand to automation.

## Finding your way around

<div class="card-grid">
  <a class="card" href="quickstart.html">
    <span class="card-no">MANUAL</span>
    <span class="card-title">Quickstart</span>
    <span class="card-desc">A working database in five commands, CLI and Python side by side.</span>
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
  <a class="card" href="notebooks.html">
    <span class="card-no">MANUAL</span>
    <span class="card-title">Notebooks</span>
    <span class="card-desc">In-terminal Jupyter notebooks with a kernel that outlives the command, and <code>%%sql</code> cells that skip the interpreter.</span>
  </a>
  <a class="card" href="../api/">
    <span class="card-no">REFERENCE</span>
    <span class="card-title">Python API</span>
    <span class="card-desc"><code>h5i_db.Database</code>, query results, mutation plans, and typed exceptions.</span>
  </a>
  <a class="card" href="../cookbook/">
    <span class="card-no">TUTORIALS</span>
    <span class="card-title">Cookbook</span>
    <span class="card-desc">Executed notebooks: fundamentals, market data engineering, alpha research, risk &amp; production, event-driven backtesting, prediction markets, performance analytics.</span>
  </a>
</div>

## Where to go next

- Never used h5i-db? Start with [Installation](installation.html), then the
  [Quickstart](quickstart.html).
- Coming from pandas/Polars research code? The
  [Cookbook fundamentals](../cookbook/#00_fundamentals) teach the database
  concepts through market-data examples.
- Running it in production? The [Operations guide](operations.html) covers
  backup, vacuum, compaction, and the recovery runbook.
- Wiring it into an agent or pipeline? See
  [Agents & automation](agents.html) for the machine contract, and
  [Notebooks](notebooks.html) for a session whose state survives between
  commands.
- Loading vendor data? The [Data on-ramp](data-onramp.html) turns archives,
  bar files, trade dumps and live captures into the canonical tables a
  [backtest](backtest.html) reads.
