# h5i-db Roadmap — Production-Readiness Review

Findings from a full-codebase review (2026-07-22, branch `improve-poc`, ~8,200 lines
across `h5i-db-core`, `h5i-db-query`, `h5i-db-cli`, `h5i-db-ui`, `h5i-db-python`),
plus a design-vs-delivery and operational-readiness audit against `DESIGN.md`.

**Verdict.** The consistency core is unusually strong for a PoC: CAS HEAD swap with
fault-injection tests on the shipped code path, blake3 manifest checksum chains, a
coherent `{code, message, retryable, hint}` error contract across CLI/UI/Python, and
honest benchmarks. What separates it from production-grade falls into the four
buckets below, ordered by priority.

---

## 1. Correctness & durability blockers

| # | Sev | Item | Where |
|---|-----|------|-------|
| 1.1 | Critical | **Segments are never fsynced before the HEAD swap.** `SegmentWriter::flush` uses a plain put; the only `sync_objects` in the commit path covers the manifest. After power loss, a *committed* version can reference torn/empty Parquet files — violating the invariant stated at `backend.rs:16`. Fix: sync segment paths together with the manifest before the head swap. | `core/src/segment.rs:438`, `core/src/database.rs:631` |
| 1.2 | Critical | **`time_bucket` division-by-zero panic on user SQL.** Month widths are never validated: `time_bucket('0mo', ts)` or `INTERVAL '0' MONTH` panics at execution — fatal under the workspace `panic = "abort"`. Also: negative months accepted, `'999999y'` wraps. Fix: validate `months > 0` in both parse paths + `checked_mul`. | `query/src/functions.rs:96-100,144-152` |
| 1.3 | High | **Stale-lock breaking admits two live writers.** A lock older than 60s is unlinked, but head revalidation happens before `publish`, not before the final HEAD rename — a slow-but-alive writer plus a lock-breaker can both commit; last rename wins and HEAD's `manifest_checksum` can mismatch → readers see `Corruption`. Also unlink races between two breakers, and `FsLock::drop` unlinks by path unconditionally. Fix: OS-level `flock` on an open fd (re-verifying ownership before the rename only narrows the race window — it cannot close it). | `core/src/backend.rs:130-140,223-240` |
| 1.4 | High | **`block_in_place` panics on current-thread Tokio runtimes.** Any embedder on `flavor = "current_thread"` panics on the first `SELECT * FROM h5i('t')`. Fix: check `runtime_flavor()`, fall back to a scoped thread with a mini runtime. | `query/src/udtf.rs:27-36` |
| 1.5 | High | **Python wheels can abort the host interpreter.** Release builds inherit `panic = "abort"`, so pyo3 cannot convert panics to exceptions; combined with ~10 `serde_json::to_string(...).unwrap()` calls, any panic kills the user's process/kernel. Fix: unwind profile for the wheel + replace unwraps with `PyErr`. | workspace `Cargo.toml`, `python/src/lib.rs:153` et al. |
| 1.6 | High | **Review UI open to DNS rebinding / CSRF.** Loopback bind but no auth token, no Host-header validation, no CSRF header — a malicious web page can run arbitrary SQL and, with `--allow-mutations`, apply/discard plans. Fix: startup token + Host check + custom header on POSTs. | `ui/src/lib.rs:40-91,363-380` |

## 2. Performance

| # | Item | Where |
|---|------|-------|
| 2.1 | **`asof_join` UDTF bypasses all pruning/projection/limit pushdown** — full scans of both tables, full-width right side buffered; the flagship query shape pays worst-case cost. Forward `Inexact` left-side filters, widened right-side time bounds, and projections to the child scans. Single biggest user-visible perf win. | `query/src/asof.rs:817-837` |
| 2.2 | **ASOF right side buffered outside the memory pool** — invisible to `FairSpillPool`/`memory_limit`; the one operator that can OOM despite limits. Register a `MemoryConsumer` reservation; streaming merge/spill later. | `query/src/asof.rs:431-444` |
| 2.3 | **No `TableProvider::statistics()`** despite exact rows/bytes/min/max in the manifest — no metadata-only `COUNT(*)`/`MIN`/`MAX` (DESIGN §7 Tier-1 "mandatory"), no join-side selection. Free planner win: fold manifest stats into `Statistics` with `Precision::Exact`, ideally post-pruning via `FileScanConfigBuilder::with_statistics`. | `query/src/provider.rs` |
| 2.4 | **Write path materializes everything**: `write` concats the full input into one batch and one unbounded segment (`target_segment_bytes` ineffective); the core `Database::scan` API collects all matching batches (the DataFusion provider path *does* stream — see Strengths — the gap is the core API and everything built on it); CLI ingest reads whole files into memory; CLI query collects before printing. Theme: stream end-to-end (chunked sort + k-way merge on write; `impl Stream` scan API; per-batch readers on ingest; `execute_stream` on output). | `core/src/database.rs:757,1112`, `core/src/segment.rs:245,372-399`, `cli/src/ingest.rs:27-36`, `cli/src/main.rs:551` |
| 2.5 | **Asof hot-loop allocations**: per-row `OwnedRow` on build *and* probe; per-batch `RowConverter` rebuild; whole time column copied per batch. Special-case empty `by`; raw-entry map keyed on `Row::data()`. | `query/src/asof.rs:515-519,621-635,577-589` |
| 2.6 | **`AsOfJoinExec` hides its output ordering and parallelism** — declares no ordering (forcing re-sorts after joins) and is hard single-partition. Declare `left_on ASC` in `EquivalenceProperties`; build `SortPreservingMergeExec` directly; roadmap: hash-repartition by `by` keys. | `query/src/asof.rs:296-313,344-351` |
| 2.7 | **`vwap`/`wavg` lack `retract_batch`** — rolling-window frames re-accumulate from scratch, O(n·w). The (Σvw, Σw) state is trivially retractable. | `query/src/finance.rs:125-171` |
| 2.8 | **Symbol/entity predicates never prune** — min/max on interleaved tick data is useless for `symbol = 'X'`; `contained()` is a stub (DESIGN Phase 3 promised blooms). Add per-segment distinct sets or bloom filters to `ColumnStats` + Parquet column blooms at write. | `query/src/pruning.rs:131-138` |
| 2.9 | **Session/cache tension** — session-registered table names are snapshot-bound, so fresh data under those names needs a new `H5iSession` (`h5i('t')` is exempt: it re-resolves latest at planning time), and a new session drops the footer-metadata cache (~40% of warm latency) and re-resolves tables serially. Add shared `Arc<RuntimeEnv>` option, `refresh()`, concurrent resolves. | `query/src/session.rs:47-137` |
| 2.10 | **O(n²) manifest growth** — every commit rewrites the full pretty-printed segment list; small frequent appends pay O(segments) each. Manifest deltas or compact encoding; longer term a WAL/group-commit layer. | `core/src/manifest.rs:144-146` |
| 2.11 | **`batch_is_sorted` does a full lexsort** (O(n log n)) per batch on the append hot path; `time_values_i64` copies the time column up to 3× per batch. Pairwise O(n) check instead. | `core/src/segment.rs:196-236` |

## 3. Production-grade operational gaps

| # | Item | Where / fix |
|---|------|-------------|
| 3.1 | **Wheel not published** — README promises `pip install h5i-db` but no PyPI project exists; release.yml only attaches to GitHub Releases. (The cargo path is fine: README says `cargo install --path`, which works from source.) Publish via maturin, or drop the pip mention. | `.github/workflows/release.yml`, `README.md:19` |
| 3.2 | **`tracing` never initialized** — subscriber never installed in the CLI, no `TraceLayer` on the UI; all diagnostics silently dropped and mutation apply/discard (an audit surface) is unlogged. One-line init honoring `RUST_LOG`, log to stderr. | `cli/src/main.rs`, `ui/src/lib.rs` |
| 3.3 | **No version GC/retention** — every historical version pins its segments forever; unbounded storage, compliance deletion impossible, vacuum cost O(V). Note: naive deletion is not an option — `as_of` binary-searches directly-addressed sequences `0..head` and the parent-checksum chain assumes ancestors exist, so expiring versions needs a retained-chain anchor (checkpoint manifest) or version index, plus snapshot-pin awareness. | `core/src/database.rs:501-524,1345-1351` |
| 3.4 | **Vacuum edges**: orphaned table dirs not in the catalog are unreachable forever; vacuum can destroy an in-flight commit — and no grace floor fixes this, because segments are staged *before* the writer lock is acquired (and at plan time for plan/apply), so staged-but-uncommitted segments can be arbitrarily old; needs staging markers/leases or vacuum–writer coordination. `plan_protected_paths` fails *open* on storage errors. | `core/src/database.rs:1330-1341,1361-1370`, `core/src/plan.rs:503-509` |
| 3.5 | **Catalog TOCTOU**: `create_table`/`rename_table`/snapshot-create/policy writes are check-then-put with no CAS; `drop_table` takes no writer lock. Route through create-new/conditional-put semantics like HEAD. | `core/src/database.rs:286-374`, `core/src/snapshot.rs:72-78`, `core/src/policy.rs:97-103` |
| 3.6 | **Checksums not verified on normal reads**; `ReadAt::Version`/`AsOf` skip manifest verification entirely despite the parent-checksum chain existing to prove it; Parquet page CRCs not enabled at write (cheap). | `core/src/segment.rs:504-573`, `core/src/database.rs:437-459` |
| 3.7 | **UI scratchpad runs unbounded SQL** — no timeout/memory-limit/concurrency cap (row cap limits output, not execution). Wrap in `tokio::time::timeout` + session memory limit + `ConcurrencyLimitLayer`. | `ui/src/lib.rs:389-402` |
| 3.8 | **Python API**: GIL held for entire queries (no `allow_threads` anywhere — a 60 s query freezes all Python threads); no `close()`/context manager; one multi-thread runtime per handle; all SQL errors collapse to `ValueError`; missing `drop_table`/`schema`/`policy`/`compact`/`list_plans`; no timeout/max-rows knobs. | `python/src/lib.rs:92-98,186-207` |
| 3.9 | **Empty results lose their schema** in both CLI `--format arrow` (zero bytes) and the Python IPC path (`pa.table({})`). Always emit a schema-only IPC stream. | `cli/src/output.rs:57-60`, `python/src/lib.rs:239` |
| 3.10 | **Ops story missing**: no backup/restore doc (immutable segments make it nearly trivial — document the vacuum/CAS race), no operator guide (vacuum cadence, compaction, plan-TTL hygiene, disk math, NFS/WSL caveats), no torn-HEAD recovery runbook. | docs/ |
| 3.11 | **CI gaps**: no format-compatibility gate (golden old-format fixtures), no Windows tests despite shipping Windows binaries (rename-based CAS is exactly what differs there), no MSRV job (`rust-version = 1.85` can rot), no cargo-deny/audit, no fuzzing (manifest JSON, CSV/Arrow ingest, SQL), no perf-trend tracking. | `.github/workflows/ci.yml` |
| 3.12 | **CLI polish**: broken-pipe exits 5 instead of quiet; execution-time DataFusion errors misclassified as internal (exit 5); `--target-segment-mb` unchecked multiply; stdin auto-format defaults to parquet with a cryptic error; no progress reporting for long ops; `--max-bytes` promised in DESIGN §8 but absent. | `cli/src/main.rs:410,551-553`, `cli/src/output.rs:30-67` |
| 3.13 | **Segment-count limit is a hard write failure** after segments were already uploaded (orphans), with no auto-compaction. Check in `write_prologue`; trigger opportunistic compaction. | `core/src/database.rs:578-586` |
| 3.14 | **Misc correctness debt**: `filter_batches_by_time` panics on null time value from a corrupt segment (`segment.rs:615`); `append_with_retry` doesn't retry `LockTimeout` though it's classified retryable; `dedup_segments` doc claims deletion it never does; blocking fs I/O (incl. fsyncs) inline on the async executor in the commit path (`backend.rs:206-241`); UI result JSON collapses duplicate column names and reports no `truncated` flag; scan-metrics collector is session-global (concurrent queries interleave). | various |

## 4. Usefulness roadmap (features, not bugs)

Ranked by impact for the target quant/agent audience:

1. **Schema evolution** (add-column/null-backfill, widen) — the deliberate gap that
   hurts most; any evolving feed forces full rewrites. ArcticDB ships this today, so
   it is currently a differentiator *against* h5i-db.
2. **Gapfill / LOCF / interpolate** — table stakes for the finance positioning.
3. **Incremental queries between versions** — manifests already store
   `created_by_sequence`, so "rows added between v1 and v2" is a segment
   set-difference away *on append-only chains*. Compaction, `overwrite_range`,
   and `restore` rewrite or re-reference segments, so the set-difference is not
   "added rows" in general — gate the fast path on chain ops (the manifest
   records `op` per version) and fall back or error otherwise; real change
   tracking is the long-term answer. Still a genuine differentiator; makes
   incremental OHLCV maintenance nearly free.
4. **SQL `ASOF JOIN` syntax** via `RelationPlanner` (DESIGN §6.4 promised it; only
   the UDTF exists), plus `resample`/`rolling` sugar and a timezone argument for
   `time_bucket` (UTC-only today — DST-crossing daily bars misalign).
5. **Streaming/tailing** — a `TailProvider` polling manifest sequence + unbounded
   streams; today everything is snapshot-bound and batch.
6. **S3 / object-store backend** — only the local backend is constructible; the
   existence-based lock file is unsafe on NFS. Needs conditional-PUT HEAD CAS.
7. **Multi-table atomic commits** — per-table commits mean no consistent
   cross-table ingest (acknowledged in DESIGN §11.2).
8. **Cross-version joins** (`h5i('t',1) JOIN h5i('t',2)`) work today — add a test
   and a doc example; it's a differentiator hiding in plain sight.

## Credibility items (do early, they're cheap)

- **Amend DESIGN.md §13** — the Phase 2/4 ✅ marks overstate delivery: metadata-only
  aggregates, decoded-batch cache (shipped as footer cache only), quant-idiom
  rewrite rules, SQL ASOF syntax, resample sugar, bloom pruning, DuckDB
  differential tests, and the ≤10 % overhead gate (measured ~20 %) are all unmet
  within "✅" phases. The doc's honesty is its strongest asset — keep it that way.
- **Benchmarks**: DuckDB, pandas, and PyArrow baselines landed
  (`benchmarks/compare_baselines.py`). Still open: re-run on non-WSL bare-metal
  x86_64 with symmetric methodology (h5i-db is single-run, baselines best-of-3 —
  indefensible either way; use median of N≥5 both sides), the promised ArcticDB
  baseline, a Polars `set_sorted` variant to preempt the obvious objection, and
  a scaling curve (segments → thousands, versions → 10⁴).
- **README**: fix the broken `DESIGN_CLAUDE.md` link (renamed to `DESIGN.md` in
  52d9dbf); note the ASOF row implies SQL parity that doesn't exist yet.

## Suggested attack order

1. **Days**: 1.1 segment fsync · 1.2 `time_bucket` validation · 1.4 runtime-flavor
   check · 1.5 Python unwind + unwraps · 3.2 tracing init · 1.6 UI token/Host check ·
   README link · DESIGN §13 honesty pass.
2. **~1–2 weeks**: 2.1/2.2 asof pushdown + memory reservation · 2.3 `statistics()` ·
   2.4 streaming ingest/output · 3.7 UI query limits · 1.3 flock-based writer lock ·
   3.5 catalog CAS · 3.9 empty-result schema · 3.11 MSRV/Windows/format-compat CI ·
   3.1 publish to PyPI/crates.io · 3.8 GIL release.
3. **Roadmap**: schema evolution · gapfill/LOCF · 3.3 retention/GC · incremental
   queries · SQL ASOF syntax · 2.8 symbol pruning · 2.9 session refresh ·
   streaming ingest · S3 backend · 2.10 manifest deltas/WAL.

## Strengths worth preserving

- HEAD swap is textbook (temp + fsync + rename + dir fsync, CAS revalidated inside
  the critical section); fault-injection `CommitHook` exercises every commit step on
  the *shipped* code path.
- Integrity design: blake3 parent-checksum chain, self-checksummed specs/catalog/
  snapshots/plans, precise `Corruption {object, detail}` errors, hashed path names
  with tamper detection.
- Scan path is genuinely streaming with projection/limit pushdown and per-segment
  parallelism; declared output ordering is *sound* (time column enforced as first
  sort key) — that's what wins OHLCV 11× without a sort.
- Pruning fails open everywhere; correctness never depends on stats.
- Plan/apply review flow: checksummed, TTL'd, vacuum-protected, fail-closed on
  tamper/expiry; stale plans 409, never silent re-plan — tested end-to-end from
  CLI and UI.
- Coherent error contract (stable exit codes + machine-readable envelope) verified
  by tests that run the real binary; least-privilege read-only opens layered under
  the UI's HTTP guard; XSS-proof frontend by construction.
- Honest benchmark write-up (interleaved cache controls, disclosed bias) and an
  OOM-safe CI matrix with a real wheel install smoke test.
