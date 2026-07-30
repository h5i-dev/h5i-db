# Quant Workflow Layer — Product Roadmap

Status: v2, 2026-07-29 (branch `qpian`). v1 (same date) scoped the layer to
analytics only and listed backtesting as a non-goal; v2 adds Part B, a
production-grade backtest program, and supersedes that non-goal (revised
boundary in §11). The engine-side scope rule in `ROADMAP.md` Part VII is
narrowed, not violated: the *engine crates* still borrow no backtester
mechanisms; the backtester is a separate layer crate that consumes the
engine (P6).

This document is the product roadmap for a user-facing quant research
layer built on top of the h5i-db engine: factor analysis in the spirit of
`alphalens`, performance tearsheets in the spirit of `pyfolio`/`empyrical`,
a production-grade event-driven backtester with prediction markets as the
first venue, and the versioning-native workflows none of those references
could offer. It is a companion to `ROADMAP.md`: where an engine capability
is already tracked there (Parts III-VII), this document cites the item ID
and adds only the product surface, sequencing, and acceptance criteria.
Where the two disagree, `ROADMAP.md` governs engine internals and this
document governs the user-facing API, packaging, and launch sequencing.

Sources: source-level API inventories of `~/Ref/alphalens` and
`~/Ref/pyfolio`; architecture studies of `~/Ref/nautilus_trader`,
`~/Ref/vectorbt`, `~/Ref/Lean`, and `~/Ref/prediction-market-backtesting`
(all 2026-07-29); the capability inventory of this repository at
`5b157e35`; and the zipline/qlib study behind `ROADMAP.md` Part VII.

---

## 1. Thesis

A high-performance versioned time-series engine is infrastructure, and
infrastructure markets poorly on its own. Every storage engine that won a
quant audience did it through a workflow, not a benchmark: kdb+ through
tick analytics, ArcticDB through Man Group's research stack, qlib through
its factor loop. The plan is therefore to ship a professional quant
workflow surface, from factor research through production-grade
backtesting, that a practitioner can use within five minutes of
`pip install h5i-db`, with every heavy computation running inside the
engine.

Four facts make this a good bet now:

1. **The incumbent analytics stack is a graveyard.** `alphalens`,
   `pyfolio`, and `zipline` have been unmaintained since Quantopian shut
   down in 2020; the community `*-reloaded` forks are maintenance-mode
   pandas code. The library names are still what practitioners search
   for. A maintained, engine-backed alternative inherits that search
   traffic.
2. **The differentiator is correctness, not speed.** Factor statistics are
   rarely a speed bottleneck. What pandas stacks structurally cannot offer
   is what this engine already has: point-in-time reads enforced by
   construction (`ResearchPin` event-time cutoff), immutable data versions
   under every result, copy-on-write forks for experiment sweeps, and
   arrival-time restatement detection. "A Sharpe you can cite" (VII-D2) is
   the tagline; the quant layer is its delivery vehicle.
3. **The analytics primitives already exist.** As-of joins, `cs_rank`,
   `cs_winsorize`, `cs_demean`, `cs_zscore`, `ts_corr`, `ts_cov`, `ewma`,
   `vwap`, rolling moments, `gapfill`, `resample`, and the `forks` UDTF
   are implemented and tested. What is missing is the portfolio-level
   aggregation layer (returns series, IC pipelines, quantile portfolios,
   drawdown analytics) and a rendering surface. That is a bounded amount
   of work, most of it in Python and SQL composition rather than new
   engine code.
4. **In backtesting, the hard unsolved part is the data plane, and the
   data plane is this project.** A source study of a production
   nautilus-based prediction-market backtesting stack
   (`~/Ref/prediction-market-backtesting`) shows the engineering effort
   concentrated almost entirely outside the event loop: staged loaders,
   versioned manifest-bearing caches, window-semantics bugs, coverage
   accounting, gap handling, and point-in-time redaction of
   resolution-revealing metadata. Its internal Rust migration plan
   (`internal/v4-rust-data-loading-plan.md`) independently specifies,
   almost item for item, what h5i-db already ships: canonical Arrow
   schemas, per-write manifests with requested-vs-loaded windows and
   content hashes, versioned caches, and a single owner for window
   semantics. Meanwhile the incumbent engines each leave a flank open:
   nautilus' data layer is hand-rolled DataFusion-plus-filenames,
   vectorbt's execution model is a documented toy (one order per bar,
   lookahead left to the user), and Lean's point-in-time universe
   selection, the feature hardest to bolt on, is exactly what a versioned
   store does natively. A backtester whose data plane is a versioned DB
   is a differentiated position, not an entry into someone else's moat.

**Monorepo decision (recorded):** the quant layer lives in this repository.
One star target, one README, and quant-workflow search traffic lands on the
engine. The costs (issue noise, release coupling, scope creep) are
controlled by the dependency rules in §3. Revisit only if the layer grows
storage opinions of its own; that would be the signal it should have been
a separate project.

---

## 2. Product principles

These are binding on every phase below.

- **P1: Point-in-time by default.** Every pipeline in the quant layer runs
  against a pinned read (`version=`, `as_of=`, `snapshot=`) and, where a
  decision time exists, an event-time cutoff. An unpinned run is allowed
  but is labeled as such in every output. Lookahead safety is a property of
  the API, not a discipline demanded of the user.
- **P2: Version-attributed outputs.** Every report, table, tearsheet, and
  backtest run carries a provenance header: data version SHA, pin axes
  (commit time and event time), embargo, and parameter hash. Regenerating
  from the same pin reproduces byte-identical numbers (VII-D2 acceptance
  criteria apply).
- **P3: Engine-first computation.** Panel-scale math (anything indexed by
  `(date, asset)`) compiles to SQL and runs in the engine. Python touches
  only per-date or per-quantile aggregates (thousands of rows, not
  millions). "It is a thin client over the engine" must remain literally
  true; it is also the performance story against pandas ports.
- **P4: One implementation, two surfaces.** Every headline capability is
  reachable from both Python (`h5i_db.quant`) and the CLI
  (`h5i quant ...`), sharing the engine-side implementation. This refines
  VII-D2's delegation rule: the headline stat set rides engine SQL (via
  VII-B7 aggregates plus plain window arithmetic) so the CLI and Python
  agree to the bit; only the long tail of exotic ratios and statistical
  tests is delegated to `quantstats`/`statsmodels` behind an optional
  Python extra, exactly as VII-D2 prescribes.
- **P5: Golden-tested against the reference implementations.** Every ported
  statistic is verified by executing `alphalens-reloaded` /
  `empyrical-reloaded` on the same synthetic full-mantissa fixture and
  comparing numbers, not by reading formulas; the backtest kernel is
  verified differentially against nautilus on shared scenarios (§9.4).
  Documented, deliberate divergences (tie handling, NaN policy) live in a
  divergence ledger (§9.3), never as silent differences.
- **P6: The quant layer depends on the engine, never the reverse.**
  `h5i-db-core`, `h5i-db-query`, and their CI stay green with the quant
  layer (including `h5i-db-backtest`) deleted. No quant-specific code
  paths inside the engine crates beyond the general-purpose functions
  already tracked in `ROADMAP.md`.
- **P7: Simulation, not execution.** The backtester simulates venues
  against recorded data; it never routes a live order. The
  `ExecutionClient` seam is designed so a live adapter *could* exist
  (that is what keeps the simulator honest), but brokerage adapters, live
  order routing, and operational alerting are out of scope (§11).
- **P8: Determinism is a feature, not a test property.** A backtest run
  is a pure function of (data pin, strategy, config, seed). Two runs from
  the same inputs are byte-identical: no wall clock, no unseeded
  randomness, no iteration-order dependence, deterministic tie-breaks in
  every queue. This is what makes agent-driven development and honest
  trial accounting (VII-D1) possible.

---

## 3. Packaging and architecture

| Component | Location | Contents |
|---|---|---|
| Python package | `crates/h5i-db-python/python/h5i_db/quant/` | The primary analytics surface. Pure Python composing `LazyFrame`/SQL; submodules `factor`, `perf`, `report`, `sweep`, `backtest` (the runner/config surface for Part B). |
| Backtest kernel | `crates/h5i-db-backtest` (new, Part B) | Deterministic event-driven simulation kernel, venue models, instrument models, accounts. Depends on `h5i-db-core`/`h5i-db-query`; exposed to Python through the existing Arrow-IPC native shim pattern. |
| CLI verbs | `crates/h5i-db-cli` | Nested subcommands `quant { factor, tearsheet, ic, sweep }` and `backtest { run, sweep, report, verify }` following the existing `fork {…}` pattern. Emits JSON, table, or self-contained HTML. |
| Engine functions | `crates/h5i-db-query` | Only general-purpose SQL functions, tracked in `ROADMAP.md` (VII-B1 remainder, VII-B5, VII-B7). This document adds no engine items. |
| Report renderer | shared template in `crates/h5i-db-cli` (reused by UI) | Self-contained single-file HTML in the established cookbook/film pattern: inline JS and CSS, embedded JSON data, no CDN, no build step. |
| UI route | `crates/h5i-db-ui` | `GET /report/{kind}/{name}` serving rendered reports (factor, tearsheet, backtest run, sweep comparison), following the existing single-embedded-asset handler pattern. Read-only. |
| Python extra | `h5i-db[quant]` | Optional: `quantstats`, `statsmodels` for the delegated long tail. The core quant layer must work without the extra. |

Dependency rules: `quant/` (Python) may import `h5i_db` internals; nothing
outside `quant/` may import `quant/`. `h5i-db-backtest` may depend on
`h5i-db-core` and `h5i-db-query`; no engine crate references backtest
concepts by name. The CLI may depend on both. `pandas` remains a soft
dependency used only at the interop boundary (`to_pandas()`), never inside
computations.

For Part A (analytics) no new Rust crate is created: the engine work rides
existing crates and the composition layer is Python. `h5i-db-backtest` is
Part B's crate and does not begin before milestone B0 (§10).

**Licensing constraint (binding).** nautilus_trader is LGPL-3.0, and parts
of the studied prediction-market stack are LGPL-derived. Part B borrows
*architecture* (documented in §8 with attribution to the studies), never
code: no ported functions, no translated files. Reference implementations
are used as black-box differential oracles only (§9.4). Anything requiring
line-level reading of LGPL sources to reimplement gets a clean-room note
in the PR.

---

## 4. Phase Q1 — Factor analysis (`h5i_db.quant.factor`)

Alphalens-parity factor evaluation, computed in the engine. This is the
flagship analytics phase: it exercises the differentiators (as-of
semantics, pins) on day one and produces the primary marketing asset.
Consumes `ROADMAP.md` VII-D3 and closes it.

### 4.1 API surface

Input model: a long-format factor table (`ts, asset, factor[, group]`) and
a price table (`ts, asset, price`), both readable at any pin. This is the
engine-native equivalent of alphalens' `(date, asset)` MultiIndex Series
plus wide price frame; a `from_pandas` shim accepts the exact alphalens
input shapes for migration.

```python
panel = quant.factor.build_panel(
    db,                       # or a fork / pinned Database
    factor="signals",         # table name or LazyFrame
    prices="prices",
    periods=(1, 5, 10),       # forward-return horizons, in bars
    quantiles=5,              # or bins=..., mutually exclusive
    group=None,               # column, table, or {asset: group} mapping
    binning_by_group=False,
    zero_aware=False,
    filter_zscore=20,
    max_loss=0.35,
    as_of=None, version=None, snapshot=None,   # P1: the pin
    event_time_cutoff=None,                    # ResearchPin embargo
)
```

`build_panel` returns a `FactorPanel`: a pinned `LazyFrame` of
`(ts, asset, factor, group, factor_quantile, fwd_1, fwd_5, fwd_10)` plus
metadata (pin, calendar, loss accounting). All analytics are methods that
compile to SQL and return small results:

| `FactorPanel` method | alphalens equivalent | Notes |
|---|---|---|
| `.ic(by_group=False)` | `factor_information_coefficient` | Per-date Spearman IC time series, one column per horizon. |
| `.mean_ic(by="M", by_group=False)` | `mean_information_coefficient` | Resampled via `time_bucket`. |
| `.quantile_returns(by_date=False, by_group=False, demeaned=True)` | `mean_return_by_quantile` | Returns `(mean, std_error)`. |
| `.spread(upper=Q, lower=1)` | `compute_mean_returns_spread` | Top-minus-bottom with standard error. |
| `.turnover(period=1)` | `quantile_turnover` | Per-quantile membership churn. |
| `.rank_autocorrelation(period=1)` | `factor_rank_autocorrelation` | Factor stability proxy. |
| `.weights(demeaned=True, group_adjust=False, equal_weight=False)` | `factor_weights` | Gross leverage 1 normalization. |
| `.returns(...)` | `factor_returns` | Factor-portfolio return series per horizon. |
| `.alpha_beta(...)` | `factor_alpha_beta` | Via `regr_slope`/`regr_intercept` against the universe mean return. |
| `.cumulative_returns(period)` | `factor_cumulative_returns` | Equity curve of the simulated factor portfolio. |
| `.ic_decay()` | (qlib `SigAnaRecord` analogue) | IC as a function of horizon, one query, no per-horizon loop (VII-D3 acceptance). |
| `.loss_report()` | `get_clean_factor` printout | Rows dropped in forward-return vs binning phases; raises when `max_loss` exceeded. |
| `.tearsheet(path=None)` | `create_full_tear_sheet` | §6 renderer; returns HTML or writes file. |

CLI: `h5i quant factor --db <path> --factor signals --prices prices
--periods 1,5,10 --quantiles 5 --as-of <pin> --out factor_report.html`
(plus `--format json` for agents).

### 4.2 Implementation notes (SQL lowering)

- **Forward returns:** `lead(price, p) OVER (PARTITION BY asset ORDER BY
  ts) / price - 1`, reindexed to factor timestamps via as-of join when the
  two tables' calendars differ. The `filter_zscore` clip is a whole-column
  window (`AVG`/`STDDEV` over unbounded frame) plus `CASE`.
- **Quantization:** `ntile(q) OVER (PARTITION BY ts [, group] ORDER BY
  factor)` for the quantiles path (equal-count, matching `qcut` up to tie
  placement; divergence ledger entry required). Equal-width `bins` via
  arithmetic on per-date min/max windows. `zero_aware` as two `ntile`
  passes over the sign-split halves, offset per alphalens semantics.
- **IC:** `corr(cs_rank(factor), cs_rank(fwd_p)) GROUP BY ts`. Spearman
  = Pearson on ranks; `corr` in a `GROUP BY` (not a sliding frame) is fine
  on DataFusion 54 (the sliding-frame limitation behind `ts_corr` does not
  apply here).
- **Turnover / rank autocorrelation:** self-join of quantile membership
  (respectively per-date rank vector) at `t` and `t - period` on the bar
  calendar; membership churn as anti-join counts.
- **Cumulative returns:** `exp(SUM(ln(1 + r)) OVER (ORDER BY ts)) - 1`.
- **Calendar inference:** alphalens' pandas-frequency inference is replaced
  by bar arithmetic on the observed timestamp grid (`resample`/
  `time_bucket` machinery). Horizons are in bars, labeled by the modal
  observed gap, matching alphalens' labeling behavior.
- **Deferred:** the event-study family
  (`average_cumulative_return_by_quantile`, `create_event_returns_tear_sheet`)
  needs a windowed range join around each signal date; it depends on the
  window-join gap tracked in `ROADMAP.md` (a deliberate Phase 5 engine
  gap) and ships only when that lands. The cookbook already covers the
  manual pattern (`02_alpha_research/06_event_study`).

### 4.3 Acceptance criteria

- IC, mean IC, quantile returns, spread, turnover, and rank
  autocorrelation match `alphalens-reloaded` on a full-mantissa synthetic
  fixture (multi-year, multi-asset, with NaNs, gaps, and a group mapping)
  within documented tolerance; quantile assignment differences are
  tie-cases only and are enumerated by the ledger test, not waved through.
- `max_loss` accounting reproduces alphalens' phase attribution (forward
  returns vs binning) on a fixture designed to lose rows in both phases.
- The whole pipeline runs against `version=`, `as_of=`, `snapshot=`, and a
  fork, and two runs against the same pin are byte-identical.
- With an `event_time_cutoff`, no forward return whose formation window
  crosses the cutoff appears in any output (property-tested, not spot
  checked).
- `.ic_decay()` plans a single horizon-join query (verified via `EXPLAIN`,
  per VII-D3).
- The engine path beats `alphalens-reloaded` end to end on the same machine,
  and the benchmark script is committed under `benchmarks/`. **Measured
  (2026-07-29, `benchmarks/compare_alphalens.py`, 300 assets × 4 years =
  302k rows, this machine): 2.93× with `deterministic=False`, 1.28× with the
  reproducible default.**

  The first measurement was 0.91× — *slower* than alphalens — under the
  reproducible default, and that is worth keeping in the record. The cause
  was not single-partition execution as first assumed but the SQL above it:
  `turnover` and `rank_autocorrelation` ranked distinct dates in a separate
  CTE and joined it back, which read the panel twice, and the panel is the
  expensive part. Ranking with `dense_rank()` over the panel's own rows
  gives each date its ordinal directly, because equal timestamps share a
  rank. One pass instead of two took turnover from 790 ms to 465 ms and
  rank autocorrelation from 722 ms to 448 ms, with the 30 golden tests
  unchanged. Reproducibility still costs about 2.3× against parallel
  execution; it no longer costs anything against the reference
  implementation.

### 4.4 Marketing deliverables

- Cookbook page `02_alpha_research/11_factor_evaluation` built on the new
  API, replacing the hand-rolled parts of `05_factor_construction`.
- README section "Factor research with time travel" with a hero image:
  the same factor evaluated at two data versions, showing a restatement
  changing the IC, something no pandas stack can render honestly.
- Blog-length announcement draft (docs site) targeting the searches
  "alphalens alternative", "alphalens maintained", "point-in-time factor
  analysis".

---

## 5. Phase Q2 — Performance stats and tearsheets (`h5i_db.quant.perf`)

Pyfolio-parity portfolio analytics on a returns series. Note the studied
finding: pyfolio's ratio functions are deprecated shims over `empyrical`;
the port target for the math is empyrical's implementations plus pyfolio's
genuine contributions (drawdown episode machinery, rolling stats,
`perf_stats` aggregation, tear sheet composition).

### 5.1 API surface

Minimum input is a returns series (matching
`pyfolio.create_returns_tear_sheet(returns)`): a `(ts, ret)` table,
`LazyFrame`, the output of `FactorPanel.returns()`, or a Part B backtest
run's equity table. Optional benchmark unlocks alpha/beta and rolling
beta.

| Function | Source of math | Implementation |
|---|---|---|
| `perf_stats(returns, benchmark=None)` | `empyrical` stat set + `value_at_risk`, skew, kurtosis | Engine SQL: mean/std/moments via stock aggregates and existing `skew`/`kurt`; max drawdown, VaR/CVaR, Sortino denominators via VII-B7 aggregates when landed, interim SQL forms until then. |
| `drawdown_table(returns, top=10)` | pyfolio `gen_drawdown_table` | Underwater series in SQL (running max of cumulative log return); episode segmentation (peak/valley/recovery, top-N non-overlapping) in Python over the per-date series. |
| `rolling_volatility / rolling_sharpe(returns, window)` | pyfolio | Plain SQL window frames. |
| `rolling_beta(returns, benchmark, window)` | pyfolio | `ts_cov`/rolling variance ratio (the sliding-frame `corr` limitation is why `ts_corr`/`ts_cov` exist). |
| `interesting_periods(returns)` | pyfolio `extract_interesting_date_ranges` | Static named windows table, trivially maintained. |
| `tearsheet(returns, benchmark=None, path=None)` | pyfolio `create_simple/returns_tear_sheet` | §6 renderer. |
| delegated long tail | `quantstats` / `statsmodels` behind `h5i-db[quant]` | Omega, capture ratios, Kelly, ADF/KPSS/Hurst, etc., per VII-D2: wrapped, not reimplemented, and absent (with a clear error) when the extra is not installed. |

Headline `perf_stats` set (engine-computed, P4): annual return, cumulative
return, annual volatility, Sharpe, Sortino, Calmar, max drawdown, tail
ratio, stability (R² of the log equity line), skew, kurtosis, daily VaR,
and with a benchmark: alpha, beta.

Positions/transactions analytics (gross leverage, turnover, round trips,
capacity) activate when Part B backtest runs produce positions and fills
as tables (§8.4); `perf_stats(returns)` alone is the launch scope,
mirroring pyfolio's own minimum viable tear sheet. Bootstrap/cone
forecasting is out of scope until someone asks.

CLI: `h5i quant tearsheet --db <path> --returns "<table|SQL>"
[--benchmark ...] --as-of <pin> --out tearsheet.html | --format json`.

### 5.2 Acceptance criteria

- Every headline stat matches `empyrical-reloaded` on golden fixtures
  (including edge fixtures: all-negative returns, single drawdown spanning
  the full sample, sub-year history) to documented tolerance; the
  drawdown table matches pyfolio's on peak/valley/recovery dates exactly.
- Python and CLI produce identical JSON for the same pin (P4, tested).
- The tearsheet HTML is fully self-contained (no network requests,
  verified), renders in the docs pipeline, and carries the P2 provenance
  header.
- Delegated functions raise an actionable error naming the extra when
  `quantstats` is missing, and their values pass through unmodified when
  present (VII-D2: "we are not a second implementation" for the long
  tail).

### 5.3 Marketing deliverables

- The README hero asset: a rendered tearsheet (static image + linked live
  HTML) at the top of the README, above the engine benchmarks.
- Cookbook page `03_risk_and_production/03_tearsheet` and a rewrite of
  `01_var_expected_shortfall` onto the new functions once VII-B7 lands
  (that rewrite is a VII-B7 acceptance criterion; coordinate, don't
  duplicate).

---

## 6. Report renderer and UI integration

One renderer serves all phases: a self-contained single-file HTML
template (inline CSS/JS, embedded JSON payload, no CDN, no build step),
consistent with the cookbook `.html` pages and the UI's single-embedded-
asset convention. Charts are hand-rolled SVG/canvas in vanilla JS like the
existing fork monitor; no charting dependency.

- Section 1 of every report is the provenance header (P2): version SHA,
  pins, embargo, parameters, generation command line (so a reviewer can
  regenerate it).
- The CLI writes files; the UI serves the same renderer at
  `GET /report/{kind}/{name}` behind the existing request guard,
  read-only, listing recent reports on the overview page.
- JSON is a first-class co-output of every report for agent consumption
  (the same payload the HTML embeds).
- Part B adds two report kinds: a backtest run report (equity, drawdown,
  fills, per-market settlement attribution) and a sweep comparison report
  (cross-fork aggregation).

---

## 7. Phase Q3 — Versioning-native differentiators

The features that make this layer something other than a port. These are
thin on math and heavy on wiring into the engine's versioning surface,
which is the point: nobody else can build them.

| Item | Description | Depends on |
|---|---|---|
| Q3.1 Reproducible research runs | `panel.tearsheet()` and `perf.tearsheet()` register a run manifest (pin, parameter hash, result digest) so any report can be regenerated and verified with `h5i quant verify <report>`. This is the product face of the run ledger (`ROADMAP.md` #14). | run ledger #14 |
| Q3.2 Fork sweeps | `quant.sweep(db, params, fn)`: fork per parameter combination via `fork_many`, evaluate the pipeline in each, aggregate results across forks with the `forks` UDTF into a single comparison table and report. Part B backtest sweeps (§8.5) reuse this machinery unchanged. | exists today |
| Q3.3 Restatement impact | `panel.restatement_report()`: given two versions of the inputs, recompute IC/quantile returns on both pins and diff, using `arrival_delta` to attribute the change to restated rows. Answers "did the vendor's revision change my alpha". | exists today |
| Q3.4 Overfitting statistics | Deflated Sharpe, PBO/CSCV, minimum track record, wired to the run ledger's honest trial count; surfaced in the tearsheet header ("this Sharpe survived N trials"). Product surface of VII-D1; ships when #14 does. Backtest sweeps feed it the trial matrix. | VII-D1, #14 |
| Q3.5 PIT fundamentals in the factor API | `build_panel` accepts arrival-time-aware fundamental inputs once VII-A2's `pit()` surface lands, making the embargo cover fact-level arrival, not just commit time. | VII-A2, RFC #17 |

Q3.1-Q3.3 are the launch-worthy subset and gate only on Q1; Q3.4/Q3.5
track their engine dependencies.

---

## 8. Part B — Production-grade backtesting (`h5i-db-backtest`)

An event-driven backtester whose data plane, run storage, and audit trail
are the versioned DB. Prediction markets are the first venue; the
abstractions are venue-general from day one so crypto perps and equities
follow without rework. This is deliberately a multi-milestone program: the
kernel is kept small and the scope pressure is absorbed by the model
traits, not by engine flags.

### 8.1 Why this is winnable, and why here

Evidence from the reference studies, recorded so the strategic bet stays
inspectable:

- In the studied production prediction-market stack, the event loop,
  matching, and accounting are stock nautilus; essentially all of the
  project-owned engineering is data-plane work (staged loaders, versioned
  caches with manifests, window semantics, gap handling, coverage
  accounting, PIT redaction) and result policies. That is the half of a
  backtester h5i-db already implements as a database: manifests are
  commits, cache versions are table versions, requested-vs-loaded windows
  are query pins, "reset derived state on a gap, loudly" is exactly what
  versioned deltas plus policies express. The moat is real: their own
  internal plan is a wish list for this storage layer.
- The competitive flanks are open. nautilus is excellent at simulation
  but its persistence layer is hand-rolled (DataFusion with
  filename-encoded time ranges); vectorbt's execution semantics are
  documented as unsafe (one order per symbol per bar, same-bar lookahead
  explicitly left to the user, `call_seq='auto'` presumes known prices);
  no maintained engine supports categorical prediction markets at all
  (the studied stack emulates them with paired binary instruments by
  convention); Lean-style point-in-time universe selection is absent from
  both nautilus and vectorbt and is precisely a versioned-store feature
  (VII-A3 spans).
- Determinism plus differential oracles make this tractable for
  agent-driven development (the honest answer to "a backtester is
  multi-year work"): every subsystem below has a machine-checkable
  acceptance criterion, either a property (P8 determinism, no-lookahead)
  or a black-box parity target (nautilus on shared scenarios). Agents can
  build against those oracles without taste-based review being the
  bottleneck.

### 8.2 Kernel architecture

The minimal deterministic spine, borrowed as *design* from the nautilus
study (no code; §3 licensing constraint):

- **Single-threaded, deterministic run kernel.** No async, no parallelism
  inside a run; parallelism happens across runs via forks (§8.5). All
  inter-component communication is in-process and ordered.
- **Dual timestamps everywhere.** Every replayed record carries
  `ts_event` (when it happened at the venue) and `ts_init` (when the
  system could have known it); replay order is by `ts_init` with an
  explicit per-stream tie-break priority. This aligns with, and is the
  backtest-side consumer of, the fact-level `available_at` axis (RFC #17,
  VII-A2). It is the schema decision that cannot be retrofitted, so it is
  a B0 deliverable, not a refinement.
- **Replay is a k-way merge over sorted Arrow streams** from pinned
  tables, exploiting the storage layer's declared sort order (the engine
  already avoids re-sorting; nautilus has to configure DataFusion to
  preserve file order, we get it from the manifest). Two priority queues:
  the data merge and a timer accumulator, both with total-order
  deterministic tie-breaks (never a UUID or hash order).
- **Loop invariant, in order per data item:** advance clocks, fire
  elapsed timers, venue sees the data, then strategies see the data, then
  queued strategy commands drain (orders submitted during callbacks are
  deferred, never executed inline), then the latency queue settles.
  Once per distinct timestamp: periodic venue modules (funding,
  liquidation checks, settlement observability).
- **Four small model traits, no venue flag soup:** `FillModel` (with the
  return-a-synthetic-order-book escape hatch, so L1 synthetic liquidity,
  L2 real book, and bar-derived pseudo-quotes share one matching path),
  `FeeModel`, `LatencyModel`, `VenueModule` (periodic processes returning
  balance adjustments: funding, fee waivers, liquidations). The studied
  venue config with ~35 booleans is the anti-pattern: behavior variation
  is a model impl, not a flag.
- **Models attach to the instrument, not the venue** (the Lean lesson):
  a mixed-asset backtest (a perp hedging a prediction market) needs
  per-instrument fee/fill/settlement models under one venue clock.
- **Accounting:** positions are folds over fill events; the portfolio is
  a pure projection over positions and marks; money and prices are
  fixed-point integers, never floats. Cash accounts first; margin is a
  venue-tier concern (B3).
- **Strategy-facing reads go through a `ResearchPin`** with the event-time
  cutoff advanced by the replay clock: a strategy querying the DB
  mid-backtest (for history, features, or universe membership)
  structurally cannot read past "now". No other backtester enforces this
  at the storage layer; it is the headline correctness claim.

### 8.3 Strategy API: two tiers

- **Tier 1: signal replay (callback-free).** The strategy is data: a
  pinned query producing target positions or order intents
  `(ts, instrument, target | order, limit_price?, tif?)`, computed with
  the existing `LazyFrame`/SQL surface. The kernel replays market data
  and executes the intents through the full matching/fee/latency path.
  This covers most systematic research (the vectorbt use case) with
  event-accurate execution instead of vectorbt's one-order-per-bar
  semantics, is trivially Rust-fast (no language boundary in the loop:
  the vectorbt Rust-backend lesson), and is the agent-native mode: agents
  generate queries, not stateful callback code.
- **Tier 2: event-driven strategies.** A Rust `Strategy` trait
  (`on_trade`, `on_book_deltas`, `on_bar`, `on_time_event`,
  `on_order_filled`, ...) for path-dependent logic (market making, stop
  management). Python strategies come later via batch callbacks
  (`on_bars(&[Bar])`-style, amortizing the boundary crossing; the
  nautilus lesson is that per-event Python callbacks must be designed
  around from day one, so the batch shape is the contract from the
  start).

Tier 1 ships first and is the marketing surface; Tier 2 is what makes
"production-grade" true.

### 8.4 Runs are forks

A backtest run executes inside a fork of the pinned input data and writes
its outputs as ordinary tables in that fork: `bt_orders`, `bt_fills`,
`bt_positions`, `bt_equity`, `bt_run` (the manifest: pin, config hash,
code identity, seed, requested vs simulated window, coverage). This is
the design keystone; everything downstream falls out of the existing
surface:

- Results are queryable with the same SQL/LazyFrame API as market data;
  the Q2 tearsheet consumes `bt_equity` directly; positions/transactions
  analytics (§5.1) activate for free.
- `fork_diff` answers "what changed between run A and run B" at the fill
  level; the `forks` UDTF aggregates a sweep in one query; `promote`
  publishes a blessed run's results; `drop_fork_tree` disposes of a sweep.
- The run ledger (#14) has an authoritative record per run for honest
  trial counting (Q3.4), and the review UI's fork monitor shows live
  sweep progress with no new plumbing.
- Every fill is traceable to the exact data version that produced it
  (P2), which is the auditability claim behind "production-grade".

### 8.5 Venue roadmap

**V1: prediction markets (the wedge).** Underserved (no maintained engine
supports them), data-plane-heavy (our strength), and small enough to be
honest about: no corporate actions, no margin conventions, bounded
prices, terminal resolution. Design decisions, informed by the studied
stack's gaps:

- **N-outcome categorical instruments from day one.** Outcomes are a
  first-class dimension of one instrument (binary is the 2-outcome
  case), with per-outcome books and a completeness invariant (prices sum
  to ~1) available to strategies and fill models. The studied stack's
  binary-pair-by-convention emulation is recorded as the anti-pattern.
- **Settlement is a post-run result policy gated on observability**, not
  a synthetic stream event: residual positions are marked to the
  resolved outcome only if resolution became observable within the
  simulated window (`simulated_through >= settlement_observable`);
  otherwise mark-to-market PnL stands and the report says so. Both
  numbers (`market_exit_pnl`, settlement-adjusted PnL) are kept, with
  explicit adjustment deltas, in `bt_equity`.
- **Resolution metadata is redacted by policy at read time.** Any field
  that reveals the outcome (`result`, `settlement_value`, winner flags,
  close state) is stripped from strategy-visible reads via the data
  policy layer plus the event-time cutoff; the versioned store makes
  this leak *worse* by default (the latest row knows the answer), and
  fixing it by construction rather than by adapter-side sanitization is
  the differentiated claim. The redacted slice remains available to
  post-run analytics on a separate surface.
- **Curved fee models** (`fee = q * rate * p * (1-p)` and venue
  variants, maker rebates as negative commission) as `FeeModel` impls
  with venue-documented rounding.
- **Data:** canonical book-delta and trade tables (schema per §8.6) with
  Polymarket historical data as the reference ingestion; Kalshi
  instrument/fee support lands with whatever historical book data is
  obtainable (the studied stack found L2 history to be the blocker;
  trade-only replay with synthetic-book fills is the honest fallback and
  the fill model trait supports it).

**V2: crypto perpetuals (Hyperliquid first).** 24/7 (no trading
calendars), funding as a `VenueModule`, mark/index price streams,
liquidation checks, margin accounts. Well-served by nautilus for CEXes,
but the versioned-data angle (funding/mark restatements, PIT feature
reads) still differentiates, and it is the user base adjacent to
prediction markets.

**V3: equities.** Last, deliberately: calendars, corporate actions, and
adjustment semantics are the swamp that consumed zipline, and they gate
on engine-tier work already tracked (VII-A1 adjustments, VII-A3 symbol
identity and PIT universe membership). When it lands, Lean-style
point-in-time universe selection over versioned membership spans is the
headline feature.

### 8.6 Data plane (B0)

The part with standalone value even before the kernel exists, and the
first milestone: canonical, venue-neutral market-data schemas as h5i
tables (book deltas with `event_index`/last-flag grouping, trades,
bars, instrument reference data, resolution/settlement events), each
carrying `ts_event`/`ts_init`, with ingestion recipes for the reference
vendors. Loader lessons adopted as requirements: window semantics are
half-open and owned in one place; requested vs loaded windows and
coverage ratios are first-class result fields; a gap in incremental book
data resets derived book state loudly rather than replaying across the
hole; every ingestion writes a manifest (= commit) recording source,
window, counts, and content hash. Vendor-specific fetchers live as
documented scripts/cookbook recipes, not core API surface, until a
second venue proves the abstraction.

### 8.7 Acceptance criteria

- **Determinism (P8):** two runs from the same (pin, config, seed) are
  byte-identical across `bt_*` tables, on every milestone, property-
  tested with adversarial fixtures (simultaneous timestamps across
  streams, timer/data ties, partial fills).
- **No lookahead, structurally:** a strategy that attempts to read past
  the replay clock gets cut off by the pin (tested); a strategy granted
  raw instrument metadata cannot observe resolution fields (tested
  against a deliberately leaky fixture).
- **Differential parity:** on a shared scenario suite (L2 book replay,
  taker fills, fees, latency), fills and PnL match nautilus within
  documented tolerance, treating nautilus as a black-box oracle; every
  deliberate divergence (e.g. adaptive bar high/low ordering as our
  default) is a ledger entry (§9.3).
- **Settlement honesty:** a replay window ending before resolution never
  books settlement PnL (property-tested); settlement adjustments
  reconcile exactly against the engine's own position ledger, not a
  parallel reconstruction.
- **Throughput:** a full-depth L2 day replays in seconds, benchmarked
  and committed under `benchmarks/`; sweep scaling is linear in forks up
  to machine parallelism.
- **Provenance:** `h5i backtest verify <run>` re-executes from the
  manifest and confirms digest equality (Q3.1 machinery).

### 8.8 Marketing deliverables

- The claim: the first production-grade backtester for categorical
  prediction markets, with settlement-honest PnL and structurally
  enforced no-lookahead, on a versioned data plane. Each clause is
  checkable against §8.7.
- Cookbook series: ingest Polymarket history, replay a market, run a
  Tier 1 signal strategy, sweep it across forks, read the comparison
  report; then the same strategy re-run after a data restatement
  (Q3.3 applied to backtests).
- A "why your backtest lied to you" docs page mapping each classic
  failure (lookahead, survivorship, fabricated settlement, silent gaps)
  to the mechanism here that makes it impossible, citing the acceptance
  tests.
- Benchmark and honesty comparison vs vectorbt (speed and execution
  semantics) and nautilus (data-plane ergonomics), with scripts
  committed.

---

## 8a. Part C — Agent-native experiment management

Part B assumes a human runs backtests. The actual operator is increasingly
a fleet of agents running thousands of trials, and that changes what the
layer above the backtester must do. When an agent tries 5,000 strategies
and reports the best one, the search itself is the overfitting risk, and
the human's scarce resource is no longer compute but attention and
statistical validity. This part is the thin layer that manages both.

The design bar is §8.4's: a run is a branch with tables on it, and an
experiment is a row with runs under it. Everything here is tables, fork
metadata, and one invariant; nothing is a parallel object system, and
this is explicitly not an ML experiment tracker (MLflow/W&B territory,
§11 stands).

### 8a.1 What the substrate already provides

Recorded so nobody rebuilds these as new first-class objects:

| Concept | Existing mechanism |
|---|---|
| Trial (strategy + params + data snapshot, fixed) | `RunSpec` + fork-per-run + pinned `ReadAt` + `digest()` (§8.4, `run.rs`). Content-addressed and re-checkable. |
| Evaluation policy (fees, slippage, risk, splits) | Typed `BacktestConfig` sections, all folded into the config digest; `ValidationWindows` with explicit train/holdout and no guessed embargo. |
| Experiment (one hypothesis) | `BacktestStudy` in embryo: study_id, base config, grid, per-trial metadata. Missing only a hypothesis field and accumulation over time. |
| Search budget | A `COUNT(*)` over the run ledger, not an object (§8a.2). |
| Promotion | `promote()` (first wins) plus one guard: freeze-before-reveal (§8a.3). Named pipeline stages are ceremony; the guard is the substance. |

### 8a.2 The trial-ledger invariant, and dedup

**There is no way to obtain a score without creating a recorded trial.**
This already holds by construction (a run happens inside a fork and
writes `bt_run`); it is promoted here from an accident to a guarantee,
because every honest statistic downstream leans on it. The search budget
is then a query, and Q3.4's deflated Sharpe reads a trial count that
cannot be understated.

The agent-native corollary is **dedup by digest**: agents re-submit
identical configs constantly (retries, restarts, forgetful loops), which
no human-oriented tracker had to handle. A trial whose config digest
matches an existing run returns the recorded result instead of
re-executing, and does not increment the trial count. This keeps the
budget statistic honest in the other direction (a retry is not evidence
of a wider search) and makes agent retry loops free.

### 8a.3 Sealed evaluation with disclosure accounting

The one genuinely new mechanism, and the only item in this part that
*bounds* overfitting rather than measuring it. A sealed set is a pinned
holdout (snapshot + window) that research trials never touch; candidates
are evaluated against it only through a sealed-eval endpoint, under three
rules:

- **Freeze before reveal.** Sealed metrics are disclosed only for a
  config digest that was committed before the evaluation ran. No
  tweaking a candidate after seeing its holdout number.
- **Every disclosure is counted.** The number of answers the sealed set
  has given is recorded per sealed set and per experiment, and is the
  budget that actually matters: 5,000 research trials with one
  disclosure is a valid test; fifty peeks is a burned holdout regardless
  of how disciplined the research phase looked. This is the private
  leaderboard / reusable-holdout discipline applied to backtests.
- **Disclosure can be coarsened.** Optionally return pass/fail against a
  pre-registered threshold instead of raw metrics, so each disclosure
  leaks less.

Honesty about the boundary: this is a single-user embedded database with
no auth layer, so sealing is a *protocol*, not a cryptographic wall. An
agent could read the holdout tables directly. The DB's job is to make
that visible, not impossible: sealed sets are declared, reads against
them are attributable in the ledger, and the disclosure count is itself
a recorded, queryable fact. The organizational boundary (the searching
subagent never calls sealed-eval; the orchestrator or the human does)
supplies the enforcement.

### 8a.4 Experiments and lineage as cheap metadata

- `bt_experiment`: id, hypothesis (free text), sealed-set reference,
  created-at. Trials carry an experiment id in `bt_run`/fork metadata.
  A leaderboard is a query over the experiment's trials; the deflated
  Sharpe header ("this Sharpe survived N trials") comes from the same
  scan.
- **Lineage is recorded as a claim, not a fact.** `parent_run_ids` plus
  a rationale string on the run's fork metadata, self-reported by the
  agent that authored the trial. Useful as raw material for "how did we
  get here"; never trusted for statistics (the trial count is, because
  it cannot be faked downward). A `run_id → h5i context snapshot` link
  is the audited upgrade: the workspace tool already records the agent's
  reasoning trace per commit, which makes lineage observable rather than
  self-reported. No DAG UI until the recorded lineage proves it earns
  one.

### 8a.5 Attention-routing UI

With thousands of trials the review UI's product is routing scarce human
attention, not displaying data. The model is herdr's, adopted directly:
every item carries `(state, seen)`, attention priority is ordered, and
containers roll up to the max priority of their children.

- Per-trial states, in priority order: **needs-decision** (a sealed
  evaluation or promotion is waiting on the human) > **failed/warned**
  (`RunReport.warnings()` already emits exactly the right signals:
  silent strategies, thin coverage, orders that never met a book) >
  **finished-unseen** > **running** > **seen**. Done-but-unreviewed
  outranks running; blocked-on-human outranks everything.
- An experiment inherits the max priority of its trials; the sidebar
  sorts by priority; the badge is the count of unseen-with-warnings.
  `seen` flips only when the human opens the trial detail.
- The leaderboard remains one tab among several, not the frame. What
  changed since the human last looked, what is warning, and what awaits
  a decision are the frame.

### 8a.6 Retention

5,000 trials is 5,000 forks. Policy, not accumulation: keep promoted
runs, the leaderboard top-k per experiment, and anything referenced by
lineage; `drop_fork_tree` the rest on experiment close. Constraint
carried from the engine: vacuum's orphan sweep reads the global catalog
only, so trial-fork GC must go through roots that sweep respects.

### 8a.7 Acceptance criteria

- **No off-ledger score:** property test that every metric row in any
  `bt_*` table traces to a `bt_run` entry; the trial count over a
  scripted agent session equals the number of distinct configs it tried.
- **Dedup:** submitting the same config twice yields one run, a cached
  second result, and an unchanged trial count.
- **Sealed:** evaluation against a sealed set with an unfrozen digest is
  refused (tested); each disclosure increments a queryable counter;
  coarse disclosure reveals only the pre-registered comparison.
- **Attention rollup:** done-unseen outranks running, needs-decision
  outranks everything, and container state equals max of children
  (mirroring herdr's own tests).
- **Retention:** GC over a closed experiment never collects promoted,
  top-k, or lineage-referenced forks (property-tested).

---

## 9. Cross-cutting: testing and QA

### 9.1 Golden reference harness

A dedicated venv fixture (pattern from `benchmarks/compare_baselines.py`)
pinning `alphalens-reloaded`, `pyfolio-reloaded`, `empyrical-reloaded`,
and `quantstats`. Harness generates synthetic full-mantissa panels
(seeded, committed as Parquet fixtures), runs reference and h5i
implementations, and asserts per-statistic tolerances. Runs in CI on the
Python test job; the fixture generator is committed, the fixtures are
regenerable.

### 9.2 Execution-based testing

Per the standing project lesson: SQL-generating code is tested by
executing the generated SQL and comparing numbers across verb orderings
and pin types, never by string-comparing SQL. Property tests cover the
P1 guarantees (cutoff exclusion, pin determinism) and the P8 guarantees
(run determinism, no-lookahead).

### 9.3 Divergence ledger

`docs-src/manual/quant_divergences.md`: every known, deliberate difference
from the reference implementations (ntile vs qcut tie placement, NaN
policy, calendar labeling, fill-ordering choices vs nautilus), each with a
fixture test that pins the divergence so it cannot drift silently. An
undocumented divergence found later is a bug by definition.

### 9.4 Backtest differential oracle

The nautilus-based reference stack is used as a black-box oracle: shared
scenario definitions (data + orders in, fills + PnL out) executed on both
sides in CI-adjacent tooling (the oracle side runs in the harness venv,
not in the wheel). Oracle use is black-box only (§3 licensing
constraint). Scenario coverage grows with each kernel feature; a kernel
PR without a scenario or property test does not merge.

---

## 10. Sequencing and milestones

No calendar dates; order and gates only. Part A and Part B interleave:
B0 is data-plane work that can proceed in parallel with M1-M3, and the
analytics milestones produce the tearsheet surface Part B reports plug
into.

| Milestone | Contents | Gate |
|---|---|---|
| M1 | `quant.factor` core: `build_panel`, IC family, quantile returns, spread, turnover, loss accounting; golden harness (§9.1); CLI `quant factor --format json` | Q1 acceptance criteria §4.3 |
| M2 | Renderer (§6) + factor tearsheet HTML; README hero + cookbook page; UI `/report` route | self-contained HTML verified; provenance header |
| M3 | `quant.perf`: headline `perf_stats`, drawdown table, rolling stats, returns tearsheet; CLI `quant tearsheet`; delegated long tail behind the extra | Q2 acceptance criteria §5.2 |
| M4 | Q3.1 reproducible runs + Q3.2 fork sweeps + Q3.3 restatement impact; agent-facing JSON everywhere | pin-verify round trip; sweep over N forks aggregates in one query |
| M5 | Benchmarks vs the reference analytics stack committed and cited in README; announcement post; localized README sections | numbers reproduced on a second machine |
| B0 | Canonical market-data schemas + Polymarket historical ingestion + manifests/coverage (§8.6); no kernel yet | gap/coverage/window semantics property-tested; a replayable dataset ships as a demo fixture |
| B1 | Kernel spine (§8.2) + Tier 1 signal replay + cash account + taker fills over replayed L2 + categorical instruments + observability-gated settlement; runs-as-forks (§8.4); run report | §8.7 determinism, no-lookahead, settlement honesty; tearsheet consumes `bt_equity` |
| B2 | Tier 2 Rust strategy trait; passive book fills + queue position; latency + fee models; differential oracle suite (§9.4) | §8.7 parity criteria; scenario suite in CI |
| B3 | Sweeps at scale over forks + comparison report + run ledger/Q3.4 wiring; Python batch-callback strategies; V2 venue (perps: funding module, margin, liquidation) | linear sweep scaling; DSR reads trial count from ledger |
| B4+ | V3 equities (gated on VII-A1/A3); L3/MBO fill realism as data permits | tracked engine dependencies land first |
| C1 | Trial-ledger invariant + digest dedup; `bt_experiment` + lineage metadata (§8a.2, §8a.4) | no-off-ledger-score property test; dedup criteria §8a.7 |
| C2 | Sealed evaluation: freeze-before-reveal, disclosure counter, coarse disclosure (§8a.3) | sealed criteria §8a.7; Q3.4 header reads trial count and disclosure count |
| C3 | Attention-routing UI + retention policy (§8a.5, §8a.6) | rollup and retention criteria §8a.7 |

M1 and B1 are deliberately the two largest single milestones: everything
after each has a demoable surface. C1 gates on B1 only (it needs runs and
the ledger, not realism); C2/C3 are independent of B2+ and can interleave
with it.

---

## 10a. Implementation status (2026-07-29, branch `qpian`)

Part A is built and tested; Part B has not been started. What exists:

| Milestone | State | Where |
|---|---|---|
| M1 factor evaluation | **done** | `h5i_db.quant.factor`; 30 tests, 20 of them golden comparisons against `alphalens-reloaded` 0.4.6 |
| M2 renderer | **done** | `h5i_db.quant.report`; self-containment, provenance-first, both themes and the table view are asserted; rendered and inspected in a headless browser |
| M3 performance statistics | **done** | `h5i_db.quant.perf`; 21 tests against `empyrical-reloaded` 0.5.12 including the edge fixtures |
| M4 sweeps / verify / restatement | **done** | `h5i_db.quant.sweep`; 12 tests |
| CLI | **done, relocated** | `python -m h5i_db.quant`; see the deviation below |
| M5 benchmarks | **done, and the claim was wrong** | `benchmarks/compare_alphalens.py`; see §4.3 |
| Docs | **done** | `docs-src/manual/quant.md`, examples executed in CI, divergence ledger D1–D8 |
| UI `/report` route | **done** | `GET /reports`, `/report/{name}`, `/api/reports`; traversal-refusing name check |
| README quant section | **done** | |
| Cookbook pages | **out of this repo** | the cookbook is the sibling `h5i-db-cookbook` repository |
| Event-study family (§4.2) | blocked | needs the window join, a tracked engine gap |

Part B is under way in `crates/h5i-db-backtest` (117 tests):

| Item | State | Notes |
|---|---|---|
| Fixed-point money, dual `ts_event`/`ts_init` | **done** | `types.rs`; causality violations rejected at construction |
| Half-open windows + coverage | **done** | `window.rs`; one owner, per §8.6 |
| N-outcome instruments | **done** | `instrument.rs`; binary is the 2-outcome case |
| L2 book with gap invalidation | **done** | `book.rs`; a gap is an error, not a warning |
| Deterministic k-way merge | **done** | `replay.rs`; total order on explicit fields |
| Clock + timer queue | **done** | `clock.rs`; no UUIDs, no wall clock |
| Orders, positions-as-fold, portfolio | **done** | `position.rs`; rebuildable from the fill log |
| Fee / fill / latency / module traits | **done** | `models.rs`; curved prediction-market fees |
| Observability-gated settlement | **done** | `settlement.rs` |
| Kernel loop + Tier 1 signal replay | **done** | `engine.rs`; §8.2's invariant, tested |
| Canonical tables + read/write | **done** | `schema.rs`, `store.rs`; round-trip tested including empty snapshots and truncated-snapshot refusal |
| Runs-as-forks (`bt_*` tables) | **done** | `run.rs`; base stays clean, positions rebuild from stored `bt_fills` |
| Equity curve → tearsheet | **done** | `quant.from_levels(fork, "bt_equity")` |
| Tier 2 Rust strategy trait | partial | the trait exists and is used; queue-position passive fills (B2) do not |
| Vendor ingestion (Polymarket) | not started | B0's last piece: a loader that turns vendor archives into `book_deltas` |
| Differential oracle vs nautilus | not started | §9.4 |
| Python bindings for the kernel | not started | a run is driven from Rust today; Python consumes its output tables |
| Perpetuals (funding, margin, liquidation) | **done** | `account.rs`, funding as a market event |
| Queue-position fills, amendment, self-trade prevention | **done** | L3/MBO still absent -- no L3 data to model against |
| Idempotent ingestion | **done** | content digest + `ingest_log`; backfill via `replace_range` remains a gap |
| Streaming replay | **done** | lazy sources, 5.5M events/s measured in `tests/scale.rs` |
| Execution seam | **done** | `ExecutionClient`, shared by simulator and any live adapter |
| Run metrics | **done** | `explain_silence()` |
| Overfitting / validation / cost calibration | **done** | `quant.overfitting`, `quant.validation`, `quant.costs` |
| Corporate actions, equities | **not started** | gated on VII-A1/A3 |
| Differential oracle vs nautilus | **not started** | §9.4 |

The kernel is connected to the database: a run reads pinned market data,
replays deterministically, and writes its results back as tables on its own
fork, where the existing tearsheet, `fork_diff` and cross-fork machinery
picks them up unchanged. What remains for Part B is **data in and language
out** — a vendor loader at one end, Python bindings at the other — plus the
realism work (queue position, the nautilus differential suite) and the later
venues.

Part C (§8a) is design only, though its substrate is further along than
its milestone rows suggest: config digests, fork-per-run pinning,
`BacktestStudy` with explicit train/holdout windows, leaderboards,
`promote()`, and `quant.overfitting` all exist today. What does not exist
is any of the layer itself: the ledger invariant as a tested guarantee,
digest dedup, `bt_experiment`/lineage metadata, sealed evaluation, the
attention UI, and retention policy.

Three deviations from this document, each recorded where it lives:

1. **CLI surface.** §3 puts `quant` verbs in `crates/h5i-db-cli`. Building
   them there would mean a second implementation of every statistic's SQL,
   which is what P4 exists to prevent, so the CLI is
   `python -m h5i_db.quant` instead. The native verb becomes possible once
   the SQL builders move to Rust with Python binding to them; until then one
   implementation beats two agreeing surfaces.
2. **`deterministic=True` is a new default** that §2 implied but did not
   name. Parallel plans combine partial float aggregates in a
   nondeterministic order, so "byte-identical from the same pin" is false
   under default DataFusion settings. `Database.sql(target_partitions=…)`
   was added to the engine surface (general-purpose, not quant-specific) and
   the quant layer defaults to single-partition.
3. **Version pins are per table.** A bare `version=2` cannot pin two tables
   with independent histories; `Pin.version` also accepts a
   `{table: version}` mapping, and snapshots remain the idiomatic
   multi-table pin.

## 11. Non-goals

Recorded so that scope pressure has something to push against. Revised in
v2: backtest simulation moved into scope (Part B); the boundary is now
live execution. The quant layer will not include:

- Live order routing, brokerage/exchange adapters, or operational
  alerting (Telegram and friends). The `ExecutionClient` seam exists so
  the simulator stays honest, but live trading is a different product
  with a different risk posture; revisit only as its own roadmap with its
  own safety review.
- Portfolio optimization, covariance shrinkage, or solver-bound risk
  measures (buy, don't build: `Riskfolio-Lib`, `skfolio`; see VII-B7's
  exclusion list).
- Interactive visualization or a plotting/charting API; the renderer
  emits finished self-contained reports, not figure objects or dashboards.
- ML platform features (sklearn integrations, model registries, training
  loops); the boundary is data and evaluation, and the qlib comparison in
  `ROADMAP.md` Part VII already records this.
- An options/derivatives pricing library; venue support means market
  mechanics (§8.5), not valuation models.
- Reimplementations of the delegated long tail (VII-D2 stands: wrap
  `quantstats`/`statsmodels`, never fork their math).

## 12. Risks

- **Semantics drift vs pandas references.** qcut tie handling, NaN
  propagation, and calendar inference are where a port silently diverges.
  Mitigation: §9.1 golden harness plus §9.3 ledger; divergences are
  enumerated, tested facts.
- **Backtester scope creep.** "Production-grade" is an unbounded license
  to add realism. Mitigation: the four model traits absorb realism as
  pluggable impls with per-impl acceptance tests; venue flags are
  forbidden by design review; §11 is binding; the venue roadmap adds one
  venue at a time with a demoable gate.
- **Kernel correctness is subtle and failure is silent.** Wrong fill
  ordering or a one-tick lookahead produces plausible, wrong PnL.
  Mitigation: P8 determinism as a property test, the differential oracle
  (§9.4), and the no-lookahead structural guarantee carried by the pin
  rather than by reviewer vigilance.
- **LGPL contamination from reference engines.** Mitigation: §3 binding
  constraint (architecture only, black-box oracles, clean-room notes);
  CI check that no source file cites nautilus paths outside docs.
- **Release coupling.** A quant-layer bug should never block an engine
  release. Mitigation: P6 dependency rule; quant and backtest tests are
  separate CI jobs that can be marked non-blocking for engine-only
  releases.
- **Maintenance surface of delegated deps.** `quantstats` is active today;
  if it stalls, the delegation boundary (a generic `f(returns) -> scalar`
  wrapper, per VII-D2) makes swapping or vendoring cheap.
- **Prediction-market data access.** The wedge depends on obtainable
  historical order-book data; the studied stack found Kalshi L2 history
  to be a hard blocker and vendor schemas to churn. Mitigation: B0's
  canonical schemas keep vendors at arm's length; trade-only replay with
  synthetic-book fills is a supported honest fallback; coverage fields
  make thin data visible instead of silently optimistic.
- **Two roadmaps drifting apart.** This document and `ROADMAP.md` Part VII
  share territory. Rule: engine items live only there, product items only
  here, cross-references by ID; any edit that moves an item updates both.
