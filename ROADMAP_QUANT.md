# Quant Workflow Layer — Product Roadmap

Status: draft v1, 2026-07-29 (branch `qpian`).

This document is the product roadmap for a user-facing quant research layer
(`h5i quant`) built on top of the h5i-db engine: factor analysis in the
spirit of `alphalens`, performance tearsheets in the spirit of
`pyfolio`/`empyrical`, and the versioning-native workflows that neither of
those libraries could offer. It is a companion to `ROADMAP.md`: where an
engine capability is already tracked there (Parts III-VII), this document
cites the item ID and adds only the product surface, sequencing, and
acceptance criteria. Where the two disagree, `ROADMAP.md` governs engine
internals and this document governs the user-facing API, packaging, and
launch sequencing.

Sources: source-level API inventories of `~/Ref/alphalens` and
`~/Ref/pyfolio` (2026-07-29), the capability inventory of this repository at
`5b157e35`, and the zipline/qlib study behind `ROADMAP.md` Part VII.

---

## 1. Thesis

A high-performance versioned time-series engine is infrastructure, and
infrastructure markets poorly on its own. Every storage engine that won a
quant audience did it through a workflow, not a benchmark: kdb+ through tick
analytics, ArcticDB through Man Group's research stack, qlib through its
factor loop. The plan is therefore to ship a small, professional quant
research surface that a practitioner can use within five minutes of
`pip install h5i-db`, with every heavy computation running inside the
engine.

Three facts make this a good bet now:

1. **The incumbent stack is a graveyard.** `alphalens`, `pyfolio`, and
   `zipline` have been unmaintained since Quantopian shut down in 2020; the
   community `*-reloaded` forks are maintenance-mode pandas code. The
   library names are still what practitioners search for. A maintained,
   engine-backed alternative inherits that search traffic.
2. **The differentiator is correctness, not speed.** Factor statistics are
   rarely a speed bottleneck. What pandas stacks structurally cannot offer
   is what this engine already has: point-in-time reads enforced by
   construction (`ResearchPin` event-time cutoff), immutable data versions
   under every result, copy-on-write forks for experiment sweeps, and
   arrival-time restatement detection. "A Sharpe you can cite" (VII-D2) is
   the tagline; the quant layer is its delivery vehicle.
3. **The primitives already exist.** As-of joins, `cs_rank`,
   `cs_winsorize`, `cs_demean`, `cs_zscore`, `ts_corr`, `ts_cov`, `ewma`,
   `vwap`, rolling moments, `gapfill`, `resample`, and the `forks` UDTF are
   implemented and tested. What is missing is the portfolio-level
   aggregation layer (returns series, IC pipelines, quantile portfolios,
   drawdown analytics) and a rendering surface. That is a bounded amount of
   work, most of it in Python and SQL composition rather than new engine
   code.

**Monorepo decision (recorded):** the quant layer lives in this repository.
One star target, one README, and quant-workflow search traffic lands on the
engine. The costs (issue noise, release coupling, scope creep) are
controlled by the dependency rules in §3. Revisit only if the layer grows
storage or execution opinions of its own; that would be the signal it
should have been a separate project.

---

## 2. Product principles

These are binding on every phase below.

- **P1: Point-in-time by default.** Every pipeline in the quant layer runs
  against a pinned read (`version=`, `as_of=`, `snapshot=`) and, where a
  decision time exists, an event-time cutoff. An unpinned run is allowed
  but is labeled as such in every output. Lookahead safety is a property of
  the API, not a discipline demanded of the user.
- **P2: Version-attributed outputs.** Every report, table, and tearsheet
  carries a provenance header: data version SHA, pin axes (commit time and
  event time), embargo, and parameter hash. Regenerating from the same pin
  reproduces byte-identical numbers (VII-D2 acceptance criteria apply).
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
  comparing numbers, not by reading formulas. Documented, deliberate
  divergences (tie handling, NaN policy) live in a divergence ledger
  (§8.3), never as silent differences.
- **P6: The quant layer depends on the engine, never the reverse.**
  `h5i-db-core`, `h5i-db-query`, and their CI stay green with the quant
  layer deleted. No quant-specific code paths inside the engine crates
  beyond the general-purpose functions already tracked in `ROADMAP.md`.
- **P7: Analytics, not execution.** No event-driven backtester, no order
  simulation, no live trading, ever (the Part VII scope rule). §10 has the
  full non-goals list.

---

## 3. Packaging and architecture

| Component | Location | Contents |
|---|---|---|
| Python package | `crates/h5i-db-python/python/h5i_db/quant/` | The primary surface. Pure Python composing `LazyFrame`/SQL; submodules `factor`, `perf`, `report`, `sweep`. No new native code initially. |
| CLI verbs | `crates/h5i-db-cli` | New nested subcommand `quant { factor, tearsheet, ic, sweep }` following the existing `fork {…}` pattern. Emits JSON, table, or self-contained HTML. |
| Engine functions | `crates/h5i-db-query` | Only general-purpose SQL functions, tracked in `ROADMAP.md` (VII-B1 remainder, VII-B5, VII-B7). This document adds no engine items. |
| Report renderer | shared template in `crates/h5i-db-cli` (reused by UI) | Self-contained single-file HTML in the established cookbook/film pattern: inline JS and CSS, embedded JSON data, no CDN, no build step. |
| UI route | `crates/h5i-db-ui` | `GET /report/{kind}/{name}` serving rendered reports, following the existing single-embedded-asset handler pattern. Read-only. |
| Python extra | `h5i-db[quant]` | Optional: `quantstats`, `statsmodels` for the delegated long tail. The core quant layer must work without the extra. |

Dependency rules: `quant/` (Python) may import `h5i_db` internals; nothing
outside `quant/` may import `quant/`. The CLI `quant` module may depend on
`h5i-db-query`; `h5i-db-query` never references quant concepts by name.
`pandas` remains a soft dependency used only at the interop boundary
(`to_pandas()`), never inside computations.

A dedicated Rust crate (`h5i-db-quant`) is deliberately **not** created in
Q1-Q3. The engine work rides existing crates; the composition layer is
Python. Create the crate only if profiling shows a per-date sequential
computation (drawdown episode detection, bootstrap resampling) that is both
hot and awkward in SQL; that is a Q4+ decision.

---

## 4. Phase Q1 — Factor analysis (`h5i_db.quant.factor`)

Alphalens-parity factor evaluation, computed in the engine. This is the
flagship phase: it exercises the differentiators (as-of semantics, pins)
on day one and produces the primary marketing asset. Consumes `ROADMAP.md`
VII-D3 and closes it.

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
- **Deferred to Q3+:** the event-study family
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
- A panel of 2,500 assets × 15 years of daily bars builds and evaluates
  faster than `alphalens-reloaded` end to end on the same machine, and the
  benchmark script is committed under `benchmarks/`.

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
`LazyFrame`, or the output of `FactorPanel.returns()`. Optional benchmark
unlocks alpha/beta and rolling beta.

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

Deferred within this phase: positions/transactions analytics (gross
leverage, turnover, round trips, capacity) until Q4's portfolio bridge
produces positions; `perf_stats(returns)` alone is the launch scope,
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

One renderer serves both phases: a self-contained single-file HTML
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

---

## 7. Phase Q3 — The differentiators

The features that make this layer something other than "alphalens in
Rust". These are thin on math and heavy on wiring into the engine's
versioning surface, which is the point: nobody else can build them.

| Item | Description | Depends on |
|---|---|---|
| Q3.1 Reproducible research runs | `panel.tearsheet()` and `perf.tearsheet()` register a run manifest (pin, parameter hash, result digest) so any report can be regenerated and verified with `h5i quant verify <report>`. This is the product face of the run ledger (`ROADMAP.md` #14). | run ledger #14 |
| Q3.2 Fork sweeps | `quant.sweep(db, params, fn)`: fork per parameter combination via `fork_many`, evaluate the factor pipeline in each, aggregate results across forks with the `forks` UDTF into a single comparison table and report. The agent-facing story: an AI agent explores a factor family without ever mutating shared data, and the sweep is auditable. | exists today |
| Q3.3 Restatement impact | `panel.restatement_report()`: given two versions of the inputs, recompute IC/quantile returns on both pins and diff, using `arrival_delta` to attribute the change to restated rows. Answers "did the vendor's revision change my alpha". | exists today |
| Q3.4 Overfitting statistics | Deflated Sharpe, PBO/CSCV, minimum track record, wired to the run ledger's honest trial count; surfaced in the tearsheet header ("this Sharpe survived N trials"). Product surface of VII-D1; ships when #14 does. | VII-D1, #14 |
| Q3.5 PIT fundamentals in the factor API | `build_panel` accepts arrival-time-aware fundamental inputs once VII-A2's `pit()` surface lands, making the embargo cover fact-level arrival, not just commit time. | VII-A2, RFC #17 |

Q3.1-Q3.3 are the launch-worthy subset and gate only on Q1; Q3.4/Q3.5
track their engine dependencies.

---

## 8. Cross-cutting: testing and QA

### 8.1 Golden reference harness

A dedicated venv fixture (pattern from `benchmarks/compare_baselines.py`)
pinning `alphalens-reloaded`, `pyfolio-reloaded`, `empyrical-reloaded`,
and `quantstats`. Harness generates synthetic full-mantissa panels
(seeded, committed as Parquet fixtures), runs reference and h5i
implementations, and asserts per-statistic tolerances. Runs in CI on the
Python test job; the fixture generator is committed, the fixtures are
regenerable.

### 8.2 Execution-based testing

Per the standing project lesson: SQL-generating code is tested by
executing the generated SQL and comparing numbers across verb orderings
and pin types, never by string-comparing SQL. Property tests cover the
P1 guarantees (cutoff exclusion, pin determinism).

### 8.3 Divergence ledger

`docs-src/manual/quant_divergences.md`: every known, deliberate difference
from the reference libraries (ntile vs qcut tie placement, NaN policy,
calendar labeling), each with a fixture test that pins the divergence so
it cannot drift silently. An undocumented divergence found later is a bug
by definition.

---

## 9. Sequencing and milestones

No calendar dates; order and gates only.

| Milestone | Contents | Gate |
|---|---|---|
| M1 | `quant.factor` core: `build_panel`, IC family, quantile returns, spread, turnover, loss accounting; golden harness (§8.1); CLI `quant factor --format json` | Q1 acceptance criteria §4.3 |
| M2 | Renderer (§6) + factor tearsheet HTML; README hero + cookbook page; UI `/report` route | self-contained HTML verified; provenance header |
| M3 | `quant.perf`: headline `perf_stats`, drawdown table, rolling stats, returns tearsheet; CLI `quant tearsheet`; delegated long tail behind the extra | Q2 acceptance criteria §5.2 |
| M4 | Q3.1 reproducible runs + Q3.2 fork sweeps + Q3.3 restatement impact; agent-facing JSON everywhere | pin-verify round trip; sweep over N forks aggregates in one query |
| M5 | Benchmarks vs the reference stack committed and cited in README; announcement post; localized README sections | numbers reproduced on a second machine |
| M6+ | Event-study family (window join), Q3.4 overfitting stats (#14), Q3.5 PIT fundamentals (VII-A2), positions/transactions analytics (Q4 bridge) | tracked engine dependencies land first |

M1 is deliberately the largest single milestone: everything after it has a
demoable surface.

---

## 10. Non-goals

Recorded so that scope pressure has something to push against. The quant
layer will not include:

- An event-driven backtester, order simulation, slippage models, trading
  calendars as a scheduling service, or live trading (the Part VII scope
  rule; `nautilus_trader`/`vectorbt` own that ground, and positioning
  h5i-db as the data layer beneath them is the better trade).
- Portfolio optimization, covariance shrinkage, or solver-bound risk
  measures (buy, don't build: `Riskfolio-Lib`, `skfolio`; see VII-B7's
  exclusion list).
- A plotting library or charting API; the renderer emits finished
  reports, not figure objects.
- Data vendor ingestion adapters (a separate concern; the existing
  `ingest` surface is the boundary).
- Reimplementations of the delegated long tail (VII-D2 stands: wrap
  `quantstats`/`statsmodels`, never fork their math).

## 11. Risks

- **Semantics drift vs pandas references.** qcut tie handling, NaN
  propagation, and calendar inference are where a port silently diverges.
  Mitigation: §8.1 golden harness plus §8.3 ledger; divergences are
  enumerated, tested facts.
- **Scope creep toward a backtester.** `factor_cumulative_returns` is two
  steps from "just add transaction costs". Mitigation: §10 is binding;
  cost modeling beyond a flat haircut parameter is out.
- **Release coupling.** A quant-layer bug should never block an engine
  release. Mitigation: P6 dependency rule; quant tests are a separate CI
  job that can be marked non-blocking for engine-only releases.
- **Maintenance surface of delegated deps.** `quantstats` is active today;
  if it stalls, the delegation boundary (a generic `f(returns) -> scalar`
  wrapper, per VII-D2) makes swapping or vendoring cheap.
- **Two roadmaps drifting apart.** This document and `ROADMAP.md` Part VII
  share territory. Rule: engine items live only there, product items only
  here, cross-references by ID; any edit that moves an item updates both.
