//! Running a backtest against the database (ROADMAP_QUANT.md §8.4).
//!
//! A run executes **inside a fork** and writes its results there as ordinary
//! tables. That one decision is what the rest falls out of: results are
//! queryable with the same SQL as market data, two runs diff at fill level
//! with `fork_diff`, a sweep aggregates with one cross-fork scan, and a
//! blessed run is published with `promote`. None of it needed new
//! machinery, because a run is just a branch with tables on it.
//!
//! The order of operations here is also the look-ahead guarantee. Market
//! data is read before the run; **resolutions are read after it**, by the
//! settlement step alone. There is no point in the sequence at which the
//! strategy could reach the answer, because at the time it runs the answer
//! has not been loaded.

use std::collections::BTreeMap;

use h5i_db_core::Database;
use h5i_db_core::database::ReadAt;

use crate::engine::{Engine, EngineBuilder, RunResult, Strategy};
use crate::error::{BacktestError, Result};
use crate::instrument::{InstrumentId, InstrumentSet, OutcomeId};
use crate::replay::{Replay, priority};
use crate::settlement::{Resolution, SettlementReport, settle, validate_resolutions};
use crate::store;
use crate::types::{Money, Price, UnixNanos};
use crate::window::{Coverage, TimeWindow};

/// What to run, and over what.
#[derive(Clone, Debug)]
pub struct RunSpec {
    /// Names the fork and the `bt_run` row.
    pub run_id: String,
    /// The slice of history to replay. `None` replays everything available.
    pub window: Option<TimeWindow>,
    pub starting_cash: Money,
    /// The read point for market data. A pinned run is reproducible; an
    /// unpinned one is recorded as such.
    ///
    /// Prefer `Snapshot` or `AsOf`: those name one instant across every
    /// table, which is what a run needs. `Version(n)` is a *per-table*
    /// concept -- table A's version 3 has nothing to do with table B's -- so
    /// applying one integer to a whole run only means something when the
    /// tables genuinely share a history. Tables with no version at the pin
    /// read as empty rather than failing, which is what "that data did not
    /// exist yet" looks like from the past.
    pub read_at: ReadAt,
    /// Equity curve resolution, in nanoseconds of simulated time.
    pub equity_interval_nanos: i64,
    /// Refuse to run when the loaded data covers less than this fraction of
    /// the requested window.
    pub minimum_coverage: Option<f64>,
    /// How the execution model was configured, as the caller describes it.
    ///
    /// The fee, fill, latency and margin models are boxed trait objects on
    /// the builder, so a `RunSpec` cannot inspect them, yet they change the
    /// result completely: two runs identical here but priced under different
    /// fee curves are different computations. Whoever assembles the models
    /// records them as a string, and `digest` folds it in. Leaving it `None`
    /// says only "the caller did not describe its models", which `digest`
    /// then reports as an unfingerprinted run rather than pretending the
    /// models matched.
    pub execution_fingerprint: Option<String>,
}

impl RunSpec {
    pub fn new(run_id: impl Into<String>, starting_cash: Money) -> Self {
        Self {
            run_id: run_id.into(),
            window: None,
            starting_cash,
            read_at: ReadAt::Latest,
            equity_interval_nanos: crate::engine::DEFAULT_EQUITY_INTERVAL_NANOS,
            minimum_coverage: None,
            execution_fingerprint: None,
        }
    }

    /// Record how the execution model was configured, so `digest` covers it.
    pub fn execution_fingerprint(mut self, fingerprint: impl Into<String>) -> Self {
        self.execution_fingerprint = Some(fingerprint.into());
        self
    }

    pub fn window(mut self, window: TimeWindow) -> Self {
        self.window = Some(window);
        self
    }

    pub fn read_at(mut self, at: ReadAt) -> Self {
        self.read_at = at;
        self
    }

    pub fn equity_interval_nanos(mut self, nanos: i64) -> Self {
        self.equity_interval_nanos = nanos;
        self
    }

    pub fn minimum_coverage(mut self, fraction: f64) -> Self {
        self.minimum_coverage = Some(fraction);
        self
    }

    /// The fork this run's results live on.
    pub fn fork_name(&self) -> String {
        format!("bt-{}", self.run_id)
    }

    /// A stable hash of what this spec says about the computation.
    ///
    /// Deliberately *not* keyed on `run_id`: a run is identified by its
    /// inputs, so re-running the same computation under a new name has to
    /// land on the same digest or the ledger cannot recognise it.
    ///
    /// The spec cannot see the execution models, so anything it does not
    /// know about has to arrive through `execution_fingerprint`. A spec
    /// carrying no fingerprint hashes as one, and two such runs matching
    /// means only that their *specs* agree: it is not evidence that they
    /// were priced the same way.
    pub fn digest(&self) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.starting_cash.raw().to_le_bytes());
        hasher.update(&self.equity_interval_nanos.to_le_bytes());
        match self.window {
            Some(window) => {
                hasher.update(b"w");
                hasher.update(&window.start().get().to_le_bytes());
                hasher.update(&window.end().get().to_le_bytes());
            }
            None => {
                hasher.update(b"-");
            }
        }
        hasher.update(format!("{:?}", self.read_at).as_bytes());
        match &self.execution_fingerprint {
            Some(fingerprint) => {
                hasher.update(b"x");
                hasher.update(fingerprint.as_bytes());
            }
            None => {
                hasher.update(b"-");
            }
        }
        hasher.finalize().to_hex().to_string()
    }
}

/// One scored forecast: what the strategy said, what the market said, and
/// what happened.
///
/// The triple a Brier score, a reliability curve or an advantage-over-market
/// series is computed from. It is assembled *after* the run, from the
/// forecasts the strategy stated during it and the resolutions loaded once
/// it had finished, so producing it cannot be a route by which the answer
/// reaches the strategy.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CalibrationSample {
    pub ts: UnixNanos,
    pub instrument: InstrumentId,
    pub outcome: OutcomeId,
    /// What the strategy said the probability was.
    pub forecast: Price,
    /// What the market said at the same instant, where a book was quoting.
    pub market: Option<Price>,
    /// What the outcome paid: one, zero, or a fraction for a partial
    /// settlement.
    pub realized: Price,
    /// The strategy's own label for this forecast, if it set one.
    pub tag: Option<String>,
}

/// Why a stated forecast could not be scored.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct UnscoredForecast {
    pub instrument: InstrumentId,
    pub outcome: OutcomeId,
    pub reason: String,
    pub count: usize,
}

/// Everything a finished run produced.
#[derive(Clone, Debug)]
pub struct RunReport {
    pub run_id: String,
    pub fork: String,
    pub digest: String,
    pub result: RunResult,
    pub settlement: SettlementReport,
    pub coverage: Option<Coverage>,
    pub instruments: InstrumentSet,
    /// How the markets this run traded ended. Loaded after the run, and
    /// carried here so a report can score forecasts and attribute
    /// settlement without a second read.
    pub resolutions: Vec<Resolution>,
}

impl RunReport {
    /// Warnings a reader must see: unsettled positions, thin coverage,
    /// orders that never met a book.
    pub fn warnings(&self) -> Vec<String> {
        let mut out = self.settlement.warnings();
        // An order submitted before its instrument's first book has nothing
        // to match against and cancels. That is correct, and silent, and
        // exactly the shape of a strategy that looks like it did nothing
        // for no visible reason -- so it is reported.
        let unfilled = self
            .result
            .orders
            .iter()
            .filter(|order| {
                order.status == crate::order::OrderStatus::Cancelled && order.filled.is_zero()
            })
            .count();
        if unfilled > 0 {
            out.push(format!(
                "{unfilled} order(s) were cancelled without filling; the \
                 usual cause is acting before the instrument's first book \
                 update, or a limit the book never reached"
            ));
        }
        if let Some(reason) = self.result.metrics.explain_silence() {
            out.push(reason);
        }
        if let Some(coverage) = self.coverage
            && !coverage.is_complete()
        {
            out.push(format!(
                "data covered {:.1}% of the requested window",
                coverage.ratio() * 100.0
            ));
        }
        for dropped in self.unscored_forecasts() {
            out.push(format!(
                "{} forecast(s) on {} outcome {} were not scored: {}",
                dropped.count, dropped.instrument, dropped.outcome, dropped.reason
            ));
        }
        for operation in &self.result.set_operations {
            if let Some(reason) = &operation.rejected {
                out.push(format!(
                    "a {} of {} was refused: {reason}",
                    operation.kind.as_str(),
                    operation.instrument
                ));
            }
        }
        out
    }

    /// The scored forecasts this run produced.
    ///
    /// One sample per forecast the strategy stated on a market that
    /// resolved to something worth scoring. Feed these to a Brier score, a
    /// reliability curve, or an advantage-over-market series.
    ///
    /// Two kinds of forecast are dropped rather than scored, and both are
    /// reported by [`RunReport::unscored_forecasts`] so the sample size is
    /// never silently smaller than the strategy thinks. A market with no
    /// known resolution has no outcome to score against. A market that
    /// **voided**, or otherwise paid every outcome the same, scores every
    /// forecast identically no matter what it said -- including forecasts
    /// that were confidently wrong -- so including it does not measure a
    /// forecaster, it dilutes the sample that does.
    pub fn calibration_samples(&self) -> Vec<CalibrationSample> {
        let by_instrument = self.resolutions_by_instrument();
        self.result
            .forecasts
            .iter()
            .filter_map(|forecast| {
                let resolution = by_instrument.get(&forecast.instrument)?;
                if !resolution.payout.is_informative() {
                    return None;
                }
                Some(CalibrationSample {
                    ts: forecast.ts,
                    instrument: forecast.instrument.clone(),
                    outcome: forecast.outcome,
                    forecast: forecast.probability,
                    market: forecast.market,
                    realized: resolution.settlement_price(forecast.outcome),
                    tag: forecast.tag.clone(),
                })
            })
            .collect()
    }

    /// Forecasts that could not be scored, grouped by market and outcome.
    pub fn unscored_forecasts(&self) -> Vec<UnscoredForecast> {
        let by_instrument = self.resolutions_by_instrument();
        let mut grouped: BTreeMap<(InstrumentId, OutcomeId), (String, usize)> = BTreeMap::new();
        for forecast in &self.result.forecasts {
            let reason = match by_instrument.get(&forecast.instrument) {
                None => "this market has no known resolution",
                Some(resolution) if !resolution.payout.is_informative() => {
                    "this market paid every outcome the same, so any forecast \
                     scores identically against it"
                }
                Some(_) => continue,
            };
            let entry = grouped
                .entry((forecast.instrument.clone(), forecast.outcome))
                .or_insert((reason.to_string(), 0));
            entry.1 += 1;
        }
        grouped
            .into_iter()
            .map(
                |((instrument, outcome), (reason, count))| UnscoredForecast {
                    instrument,
                    outcome,
                    reason,
                    count,
                },
            )
            .collect()
    }

    fn resolutions_by_instrument(&self) -> BTreeMap<&InstrumentId, &Resolution> {
        self.resolutions
            .iter()
            .map(|resolution| (&resolution.instrument, resolution))
            .collect()
    }
}

/// Run a strategy against stored market data, inside a fork of it.
///
/// `configure` receives the engine builder so a caller can attach fee,
/// fill and latency models without this function growing a parameter per
/// model.
pub async fn run_in_fork<F>(
    base: &Database,
    spec: RunSpec,
    strategy: &mut dyn Strategy,
    configure: F,
) -> Result<RunReport>
where
    F: FnOnce(EngineBuilder) -> EngineBuilder,
{
    // Note for anyone optimising this: creating the `bt_*` tables on the base
    // before forking would let the fork inherit them, saving five metadata
    // commits per run (~65 ms, measured in `benches/replay_path.rs`). Do not.
    // `a_run_lives_on_a_fork_and_leaves_the_base_untouched` is the guarantee
    // that a run touches nothing outside its branch, and an empty `bt_fills`
    // appearing in the base catalog because someone once ran a backtest is
    // exactly the kind of leak that guarantee exists to forbid. The saving is
    // real and belongs in core, as a batched create that takes the metadata
    // lock once, not in a fork's isolation.
    let fork_name = spec.fork_name();
    let mut meta = serde_json::Map::new();
    meta.insert(
        "backtest_run".to_string(),
        serde_json::Value::String(spec.run_id.clone()),
    );
    meta.insert(
        "config_digest".to_string(),
        serde_json::Value::String(spec.digest()),
    );
    base.create_fork(
        &fork_name,
        Some(format!("backtest run {}", spec.run_id)),
        None,
        meta,
    )
    .await
    .map_err(|error| BacktestError::invalid(error.to_string()))?;

    let fork = base
        .open_fork(&fork_name)
        .await
        .map_err(|error| BacktestError::invalid(error.to_string()))?;

    run_in_place(&fork, spec, strategy, configure).await
}

/// Run against an already-opened database (usually a fork).
///
/// Separated from [`run_in_fork`] so a caller managing its own forks -- a
/// sweep, say -- does not have to create a second one.
pub async fn run_in_place<F>(
    db: &Database,
    spec: RunSpec,
    strategy: &mut dyn Strategy,
    configure: F,
) -> Result<RunReport>
where
    F: FnOnce(EngineBuilder) -> EngineBuilder,
{
    let started_at = UnixNanos::new(spec.window.map(|w| w.start().get()).unwrap_or_default());

    // Market data first. Resolutions are deliberately not read here.
    let instruments = store::read_instruments(db, spec.read_at.clone()).await?;
    if instruments.is_empty() {
        return Err(BacktestError::invalid(
            "no instruments are registered; write the instruments table before running",
        ));
    }
    // Book events are still collected: reconstructing snapshots needs
    // state that spans rows, and the grouping is what makes a truncated
    // snapshot detectable. Trades and funding stream, which is where the
    // volume is on a tick day.
    //
    // The run's own tables are created alongside that read rather than
    // after the replay. Creating them is almost pure fsync latency -- five
    // specs, five empty manifests, five HEADs, five catalog entries, and
    // nothing to compute -- and it depends on nothing the replay produces,
    // so `write_run` doing it at the end put a metadata barrier in series
    // with work that could have hidden it. Reading the book is the one
    // phase reliably long enough to hide it behind.
    //
    // `write_run` still creates them, because it is called directly
    // elsewhere and must stand alone. By then they exist, so that call
    // costs one catalog listing and commits nothing.
    let (_, book_events) = futures::future::try_join(
        store::create_run_tables(db),
        store::read_book_events(db, spec.read_at.clone(), spec.window),
    )
    .await?;
    let trades = store::trade_source(db, spec.read_at.clone(), spec.window).await?;
    // Funding only exists for perpetuals; the reader treats an absent
    // table as no funding rather than as a failure.
    let funding = store::funding_source(db, spec.read_at.clone(), spec.window).await?;
    // The venue's own mark and oracle, where it publishes them. Absent for
    // any venue that does not, which is most of them.
    let references = store::reference_source(db, spec.read_at.clone(), spec.window).await?;
    // Splits, dividends and delistings. Absent for a venue whose instruments
    // have no issuer to act on them, which is every venue here except equities.
    let corporate = store::corporate_source(db, spec.read_at.clone(), spec.window).await?;

    let coverage = spec.window.map(|requested| {
        // Coverage is measured on the book stream, which is the one that
        // is materialised; adding the streamed ones would mean draining
        // them, which is exactly what streaming exists to avoid.
        let observed: Vec<i64> = book_events.iter().map(|record| record.ts().get()).collect();
        match (observed.iter().min(), observed.iter().max()) {
            (Some(first), Some(last)) => {
                // The loaded span is what arrived, not what was asked for.
                match TimeWindow::new(UnixNanos::new(*first), UnixNanos::new(last + 1)) {
                    Ok(loaded) => Coverage::partial(requested, loaded, &[]),
                    Err(_) => Coverage::empty(requested),
                }
            }
            _ => Coverage::empty(requested),
        }
    });
    if let (Some(coverage), Some(minimum)) = (coverage, spec.minimum_coverage) {
        coverage.require(minimum)?;
    }

    let mut replay = Replay::builder()
        .stream("book", priority::SNAPSHOT, book_events)
        .source("trades", priority::TRADE, trades)
        .source("corporate", priority::CORPORATE, corporate)
        .source("references", priority::REFERENCE, references)
        .source("funding", priority::FUNDING, funding)
        .build()?;

    let builder = Engine::builder(instruments.clone())
        .starting_cash(spec.starting_cash)
        .start(started_at)
        .equity_interval_nanos(spec.equity_interval_nanos)?;
    let mut engine = configure(builder).build();
    let result = engine.run(&mut replay, strategy)?;

    // Only now, with the run finished and the strategy unable to influence
    // anything, does the answer get loaded.
    let resolutions = store::read_resolutions(db, spec.read_at.clone()).await?;
    // A resolution that disagrees with the market it names is a mapping
    // mistake, and settling on it would pay the wrong leg silently.
    validate_resolutions(&resolutions, &instruments)?;
    let portfolio = crate::position::Portfolio::replay(&result.fills)?;
    let marks: BTreeMap<_, _> = result.marks.clone();
    let settlement = settle(&portfolio, &resolutions, result.simulated_through, &marks)?;

    let digest = spec.digest();
    store::write_run(db, &spec.run_id, &digest, &result, &settlement, started_at).await?;

    Ok(RunReport {
        run_id: spec.run_id,
        fork: db.fork_name().unwrap_or("").to_string(),
        digest,
        result,
        settlement,
        coverage,
        instruments,
        resolutions,
    })
}
