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

use h5i_db_core::database::ReadAt;
use h5i_db_core::Database;

use crate::engine::{Engine, EngineBuilder, RunResult, Strategy};
use crate::error::{BacktestError, Result};
use crate::instrument::InstrumentSet;
use crate::replay::{priority, Replay};
use crate::settlement::{settle, SettlementReport};
use crate::store;
use crate::types::{Money, UnixNanos};
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
        }
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

    /// A stable hash of everything that determines the result.
    ///
    /// Two runs agreeing on this digest are the same computation, so a
    /// stored run can be regenerated and checked rather than trusted.
    pub fn digest(&self) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.run_id.as_bytes());
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
        hasher.finalize().to_hex().to_string()
    }
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
                order.status == crate::order::OrderStatus::Cancelled
                    && order.filled.is_zero()
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
        out
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
    let started_at = UnixNanos::new(
        spec.window
            .map(|w| w.start().get())
            .unwrap_or_default(),
    );

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
    let book_events = store::read_book_events(db, spec.read_at.clone(), spec.window).await?;
    let trades = store::trade_source(db, spec.read_at.clone(), spec.window).await?;
    // Funding only exists for perpetuals; the reader treats an absent
    // table as no funding rather than as a failure.
    let funding = store::funding_source(db, spec.read_at.clone(), spec.window).await?;

    let coverage = spec.window.map(|requested| {
        // Coverage is measured on the book stream, which is the one that
        // is materialised; adding the streamed ones would mean draining
        // them, which is exactly what streaming exists to avoid.
        let observed: Vec<i64> = book_events
            .iter()
            .map(|record| record.ts().get())
            .collect();
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
    let portfolio = crate::position::Portfolio::replay(&result.fills)?;
    let marks: BTreeMap<_, _> = result.marks.clone();
    let settlement = settle(
        &portfolio,
        &resolutions,
        result.simulated_through,
        &marks,
    )?;

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
    })
}
