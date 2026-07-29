//! Venue adapters: vendor payloads into canonical market-data tables.
//!
//! # Why this is its own crate
//!
//! The real extension point of this system is **the canonical tables**, not
//! a Rust interface. The backtest kernel reads `book_deltas`, `trades`,
//! `funding` and `instruments`; anything that can write those is a loader,
//! in any language and any process. That boundary is stronger than a plugin
//! ABI would be: ingest becomes a versioned commit with provenance, a
//! misbehaving loader corrupts a fork rather than a process, and there is no
//! runtime channel to make fast because loaders and the kernel never run at
//! the same time.
//!
//! What this crate adds on top of that boundary is *convenience with
//! tests*: the wrong-able part of ingesting market data is the mapping --
//! which field is the timestamp, in what unit, whether a zero size means an
//! empty level or a deleted one, when a bar became knowable -- and doing it
//! once, centrally, beats every consumer rediscovering it.
//!
//! It is separate from the kernel for reasons of hygiene rather than
//! performance. Venues bring dependencies (JSON today, FIX or protobuf
//! tomorrow) that no simulation needs, and they break on the venue's
//! schedule: a vendor renaming a field should not force a version bump of
//! the matching engine.
//!
//! # Parse, do not fetch
//!
//! Every function here takes bytes a caller already downloaded. HTTP,
//! credentials, pagination and rate limits belong in a script; keeping them
//! out means the whole mapping is a pure function and every case is tested
//! offline against recorded payload shapes.
//!
//! # The shared shape
//!
//! Venues do not implement a common trait. Their inputs genuinely differ --
//! Polymarket addresses outcomes by token, Hyperliquid by coin and interval
//! -- and forcing one signature would fit neither. What they share is an
//! *output*: [`IngestPlan`], which [`write_plan`] commits to the canonical
//! tables.

use h5i_db_backtest::error::{BacktestError, Result};
use h5i_db_backtest::event::Record;
use h5i_db_backtest::instrument::Instrument;
use h5i_db_backtest::settlement::Resolution;
use h5i_db_backtest::store;
use h5i_db_backtest::types::UnixNanos;
use h5i_db_backtest::window::TimeWindow;
use h5i_db_core::Database;

pub mod hyperliquid;
pub mod polymarket;

/// Everything one venue load produced, ready to commit.
///
/// The common shape venues agree on. Building a plan is pure; writing it is
/// the only step that touches a database, so a caller can inspect, filter
/// or merge plans before anything is committed.
#[derive(Clone, Debug, Default)]
pub struct IngestPlan {
    /// Which adapter produced this, recorded for provenance.
    pub vendor: &'static str,
    pub instruments: Vec<Instrument>,
    /// Snapshots, incremental updates and explicit gaps.
    pub book_events: Vec<Record>,
    pub trades: Vec<Record>,
    /// Perpetual funding, empty for venues that have none.
    pub funding: Vec<Record>,
    /// How markets resolved. Never read on the strategy path.
    pub resolutions: Vec<Resolution>,
}

impl IngestPlan {
    pub fn new(vendor: &'static str) -> Self {
        Self {
            vendor,
            ..Default::default()
        }
    }

    pub fn with_instruments(mut self, instruments: Vec<Instrument>) -> Self {
        self.instruments = instruments;
        self
    }

    pub fn with_book_events(mut self, records: Vec<Record>) -> Self {
        self.book_events = records;
        self
    }

    pub fn with_trades(mut self, records: Vec<Record>) -> Self {
        self.trades = records;
        self
    }

    pub fn with_funding(mut self, records: Vec<Record>) -> Self {
        self.funding = records;
        self
    }

    pub fn with_resolutions(mut self, resolutions: Vec<Resolution>) -> Self {
        self.resolutions = resolutions;
        self
    }

    /// Merge another plan into this one, keeping every stream sorted.
    pub fn merge(mut self, other: IngestPlan) -> Self {
        self.instruments.extend(other.instruments);
        self.book_events.extend(other.book_events);
        self.trades.extend(other.trades);
        self.funding.extend(other.funding);
        self.resolutions.extend(other.resolutions);
        self.sort();
        self
    }

    fn sort(&mut self) {
        for stream in [
            &mut self.book_events,
            &mut self.trades,
            &mut self.funding,
        ] {
            stream.sort_by_key(|record| record.ts().get());
        }
    }

    pub fn record_count(&self) -> usize {
        self.book_events.len() + self.trades.len() + self.funding.len()
    }

    pub fn is_empty(&self) -> bool {
        self.record_count() == 0 && self.instruments.is_empty()
    }

    /// The span the loaded records actually cover.
    ///
    /// The *loaded* window, which is a different fact from whatever window
    /// was requested; keeping them apart is what lets coverage be reported
    /// rather than assumed.
    pub fn loaded_window(&self) -> Option<TimeWindow> {
        let stamps = self
            .book_events
            .iter()
            .chain(&self.trades)
            .chain(&self.funding)
            .map(|record| record.ts().get());
        let (min, max) = stamps.fold((i64::MAX, i64::MIN), |(lo, hi), ts| {
            (lo.min(ts), hi.max(ts))
        });
        if min > max {
            return None;
        }
        TimeWindow::new(UnixNanos::new(min), UnixNanos::new(max + 1)).ok()
    }

    /// Reject a plan that could not be replayed.
    ///
    /// Checked before anything is written, because a stream that is out of
    /// order or references an unknown instrument produces a database that
    /// looks fine and fails at run time, one layer away from the cause.
    pub fn validate(&self) -> Result<()> {
        let known: Vec<&str> = self
            .instruments
            .iter()
            .map(|instrument| instrument.id.as_str())
            .collect();
        for (name, stream) in [
            ("book_events", &self.book_events),
            ("trades", &self.trades),
            ("funding", &self.funding),
        ] {
            let mut previous: Option<i64> = None;
            for record in stream {
                let ts = record.ts().get();
                if let Some(prev) = previous
                    && ts < prev
                {
                    return Err(BacktestError::OutOfOrder {
                        stream: name.to_string(),
                        ts,
                        previous: prev,
                    });
                }
                previous = Some(ts);
                if !known.is_empty() && !known.contains(&record.instrument.as_str()) {
                    return Err(BacktestError::UnknownInstrument(
                        record.instrument.to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Commit a plan to the canonical tables, creating them if needed.
pub async fn write_plan(db: &Database, plan: &IngestPlan, known_at: UnixNanos) -> Result<()> {
    plan.validate()?;
    store::create_market_data_tables(db).await?;
    if !plan.instruments.is_empty() {
        store::write_instruments(db, &plan.instruments, known_at).await?;
    }
    if !plan.book_events.is_empty() {
        store::write_book_events(db, &plan.book_events).await?;
    }
    if !plan.trades.is_empty() {
        store::write_trades(db, &plan.trades).await?;
    }
    if !plan.funding.is_empty() {
        store::write_funding(db, &plan.funding).await?;
    }
    if !plan.resolutions.is_empty() {
        store::write_resolutions(db, &plan.resolutions).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use h5i_db_backtest::event::MarketEvent;
    use h5i_db_backtest::instrument::{InstrumentId, OutcomeId};
    use h5i_db_backtest::types::Stamps;

    fn gap(instrument: &str, at: i64) -> Record {
        Record::new(
            Stamps::immediate(UnixNanos::new(at)),
            InstrumentId::new(instrument).unwrap(),
            OutcomeId::FIRST,
            MarketEvent::Gap,
        )
    }

    #[test]
    fn a_plan_reports_the_window_it_actually_loaded() {
        let plan = IngestPlan::new("test")
            .with_book_events(vec![gap("m", 100), gap("m", 300)]);
        let window = plan.loaded_window().unwrap();
        assert_eq!(window.start().get(), 100);
        assert_eq!(window.end().get(), 301, "half-open past the last record");
        assert!(IngestPlan::new("test").loaded_window().is_none());
    }

    #[test]
    fn merging_keeps_every_stream_sorted() {
        let first = IngestPlan::new("a").with_book_events(vec![gap("m", 10), gap("m", 30)]);
        let second = IngestPlan::new("b").with_book_events(vec![gap("m", 20)]);
        let merged = first.merge(second);
        let order: Vec<i64> = merged.book_events.iter().map(|r| r.ts().get()).collect();
        assert_eq!(order, vec![10, 20, 30]);
    }

    #[test]
    fn validation_catches_an_unsorted_stream_before_it_is_written() {
        let plan = IngestPlan::new("t").with_trades(vec![gap("m", 50), gap("m", 10)]);
        assert!(matches!(
            plan.validate(),
            Err(BacktestError::OutOfOrder { .. })
        ));
    }

    #[test]
    fn validation_catches_a_record_for_an_unregistered_instrument() {
        let plan = IngestPlan::new("t")
            .with_instruments(vec![
                h5i_db_backtest::instrument::Instrument::binary("known", "v").unwrap(),
            ])
            .with_book_events(vec![gap("stranger", 1)]);
        assert!(matches!(
            plan.validate(),
            Err(BacktestError::UnknownInstrument(_))
        ));
    }

    #[test]
    fn a_plan_with_no_instruments_does_not_police_instrument_names() {
        // Loading market data for instruments registered by an earlier plan
        // is legitimate; only a plan that names instruments enforces them.
        let plan = IngestPlan::new("t").with_book_events(vec![gap("anything", 1)]);
        assert!(plan.validate().is_ok());
    }

    #[test]
    fn record_counts_exclude_reference_data() {
        let plan = IngestPlan::new("t")
            .with_instruments(vec![
                h5i_db_backtest::instrument::Instrument::binary("m", "v").unwrap(),
            ])
            .with_book_events(vec![gap("m", 1)])
            .with_trades(vec![gap("m", 2)]);
        assert_eq!(plan.record_count(), 2);
        assert!(!plan.is_empty());
        assert!(IngestPlan::new("t").is_empty());
    }
}
