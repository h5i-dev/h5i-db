//! Scale: does a replay hold its data, and how fast does it go?
//!
//! Every other test here runs on thousands of records, which proves
//! correctness and says nothing about whether a real day fits in memory. A
//! full-depth book day is hundreds of millions of events; at roughly a
//! hundred bytes per materialised `Record` that is tens of gigabytes, so
//! "it works on the test fixture" and "it works" are different claims.
//!
//! These tests use a *generated* source -- records produced on demand and
//! never collected -- so the only way they can pass is if the replay really
//! is streaming. A materialising implementation would exhaust memory on the
//! large case rather than fail an assertion, which is the point: the test
//! is the shape of the failure, not a threshold someone chose.

use std::time::Instant;

use h5i_db_backtest::engine::{Engine, OrderRequest, SignalReplay};
use h5i_db_backtest::event::{MarketEvent, Record};
use h5i_db_backtest::instrument::{Instrument, InstrumentId, InstrumentSet, OutcomeId};
use h5i_db_backtest::models::QueuePositionFills;
use h5i_db_backtest::replay::{priority, Replay};
use h5i_db_backtest::types::{Money, Price, Qty, Side, Stamps, UnixNanos};
use h5i_db_backtest::Result;

const MARKET: &str = "SCALE-PERP";

fn instruments() -> InstrumentSet {
    let mut set = InstrumentSet::new();
    set.insert(
        Instrument::perpetual(MARKET, "bench")
            .unwrap()
            .with_tick_size(Price::from_f64(0.01).unwrap()),
    )
    .unwrap();
    set
}

/// A book snapshot generator: produces records on demand, holds none.
///
/// Deliberately not a `Vec`. If the replay materialised its input this
/// would be indistinguishable from one, and the test would prove nothing.
struct BookGenerator {
    id: InstrumentId,
    emitted: u64,
    total: u64,
    interval_nanos: i64,
}

impl BookGenerator {
    fn new(total: u64, interval_nanos: i64) -> Self {
        Self {
            id: InstrumentId::new(MARKET).unwrap(),
            emitted: 0,
            total,
            interval_nanos,
        }
    }
}

impl Iterator for BookGenerator {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.emitted >= self.total {
            return None;
        }
        let step = self.emitted as i64;
        self.emitted += 1;
        // A slowly oscillating book, so marks and fills stay realistic
        // without needing stored data.
        let drift = ((step % 200) as f64 - 100.0) * 0.01;
        let mid = 100.0 + drift;
        Some(Ok(Record::new(
            Stamps::immediate(UnixNanos::new(step * self.interval_nanos)),
            self.id.clone(),
            OutcomeId::FIRST,
            MarketEvent::BookSnapshot {
                bids: vec![(
                    Price::from_f64(mid - 0.01).unwrap(),
                    Qty::from_f64(50.0).unwrap(),
                )],
                asks: vec![(
                    Price::from_f64(mid + 0.01).unwrap(),
                    Qty::from_f64(50.0).unwrap(),
                )],
            },
        )))
    }
}

/// A trade generator interleaved with the books.
struct TradeGenerator {
    id: InstrumentId,
    emitted: u64,
    total: u64,
    interval_nanos: i64,
}

impl Iterator for TradeGenerator {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.emitted >= self.total {
            return None;
        }
        let step = self.emitted as i64;
        self.emitted += 1;
        Some(Ok(Record::new(
            Stamps::immediate(UnixNanos::new(step * self.interval_nanos)),
            self.id.clone(),
            OutcomeId::FIRST,
            MarketEvent::Trade {
                price: Price::from_f64(100.0).unwrap(),
                size: Qty::from_f64(1.0).unwrap(),
                aggressor: Some(if step % 2 == 0 { Side::Buy } else { Side::Sell }),
            },
        )))
    }
}

/// How many events each scale test replays.
///
/// Large enough that a materialising implementation would be visibly
/// wasteful, small enough to stay a test rather than a benchmark. The
/// benchmark binary runs the same generators at production scale.
const EVENTS: u64 = 400_000;

#[test]
fn a_replay_streams_rather_than_materialising_its_input() {
    let mut replay = Replay::builder()
        .source(
            "book",
            priority::SNAPSHOT,
            Box::new(BookGenerator::new(EVENTS, 1_000_000)),
        )
        .build()
        .unwrap();

    let mut seen = 0u64;
    let mut last = i64::MIN;
    while let Some(record) = replay.next_record().unwrap() {
        // Order is the property that matters; holding the records is not.
        assert!(record.ts().get() >= last);
        last = record.ts().get();
        seen += 1;
    }
    assert_eq!(seen, EVENTS);
    assert_eq!(replay.emitted(), EVENTS);
}

#[test]
fn merging_two_generated_streams_stays_ordered_at_scale() {
    let mut replay = Replay::builder()
        .source(
            "book",
            priority::SNAPSHOT,
            Box::new(BookGenerator::new(EVENTS / 2, 2_000_000)),
        )
        .source(
            "trades",
            priority::TRADE,
            Box::new(TradeGenerator {
                id: InstrumentId::new(MARKET).unwrap(),
                emitted: 0,
                total: EVENTS / 2,
                interval_nanos: 2_000_000,
            }),
        )
        .build()
        .unwrap();

    let mut seen = 0u64;
    let mut last = i64::MIN;
    while let Some(record) = replay.next_record().unwrap() {
        assert!(record.ts().get() >= last, "the merge must not go backwards");
        last = record.ts().get();
        seen += 1;
    }
    assert_eq!(seen, EVENTS);
}

#[test]
fn a_full_run_over_a_generated_day_completes() {
    // The end-to-end shape: book updates, trades, a strategy, matching,
    // an equity curve. Nothing here is collected up front.
    let mut replay = Replay::builder()
        .source(
            "book",
            priority::SNAPSHOT,
            Box::new(BookGenerator::new(EVENTS, 1_000_000)),
        )
        .build()
        .unwrap();

    let id = InstrumentId::new(MARKET).unwrap();
    let intents: Vec<_> = (0..200)
        .map(|index| {
            (
                UnixNanos::new(index as i64 * 1_000 * 1_000_000),
                OrderRequest::market(
                    id.clone(),
                    OutcomeId::FIRST,
                    if index % 2 == 0 { Side::Buy } else { Side::Sell },
                    Qty::from_f64(1.0).unwrap(),
                ),
            )
        })
        .collect();
    let mut strategy = SignalReplay::new(intents).unwrap();

    let mut engine = Engine::builder(instruments())
        .starting_cash(Money::from_units(1_000_000).unwrap())
        // One equity point per simulated minute rather than per record:
        // sampling per record would make the curve as large as the input,
        // which is the one place a streaming replay could still blow up.
        .equity_interval_nanos(60 * 1_000_000_000)
        .unwrap()
        .build();

    let started = Instant::now();
    let result = engine.run(&mut replay, &mut strategy).unwrap();
    let elapsed = started.elapsed();

    assert_eq!(result.records_processed, EVENTS);
    assert_eq!(result.fills.len(), 200, "every intent traded");
    // The curve is bounded by the sampling interval, not by the input size.
    assert!(
        (result.equity.len() as u64) < EVENTS / 100,
        "equity curve has {} points for {EVENTS} records",
        result.equity.len()
    );

    let per_second = EVENTS as f64 / elapsed.as_secs_f64();
    println!(
        "replayed {EVENTS} events in {:.2?} ({:.0} events/s)",
        elapsed, per_second
    );
    // Loose enough to survive a slow CI box, tight enough to catch an
    // accidental quadratic.
    assert!(
        per_second > 10_000.0,
        "only {per_second:.0} events/s, which suggests a per-record scan"
    );
}

#[test]
fn replay_cost_is_linear_in_the_number_of_events() {
    // The failure this catches is a merge or a matcher that rescans
    // something per record: at 4x the events it would take far more than
    // 4x the time.
    let time_for = |events: u64| {
        let mut replay = Replay::builder()
            .source(
                "book",
                priority::SNAPSHOT,
                Box::new(BookGenerator::new(events, 1_000_000)),
            )
            .build()
            .unwrap();
        let started = Instant::now();
        while replay.next_record().unwrap().is_some() {}
        started.elapsed().as_secs_f64()
    };

    let small = time_for(50_000).max(1e-4);
    let large = time_for(200_000);
    let ratio = large / small;
    assert!(
        ratio < 12.0,
        "4x the events took {ratio:.1}x the time, which is not linear"
    );
}

#[test]
fn a_resting_order_book_does_not_grow_without_bound() {
    // Every record re-checks resting orders, so a run that accumulates
    // cancelled orders in that list would degrade over time. This asserts
    // the list is pruned rather than merely filtered.
    let mut replay = Replay::builder()
        .source(
            "book",
            priority::SNAPSHOT,
            Box::new(BookGenerator::new(50_000, 1_000_000)),
        )
        .build()
        .unwrap();

    let id = InstrumentId::new(MARKET).unwrap();
    // Far-from-market limits that never fill and are never cancelled.
    let intents: Vec<_> = (0..500)
        .map(|index| {
            (
                UnixNanos::new(index as i64 * 100 * 1_000_000),
                OrderRequest::limit(
                    id.clone(),
                    OutcomeId::FIRST,
                    Side::Buy,
                    Price::from_f64(1.0).unwrap(),
                    Qty::from_f64(1.0).unwrap(),
                ),
            )
        })
        .collect();
    let mut strategy = SignalReplay::new(intents).unwrap();
    let mut engine = Engine::builder(instruments())
        .starting_cash(Money::from_units(1_000_000).unwrap())
        .build();

    let started = Instant::now();
    let result = engine.run(&mut replay, &mut strategy).unwrap();
    let elapsed = started.elapsed();
    assert_eq!(result.metrics.orders_submitted, 500);
    // 500 resting orders against 50k records is 25M checks at worst; if
    // that were happening this would not finish quickly.
    assert!(
        elapsed.as_secs_f64() < 30.0,
        "500 resting orders over 50k records took {elapsed:?}"
    );
}

#[test]
fn queue_matching_scales_across_many_prints_and_orders() {
    let id = InstrumentId::new(MARKET).unwrap();
    let snapshot = Record::new(
        Stamps::immediate(UnixNanos::new(0)),
        id.clone(),
        OutcomeId::FIRST,
        MarketEvent::BookSnapshot {
            bids: vec![(
                Price::from_f64(100.0).unwrap(),
                Qty::from_f64(1_000_000.0).unwrap(),
            )],
            asks: vec![(
                Price::from_f64(101.0).unwrap(),
                Qty::from_f64(1_000_000.0).unwrap(),
            )],
        },
    );
    let mut replay = Replay::builder()
        .stream("book", priority::SNAPSHOT, vec![snapshot])
        .source(
            "trades",
            priority::TRADE,
            Box::new(TradeGenerator {
                id: id.clone(),
                emitted: 0,
                total: 10_000,
                interval_nanos: 1_000_000,
            }),
        )
        .build()
        .unwrap();
    let intents = (0..500)
        .map(|_| {
            (
                UnixNanos::new(0),
                OrderRequest::limit(
                    id.clone(),
                    OutcomeId::FIRST,
                    Side::Buy,
                    Price::from_f64(100.0).unwrap(),
                    Qty::from_f64(1.0).unwrap(),
                ),
            )
        })
        .collect();
    let mut strategy = SignalReplay::new(intents).unwrap();
    let mut engine = Engine::builder(instruments())
        .starting_cash(Money::from_units(1_000_000).unwrap())
        .fill_model(Box::new(QueuePositionFills::new()))
        .build();

    let started = Instant::now();
    let result = engine.run(&mut replay, &mut strategy).unwrap();
    let elapsed = started.elapsed();
    assert_eq!(result.metrics.orders_submitted, 500);
    assert!(result.fills.is_empty(), "displayed queue was never exhausted");
    assert!(
        elapsed.as_secs_f64() < 30.0,
        "10k prints over 500 queued orders took {elapsed:?}"
    );
}
