//! End-to-end kernel tests (ROADMAP_QUANT.md §8.7).
//!
//! These assert the properties the design sells: determinism, ordering that
//! keeps the venue ahead of the strategy, latency as a queue rather than a
//! call-site concern, and settlement that refuses to book what the run never
//! reached.

use std::collections::BTreeMap;

use h5i_db_backtest::book::BookDelta;
use h5i_db_backtest::engine::{Context, Engine, OrderRequest, RunResult, SignalReplay, Strategy};
use h5i_db_backtest::event::{MarketEvent, Record};
use h5i_db_backtest::instrument::{Instrument, InstrumentId, InstrumentSet, OutcomeId};
use h5i_db_backtest::models::{ConstantLatency, PredictionMarketFees, TickSlippage};
use h5i_db_backtest::order::{OrderStatus, TimeInForce};
use h5i_db_backtest::replay::{priority, Replay};
use h5i_db_backtest::settlement::{settle, Resolution};
use h5i_db_backtest::types::{Money, Price, Qty, Side, Stamps, UnixNanos};
use h5i_db_backtest::{BacktestError, Result};

const MARKET: &str = "will-x-happen";

fn instrument_id() -> InstrumentId {
    InstrumentId::new(MARKET).unwrap()
}

fn instruments() -> InstrumentSet {
    let mut set = InstrumentSet::new();
    set.insert(
        Instrument::binary(MARKET, "polymarket")
            .unwrap()
            .with_settlement_observable(UnixNanos::new(10_000)),
    )
    .unwrap();
    set
}

fn price(value: f64) -> Price {
    Price::from_f64(value).unwrap()
}

fn qty(value: f64) -> Qty {
    Qty::from_f64(value).unwrap()
}

fn money(value: f64) -> Money {
    Money::from_f64(value).unwrap()
}

fn ts(value: i64) -> UnixNanos {
    UnixNanos::new(value)
}

fn snapshot(at: i64, bid: f64, bid_size: f64, ask: f64, ask_size: f64) -> Record {
    Record::new(
        Stamps::immediate(ts(at)),
        instrument_id(),
        OutcomeId::FIRST,
        MarketEvent::BookSnapshot {
            bids: vec![(price(bid), qty(bid_size))],
            asks: vec![(price(ask), qty(ask_size))],
        },
    )
}

fn deep_snapshot(at: i64) -> Record {
    Record::new(
        Stamps::immediate(ts(at)),
        instrument_id(),
        OutcomeId::FIRST,
        MarketEvent::BookSnapshot {
            bids: vec![(price(0.40), qty(100.0)), (price(0.39), qty(200.0))],
            asks: vec![(price(0.42), qty(50.0)), (price(0.43), qty(300.0))],
        },
    )
}

/// Buys once, at the first event it sees.
struct BuyOnce {
    quantity: Qty,
    submitted: bool,
    tif: TimeInForce,
    limit: Option<Price>,
}

impl BuyOnce {
    fn market(quantity: Qty) -> Self {
        Self {
            quantity,
            submitted: false,
            tif: TimeInForce::ImmediateOrCancel,
            limit: None,
        }
    }
}

impl Strategy for BuyOnce {
    fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
        if self.submitted {
            return Ok(());
        }
        self.submitted = true;
        let request = match self.limit {
            Some(limit) => OrderRequest::limit(
                instrument_id(),
                OutcomeId::FIRST,
                Side::Buy,
                limit,
                self.quantity,
            ),
            None => OrderRequest::market(
                instrument_id(),
                OutcomeId::FIRST,
                Side::Buy,
                self.quantity,
            ),
        }
        .with_time_in_force(self.tif);
        ctx.submit(request);
        Ok(())
    }
}

fn run_with(strategy: &mut dyn Strategy, records: Vec<Record>, cash: f64) -> Result<RunResult> {
    let mut replay = Replay::builder()
        .stream("book", priority::SNAPSHOT, records)
        .build()?;
    let mut engine = Engine::builder(instruments())
        .starting_cash(money(cash))
        .build();
    engine.run(&mut replay, strategy)
}

#[test]
fn a_market_order_walks_the_book() {
    let mut strategy = BuyOnce::market(qty(200.0));
    let result = run_with(
        &mut strategy,
        vec![deep_snapshot(1_000), deep_snapshot(2_000)],
        1_000.0,
    )
    .unwrap();

    // 50 at 0.42 then 150 at 0.43.
    assert_eq!(result.fills.len(), 2);
    assert_eq!(result.fills[0].price, price(0.42));
    assert_eq!(result.fills[0].quantity, qty(50.0));
    assert_eq!(result.fills[1].price, price(0.43));
    assert_eq!(result.fills[1].quantity, qty(150.0));
    // Cash spent: 50*0.42 + 150*0.43 = 21 + 64.5
    assert_eq!(result.final_cash, money(1_000.0 - 85.5));
}

#[test]
fn the_strategy_sees_a_book_the_venue_has_already_processed() {
    // The strategy submits on the first record; the fill must use the book
    // that record established, not the previous one.
    struct Observer {
        seen_ask: Option<Price>,
    }
    impl Strategy for Observer {
        fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
            if self.seen_ask.is_none() {
                self.seen_ask = ctx.best_ask(&instrument_id(), OutcomeId::FIRST);
            }
            Ok(())
        }
    }
    let mut strategy = Observer { seen_ask: None };
    run_with(&mut strategy, vec![snapshot(1_000, 0.40, 10.0, 0.42, 10.0)], 100.0).unwrap();
    assert_eq!(
        strategy.seen_ask,
        Some(price(0.42)),
        "the venue must have applied the snapshot before the callback"
    );
}

#[test]
fn orders_are_queued_not_executed_inside_the_callback() {
    // Cash observed during the callback must not yet reflect the order the
    // callback just submitted.
    struct CheckCash {
        cash_during_callback: Vec<Money>,
    }
    impl Strategy for CheckCash {
        fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
            self.cash_during_callback.push(ctx.cash());
            ctx.submit(OrderRequest::market(
                instrument_id(),
                OutcomeId::FIRST,
                Side::Buy,
                qty(10.0),
            ));
            Ok(())
        }
    }
    let mut strategy = CheckCash {
        cash_during_callback: Vec::new(),
    };
    let result = run_with(
        &mut strategy,
        vec![deep_snapshot(1_000), deep_snapshot(2_000)],
        100.0,
    )
    .unwrap();
    assert_eq!(
        strategy.cash_during_callback[0],
        money(100.0),
        "submitting must not settle inside the callback"
    );
    assert!(result.final_cash < money(100.0));
}

#[test]
fn latency_delays_when_the_venue_sees_an_order() {
    let mut replay = Replay::builder()
        .stream(
            "book",
            priority::SNAPSHOT,
            vec![
                deep_snapshot(1_000_000),
                deep_snapshot(1_500_000),
                deep_snapshot(9_000_000),
            ],
        )
        .build()
        .unwrap();
    let mut engine = Engine::builder(instruments())
        .starting_cash(money(1_000.0))
        // 5 ms is far longer than the gap to the second record.
        .latency_model(Box::new(ConstantLatency::millis(5, 5)))
        .build();
    let mut strategy = BuyOnce::market(qty(10.0));
    let result = engine.run(&mut replay, &mut strategy).unwrap();

    assert_eq!(result.fills.len(), 1);
    assert_eq!(
        result.fills[0].ts,
        ts(9_000_000),
        "the order must not fill before the venue could have seen it"
    );
}

#[test]
fn a_resting_limit_order_fills_when_the_book_comes_to_it() {
    struct RestThenWait {
        submitted: bool,
    }
    impl Strategy for RestThenWait {
        fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
            if !self.submitted {
                self.submitted = true;
                ctx.submit(OrderRequest::limit(
                    instrument_id(),
                    OutcomeId::FIRST,
                    Side::Buy,
                    price(0.35),
                    qty(10.0),
                ));
            }
            Ok(())
        }
    }
    let mut strategy = RestThenWait { submitted: false };
    let result = run_with(
        &mut strategy,
        vec![
            snapshot(1_000, 0.40, 10.0, 0.42, 10.0),
            // Still above the limit: no fill.
            snapshot(2_000, 0.36, 10.0, 0.37, 10.0),
            // Now the offer reaches it.
            snapshot(3_000, 0.30, 10.0, 0.34, 10.0),
        ],
        100.0,
    )
    .unwrap();

    assert_eq!(result.fills.len(), 1);
    assert_eq!(result.fills[0].ts, ts(3_000));
    assert_eq!(result.fills[0].price, price(0.34), "fills at the book, not the limit");
}

#[test]
fn immediate_or_cancel_does_not_rest() {
    let mut strategy = BuyOnce::market(qty(500.0));
    let result = run_with(
        &mut strategy,
        vec![deep_snapshot(1_000), deep_snapshot(2_000)],
        1_000.0,
    )
    .unwrap();
    // The book holds 350; the rest is cancelled rather than left resting.
    let order = &result.orders[0];
    assert_eq!(order.filled, qty(350.0));
    assert_eq!(order.status, OrderStatus::Cancelled);
}

#[test]
fn fill_or_kill_is_all_or_nothing() {
    let mut strategy = BuyOnce::market(qty(500.0));
    strategy.tif = TimeInForce::FillOrKill;
    let result = run_with(
        &mut strategy,
        vec![deep_snapshot(1_000), deep_snapshot(2_000)],
        1_000.0,
    )
    .unwrap();
    assert!(result.fills.is_empty(), "a partial fill must kill the order");
    assert_eq!(result.orders[0].status, OrderStatus::Cancelled);
    assert_eq!(result.final_cash, money(1_000.0));
}

#[test]
fn reduce_only_never_opens_or_flips_a_position() {
    struct SellReduceOnly {
        submitted: bool,
    }
    impl Strategy for SellReduceOnly {
        fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
            if !self.submitted {
                self.submitted = true;
                ctx.submit(
                    OrderRequest::market(
                        instrument_id(),
                        OutcomeId::FIRST,
                        Side::Sell,
                        qty(10.0),
                    )
                    .reduce_only(),
                );
            }
            Ok(())
        }
    }
    let mut strategy = SellReduceOnly { submitted: false };
    let result = run_with(
        &mut strategy,
        vec![deep_snapshot(1_000), deep_snapshot(2_000)],
        100.0,
    )
    .unwrap();
    assert!(
        result.fills.is_empty(),
        "with no position to reduce, a reduce-only order must not trade"
    );
    assert_eq!(result.orders[0].status, OrderStatus::Cancelled);
}

#[test]
fn fees_are_charged_and_reduce_cash() {
    let mut replay = Replay::builder()
        .stream(
            "book",
            priority::SNAPSHOT,
            vec![deep_snapshot(1_000), deep_snapshot(2_000)],
        )
        .build()
        .unwrap();
    let mut engine = Engine::builder(instruments())
        .starting_cash(money(1_000.0))
        .fee_model(Box::new(PredictionMarketFees::new(0.07).unwrap()))
        .build();
    let mut strategy = BuyOnce::market(qty(50.0));
    let result = engine.run(&mut replay, &mut strategy).unwrap();

    let fill = &result.fills[0];
    // 0.07 * 50 * 0.42 * 0.58
    assert_eq!(fill.commission, money(0.07 * 50.0 * 0.42 * 0.58));
    assert_eq!(result.commissions, fill.commission);
    assert_eq!(
        result.final_cash,
        money(1_000.0)
            .checked_sub(money(50.0 * 0.42)).unwrap()
            .checked_sub(fill.commission).unwrap()
    );
}

#[test]
fn slippage_moves_the_fill_price_against_the_taker() {
    let mut replay = Replay::builder()
        .stream(
            "book",
            priority::SNAPSHOT,
            vec![deep_snapshot(1_000), deep_snapshot(2_000)],
        )
        .build()
        .unwrap();
    let mut engine = Engine::builder(instruments())
        .starting_cash(money(1_000.0))
        .fill_model(Box::new(TickSlippage::new(2, price(0.01))))
        .build();
    let mut strategy = BuyOnce::market(qty(10.0));
    let result = engine.run(&mut replay, &mut strategy).unwrap();
    assert_eq!(result.fills[0].price, price(0.44), "0.42 plus two ticks");
}

#[test]
fn a_run_is_reproducible() {
    // The property the whole design rests on (P8).
    let run_once = || {
        let mut strategy = BuyOnce::market(qty(120.0));
        run_with(
            &mut strategy,
            vec![
                deep_snapshot(1_000),
                deep_snapshot(2_000),
                deep_snapshot(3_000),
            ],
            1_000.0,
        )
        .unwrap()
    };
    let first = run_once();
    for _ in 0..5 {
        assert_eq!(run_once(), first, "a run must be a pure function of inputs");
    }
}

#[test]
fn a_gap_in_the_feed_stops_the_run_rather_than_inventing_a_book() {
    let records = vec![
        deep_snapshot(1_000),
        Record::new(
            Stamps::immediate(ts(2_000)),
            instrument_id(),
            OutcomeId::FIRST,
            MarketEvent::Gap,
        ),
        // An incremental update after a gap, with no snapshot between.
        Record::new(
            Stamps::immediate(ts(3_000)),
            instrument_id(),
            OutcomeId::FIRST,
            MarketEvent::BookDelta(BookDelta::set(Side::Buy, price(0.5), qty(10.0))),
        ),
    ];
    let mut strategy = BuyOnce::market(qty(1.0));
    let err = run_with(&mut strategy, records, 100.0).unwrap_err();
    assert!(
        matches!(err, BacktestError::BookGap { .. }),
        "got {err:?}"
    );
}

#[test]
fn signal_replay_needs_no_callback_code() {
    // Tier 1: the strategy is data.
    let mut strategy = SignalReplay::new(vec![
        (
            ts(1_000),
            OrderRequest::market(instrument_id(), OutcomeId::FIRST, Side::Buy, qty(10.0)),
        ),
        (
            ts(3_000),
            OrderRequest::market(instrument_id(), OutcomeId::FIRST, Side::Sell, qty(10.0)),
        ),
    ])
    .unwrap();

    let result = run_with(
        &mut strategy,
        vec![
            deep_snapshot(1_000),
            deep_snapshot(2_000),
            deep_snapshot(3_000),
        ],
        1_000.0,
    )
    .unwrap();
    assert_eq!(result.fills.len(), 2);
    assert_eq!(result.fills[0].side, Side::Buy);
    assert_eq!(result.fills[1].side, Side::Sell);
    // Bought at 0.42, sold at 0.40: a 0.02 loss on 10 contracts.
    assert_eq!(result.realized_pnl, money(-0.2));
}

#[test]
fn signal_intents_must_be_ordered() {
    let out_of_order = SignalReplay::new(vec![
        (
            ts(3_000),
            OrderRequest::market(instrument_id(), OutcomeId::FIRST, Side::Buy, qty(1.0)),
        ),
        (
            ts(1_000),
            OrderRequest::market(instrument_id(), OutcomeId::FIRST, Side::Buy, qty(1.0)),
        ),
    ]);
    assert!(out_of_order.is_err());
}

#[test]
fn settlement_is_refused_when_the_run_ends_before_resolution() {
    // The end-to-end version of the flagship guarantee: hold a position,
    // stop the run early, and confirm no resolution profit appears.
    let mut strategy = BuyOnce::market(qty(100.0));
    let result = run_with(
        &mut strategy,
        vec![deep_snapshot(1_000), deep_snapshot(2_000)],
        1_000.0,
    )
    .unwrap();
    assert_eq!(result.simulated_through, Some(ts(2_000)));

    let portfolio = h5i_db_backtest::position::Portfolio::replay(&result.fills).unwrap();
    let resolution = Resolution::new(instrument_id(), OutcomeId::FIRST, ts(10_000));

    let refused = settle(
        &portfolio,
        &[resolution.clone()],
        result.simulated_through,
        &result.marks,
    )
    .unwrap();
    assert!(!refused.was_applied());
    assert_eq!(refused.total_adjustment, Money::ZERO);

    // The same position in a run that did reach resolution settles.
    let applied = settle(&portfolio, &[resolution], Some(ts(20_000)), &result.marks).unwrap();
    assert!(applied.was_applied());
    // The 100 contracts walked two levels (50 at 0.42, 50 at 0.43), so
    // the average is 0.425 and settlement at 1.00 is worth 57.5.
    assert_eq!(applied.settled[0].settled_pnl, money(57.5));
}

#[test]
fn a_categorical_market_trades_every_outcome_independently() {
    let mut set = InstrumentSet::new();
    set.insert(
        Instrument::prediction_market(
            "three-way",
            "polymarket",
            vec!["A".into(), "B".into(), "C".into()],
        )
        .unwrap(),
    )
    .unwrap();
    let id = InstrumentId::new("three-way").unwrap();

    let mut records = Vec::new();
    for (index, ask) in [(0u16, 0.50), (1, 0.30), (2, 0.20)] {
        for at in [1_000, 2_000] {
            records.push(Record::new(
                Stamps::immediate(ts(at)),
                id.clone(),
                OutcomeId(index),
                MarketEvent::BookSnapshot {
                    bids: vec![(price(ask - 0.01), qty(100.0))],
                    asks: vec![(price(ask), qty(100.0))],
                },
            ));
        }
    }
    records.sort_by_key(|r| r.stamps.ts_init.get());

    let mut strategy = SignalReplay::new(
        (0..3u16)
            .map(|outcome| {
                (
                    // At ts 1_000 only the first outcome's book has been
                    // applied; by 2_000 all three exist.
                    ts(2_000),
                    OrderRequest::market(id.clone(), OutcomeId(outcome), Side::Buy, qty(10.0)),
                )
            })
            .collect(),
    )
    .unwrap();

    let mut replay = Replay::builder()
        .stream("book", priority::SNAPSHOT, records)
        .build()
        .unwrap();
    let mut engine = Engine::builder(set).starting_cash(money(100.0)).build();
    let result = engine.run(&mut replay, &mut strategy).unwrap();

    assert_eq!(result.fills.len(), 3, "one position per outcome");
    let portfolio = h5i_db_backtest::position::Portfolio::replay(&result.fills).unwrap();
    assert_eq!(portfolio.open_positions().count(), 3);

    // A complete set costs 1.00 and pays 1.00 whichever outcome wins.
    let resolution = Resolution::new(id, OutcomeId(1), ts(5_000));
    let report = settle(&portfolio, &[resolution], Some(ts(9_000)), &BTreeMap::new()).unwrap();
    let total: i64 = report.settled.iter().map(|s| s.settled_pnl.raw()).sum();
    assert_eq!(Money::from_raw(total), Money::ZERO);
}

#[test]
fn timers_fire_before_the_data_at_their_instant() {
    struct Timed {
        events: Vec<(String, i64)>,
        armed: bool,
    }
    impl Strategy for Timed {
        fn on_start(&mut self, ctx: &mut Context<'_>) -> Result<()> {
            ctx.set_timer("rebalance", ts(2_000));
            Ok(())
        }
        fn on_event(&mut self, ctx: &mut Context<'_>, _r: &Record) -> Result<()> {
            self.events.push(("data".into(), ctx.now().get()));
            let _ = self.armed;
            Ok(())
        }
        fn on_timer(
            &mut self,
            ctx: &mut Context<'_>,
            event: &h5i_db_backtest::clock::TimeEvent,
        ) -> Result<()> {
            self.events.push((event.name.clone(), ctx.now().get()));
            Ok(())
        }
    }
    let mut strategy = Timed {
        events: Vec::new(),
        armed: false,
    };
    run_with(
        &mut strategy,
        vec![deep_snapshot(1_000), deep_snapshot(2_000), deep_snapshot(3_000)],
        100.0,
    )
    .unwrap();
    let names: Vec<&str> = strategy.events.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, vec!["data", "rebalance", "data", "data"]);
    assert_eq!(strategy.events[1].1, 2_000);
}

#[test]
fn an_unknown_instrument_is_refused_rather_than_silently_skipped() {
    let stray = Record::new(
        Stamps::immediate(ts(1_000)),
        InstrumentId::new("not-registered").unwrap(),
        OutcomeId::FIRST,
        MarketEvent::Gap,
    );
    let mut strategy = BuyOnce::market(qty(1.0));
    let err = run_with(&mut strategy, vec![stray], 100.0).unwrap_err();
    assert!(matches!(err, BacktestError::UnknownInstrument(_)));
}

#[test]
fn an_outcome_outside_the_market_is_refused() {
    let stray = Record::new(
        Stamps::immediate(ts(1_000)),
        instrument_id(),
        OutcomeId(7),
        MarketEvent::Gap,
    );
    let mut strategy = BuyOnce::market(qty(1.0));
    let err = run_with(&mut strategy, vec![stray], 100.0).unwrap_err();
    assert!(matches!(err, BacktestError::UnknownOutcome { .. }));
}
