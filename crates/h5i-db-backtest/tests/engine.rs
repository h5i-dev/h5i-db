//! End-to-end kernel tests (ROADMAP_QUANT.md §8.7).
//!
//! These assert the properties the design sells: determinism, ordering that
//! keeps the venue ahead of the strategy, latency as a queue rather than a
//! call-site concern, and settlement that refuses to book what the run never
//! reached.

use std::collections::BTreeMap;

use h5i_db_backtest::book::BookDelta;
use h5i_db_backtest::engine::{
    CommandReplay, Context, Engine, OrderRequest, ReplayCommand, RiskLimits, RunResult,
    SignalReplay, Strategy,
};
use h5i_db_backtest::event::{MarketEvent, Record};
use h5i_db_backtest::instrument::{Instrument, InstrumentId, InstrumentSet, OutcomeId};
use h5i_db_backtest::models::{ConstantLatency, PredictionMarketFees, TickSlippage};
use h5i_db_backtest::order::{OrderStatus, TimeInForce};
use h5i_db_backtest::replay::{Replay, priority};
use h5i_db_backtest::settlement::{Resolution, settle};
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
            None => {
                OrderRequest::market(instrument_id(), OutcomeId::FIRST, Side::Buy, self.quantity)
            }
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
fn account_risk_limits_reject_before_the_order_reaches_the_venue() {
    let mut replay = Replay::builder()
        .stream("book", priority::SNAPSHOT, vec![deep_snapshot(1)])
        .build()
        .unwrap();
    let limits = RiskLimits::new(Some(qty(5.0)), Some(qty(20.0)), Some(2)).unwrap();
    let mut engine = Engine::builder(instruments())
        .starting_cash(money(10_000.0))
        .risk_limits(limits)
        .build();
    let mut strategy = BuyOnce::market(qty(10.0));
    let result = engine.run(&mut replay, &mut strategy).unwrap();

    assert!(result.fills.is_empty());
    assert_eq!(result.metrics.orders_rejected_risk, 1);
    assert_eq!(result.orders[0].status, OrderStatus::Rejected);
    assert!(
        result.orders[0]
            .reject_reason
            .as_deref()
            .unwrap()
            .contains("max_order_quantity")
    );
}

#[test]
fn position_risk_includes_all_live_orders() {
    struct SubmitTwice;
    impl Strategy for SubmitTwice {
        fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
            for _ in 0..2 {
                ctx.submit(OrderRequest::limit(
                    instrument_id(),
                    OutcomeId::FIRST,
                    Side::Buy,
                    price(0.40),
                    qty(4.0),
                ));
            }
            Ok(())
        }
    }

    let mut replay = Replay::builder()
        .stream("book", priority::SNAPSHOT, vec![deep_snapshot(1)])
        .build()
        .unwrap();
    let limits = RiskLimits::new(None, Some(qty(5.0)), None).unwrap();
    let mut engine = Engine::builder(instruments())
        .starting_cash(money(10_000.0))
        .risk_limits(limits)
        .build();
    let result = engine.run(&mut replay, &mut SubmitTwice).unwrap();

    assert_eq!(result.metrics.orders_submitted, 2);
    assert_eq!(result.metrics.orders_rejected_risk, 1);
    assert_eq!(
        result
            .orders
            .iter()
            .filter(|order| order.status == OrderStatus::Rejected)
            .count(),
        1
    );
}

#[test]
fn command_replay_addresses_orders_by_client_id() {
    let commands = vec![
        (
            ts(1),
            ReplayCommand::Submit {
                client_order_id: "quote-yes".to_string(),
                request: OrderRequest::limit(
                    instrument_id(),
                    OutcomeId::FIRST,
                    Side::Buy,
                    price(0.40),
                    qty(4.0),
                ),
            },
        ),
        (
            ts(2),
            ReplayCommand::Cancel {
                client_order_id: "quote-yes".to_string(),
            },
        ),
    ];
    let mut strategy = CommandReplay::new(commands).unwrap();
    let result = run_with(
        &mut strategy,
        vec![deep_snapshot(1), deep_snapshot(2)],
        1_000.0,
    )
    .unwrap();

    assert_eq!(result.orders.len(), 1);
    assert_eq!(result.orders[0].status, OrderStatus::Cancelled);
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
    run_with(
        &mut strategy,
        vec![snapshot(1_000, 0.40, 10.0, 0.42, 10.0)],
        100.0,
    )
    .unwrap();
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
    assert_eq!(
        result.fills[0].price,
        price(0.34),
        "fills at the book, not the limit"
    );
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
    assert!(
        result.fills.is_empty(),
        "a partial fill must kill the order"
    );
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
                    OrderRequest::market(instrument_id(), OutcomeId::FIRST, Side::Sell, qty(10.0))
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
            .checked_sub(money(50.0 * 0.42))
            .unwrap()
            .checked_sub(fill.commission)
            .unwrap()
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
    assert!(matches!(err, BacktestError::BookGap { .. }), "got {err:?}");
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
        std::slice::from_ref(&resolution),
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
    let total: h5i_db_backtest::types::Raw =
        report.settled.iter().map(|s| s.settled_pnl.raw()).sum();
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
        vec![
            deep_snapshot(1_000),
            deep_snapshot(2_000),
            deep_snapshot(3_000),
        ],
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

// ---------------------------------------------------------------------------
// queue position (B2)
// ---------------------------------------------------------------------------

use h5i_db_backtest::models::QueuePositionFills;

/// Rests a buy limit at `limit` on the first record, then waits.
struct RestBuy {
    limit: Price,
    quantity: Qty,
    submitted: bool,
}

impl Strategy for RestBuy {
    fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
        if !self.submitted {
            self.submitted = true;
            ctx.submit(OrderRequest::limit(
                instrument_id(),
                OutcomeId::FIRST,
                Side::Buy,
                self.limit,
                self.quantity,
            ));
        }
        Ok(())
    }
}

fn print_at(at: i64, price_value: f64, size: f64, aggressor: Option<Side>) -> Record {
    Record::new(
        Stamps::immediate(ts(at)),
        instrument_id(),
        OutcomeId::FIRST,
        MarketEvent::Trade {
            price: price(price_value),
            size: qty(size),
            aggressor,
        },
    )
}

fn run_queued(
    strategy: &mut dyn Strategy,
    records: Vec<Record>,
    model: QueuePositionFills,
) -> Result<RunResult> {
    let mut replay = Replay::builder()
        .stream(
            "book",
            priority::SNAPSHOT,
            records
                .clone()
                .into_iter()
                .filter(|r| !matches!(r.event, MarketEvent::Trade { .. }))
                .collect(),
        )
        .stream(
            "trades",
            priority::TRADE,
            records
                .into_iter()
                .filter(|r| matches!(r.event, MarketEvent::Trade { .. }))
                .collect(),
        )
        .build()?;
    let mut engine = Engine::builder(instruments())
        .starting_cash(money(1_000.0))
        .fill_model(Box::new(model))
        .build();
    engine.run(&mut replay, strategy)
}

#[test]
fn a_resting_order_waits_behind_the_size_already_at_its_price() {
    // 100 already displayed at 0.40; a 60-lot print does not reach us.
    let mut strategy = RestBuy {
        limit: price(0.40),
        quantity: qty(10.0),
        submitted: false,
    };
    let result = run_queued(
        &mut strategy,
        vec![
            deep_snapshot(1_000),
            print_at(2_000, 0.40, 60.0, Some(Side::Sell)),
            deep_snapshot(3_000),
        ],
        QueuePositionFills::new(),
    )
    .unwrap();
    assert!(
        result.fills.is_empty(),
        "60 of 100 ahead consumed is not our turn yet"
    );
}

#[test]
fn the_queue_clears_and_then_we_fill() {
    let mut strategy = RestBuy {
        limit: price(0.40),
        quantity: qty(10.0),
        submitted: false,
    };
    let result = run_queued(
        &mut strategy,
        vec![
            deep_snapshot(1_000),
            // 100 ahead, then 30 more: 30 reaches us but we want 10.
            print_at(2_000, 0.40, 100.0, Some(Side::Sell)),
            print_at(3_000, 0.40, 30.0, Some(Side::Sell)),
            deep_snapshot(4_000),
        ],
        QueuePositionFills::new(),
    )
    .unwrap();
    assert_eq!(result.fills.len(), 1);
    assert_eq!(result.fills[0].quantity, qty(10.0));
    assert_eq!(
        result.fills[0].price,
        price(0.40),
        "a maker fills at their own limit, not at the print"
    );
    assert!(!result.fills[0].is_taker, "this is a maker fill");
}

#[test]
fn a_print_below_our_bid_does_not_reach_a_higher_resting_buy() {
    let mut strategy = RestBuy {
        limit: price(0.39),
        quantity: qty(10.0),
        submitted: false,
    };
    let result = run_queued(
        &mut strategy,
        vec![
            deep_snapshot(1_000),
            // Prints above our bid never trade against it.
            print_at(2_000, 0.41, 10_000.0, Some(Side::Sell)),
            deep_snapshot(3_000),
        ],
        QueuePositionFills::new(),
    )
    .unwrap();
    assert!(result.fills.is_empty());
}

#[test]
fn an_unknown_aggressor_does_not_fill_by_default() {
    // The conservative reading: a print with no side does not prove anyone
    // traded against our queue.
    let mut strategy = RestBuy {
        limit: price(0.40),
        quantity: qty(10.0),
        submitted: false,
    };
    let records = vec![
        deep_snapshot(1_000),
        print_at(2_000, 0.40, 5_000.0, None),
        deep_snapshot(3_000),
    ];
    let conservative =
        run_queued(&mut strategy, records.clone(), QueuePositionFills::new()).unwrap();
    assert!(conservative.fills.is_empty());

    // The permissive reading fills, and the gap between the two is the
    // measure of how much a strategy leans on queue luck.
    let mut strategy = RestBuy {
        limit: price(0.40),
        quantity: qty(10.0),
        submitted: false,
    };
    let permissive = run_queued(&mut strategy, records, QueuePositionFills::optimistic()).unwrap();
    assert_eq!(permissive.fills.len(), 1);
}

#[test]
fn without_the_queue_model_a_resting_order_only_fills_on_the_book() {
    // The default model ignores prints entirely for passive orders.
    let mut strategy = RestBuy {
        limit: price(0.40),
        quantity: qty(10.0),
        submitted: false,
    };
    let mut replay = Replay::builder()
        .stream(
            "book",
            priority::SNAPSHOT,
            vec![deep_snapshot(1_000), deep_snapshot(3_000)],
        )
        .stream(
            "trades",
            priority::TRADE,
            vec![print_at(2_000, 0.40, 10_000.0, Some(Side::Sell))],
        )
        .build()
        .unwrap();
    let mut engine = Engine::builder(instruments())
        .starting_cash(money(1_000.0))
        .build();
    let result = engine.run(&mut replay, &mut strategy).unwrap();
    assert!(result.fills.is_empty(), "no crossing book, no fill");
}

#[test]
fn queue_fills_are_reproducible() {
    let run = || {
        let mut strategy = RestBuy {
            limit: price(0.40),
            quantity: qty(25.0),
            submitted: false,
        };
        run_queued(
            &mut strategy,
            vec![
                deep_snapshot(1_000),
                print_at(2_000, 0.40, 100.0, Some(Side::Sell)),
                print_at(3_000, 0.40, 15.0, Some(Side::Sell)),
                print_at(4_000, 0.39, 40.0, Some(Side::Sell)),
                deep_snapshot(5_000),
            ],
            QueuePositionFills::new(),
        )
        .unwrap()
    };
    let first = run();
    for _ in 0..4 {
        assert_eq!(run(), first);
    }
    assert!(!first.fills.is_empty());
}

// ---------------------------------------------------------------------------
// margin and liquidation (Tier 1, item 3)
// ---------------------------------------------------------------------------

use h5i_db_backtest::account::{CashMargin, LinearMargin};
use h5i_db_backtest::instrument::Instrument as Inst;

const PERP: &str = "BTC-PERP";

fn perp_instruments() -> InstrumentSet {
    let mut set = InstrumentSet::new();
    set.insert(
        Inst::perpetual(PERP, "hyperliquid")
            .unwrap()
            .with_tick_size(price(0.5)),
    )
    .unwrap();
    set
}

fn perp_snapshot(at: i64, mid: f64) -> Record {
    Record::new(
        Stamps::immediate(ts(at)),
        InstrumentId::new(PERP).unwrap(),
        OutcomeId::FIRST,
        MarketEvent::BookSnapshot {
            bids: vec![(price(mid - 0.5), qty(1_000.0))],
            asks: vec![(price(mid + 0.5), qty(1_000.0))],
        },
    )
}

struct BuyPerp {
    quantity: Qty,
    submitted: bool,
}

impl Strategy for BuyPerp {
    fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
        if !self.submitted {
            self.submitted = true;
            ctx.submit(OrderRequest::market(
                InstrumentId::new(PERP).unwrap(),
                OutcomeId::FIRST,
                Side::Buy,
                self.quantity,
            ));
        }
        Ok(())
    }
}

fn run_perp(
    strategy: &mut dyn Strategy,
    records: Vec<Record>,
    cash: f64,
    leverage: Option<f64>,
) -> Result<RunResult> {
    let mut replay = Replay::builder()
        .stream("book", priority::SNAPSHOT, records)
        .build()?;
    let mut builder = Engine::builder(perp_instruments()).starting_cash(money(cash));
    if let Some(leverage) = leverage {
        builder = builder.margin_model(Box::new(LinearMargin::from_leverage(leverage)?));
    }
    let mut engine = builder.build();
    engine.run(&mut replay, strategy)
}

#[test]
fn without_a_margin_model_leverage_is_unlimited() {
    // The behaviour this whole feature exists to remove: 100 BTC on 1000
    // of cash, with no complaint.
    let mut strategy = BuyPerp {
        quantity: qty(100.0),
        submitted: false,
    };
    let result = run_perp(
        &mut strategy,
        vec![perp_snapshot(1_000, 100.0), perp_snapshot(2_000, 100.0)],
        1_000.0,
        None,
    )
    .unwrap();
    assert_eq!(result.fills.len(), 1);
    assert_eq!(result.rejected_for_margin, 0);
    // 100 BTC at ~100 is 10,000 of exposure against 1,000 of cash: a
    // hundredfold position nobody would have been allowed to hold.
    let portfolio = h5i_db_backtest::position::Portfolio::replay(&result.fills).unwrap();
    let position = portfolio
        .position(&InstrumentId::new(PERP).unwrap(), OutcomeId::FIRST)
        .unwrap();
    let exposure = position.exposure(price(100.0)).unwrap();
    assert!(
        exposure.to_f64() > result.starting_cash.to_f64() * 9.0,
        "exposure {exposure} against {} of cash",
        result.starting_cash
    );
    assert!(result.liquidations.is_empty(), "and never called");
}

#[test]
fn a_margin_model_refuses_an_order_the_account_cannot_fund() {
    let mut strategy = BuyPerp {
        quantity: qty(100.0),
        submitted: false,
    };
    // 100 BTC at ~100 is 10,000 notional; at 10x that needs 1,000 of
    // margin, and the account holds 500.
    let result = run_perp(
        &mut strategy,
        vec![perp_snapshot(1_000, 100.0), perp_snapshot(2_000, 100.0)],
        500.0,
        Some(10.0),
    )
    .unwrap();
    assert!(result.fills.is_empty());
    assert_eq!(result.rejected_for_margin, 1);
    assert_eq!(result.orders[0].status, OrderStatus::Rejected);
    assert!(
        result.orders[0]
            .reject_reason
            .as_deref()
            .unwrap()
            .contains("margin")
    );
}

#[test]
fn an_affordable_order_passes_the_margin_check() {
    let mut strategy = BuyPerp {
        quantity: qty(10.0),
        submitted: false,
    };
    // 10 BTC at ~100 is 1,000 notional; 10x needs 100, and we hold 500.
    let result = run_perp(
        &mut strategy,
        vec![perp_snapshot(1_000, 100.0), perp_snapshot(2_000, 100.0)],
        500.0,
        Some(10.0),
    )
    .unwrap();
    assert_eq!(result.fills.len(), 1);
    assert_eq!(result.rejected_for_margin, 0);
}

#[test]
fn a_position_that_falls_below_maintenance_is_liquidated() {
    let mut strategy = BuyPerp {
        quantity: qty(10.0),
        submitted: false,
    };
    // Enter 10 BTC at ~100 (1,000 notional, 100 initial, 50 maintenance)
    // with 120 of cash, then let the mark collapse.
    let result = run_perp(
        &mut strategy,
        vec![
            perp_snapshot(1_000, 100.0),
            perp_snapshot(2_000, 100.0),
            perp_snapshot(3_000, 92.0),
            perp_snapshot(4_000, 85.0),
            perp_snapshot(5_000, 85.0),
        ],
        120.0,
        Some(10.0),
    )
    .unwrap();

    assert!(
        !result.liquidations.is_empty(),
        "a 15% adverse move on 10x leverage must call the position"
    );
    let liquidation = &result.liquidations[0];
    assert_eq!(liquidation.instrument.as_str(), PERP);
    assert!(liquidation.equity < liquidation.maintenance);
    // The position was actually closed, not merely reported.
    let portfolio = h5i_db_backtest::position::Portfolio::replay(&result.fills).unwrap();
    assert!(
        portfolio
            .position(&InstrumentId::new(PERP).unwrap(), OutcomeId::FIRST)
            .unwrap()
            .is_flat(),
        "liquidation must flatten the book"
    );
}

#[test]
fn a_solvent_account_is_never_liquidated() {
    let mut strategy = BuyPerp {
        quantity: qty(1.0),
        submitted: false,
    };
    let result = run_perp(
        &mut strategy,
        vec![
            perp_snapshot(1_000, 100.0),
            perp_snapshot(2_000, 100.0),
            perp_snapshot(3_000, 98.0),
            perp_snapshot(4_000, 99.0),
        ],
        1_000.0,
        Some(10.0),
    )
    .unwrap();
    assert!(result.liquidations.is_empty());
    assert_eq!(result.fills.len(), 1, "the entry, and nothing else");
}

#[test]
fn a_cash_account_is_never_liquidated_however_far_the_mark_falls() {
    // A prepaid position cannot be called: the money is already spent.
    let mut replay = Replay::builder()
        .stream(
            "book",
            priority::SNAPSHOT,
            vec![
                perp_snapshot(1_000, 100.0),
                perp_snapshot(2_000, 100.0),
                perp_snapshot(3_000, 1.0),
            ],
        )
        .build()
        .unwrap();
    let mut engine = Engine::builder(perp_instruments())
        .starting_cash(money(1_000.0))
        .margin_model(Box::new(CashMargin))
        .build();
    let mut strategy = BuyPerp {
        quantity: qty(5.0),
        submitted: false,
    };
    let result = engine.run(&mut replay, &mut strategy).unwrap();
    assert!(result.liquidations.is_empty());
}

#[test]
fn liquidation_is_reproducible() {
    let run = || {
        let mut strategy = BuyPerp {
            quantity: qty(10.0),
            submitted: false,
        };
        run_perp(
            &mut strategy,
            vec![
                perp_snapshot(1_000, 100.0),
                perp_snapshot(2_000, 100.0),
                perp_snapshot(3_000, 90.0),
                perp_snapshot(4_000, 84.0),
                perp_snapshot(5_000, 84.0),
            ],
            120.0,
            Some(10.0),
        )
        .unwrap()
    };
    let first = run();
    assert!(!first.liquidations.is_empty());
    for _ in 0..4 {
        assert_eq!(run(), first);
    }
}

// ---------------------------------------------------------------------------
// amendment and self-trade prevention (Tier 1, item 2)
// ---------------------------------------------------------------------------

/// Rests a buy limit, then amends it on the next event.
struct RestThenAmend {
    limit: Price,
    quantity: Qty,
    new_quantity: Option<Qty>,
    new_limit: Option<Price>,
    id: Option<h5i_db_backtest::order::OrderId>,
    step: u32,
}

impl Strategy for RestThenAmend {
    fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
        self.step += 1;
        if self.step == 1 {
            ctx.submit(OrderRequest::limit(
                instrument_id(),
                OutcomeId::FIRST,
                Side::Buy,
                self.limit,
                self.quantity,
            ));
        } else if self.step == 2 {
            // Order ids are assigned in submission order from 1.
            self.id = Some(h5i_db_backtest::order::OrderId(1));
            ctx.amend(
                h5i_db_backtest::order::OrderId(1),
                self.new_quantity,
                self.new_limit,
            );
        }
        Ok(())
    }
}

#[test]
fn shrinking_an_order_keeps_its_queue_place() {
    let mut strategy = RestThenAmend {
        limit: price(0.40),
        quantity: qty(20.0),
        new_quantity: Some(qty(10.0)),
        new_limit: None,
        id: None,
        step: 0,
    };
    let result = run_queued(
        &mut strategy,
        vec![
            deep_snapshot(1_000),
            deep_snapshot(2_000),
            // 100 ahead, then 30 more: with priority kept, we fill.
            print_at(3_000, 0.40, 100.0, Some(Side::Sell)),
            print_at(4_000, 0.40, 30.0, Some(Side::Sell)),
            deep_snapshot(5_000),
        ],
        QueuePositionFills::new(),
    )
    .unwrap();
    assert_eq!(result.fills.len(), 1);
    assert_eq!(result.fills[0].quantity, qty(10.0), "the amended size");
}

#[test]
fn growing_an_order_sends_it_to_the_back_of_the_queue() {
    let mut strategy = RestThenAmend {
        limit: price(0.40),
        quantity: qty(10.0),
        new_quantity: Some(qty(30.0)),
        new_limit: None,
        id: None,
        step: 0,
    };
    let result = run_queued(
        &mut strategy,
        vec![
            deep_snapshot(1_000),
            deep_snapshot(2_000),
            // Exactly enough to clear the original queue, and no more.
            print_at(3_000, 0.40, 100.0, Some(Side::Sell)),
            deep_snapshot(4_000),
        ],
        QueuePositionFills::new(),
    )
    .unwrap();
    assert!(
        result.fills.is_empty(),
        "a larger order rejoins behind the size displayed at its price"
    );
}

#[test]
fn repricing_an_order_sends_it_to_the_back_of_the_queue() {
    let mut strategy = RestThenAmend {
        limit: price(0.40),
        quantity: qty(10.0),
        new_quantity: None,
        new_limit: Some(price(0.39)),
        id: None,
        step: 0,
    };
    let result = run_queued(
        &mut strategy,
        vec![
            deep_snapshot(1_000),
            deep_snapshot(2_000),
            // Clears the 0.40 queue; our order has moved to 0.39.
            print_at(3_000, 0.40, 1_000.0, Some(Side::Sell)),
            deep_snapshot(4_000),
        ],
        QueuePositionFills::new(),
    )
    .unwrap();
    assert!(result.fills.is_empty(), "a reprice loses priority");
    let order = &result.orders[0];
    assert_eq!(order.limit_price(), Some(price(0.39)), "and takes effect");
}

#[test]
fn an_amendment_below_the_filled_quantity_is_refused() {
    // Shrinking an order past what it has already traded is not a smaller
    // order, it is an inconsistent one.
    struct Overshrink {
        step: u32,
    }
    impl Strategy for Overshrink {
        fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
            self.step += 1;
            if self.step == 1 {
                // 0.42 takes the 50 displayed there and rests the other
                // 50, so the order is partially filled and still open --
                // which is the only state in which this guard can fire.
                ctx.submit(OrderRequest::limit(
                    instrument_id(),
                    OutcomeId::FIRST,
                    Side::Buy,
                    price(0.42),
                    qty(100.0),
                ));
            } else if self.step == 3 {
                ctx.amend(h5i_db_backtest::order::OrderId(1), Some(qty(1.0)), None);
            }
            Ok(())
        }
    }
    let mut strategy = Overshrink { step: 0 };
    // The offer at 0.42 is taken on the first snapshot and then moves
    // away, so the order stays half filled and open -- the only state in
    // which shrinking below the filled quantity is expressible.
    let error = run_with(
        &mut strategy,
        vec![
            snapshot(1_000, 0.40, 100.0, 0.42, 50.0),
            snapshot(2_000, 0.40, 100.0, 0.45, 100.0),
            snapshot(3_000, 0.40, 100.0, 0.45, 100.0),
            snapshot(4_000, 0.40, 100.0, 0.45, 100.0),
        ],
        1_000.0,
    )
    .unwrap_err();
    assert!(error.to_string().contains("already filled"), "{error}");
}

#[test]
fn an_order_that_would_cross_this_accounts_own_book_is_refused() {
    // A wash trade: rest a bid, then send a marketable offer into it.
    //
    // The account buys the inventory first so that the offer is one it
    // could legitimately make. Without that it is *also* a naked short,
    // which is refused earlier and would leave this test passing for the
    // wrong reason.
    struct SelfCross {
        step: u32,
    }
    impl Strategy for SelfCross {
        fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
            self.step += 1;
            if self.step == 1 {
                ctx.submit(OrderRequest::market(
                    instrument_id(),
                    OutcomeId::FIRST,
                    Side::Buy,
                    qty(10.0),
                ));
            } else if self.step == 2 {
                ctx.submit(OrderRequest::limit(
                    instrument_id(),
                    OutcomeId::FIRST,
                    Side::Buy,
                    price(0.35),
                    qty(10.0),
                ));
            } else if self.step == 3 {
                ctx.submit(OrderRequest::limit(
                    instrument_id(),
                    OutcomeId::FIRST,
                    Side::Sell,
                    price(0.30),
                    qty(10.0),
                ));
            }
            Ok(())
        }
    }
    let mut strategy = SelfCross { step: 0 };
    let result = run_with(
        &mut strategy,
        vec![
            deep_snapshot(1_000),
            deep_snapshot(2_000),
            deep_snapshot(3_000),
            deep_snapshot(4_000),
        ],
        1_000.0,
    )
    .unwrap();

    assert_eq!(result.self_trades_prevented, 1);
    let rejected = result
        .orders
        .iter()
        .find(|order| order.status == OrderStatus::Rejected)
        .expect("the crossing order is rejected");
    assert!(
        rejected
            .reject_reason
            .as_deref()
            .unwrap()
            .contains("own resting order")
    );
}

#[test]
fn orders_on_the_same_side_never_self_trade() {
    struct TwoBuys {
        step: u32,
    }
    impl Strategy for TwoBuys {
        fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
            self.step += 1;
            if self.step <= 2 {
                ctx.submit(OrderRequest::limit(
                    instrument_id(),
                    OutcomeId::FIRST,
                    Side::Buy,
                    price(0.35),
                    qty(5.0),
                ));
            }
            Ok(())
        }
    }
    let mut strategy = TwoBuys { step: 0 };
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
    assert_eq!(result.self_trades_prevented, 0);
}

// ---------------------------------------------------------------------------
// observability (Tier 2, item 9)
// ---------------------------------------------------------------------------

#[test]
fn metrics_count_what_the_run_actually_did() {
    let mut strategy = BuyOnce::market(qty(200.0));
    let result = run_with(
        &mut strategy,
        vec![deep_snapshot(1_000), deep_snapshot(2_000)],
        1_000.0,
    )
    .unwrap();
    let metrics = &result.metrics;
    assert_eq!(metrics.orders_submitted, 1);
    assert_eq!(metrics.orders_filled, 1);
    assert_eq!(metrics.fills_taker, 2, "the order walked two levels");
    assert_eq!(metrics.fills_maker, 0);
    assert_eq!(metrics.records_by_kind.get("book_snapshot"), Some(&2));
    assert!(metrics.explain_silence().is_none(), "this run traded");
}

#[test]
fn a_run_with_no_signals_explains_itself() {
    let mut strategy = SignalReplay::new(vec![]).unwrap();
    let result = run_with(&mut strategy, vec![deep_snapshot(1_000)], 100.0).unwrap();
    let reason = result.metrics.explain_silence().expect("a silent run");
    assert!(reason.contains("no orders"), "{reason}");
}

#[test]
fn a_run_whose_orders_all_bounce_says_which_wall_they_hit() {
    // Every order refused for margin: the summary is zero fills, and the
    // metrics say why.
    let mut strategy = BuyPerp {
        quantity: qty(1_000.0),
        submitted: false,
    };
    let result = run_perp(
        &mut strategy,
        vec![perp_snapshot(1_000, 100.0), perp_snapshot(2_000, 100.0)],
        100.0,
        Some(10.0),
    )
    .unwrap();
    assert_eq!(result.metrics.orders_filled, 0);
    let reason = result.metrics.explain_silence().expect("a silent run");
    assert!(reason.contains("margin"), "{reason}");
}

#[test]
fn gaps_and_maker_fills_are_counted_separately() {
    let mut strategy = RestBuy {
        limit: price(0.40),
        quantity: qty(10.0),
        submitted: false,
    };
    let result = run_queued(
        &mut strategy,
        vec![
            deep_snapshot(1_000),
            deep_snapshot(2_000),
            print_at(3_000, 0.40, 200.0, Some(Side::Sell)),
            deep_snapshot(4_000),
        ],
        QueuePositionFills::new(),
    )
    .unwrap();
    assert_eq!(result.metrics.fills_maker, 1, "a queue fill is passive");
    assert_eq!(result.metrics.fills_taker, 0);
    assert!(result.metrics.queue_joins >= 1);
    assert_eq!(result.metrics.records_by_kind.get("trade"), Some(&1));
}

#[test]
fn a_feed_gap_is_counted_before_it_becomes_an_error() {
    // The gap itself is recorded; the error only comes if an incremental
    // update follows without a snapshot.
    let records = vec![
        deep_snapshot(1_000),
        Record::new(
            Stamps::immediate(ts(2_000)),
            instrument_id(),
            OutcomeId::FIRST,
            MarketEvent::Gap,
        ),
        deep_snapshot(3_000),
    ];
    let mut strategy = SignalReplay::new(vec![]).unwrap();
    let result = run_with(&mut strategy, records, 100.0).unwrap();
    assert_eq!(result.metrics.book_gaps, 1);
    assert_eq!(result.metrics.records_by_kind.get("gap"), Some(&1));
}

// ---------------------------------------------------------------------------
// the execution seam (Tier 2, item 8)
// ---------------------------------------------------------------------------

use h5i_db_backtest::execution::{ExecutionClient, ExecutionCommand, SimulatedExecution};

/// A client that refuses everything, standing in for a venue that is down.
#[derive(Debug, Default)]
struct RefusingVenue {
    attempts: usize,
}

impl ExecutionClient for RefusingVenue {
    fn venue(&self) -> &str {
        "refusing"
    }
    fn send(&mut self, _command: ExecutionCommand, _ts: UnixNanos) -> Result<()> {
        self.attempts += 1;
        Err(h5i_db_backtest::BacktestError::invalid(
            "venue rejected the instruction",
        ))
    }
    fn sent(&self) -> usize {
        self.attempts
    }
}

#[test]
fn every_order_leaves_through_the_execution_client() {
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
        .execution_client(Box::new(SimulatedExecution::new("sim")))
        .build();
    let mut strategy = BuyOnce::market(qty(10.0));
    engine.run(&mut replay, &mut strategy).unwrap();

    assert_eq!(engine.execution().venue(), "sim");
    assert_eq!(engine.execution().sent(), 1, "one submit crossed the seam");
}

#[test]
fn a_venue_that_refuses_instructions_stops_the_run() {
    // The seam is real: a client that fails is not routed around.
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
        .execution_client(Box::new(RefusingVenue::default()))
        .build();
    let mut strategy = BuyOnce::market(qty(10.0));
    let error = engine.run(&mut replay, &mut strategy).unwrap_err();
    assert!(error.to_string().contains("rejected the instruction"));
}

#[test]
fn the_instruction_stream_is_what_two_venues_are_reconciled_on() {
    // Run the same strategy twice through separate clients and compare
    // fingerprints. This is how a live divergence would be localised: to
    // the decision, or to the venue's treatment of it.
    let fingerprint_for = |venue: &str| {
        let mut replay = Replay::builder()
            .stream(
                "book",
                priority::SNAPSHOT,
                vec![
                    deep_snapshot(1_000),
                    deep_snapshot(2_000),
                    deep_snapshot(3_000),
                ],
            )
            .build()
            .unwrap();
        let client = SimulatedExecution::new(venue);
        let mut engine = Engine::builder(instruments())
            .starting_cash(money(1_000.0))
            .execution_client(Box::new(client))
            .build();
        let mut strategy = BuyOnce::market(qty(10.0));
        engine.run(&mut replay, &mut strategy).unwrap();
        // The concrete client is recoverable for comparison.
        format!("{:?}", engine.execution().sent())
    };
    assert_eq!(fingerprint_for("sim"), fingerprint_for("live-shadow"));
}

#[test]
fn cancels_and_amendments_cross_the_seam_too() {
    struct RestAmendCancel {
        step: u32,
    }
    impl Strategy for RestAmendCancel {
        fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
            self.step += 1;
            match self.step {
                1 => ctx.submit(OrderRequest::limit(
                    instrument_id(),
                    OutcomeId::FIRST,
                    Side::Buy,
                    price(0.30),
                    qty(10.0),
                )),
                2 => ctx.amend(h5i_db_backtest::order::OrderId(1), Some(qty(5.0)), None),
                3 => ctx.cancel(h5i_db_backtest::order::OrderId(1)),
                _ => {}
            }
            Ok(())
        }
    }
    let mut replay = Replay::builder()
        .stream(
            "book",
            priority::SNAPSHOT,
            vec![
                deep_snapshot(1_000),
                deep_snapshot(2_000),
                deep_snapshot(3_000),
                deep_snapshot(4_000),
            ],
        )
        .build()
        .unwrap();
    let mut engine = Engine::builder(instruments())
        .starting_cash(money(1_000.0))
        .execution_client(Box::new(SimulatedExecution::new("sim")))
        .build();
    let mut strategy = RestAmendCancel { step: 0 };
    engine.run(&mut replay, &mut strategy).unwrap();
    assert_eq!(
        engine.execution().sent(),
        3,
        "submit, amend and cancel all reach the venue"
    );
}

#[test]
fn a_position_in_an_unconvertible_currency_suppresses_liquidation() {
    // The multi-currency half of the margin rule: a position whose
    // settlement currency has no rate to the reporting currency cannot be
    // valued, so the account cannot be judged insolvent on it.
    use h5i_db_backtest::currency::Currency;

    let mut set = InstrumentSet::new();
    set.insert(
        Inst::perpetual(PERP, "hyperliquid")
            .unwrap()
            .with_tick_size(price(0.5))
            .with_settlement_currency(Currency::new("EUR").unwrap()),
    )
    .unwrap();

    let mut replay = Replay::builder()
        .stream(
            "book",
            priority::SNAPSHOT,
            vec![
                perp_snapshot(1_000, 100.0),
                perp_snapshot(2_000, 100.0),
                perp_snapshot(3_000, 50.0),
                perp_snapshot(4_000, 50.0),
            ],
        )
        .build()
        .unwrap();
    let mut engine = Engine::builder(set)
        .starting_cash(money(120.0))
        .reporting_currency(Currency::new("USDC").unwrap())
        .margin_model(Box::new(LinearMargin::from_leverage(10.0).unwrap()))
        // No EUR/USDC rate is supplied.
        .build();
    let mut strategy = BuyPerp {
        quantity: qty(10.0),
        submitted: false,
    };
    let result = engine.run(&mut replay, &mut strategy).unwrap();

    assert!(
        result.liquidations.is_empty(),
        "an unvaluable position must not be closed out"
    );
    let state = engine
        .margin_state()
        .unwrap()
        .expect("a margin model is set");
    assert!(state.incomplete, "and the state says why");
}

#[test]
fn a_rate_makes_the_same_position_valuable_again() {
    use h5i_db_backtest::currency::{Currency, FxBook};

    let mut set = InstrumentSet::new();
    set.insert(
        Inst::perpetual(PERP, "hyperliquid")
            .unwrap()
            .with_tick_size(price(0.5))
            .with_settlement_currency(Currency::new("EUR").unwrap()),
    )
    .unwrap();

    let mut fx = FxBook::new();
    fx.set(
        Currency::new("EUR").unwrap(),
        Currency::new("USDC").unwrap(),
        price(1.0),
    )
    .unwrap();

    let mut replay = Replay::builder()
        .stream(
            "book",
            priority::SNAPSHOT,
            vec![
                perp_snapshot(1_000, 100.0),
                perp_snapshot(2_000, 100.0),
                perp_snapshot(3_000, 50.0),
                perp_snapshot(4_000, 50.0),
            ],
        )
        .build()
        .unwrap();
    let mut engine = Engine::builder(set)
        .starting_cash(money(120.0))
        .reporting_currency(Currency::new("USDC").unwrap())
        .margin_model(Box::new(LinearMargin::from_leverage(10.0).unwrap()))
        .fx(fx)
        .build();
    let mut strategy = BuyPerp {
        quantity: qty(10.0),
        submitted: false,
    };
    let result = engine.run(&mut replay, &mut strategy).unwrap();
    assert!(
        !result.liquidations.is_empty(),
        "with a rate, a 50% adverse move on 10x leverage is a liquidation"
    );
}

// ---------------------------------------------------------------------------
// corporate actions (Tier 1, item 5)
// ---------------------------------------------------------------------------

use h5i_db_backtest::corporate::CorporateAction;

const EQUITY: &str = "ACME";

fn equity_instruments() -> InstrumentSet {
    let mut set = InstrumentSet::new();
    set.insert(
        Inst::perpetual(EQUITY, "xnas")
            .unwrap()
            .with_tick_size(price(0.01)),
    )
    .unwrap();
    set
}

fn equity_snapshot(at: i64, mid: f64) -> Record {
    Record::new(
        Stamps::immediate(ts(at)),
        InstrumentId::new(EQUITY).unwrap(),
        OutcomeId::FIRST,
        MarketEvent::BookSnapshot {
            bids: vec![(price(mid - 0.01), qty(10_000.0))],
            asks: vec![(price(mid + 0.01), qty(10_000.0))],
        },
    )
}

fn action_at(at: i64, action: CorporateAction) -> Record {
    Record::new(
        Stamps::immediate(ts(at)),
        InstrumentId::new(EQUITY).unwrap(),
        OutcomeId::FIRST,
        MarketEvent::Corporate(action),
    )
}

struct BuyEquity {
    quantity: Qty,
    submitted: bool,
}

impl Strategy for BuyEquity {
    fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
        if !self.submitted {
            self.submitted = true;
            ctx.submit(OrderRequest::market(
                InstrumentId::new(EQUITY).unwrap(),
                OutcomeId::FIRST,
                Side::Buy,
                self.quantity,
            ));
        }
        Ok(())
    }
}

fn run_equity(strategy: &mut dyn Strategy, records: Vec<Record>, cash: f64) -> Result<RunResult> {
    let books: Vec<Record> = records
        .iter()
        .filter(|r| !matches!(r.event, MarketEvent::Corporate(_)))
        .cloned()
        .collect();
    let actions: Vec<Record> = records
        .into_iter()
        .filter(|r| matches!(r.event, MarketEvent::Corporate(_)))
        .collect();
    let mut replay = Replay::builder()
        .stream("book", priority::SNAPSHOT, books)
        .stream("corporate", priority::CORPORATE, actions)
        .build()?;
    let mut engine = Engine::builder(equity_instruments())
        .starting_cash(money(cash))
        .build();
    engine.run(&mut replay, strategy)
}

#[test]
fn a_split_leaves_the_position_worth_what_it_was() {
    let mut strategy = BuyEquity {
        quantity: qty(100.0),
        submitted: false,
    };
    let result = run_equity(
        &mut strategy,
        vec![
            equity_snapshot(1_000, 50.0),
            equity_snapshot(2_000, 50.0),
            action_at(3_000, CorporateAction::Split { ratio: price(2.0) }),
            equity_snapshot(4_000, 25.0),
        ],
        10_000.0,
    )
    .unwrap();

    let portfolio = h5i_db_backtest::position::Portfolio::replay(&result.fills).unwrap();
    // Portfolio::replay does not see the action, so compare against the
    // engine's own view via the equity curve instead: value is unchanged.
    let before = result
        .equity
        .iter()
        .find(|point| point.ts.get() >= 2_000)
        .unwrap()
        .equity;
    let after = result.equity.last().unwrap().equity;
    assert!(
        (before.to_f64() - after.to_f64()).abs() < 1.0,
        "a split moved equity from {before} to {after}"
    );
    let _ = portfolio;
    assert_eq!(result.metrics.corporate_actions, 1);
}

#[test]
fn a_dividend_pays_a_long_and_charges_a_short() {
    let long_result = {
        let mut strategy = BuyEquity {
            quantity: qty(100.0),
            submitted: false,
        };
        run_equity(
            &mut strategy,
            vec![
                equity_snapshot(1_000, 50.0),
                equity_snapshot(2_000, 50.0),
                action_at(
                    3_000,
                    CorporateAction::Dividend {
                        per_share: money(0.25),
                    },
                ),
                equity_snapshot(4_000, 50.0),
            ],
            10_000.0,
        )
        .unwrap()
    };
    // 100 shares at 0.25 each.
    assert_eq!(long_result.dividends_received, money(25.0));

    struct SellEquity {
        submitted: bool,
    }
    impl Strategy for SellEquity {
        fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
            if !self.submitted {
                self.submitted = true;
                ctx.submit(OrderRequest::market(
                    InstrumentId::new(EQUITY).unwrap(),
                    OutcomeId::FIRST,
                    Side::Sell,
                    qty(100.0),
                ));
            }
            Ok(())
        }
    }
    let mut shorting = SellEquity { submitted: false };
    let short_result = run_equity(
        &mut shorting,
        vec![
            equity_snapshot(1_000, 50.0),
            equity_snapshot(2_000, 50.0),
            action_at(
                3_000,
                CorporateAction::Dividend {
                    per_share: money(0.25),
                },
            ),
            equity_snapshot(4_000, 50.0),
        ],
        10_000.0,
    )
    .unwrap();
    assert_eq!(
        short_result.dividends_received,
        money(-25.0),
        "a short pays the dividend"
    );
}

#[test]
fn a_delisting_cashes_the_position_out_at_the_stated_price() {
    let mut strategy = BuyEquity {
        quantity: qty(100.0),
        submitted: false,
    };
    let result = run_equity(
        &mut strategy,
        vec![
            equity_snapshot(1_000, 50.0),
            equity_snapshot(2_000, 50.0),
            action_at(
                3_000,
                CorporateAction::Delist {
                    final_price: price(60.0),
                },
            ),
            equity_snapshot(4_000, 50.0),
        ],
        10_000.0,
    )
    .unwrap();

    let portfolio = h5i_db_backtest::position::Portfolio::replay(&result.fills).unwrap();
    assert!(
        portfolio
            .position(&InstrumentId::new(EQUITY).unwrap(), OutcomeId::FIRST)
            .unwrap()
            .is_flat(),
        "a delisting must close the position"
    );
    let settlement = result
        .fills
        .iter()
        .find(|fill| fill.tag.as_deref() == Some("delisting"))
        .expect("the cash settlement is tagged");
    assert_eq!(settlement.price, price(60.0));
    assert!(!settlement.is_taker, "a settlement crosses no book");
}

#[test]
fn a_worthless_delisting_is_a_total_loss_not_an_error() {
    let mut strategy = BuyEquity {
        quantity: qty(100.0),
        submitted: false,
    };
    let result = run_equity(
        &mut strategy,
        vec![
            equity_snapshot(1_000, 50.0),
            equity_snapshot(2_000, 50.0),
            action_at(
                3_000,
                CorporateAction::Delist {
                    final_price: Price::ZERO,
                },
            ),
            equity_snapshot(4_000, 50.0),
        ],
        10_000.0,
    )
    .unwrap();
    let portfolio = h5i_db_backtest::position::Portfolio::replay(&result.fills).unwrap();
    let position = portfolio
        .position(&InstrumentId::new(EQUITY).unwrap(), OutcomeId::FIRST)
        .unwrap();
    assert!(position.is_flat());
    // Bought around 50, settled at 0: the whole outlay is gone.
    assert!(position.realized_pnl.to_f64() < -4_900.0);
}

#[test]
fn a_split_reprices_a_resting_order_rather_than_leaving_it_marketable() {
    // The failure this prevents: a limit at 26 on a stock that halves from
    // 50 to 25 would suddenly be far through the market.
    struct RestBelow {
        submitted: bool,
    }
    impl Strategy for RestBelow {
        fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
            if !self.submitted {
                self.submitted = true;
                ctx.submit(OrderRequest::limit(
                    InstrumentId::new(EQUITY).unwrap(),
                    OutcomeId::FIRST,
                    Side::Buy,
                    price(26.0),
                    qty(100.0),
                ));
            }
            Ok(())
        }
    }
    let mut strategy = RestBelow { submitted: false };
    let result = run_equity(
        &mut strategy,
        vec![
            equity_snapshot(1_000, 50.0),
            equity_snapshot(2_000, 50.0),
            action_at(3_000, CorporateAction::Split { ratio: price(2.0) }),
            equity_snapshot(4_000, 25.0),
            equity_snapshot(5_000, 25.0),
        ],
        10_000.0,
    )
    .unwrap();

    assert!(
        result.fills.is_empty(),
        "the order should have been repriced to 13, not left at 26"
    );
    let order = &result.orders[0];
    assert_eq!(order.limit_price(), Some(price(13.0)));
    assert_eq!(order.quantity, qty(200.0), "and doubled in size");
}

#[test]
fn corporate_actions_land_before_anything_is_priced_against_them() {
    // Priority: an action at the same instant as a book update must be
    // applied first, or the book is read against a stale share count.
    let mut strategy = BuyEquity {
        quantity: qty(100.0),
        submitted: false,
    };
    let result = run_equity(
        &mut strategy,
        vec![
            equity_snapshot(1_000, 50.0),
            equity_snapshot(2_000, 50.0),
            action_at(3_000, CorporateAction::Split { ratio: price(2.0) }),
            equity_snapshot(3_000, 25.0),
            equity_snapshot(4_000, 25.0),
        ],
        10_000.0,
    )
    .unwrap();
    assert_eq!(result.metrics.corporate_actions, 1);
    let last = result.equity.last().unwrap();
    // 200 shares at 25 is the same 5,000 of exposure as 100 at 50.
    assert!(
        (last.position_value.to_f64() - 5_000.0).abs() < 20.0,
        "position value drifted to {}",
        last.position_value
    );
}
