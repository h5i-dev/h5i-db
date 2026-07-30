//! Prediction-market mechanics the kernel has to get right.
//!
//! These cover the venue behaviour that is specific to markets whose
//! outcomes sum to one: that a contract cannot be borrowed, that a complete
//! set trades against a dollar, that a market stops trading before it
//! resolves, that it need not resolve to a single winner, and that a run can
//! be scored against what the strategy actually believed.
//!
//! Each of these was, at some point, absent -- and each absence produced a
//! result that looked plausible and was wrong in a specific direction, which
//! is why the assertions here are about the direction of the error rather
//! than only about the mechanism.

use h5i_db_backtest::engine::{Context, Engine, OrderRequest, RunResult, Strategy};
use h5i_db_backtest::event::{MarketEvent, Record};
use h5i_db_backtest::instrument::{Instrument, InstrumentId, InstrumentSet, OutcomeId};
use h5i_db_backtest::models::ConstantLatency;
use h5i_db_backtest::order::{OrderStatus, TimeInForce};
use h5i_db_backtest::position::Portfolio;
use h5i_db_backtest::replay::{Replay, priority};
use h5i_db_backtest::run::RunReport;
use h5i_db_backtest::settlement::{Resolution, SettlementReport};
use h5i_db_backtest::types::{Money, Price, Qty, Side, Stamps, UnixNanos, notional};
use h5i_db_backtest::{Result, SetOperationCosts};

const MARKET: &str = "will-x-happen";
const WIDE: &str = "who-wins";

fn market_id() -> InstrumentId {
    InstrumentId::new(MARKET).unwrap()
}

fn wide_id() -> InstrumentId {
    InstrumentId::new(WIDE).unwrap()
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

fn binary_market() -> InstrumentSet {
    let mut set = InstrumentSet::new();
    set.insert(Instrument::binary(MARKET, "polymarket").unwrap())
        .unwrap();
    set
}

fn expiring_market(at: i64) -> InstrumentSet {
    let mut set = InstrumentSet::new();
    set.insert(
        Instrument::binary(MARKET, "polymarket")
            .unwrap()
            .with_expiration(ts(at)),
    )
    .unwrap();
    set
}

/// A four-way market the venue trades as one exclusive set.
fn wide_market(neg_risk: bool) -> InstrumentSet {
    let mut set = InstrumentSet::new();
    set.insert(
        Instrument::prediction_market(
            WIDE,
            "polymarket",
            vec!["A".into(), "B".into(), "C".into(), "D".into()],
        )
        .unwrap()
        .with_neg_risk(neg_risk),
    )
    .unwrap();
    set
}

/// A two-sided book on one outcome of `instrument`.
fn book(instrument: InstrumentId, outcome: OutcomeId, at: i64, bid: f64, ask: f64) -> Record {
    Record::new(
        Stamps::immediate(ts(at)),
        instrument,
        outcome,
        MarketEvent::BookSnapshot {
            bids: vec![(price(bid), qty(500.0))],
            asks: vec![(price(ask), qty(500.0))],
        },
    )
}

fn yes_book(at: i64, bid: f64, ask: f64) -> Record {
    book(market_id(), OutcomeId::FIRST, at, bid, ask)
}

fn run(
    instruments: InstrumentSet,
    strategy: &mut dyn Strategy,
    records: Vec<Record>,
    cash: f64,
) -> Result<RunResult> {
    run_configured(instruments, strategy, records, cash, |builder| builder)
}

fn run_configured(
    instruments: InstrumentSet,
    strategy: &mut dyn Strategy,
    records: Vec<Record>,
    cash: f64,
    configure: impl FnOnce(
        h5i_db_backtest::engine::EngineBuilder,
    ) -> h5i_db_backtest::engine::EngineBuilder,
) -> Result<RunResult> {
    let mut replay = Replay::builder()
        .stream("book", priority::SNAPSHOT, records)
        .build()?;
    let builder = Engine::builder(instruments).starting_cash(money(cash));
    let mut engine = configure(builder).build();
    engine.run(&mut replay, strategy)
}

/// What a scripted strategy does on one record.
type Step = Box<dyn Fn(&mut Context<'_>) -> Result<()>>;

/// Runs one closure per replayed record, so a test can say what happens on
/// step one and what happens on step two without a state machine.
struct Script {
    steps: Vec<Step>,
    seen: usize,
}

impl Script {
    fn new(steps: Vec<Step>) -> Self {
        Self { steps, seen: 0 }
    }
}

impl Strategy for Script {
    fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
        let step = self.seen;
        self.seen += 1;
        match self.steps.get(step) {
            Some(action) => action(ctx),
            None => Ok(()),
        }
    }
}

// -- 1. a prediction-market contract cannot be borrowed ---------------------

#[test]
fn selling_an_outcome_you_do_not_hold_is_refused() {
    // The bug: this used to fill, credit the account the proceeds, and post
    // the mark as collateral against a loss of one minus the mark.
    let mut strategy = Script::new(vec![Box::new(|ctx: &mut Context<'_>| {
        ctx.submit(OrderRequest::market(
            market_id(),
            OutcomeId::FIRST,
            Side::Sell,
            qty(10.0),
        ));
        Ok(())
    })]);
    let result = run(
        binary_market(),
        &mut strategy,
        vec![yes_book(1, 0.40, 0.42)],
        1_000.0,
    )
    .unwrap();

    assert!(result.fills.is_empty(), "a naked short must not fill");
    assert_eq!(result.metrics.orders_rejected_naked_short, 1);
    assert_eq!(
        result.final_cash,
        money(1_000.0),
        "no proceeds were credited"
    );
    assert_eq!(result.orders[0].status, OrderStatus::Rejected);
    let reason = result.orders[0].reject_reason.clone().unwrap();
    assert!(
        reason.contains("cannot be borrowed"),
        "the rejection must say why: {reason}"
    );
    assert!(
        reason.contains("NO"),
        "and must name the trade that expresses the same view: {reason}"
    );
}

#[test]
fn selling_what_you_hold_is_ordinary_business() {
    let mut strategy = Script::new(vec![
        Box::new(|ctx: &mut Context<'_>| {
            ctx.submit(OrderRequest::market(
                market_id(),
                OutcomeId::FIRST,
                Side::Buy,
                qty(10.0),
            ));
            Ok(())
        }),
        Box::new(|ctx: &mut Context<'_>| {
            ctx.submit(OrderRequest::market(
                market_id(),
                OutcomeId::FIRST,
                Side::Sell,
                qty(10.0),
            ));
            Ok(())
        }),
    ]);
    let result = run(
        binary_market(),
        &mut strategy,
        vec![yes_book(1, 0.40, 0.42), yes_book(2, 0.40, 0.42)],
        1_000.0,
    )
    .unwrap();

    assert_eq!(result.metrics.orders_rejected_naked_short, 0);
    assert_eq!(result.fills.len(), 2);
    // Bought at the 0.42 ask, sold into the 0.40 bid: two cents a contract.
    assert_eq!(result.realized_pnl, money(-0.2));
}

#[test]
fn two_live_sells_cannot_jointly_exceed_the_position() {
    // Either may fill first, so each must be safe on the assumption that
    // every other one already has. This is the same worst-case reading
    // `max_abs_position` uses.
    let mut strategy = Script::new(vec![
        Box::new(|ctx: &mut Context<'_>| {
            ctx.submit(OrderRequest::market(
                market_id(),
                OutcomeId::FIRST,
                Side::Buy,
                qty(10.0),
            ));
            Ok(())
        }),
        Box::new(|ctx: &mut Context<'_>| {
            for _ in 0..2 {
                ctx.submit(
                    OrderRequest::limit(
                        market_id(),
                        OutcomeId::FIRST,
                        Side::Sell,
                        price(0.90),
                        qty(6.0),
                    )
                    .with_time_in_force(TimeInForce::GoodTilCancel),
                );
            }
            Ok(())
        }),
    ]);
    let result = run(
        binary_market(),
        &mut strategy,
        vec![yes_book(1, 0.40, 0.42), yes_book(2, 0.40, 0.42)],
        1_000.0,
    )
    .unwrap();

    assert_eq!(
        result.metrics.orders_rejected_naked_short, 1,
        "the second sell would leave a short of two if both filled"
    );
}

#[test]
fn a_reduce_only_sell_is_never_a_naked_short() {
    // Matching already clamps a reduce-only order to what would close, so
    // it cannot open a short and does not need the constraint.
    let mut strategy = Script::new(vec![Box::new(|ctx: &mut Context<'_>| {
        ctx.submit(
            OrderRequest::market(market_id(), OutcomeId::FIRST, Side::Sell, qty(10.0))
                .reduce_only(),
        );
        Ok(())
    })]);
    let result = run(
        binary_market(),
        &mut strategy,
        vec![yes_book(1, 0.40, 0.42)],
        1_000.0,
    )
    .unwrap();

    assert_eq!(result.metrics.orders_rejected_naked_short, 0);
    assert!(result.fills.is_empty(), "there was nothing to reduce");
}

#[test]
fn the_constraint_can_be_lifted_to_measure_what_it_costs() {
    let mut strategy = Script::new(vec![Box::new(|ctx: &mut Context<'_>| {
        ctx.submit(OrderRequest::market(
            market_id(),
            OutcomeId::FIRST,
            Side::Sell,
            qty(10.0),
        ));
        Ok(())
    })]);
    let result = run_configured(
        binary_market(),
        &mut strategy,
        vec![yes_book(1, 0.40, 0.42)],
        1_000.0,
        |builder| builder.allow_naked_shorts(true),
    )
    .unwrap();

    assert_eq!(result.metrics.orders_rejected_naked_short, 0);
    assert_eq!(result.fills.len(), 1);
}

#[test]
fn a_perpetual_may_still_be_shorted() {
    // The constraint is about prediction-market contracts, not about sells.
    let mut set = InstrumentSet::new();
    set.insert(Instrument::perpetual("btc-perp", "hyperliquid").unwrap())
        .unwrap();
    let id = InstrumentId::new("btc-perp").unwrap();
    let records = vec![Record::new(
        Stamps::immediate(ts(1)),
        id.clone(),
        OutcomeId::FIRST,
        MarketEvent::BookSnapshot {
            bids: vec![(price(100.0), qty(10.0))],
            asks: vec![(price(101.0), qty(10.0))],
        },
    )];
    let mut strategy = Script::new(vec![Box::new(move |ctx: &mut Context<'_>| {
        ctx.submit(OrderRequest::market(
            InstrumentId::new("btc-perp").unwrap(),
            OutcomeId::FIRST,
            Side::Sell,
            qty(1.0),
        ));
        Ok(())
    })]);
    let result = run(set, &mut strategy, records, 10_000.0).unwrap();

    assert_eq!(result.metrics.orders_rejected_naked_short, 0);
    assert_eq!(result.fills.len(), 1);
}

// -- 6. a market that has closed cannot be traded ---------------------------

#[test]
fn an_order_submitted_after_the_close_is_refused() {
    let mut strategy = Script::new(vec![
        Box::new(|_ctx: &mut Context<'_>| Ok(())),
        Box::new(|ctx: &mut Context<'_>| {
            ctx.submit(OrderRequest::market(
                market_id(),
                OutcomeId::FIRST,
                Side::Buy,
                qty(10.0),
            ));
            Ok(())
        }),
    ]);
    // Data keeps arriving after the close, which is normal; trading on it
    // is not.
    let result = run(
        expiring_market(50),
        &mut strategy,
        vec![yes_book(1, 0.40, 0.42), yes_book(100, 0.40, 0.42)],
        1_000.0,
    )
    .unwrap();

    assert!(result.fills.is_empty());
    assert_eq!(result.metrics.orders_rejected_expired, 1);
    assert_eq!(result.expirations.len(), 1);
    assert_eq!(result.expirations[0].1, ts(50));
    assert!(
        result.orders[0]
            .reject_reason
            .as_deref()
            .unwrap()
            .contains("stopped trading")
    );
}

#[test]
fn an_order_that_arrives_after_the_close_is_refused() {
    // Sent in time, delivered late. Filling it would be a trade nobody
    // could have got done.
    let mut strategy = Script::new(vec![Box::new(|ctx: &mut Context<'_>| {
        ctx.submit(OrderRequest::market(
            market_id(),
            OutcomeId::FIRST,
            Side::Buy,
            qty(10.0),
        ));
        Ok(())
    })]);
    let result = run_configured(
        expiring_market(50),
        &mut strategy,
        // Sent at 1, released at 501, and the market shut at 50.
        vec![yes_book(1, 0.40, 0.42), yes_book(1_000, 0.40, 0.42)],
        1_000.0,
        |builder| {
            builder.latency_model(Box::new(ConstantLatency {
                insert: 500,
                cancel: 0,
            }))
        },
    )
    .unwrap();

    assert!(result.fills.is_empty());
    assert_eq!(result.metrics.orders_rejected_expired, 1);
    assert!(
        result.orders[0]
            .reject_reason
            .as_deref()
            .unwrap()
            .contains("before this order arrived")
    );
}

#[test]
fn a_resting_order_is_cancelled_when_the_market_closes() {
    let mut strategy = Script::new(vec![Box::new(|ctx: &mut Context<'_>| {
        ctx.submit(
            OrderRequest::limit(
                market_id(),
                OutcomeId::FIRST,
                Side::Buy,
                price(0.10),
                qty(10.0),
            )
            .with_time_in_force(TimeInForce::GoodTilCancel),
        );
        Ok(())
    })]);
    let result = run(
        expiring_market(50),
        &mut strategy,
        vec![yes_book(1, 0.40, 0.42), yes_book(100, 0.40, 0.42)],
        1_000.0,
    )
    .unwrap();

    assert_eq!(
        result.orders[0].status,
        OrderStatus::Cancelled,
        "an order that can never fill must terminate rather than rot"
    );
    assert!(
        result.orders[0]
            .reject_reason
            .as_deref()
            .unwrap()
            .contains("stopped trading at 50")
    );
}

#[test]
fn a_market_with_no_expiry_trades_throughout() {
    let mut strategy = Script::new(vec![
        Box::new(|_ctx: &mut Context<'_>| Ok(())),
        Box::new(|ctx: &mut Context<'_>| {
            ctx.submit(OrderRequest::market(
                market_id(),
                OutcomeId::FIRST,
                Side::Buy,
                qty(10.0),
            ));
            Ok(())
        }),
    ]);
    let result = run(
        binary_market(),
        &mut strategy,
        vec![yes_book(1, 0.40, 0.42), yes_book(100, 0.40, 0.42)],
        1_000.0,
    )
    .unwrap();

    assert_eq!(result.metrics.orders_rejected_expired, 0);
    assert_eq!(result.fills.len(), 1);
}

// -- 2. a complete set trades against a dollar -----------------------------

/// Mints on the first record, then optionally redeems on the second.
struct MintThenRedeem {
    instrument: InstrumentId,
    sets: Qty,
    redeem: bool,
}

impl Strategy for MintThenRedeem {
    fn on_event(&mut self, ctx: &mut Context<'_>, record: &Record) -> Result<()> {
        if record.ts() == ts(1) {
            ctx.mint(&self.instrument, self.sets);
        } else if self.redeem && record.ts() == ts(2) {
            ctx.redeem(&self.instrument, self.sets);
        }
        Ok(())
    }
}

#[test]
fn minting_a_set_costs_exactly_one_and_yields_every_outcome() {
    let mut strategy = MintThenRedeem {
        instrument: market_id(),
        sets: qty(100.0),
        redeem: false,
    };
    let result = run(
        binary_market(),
        &mut strategy,
        vec![yes_book(1, 0.40, 0.42)],
        1_000.0,
    )
    .unwrap();

    assert_eq!(result.set_operations.len(), 1);
    assert_eq!(result.set_operations[0].sets, qty(100.0));
    assert_eq!(
        result.set_operations[0].cash_delta,
        money(100.0),
        "a hundred sets cost exactly a hundred, whatever the book says"
    );
    assert_eq!(result.final_cash, money(900.0));

    let portfolio = Portfolio::replay(&result.fills).unwrap();
    for outcome in [OutcomeId(0), OutcomeId(1)] {
        assert_eq!(
            portfolio
                .position(&market_id(), outcome)
                .map(|p| p.quantity),
            Some(qty(100.0)),
            "outcome {outcome} must be held"
        );
    }
}

#[test]
fn a_mint_is_visible_in_the_fills_a_position_is_rebuilt_from() {
    // The audit guarantee: `bt_fills` alone reconstructs `bt_positions`. A
    // position that moved without a fill to explain it is a run whose
    // stored result and audit disagree.
    let mut strategy = MintThenRedeem {
        instrument: market_id(),
        sets: qty(10.0),
        redeem: false,
    };
    let result = run(
        binary_market(),
        &mut strategy,
        vec![yes_book(1, 0.40, 0.42)],
        1_000.0,
    )
    .unwrap();

    assert_eq!(result.fills.len(), 2, "one leg per outcome");
    assert!(
        result
            .fills
            .iter()
            .all(|fill| fill.tag.as_deref() == Some("mint"))
    );
    let mut legs = Money::ZERO;
    for fill in &result.fills {
        legs = legs
            .checked_add(notional(fill.price, fill.quantity).unwrap())
            .unwrap();
    }
    assert_eq!(
        legs,
        money(10.0),
        "the legs must sum to exactly one unit of cash per set"
    );
}

#[test]
fn a_mint_and_redeem_round_trip_is_cash_neutral() {
    let mut strategy = MintThenRedeem {
        instrument: market_id(),
        sets: qty(100.0),
        redeem: true,
    };
    let result = run(
        binary_market(),
        &mut strategy,
        // The book moves between the two, which must not change the total:
        // a set is worth one on the way in and one on the way out.
        vec![yes_book(1, 0.40, 0.42), yes_book(2, 0.70, 0.72)],
        1_000.0,
    )
    .unwrap();

    assert_eq!(result.set_operations.len(), 2);
    assert_eq!(result.final_cash, money(1_000.0));
    assert_eq!(
        result.realized_pnl,
        Money::ZERO,
        "however the dollar was divided across outcomes, it nets to zero"
    );
    let portfolio = Portfolio::replay(&result.fills).unwrap();
    assert_eq!(portfolio.open_positions().count(), 0);
}

#[test]
fn a_set_operation_costs_a_flat_fee_charged_once() {
    let mut strategy = MintThenRedeem {
        instrument: market_id(),
        sets: qty(100.0),
        redeem: true,
    };
    let result = run_configured(
        binary_market(),
        &mut strategy,
        vec![yes_book(1, 0.40, 0.42), yes_book(2, 0.40, 0.42)],
        1_000.0,
        |builder| builder.set_operation_costs(SetOperationCosts::flat(money(0.25)).unwrap()),
    )
    .unwrap();

    // Two operations at 25 cents each, not two legs each.
    assert_eq!(result.final_cash, money(999.5));
    assert_eq!(result.commissions, money(0.5));
}

#[test]
fn redeeming_without_the_whole_set_is_refused() {
    // Without this the redemption would manufacture a short in the leg it
    // was missing and pay cash for it.
    struct BuyOneLegThenRedeem;
    impl Strategy for BuyOneLegThenRedeem {
        fn on_event(&mut self, ctx: &mut Context<'_>, record: &Record) -> Result<()> {
            if record.ts() == ts(1) {
                ctx.submit(OrderRequest::market(
                    market_id(),
                    OutcomeId::FIRST,
                    Side::Buy,
                    qty(10.0),
                ));
            } else if record.ts() == ts(2) {
                ctx.redeem(&market_id(), qty(10.0));
            }
            Ok(())
        }
    }

    let result = run(
        binary_market(),
        &mut BuyOneLegThenRedeem,
        vec![yes_book(1, 0.40, 0.42), yes_book(2, 0.40, 0.42)],
        1_000.0,
    )
    .unwrap();

    assert_eq!(result.metrics.set_operations_rejected, 1);
    let refusal = result.set_operations[0].rejected.clone().unwrap();
    assert!(refusal.contains("outcome 1"), "{refusal}");
    let portfolio = Portfolio::replay(&result.fills).unwrap();
    assert_eq!(
        portfolio
            .position(&market_id(), OutcomeId(1))
            .map(|p| p.quantity),
        None,
        "no phantom short was opened"
    );
}

#[test]
fn minting_more_than_the_account_can_pay_for_is_refused() {
    let mut strategy = MintThenRedeem {
        instrument: market_id(),
        sets: qty(5_000.0),
        redeem: false,
    };
    let result = run(
        binary_market(),
        &mut strategy,
        vec![yes_book(1, 0.40, 0.42)],
        100.0,
    )
    .unwrap();

    assert_eq!(result.metrics.set_operations_rejected, 1);
    assert_eq!(result.final_cash, money(100.0));
    assert!(result.fills.is_empty());
}

#[test]
fn a_market_that_does_not_trade_as_a_set_cannot_be_minted() {
    // Four grouped conditions with no venue contract binding them are not
    // a set; minting across them would create a dollar out of nothing.
    let mut strategy = MintThenRedeem {
        instrument: wide_id(),
        sets: qty(10.0),
        redeem: false,
    };
    let error = run(
        wide_market(false),
        &mut strategy,
        vec![book(wide_id(), OutcomeId::FIRST, 1, 0.20, 0.22)],
        1_000.0,
    )
    .unwrap_err();
    assert!(error.to_string().contains("complete set"), "{error}");
}

#[test]
fn a_wide_negative_risk_market_mints_across_every_outcome() {
    let mut strategy = MintThenRedeem {
        instrument: wide_id(),
        sets: qty(40.0),
        redeem: false,
    };
    let result = run(
        wide_market(true),
        &mut strategy,
        vec![book(wide_id(), OutcomeId::FIRST, 1, 0.20, 0.22)],
        1_000.0,
    )
    .unwrap();

    assert_eq!(result.fills.len(), 4);
    assert_eq!(result.final_cash, money(960.0));
}

#[test]
fn a_set_operation_waits_out_its_latency() {
    // A mint that lands instantly is an arbitrage nobody could have taken.
    let mut strategy = MintThenRedeem {
        instrument: market_id(),
        sets: qty(10.0),
        redeem: false,
    };
    let result = run_configured(
        binary_market(),
        &mut strategy,
        vec![yes_book(1, 0.40, 0.42), yes_book(1_000, 0.40, 0.42)],
        1_000.0,
        |builder| {
            builder.latency_model(Box::new(ConstantLatency {
                insert: 500,
                cancel: 0,
            }))
        },
    )
    .unwrap();

    assert_eq!(result.set_operations.len(), 1);
    assert_eq!(
        result.set_operations[0].ts,
        ts(1_000),
        "the venue saw it on the record after the latency elapsed"
    );
}

// -- 3. negative-risk conversion -------------------------------------------

/// Mints enough to hold the basket, then converts or redeems.
struct MintThenConvert {
    held: Option<Vec<OutcomeId>>,
    quantity: Qty,
}

impl Strategy for MintThenConvert {
    fn on_event(&mut self, ctx: &mut Context<'_>, record: &Record) -> Result<()> {
        if record.ts() == ts(1) {
            ctx.mint(&wide_id(), qty(100.0));
        } else if record.ts() == ts(2) {
            match &self.held {
                Some(held) => ctx.convert(&wide_id(), held.clone(), self.quantity),
                // The claim under test: converting the NO side of k
                // outcomes is redeeming k - 1 sets.
                None => ctx.redeem(&wide_id(), self.quantity),
            }
        }
        Ok(())
    }
}

#[test]
fn a_conversion_is_a_redemption_of_one_fewer_set_than_outcomes_named() {
    let records = || {
        vec![
            book(wide_id(), OutcomeId::FIRST, 1, 0.20, 0.22),
            book(wide_id(), OutcomeId::FIRST, 2, 0.20, 0.22),
        ]
    };
    // Three outcomes' NO side, twenty contracts each: forty sets.
    let mut converting = MintThenConvert {
        held: Some(vec![OutcomeId(0), OutcomeId(1), OutcomeId(2)]),
        quantity: qty(20.0),
    };
    let converted = run(wide_market(true), &mut converting, records(), 1_000.0).unwrap();

    let mut redeeming = MintThenConvert {
        held: None,
        quantity: qty(40.0),
    };
    let redeemed = run(wide_market(true), &mut redeeming, records(), 1_000.0).unwrap();

    assert_eq!(converted.set_operations[1].sets, qty(40.0));
    assert_eq!(converted.final_cash, redeemed.final_cash);
    let left = Portfolio::replay(&converted.fills).unwrap();
    let right = Portfolio::replay(&redeemed.fills).unwrap();
    for outcome in [OutcomeId(0), OutcomeId(1), OutcomeId(2), OutcomeId(3)] {
        assert_eq!(
            left.position(&wide_id(), outcome).map(|p| p.quantity),
            right.position(&wide_id(), outcome).map(|p| p.quantity),
            "outcome {outcome} must land in the same place either way"
        );
    }
}

#[test]
fn a_conversion_needs_a_market_the_venue_wired_as_one_set() {
    let mut strategy = MintThenConvert {
        held: Some(vec![OutcomeId(0), OutcomeId(1)]),
        quantity: qty(10.0),
    };
    let error = run(
        wide_market(false),
        &mut strategy,
        vec![
            book(wide_id(), OutcomeId::FIRST, 1, 0.20, 0.22),
            book(wide_id(), OutcomeId::FIRST, 2, 0.20, 0.22),
        ],
        1_000.0,
    )
    .unwrap_err();
    assert!(error.to_string().contains("complete set"), "{error}");
}

#[test]
fn a_conversion_must_leave_a_residual_to_be_paid_in() {
    for held in [
        vec![OutcomeId(0)],
        vec![OutcomeId(0), OutcomeId(1), OutcomeId(2), OutcomeId(3)],
    ] {
        let mut strategy = MintThenConvert {
            held: Some(held.clone()),
            quantity: qty(10.0),
        };
        let error = run(
            wide_market(true),
            &mut strategy,
            vec![
                book(wide_id(), OutcomeId::FIRST, 1, 0.20, 0.22),
                book(wide_id(), OutcomeId::FIRST, 2, 0.20, 0.22),
            ],
            1_000.0,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("between 2 and"),
            "{held:?}: {error}"
        );
    }
}

// -- 5. a run can be scored against what the strategy believed --------------

/// States a probability on every record, and buys nothing.
struct Forecaster {
    probability: f64,
}

impl Strategy for Forecaster {
    fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
        ctx.record_forecast(&market_id(), OutcomeId::FIRST, price(self.probability))
    }
}

fn report(result: RunResult, resolutions: Vec<Resolution>) -> RunReport {
    RunReport {
        run_id: "scoring".to_string(),
        fork: String::new(),
        digest: String::new(),
        result,
        settlement: SettlementReport::default(),
        coverage: None,
        instruments: binary_market(),
        resolutions,
    }
}

#[test]
fn a_stated_forecast_carries_the_price_it_was_stated_against() {
    let mut strategy = Forecaster { probability: 0.65 };
    let result = run(
        binary_market(),
        &mut strategy,
        vec![yes_book(1, 0.40, 0.42)],
        1_000.0,
    )
    .unwrap();

    assert_eq!(result.forecasts.len(), 1);
    assert_eq!(result.forecasts[0].probability, price(0.65));
    assert_eq!(
        result.forecasts[0].market,
        Some(price(0.41)),
        "the mid the strategy was looking at, captured at the same instant"
    );
}

#[test]
fn a_forecast_outside_zero_to_one_is_refused() {
    struct Impossible;
    impl Strategy for Impossible {
        fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
            ctx.record_forecast(&market_id(), OutcomeId::FIRST, price(1.4))
        }
    }
    assert!(
        run(
            binary_market(),
            &mut Impossible,
            vec![yes_book(1, 0.40, 0.42)],
            1_000.0
        )
        .is_err()
    );
}

#[test]
fn calibration_samples_join_a_forecast_to_what_actually_happened() {
    let mut strategy = Forecaster { probability: 0.65 };
    let result = run(
        binary_market(),
        &mut strategy,
        vec![yes_book(1, 0.40, 0.42), yes_book(2, 0.45, 0.47)],
        1_000.0,
    )
    .unwrap();
    let report = report(
        result,
        vec![Resolution::new(market_id(), OutcomeId::FIRST, ts(9))],
    );

    let samples = report.calibration_samples();
    assert_eq!(samples.len(), 2);
    assert_eq!(samples[0].forecast, price(0.65));
    assert_eq!(samples[0].market, Some(price(0.41)));
    assert_eq!(samples[0].realized, Price::PROBABILITY_MAX);
    assert!(report.unscored_forecasts().is_empty());
}

#[test]
fn a_voided_market_is_dropped_from_the_sample_rather_than_diluting_it() {
    // Every forecast scores identically against a market that paid both
    // sides the same, including a confidently wrong one.
    let mut strategy = Forecaster { probability: 0.95 };
    let result = run(
        binary_market(),
        &mut strategy,
        vec![yes_book(1, 0.40, 0.42)],
        1_000.0,
    )
    .unwrap();
    let report = report(
        result,
        vec![Resolution::void(market_id(), 2, ts(9)).unwrap()],
    );

    assert!(report.calibration_samples().is_empty());
    let dropped = report.unscored_forecasts();
    assert_eq!(dropped.len(), 1);
    assert_eq!(dropped[0].count, 1);
    assert!(dropped[0].reason.contains("every outcome the same"));
    assert!(
        report
            .warnings()
            .iter()
            .any(|warning| warning.contains("were not scored"))
    );
}

#[test]
fn a_forecast_on_an_unresolved_market_is_reported_not_silently_lost() {
    let mut strategy = Forecaster { probability: 0.65 };
    let result = run(
        binary_market(),
        &mut strategy,
        vec![yes_book(1, 0.40, 0.42)],
        1_000.0,
    )
    .unwrap();
    let report = report(result, vec![]);

    assert!(report.calibration_samples().is_empty());
    assert!(
        report.unscored_forecasts()[0]
            .reason
            .contains("no known resolution")
    );
}

#[test]
fn a_partial_settlement_scores_against_the_fraction_it_paid() {
    let mut strategy = Forecaster { probability: 0.65 };
    let result = run(
        binary_market(),
        &mut strategy,
        vec![yes_book(1, 0.40, 0.42)],
        1_000.0,
    )
    .unwrap();
    let report = report(
        result,
        vec![Resolution::split(market_id(), vec![price(0.7), price(0.3)], ts(9)).unwrap()],
    );

    assert_eq!(report.calibration_samples()[0].realized, price(0.7));
}

#[test]
fn the_mark_curve_records_what_the_market_itself_forecast() {
    let mut strategy = Forecaster { probability: 0.65 };
    let result = run_configured(
        binary_market(),
        &mut strategy,
        vec![yes_book(1, 0.40, 0.42), yes_book(2, 0.60, 0.62)],
        1_000.0,
        |builder| builder.equity_interval_nanos(1).unwrap(),
    )
    .unwrap();

    let prices: Vec<Price> = result.mark_curve.iter().map(|point| point.price).collect();
    assert_eq!(prices, vec![price(0.41), price(0.61)]);
    assert!(
        result
            .mark_curve
            .iter()
            .all(|point| point.instrument == market_id())
    );
}

#[test]
fn the_mark_curve_can_be_turned_off_for_a_run_spanning_many_markets() {
    let mut strategy = Forecaster { probability: 0.65 };
    let result = run_configured(
        binary_market(),
        &mut strategy,
        vec![yes_book(1, 0.40, 0.42)],
        1_000.0,
        |builder| builder.record_mark_curve(false),
    )
    .unwrap();
    assert!(result.mark_curve.is_empty());
}
