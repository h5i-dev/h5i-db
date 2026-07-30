//! Perpetual mechanics: the prices a venue margins and funds against.
//!
//! A derivatives venue does not value your position at the mid. It
//! publishes a mark, derived from an oracle and the book, and margins
//! against that; it charges funding on the oracle. Substituting the mid for
//! either is not a small approximation, and these tests are about the
//! direction of the error rather than only the mechanism.

use h5i_db_backtest::Result;
use h5i_db_backtest::account::{LinearMargin, MarginModel, PerInstrumentMargin};
use h5i_db_backtest::engine::{
    Context, Engine, EngineBuilder, LiquidationPolicy, MarkSource, OrderRequest, RunResult,
    Strategy,
};
use h5i_db_backtest::event::{MarketEvent, Record};
use h5i_db_backtest::instrument::{Instrument, InstrumentId, InstrumentSet, OutcomeId};
use h5i_db_backtest::models::{FeeContext, FeeModel, FeeTier, TieredFees};
use h5i_db_backtest::order::{OrderId, OrderStatus, TimeInForce};
use h5i_db_backtest::replay::{Replay, priority};
use h5i_db_backtest::types::{Money, Price, Qty, Side, Stamps, UnixNanos};

const PERP: &str = "BTC-PERP";

fn perp_id() -> InstrumentId {
    InstrumentId::new(PERP).unwrap()
}

fn instruments() -> InstrumentSet {
    let mut set = InstrumentSet::new();
    set.insert(Instrument::perpetual(PERP, "hyperliquid").unwrap())
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

fn book(at: i64, bid: f64, ask: f64, size: f64) -> Record {
    Record::new(
        Stamps::immediate(ts(at)),
        perp_id(),
        OutcomeId::FIRST,
        MarketEvent::BookSnapshot {
            bids: vec![(price(bid), qty(size))],
            asks: vec![(price(ask), qty(size))],
        },
    )
}

fn reference(at: i64, mark: Option<f64>, oracle: Option<f64>) -> Record {
    Record::new(
        Stamps::immediate(ts(at)),
        perp_id(),
        OutcomeId::FIRST,
        MarketEvent::Reference {
            mark: mark.map(price),
            oracle: oracle.map(price),
        },
    )
}

fn funding(at: i64, rate: f64) -> Record {
    Record::new(
        Stamps::immediate(ts(at)),
        perp_id(),
        OutcomeId::FIRST,
        MarketEvent::Funding { rate: price(rate) },
    )
}

/// Buys once on the first record and then does nothing.
struct BuyOnce {
    quantity: Qty,
    done: bool,
}

impl BuyOnce {
    fn new(quantity: Qty) -> Self {
        Self {
            quantity,
            done: false,
        }
    }
}

impl Strategy for BuyOnce {
    fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
        if self.done {
            return Ok(());
        }
        self.done = true;
        ctx.submit(OrderRequest::market(
            perp_id(),
            OutcomeId::FIRST,
            Side::Buy,
            self.quantity,
        ));
        Ok(())
    }
}

fn run(
    strategy: &mut dyn Strategy,
    records: Vec<Record>,
    cash: f64,
    configure: impl FnOnce(EngineBuilder) -> EngineBuilder,
) -> Result<RunResult> {
    // References carry their own priority so an oracle lands before the
    // funding charged against it at the same instant.
    let (references, rest): (Vec<Record>, Vec<Record>) = records
        .into_iter()
        .partition(|record| matches!(record.event, MarketEvent::Reference { .. }));
    let mut replay = Replay::builder()
        .stream("book", priority::SNAPSHOT, rest)
        .stream("references", priority::REFERENCE, references)
        .build()?;
    let builder = Engine::builder(instruments()).starting_cash(money(cash));
    let mut engine = configure(builder).build();
    engine.run(&mut replay, strategy)
}

#[test]
fn a_position_is_valued_at_the_venues_mark_not_the_mid() {
    let mut strategy = BuyOnce::new(qty(1.0));
    let result = run(
        &mut strategy,
        vec![
            book(1, 100.0, 100.0, 10.0),
            // The book falls away to a wide, thin quote; the venue's mark
            // barely moves, because it is anchored to an oracle.
            book(2, 60.0, 140.0, 1.0),
            reference(2, Some(101.0), Some(101.0)),
        ],
        10_000.0,
        |builder| builder,
    )
    .unwrap();

    let last = result.equity.last().unwrap();
    assert_eq!(
        last.unrealized_pnl,
        money(1.0),
        "the mark says the position is a dollar up; the mid says nothing \
         useful about a book that wide"
    );
    assert_eq!(result.marks[&(perp_id(), OutcomeId::FIRST)], price(101.0));
}

#[test]
fn the_book_can_still_be_forced_to_win() {
    let mut strategy = BuyOnce::new(qty(1.0));
    let result = run(
        &mut strategy,
        vec![
            book(1, 100.0, 100.0, 10.0),
            book(2, 60.0, 140.0, 1.0),
            reference(2, Some(101.0), Some(101.0)),
        ],
        10_000.0,
        |builder| builder.mark_source(MarkSource::BookMid),
    )
    .unwrap();

    assert_eq!(result.marks[&(perp_id(), OutcomeId::FIRST)], price(100.0));
}

#[test]
fn a_wick_in_a_thin_book_does_not_liquidate_a_position_the_venue_holds() {
    // The headline case. Ten times leverage on a hundred dollars of
    // notional; a one-print collapse in the book would wipe the maintenance
    // buffer, but the venue's own mark never went there, so the venue would
    // not have closed anything.
    let records = vec![
        book(1, 100.0, 100.0, 10.0),
        book(2, 60.0, 60.0, 10.0),
        reference(2, Some(99.0), Some(99.0)),
        book(3, 100.0, 100.0, 10.0),
    ];

    let mut on_mark = BuyOnce::new(qty(10.0));
    let marked = run(&mut on_mark, records.clone(), 120.0, |builder| {
        builder.margin_model(Box::new(LinearMargin::from_leverage(10.0).unwrap()))
    })
    .unwrap();
    assert!(
        marked.liquidations.is_empty(),
        "the venue's mark stayed at 99; nothing was liquidatable"
    );

    let mut on_mid = BuyOnce::new(qty(10.0));
    let midded = run(&mut on_mid, records, 120.0, |builder| {
        builder
            .margin_model(Box::new(LinearMargin::from_leverage(10.0).unwrap()))
            .mark_source(MarkSource::BookMid)
    })
    .unwrap();
    assert!(
        !midded.liquidations.is_empty(),
        "valuing at the mid invents a liquidation, which then reads as a \
         strategy result rather than a modelling artefact"
    );
}

#[test]
fn funding_is_charged_on_the_oracle_not_the_book() {
    // Hyperliquid charges funding on the oracle precisely so a thin book
    // cannot inflate the payment. At hourly settlement the difference
    // compounds over a carry.
    let mut strategy = BuyOnce::new(qty(10.0));
    let result = run(
        &mut strategy,
        vec![
            book(1, 100.0, 100.0, 100.0),
            // The book has run away from the oracle.
            book(2, 200.0, 200.0, 100.0),
            reference(2, Some(200.0), Some(100.0)),
            funding(3, 0.001),
        ],
        10_000.0,
        |builder| builder,
    )
    .unwrap();

    // 10 units at the oracle's 100 is 1000 of exposure; a tenth of a
    // percent is one dollar. On the book's 200 it would have been two.
    assert_eq!(result.funding_paid, money(1.0));
}

#[test]
fn funding_falls_back_to_the_mark_where_no_oracle_is_published() {
    // Most venues publish neither, and a run over their data must behave
    // exactly as it did before reference prices existed.
    let mut strategy = BuyOnce::new(qty(10.0));
    let result = run(
        &mut strategy,
        vec![
            book(1, 100.0, 100.0, 100.0),
            book(2, 200.0, 200.0, 100.0),
            funding(3, 0.001),
        ],
        10_000.0,
        |builder| builder,
    )
    .unwrap();
    assert_eq!(result.funding_paid, money(2.0));
}

#[test]
fn a_strategy_can_read_the_oracle_and_the_mark_apart() {
    // The spread between them is the premium funding is computed from, and
    // a basis strategy trades exactly that.
    struct ReadBoth {
        seen: Vec<(Option<Price>, Option<Price>)>,
    }
    impl Strategy for ReadBoth {
        fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
            self.seen.push((
                ctx.mark(&perp_id(), OutcomeId::FIRST),
                ctx.oracle(&perp_id(), OutcomeId::FIRST),
            ));
            Ok(())
        }
    }

    let mut strategy = ReadBoth { seen: Vec::new() };
    run(
        &mut strategy,
        vec![
            book(1, 100.0, 100.0, 10.0),
            reference(2, Some(101.0), Some(99.0)),
        ],
        1_000.0,
        |builder| builder,
    )
    .unwrap();

    assert_eq!(strategy.seen[0], (Some(price(100.0)), None));
    assert_eq!(
        strategy.seen[1],
        (Some(price(101.0)), Some(price(99.0))),
        "the mark and the oracle are separate facts and stay separate"
    );
}

// -- post-only ------------------------------------------------------------

/// Quotes once, post-only, at a price the caller chooses.
struct QuoteOnce {
    limit: Price,
    side: Side,
    done: bool,
}

impl Strategy for QuoteOnce {
    fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
        if self.done {
            return Ok(());
        }
        self.done = true;
        ctx.submit(
            OrderRequest::limit(perp_id(), OutcomeId::FIRST, self.side, self.limit, qty(1.0))
                .post_only()
                .with_time_in_force(TimeInForce::GoodTilCancel),
        );
        Ok(())
    }
}

#[test]
fn a_post_only_quote_through_the_book_is_refused_not_filled() {
    // The venue rejects an ALO order rather than let it take. A simulator
    // that fills it charges taker fees on a strategy built to avoid them.
    let mut strategy = QuoteOnce {
        limit: price(105.0),
        side: Side::Buy,
        done: false,
    };
    let result = run(
        &mut strategy,
        vec![book(1, 99.0, 100.0, 10.0), book(2, 99.0, 100.0, 10.0)],
        10_000.0,
        |builder| builder,
    )
    .unwrap();

    assert!(result.fills.is_empty());
    assert_eq!(result.metrics.orders_rejected_post_only, 1);
    let reason = result.orders[0].reject_reason.clone().unwrap();
    assert!(reason.contains("would have crossed"), "{reason}");
}

#[test]
fn a_post_only_quote_inside_the_spread_rests() {
    let mut strategy = QuoteOnce {
        limit: price(99.5),
        side: Side::Buy,
        done: false,
    };
    let result = run(
        &mut strategy,
        vec![book(1, 99.0, 100.0, 10.0), book(2, 99.0, 100.0, 10.0)],
        10_000.0,
        |builder| builder,
    )
    .unwrap();

    assert_eq!(result.metrics.orders_rejected_post_only, 0);
    assert_eq!(result.orders[0].status, OrderStatus::Accepted);
}

#[test]
fn a_resting_post_only_order_the_book_comes_to_fills_as_a_maker() {
    // It could not have taken -- the venue would have refused it -- so a
    // fill here is the book crossing into a resting quote.
    let mut strategy = QuoteOnce {
        limit: price(99.5),
        side: Side::Buy,
        done: false,
    };
    let result = run(
        &mut strategy,
        vec![
            book(1, 99.0, 100.0, 10.0),
            book(2, 99.0, 100.0, 10.0),
            // The offer drops through the resting bid.
            book(3, 98.0, 99.0, 10.0),
        ],
        10_000.0,
        |builder| builder,
    )
    .unwrap();

    assert_eq!(result.fills.len(), 1);
    assert!(!result.fills[0].is_taker);
    assert_eq!(result.metrics.fills_maker, 1);
    assert_eq!(result.metrics.fills_taker, 0);
}

#[test]
fn a_post_only_order_needs_a_price_to_rest_at() {
    struct MarketPostOnly;
    impl Strategy for MarketPostOnly {
        fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
            ctx.submit(
                OrderRequest::market(perp_id(), OutcomeId::FIRST, Side::Buy, qty(1.0)).post_only(),
            );
            Ok(())
        }
    }
    assert!(
        run(
            &mut MarketPostOnly,
            vec![book(1, 99.0, 100.0, 10.0)],
            10_000.0,
            |builder| builder
        )
        .is_err()
    );
}

// -- tiered fees ----------------------------------------------------------

fn hyperliquid_shaped_tiers() -> Vec<FeeTier> {
    vec![
        FeeTier {
            volume_from: Money::ZERO,
            maker: price(0.00015),
            taker: price(0.00045),
        },
        FeeTier {
            volume_from: money(5_000.0),
            maker: price(0.00005),
            taker: price(0.0003),
        },
        // The top of a real schedule pays the maker.
        FeeTier {
            volume_from: money(20_000.0),
            maker: price(-0.00003),
            taker: price(0.00021),
        },
    ]
}

#[test]
fn fees_fall_as_rolling_volume_rises_and_can_turn_into_a_rebate() {
    let tiers = TieredFees::new(hyperliquid_shaped_tiers(), 1_000_000).unwrap();
    let instrument = Instrument::perpetual(PERP, "hyperliquid").unwrap();
    let charge = |volume: f64, is_taker: bool| {
        let tier = tiers.tier_for(money(volume));
        if is_taker { tier.taker } else { tier.maker }
    };

    assert_eq!(charge(0.0, false), price(0.00015));
    assert_eq!(charge(6_000.0, false), price(0.00005));
    assert!(
        charge(50_000.0, false).is_negative(),
        "a real schedule pays its top makers, and a flat model cannot say so"
    );
    assert_eq!(charge(50_000.0, true), price(0.00021));
    let _ = instrument;
}

#[test]
fn a_fill_is_priced_at_the_tier_reached_before_it() {
    let tiers = TieredFees::new(hyperliquid_shaped_tiers(), 1_000_000_000_000).unwrap();
    let instrument = Instrument::perpetual(PERP, "hyperliquid").unwrap();
    let context = |at: i64, notional_value: f64| {
        tiers
            .commission(FeeContext {
                order_id: OrderId(1),
                instrument: &instrument,
                side: Side::Buy,
                price: price(1.0),
                quantity: qty(notional_value),
                is_taker: true,
                ts: ts(at),
            })
            .unwrap()
    };

    // Six thousand of volume at the entry tier's taker rate.
    assert_eq!(context(1, 6_000.0), money(6_000.0 * 0.00045));
    // The next fill is priced at the tier that volume bought.
    assert_eq!(context(2, 1_000.0), money(1_000.0 * 0.0003));
}

#[test]
fn volume_outside_the_window_stops_counting() {
    // Hyperliquid counts fourteen days. A model that counts for ever gives
    // a strategy a tier it stopped earning months ago.
    let window = 1_000_000_000;
    let tiers = TieredFees::new(hyperliquid_shaped_tiers(), window).unwrap();
    let instrument = Instrument::perpetual(PERP, "hyperliquid").unwrap();
    let charge = |at: i64, size: f64| {
        tiers
            .commission(FeeContext {
                order_id: OrderId(1),
                instrument: &instrument,
                side: Side::Buy,
                price: price(1.0),
                quantity: qty(size),
                is_taker: true,
                ts: ts(at),
            })
            .unwrap()
    };

    charge(0, 6_000.0);
    assert_eq!(
        charge(1, 100.0),
        money(100.0 * 0.0003),
        "still in the window"
    );
    assert_eq!(
        charge(window * 3, 100.0),
        money(100.0 * 0.00045),
        "the old volume has aged out and the account is back at entry"
    );
}

#[test]
fn a_schedule_that_cannot_price_an_untraded_account_is_refused() {
    let missing_floor = vec![FeeTier {
        volume_from: money(100.0),
        maker: price(0.0),
        taker: price(0.0),
    }];
    assert!(TieredFees::new(missing_floor, 1_000).is_err());
    assert!(TieredFees::new(Vec::new(), 1_000).is_err());
    assert!(TieredFees::new(hyperliquid_shaped_tiers(), 0).is_err());

    let duplicate = vec![
        FeeTier {
            volume_from: Money::ZERO,
            maker: price(0.0),
            taker: price(0.0),
        },
        FeeTier {
            volume_from: Money::ZERO,
            maker: price(0.1),
            taker: price(0.1),
        },
    ];
    assert!(TieredFees::new(duplicate, 1_000).is_err());
}

// -- per-coin leverage and partial liquidation ----------------------------

#[test]
fn leverage_is_granted_per_coin_not_per_venue() {
    let margin = PerInstrumentMargin::new(3.0)
        .unwrap()
        .with_leverage(perp_id(), 40.0)
        .unwrap();
    let major = Instrument::perpetual(PERP, "hyperliquid").unwrap();
    let tail = Instrument::perpetual("OBSCURE-PERP", "hyperliquid").unwrap();

    // A hundred of notional: forty times leverage needs 2.50, three times
    // needs 33.33.
    assert_eq!(
        margin
            .initial_margin(&major, qty(1.0), price(100.0))
            .unwrap(),
        money(2.5)
    );
    assert!(
        margin
            .initial_margin(&tail, qty(1.0), price(100.0))
            .unwrap()
            > money(33.0),
        "a coin with no entry falls back to the tighter default, not the \
         looser one"
    );
}

#[test]
fn a_partial_liquidation_leaves_the_strategy_in_the_trade() {
    // A venue closes what it must, not what it can. The two policies give
    // materially different results from the same data, which is why it is
    // stated rather than assumed.
    let records = vec![
        book(1, 100.0, 100.0, 100.0),
        book(2, 92.0, 92.0, 100.0),
        book(3, 92.0, 92.0, 100.0),
    ];

    let mut all = BuyOnce::new(qty(10.0));
    let closed = run(&mut all, records.clone(), 120.0, |builder| {
        builder.margin_model(Box::new(LinearMargin::from_leverage(10.0).unwrap()))
    })
    .unwrap();
    assert!(!closed.liquidations.is_empty());
    let after_all = h5i_db_backtest::position::Portfolio::replay(&closed.fills).unwrap();
    assert_eq!(after_all.open_positions().count(), 0);

    let mut partial = BuyOnce::new(qty(10.0));
    let trimmed = run(&mut partial, records, 120.0, |builder| {
        builder
            .margin_model(Box::new(LinearMargin::from_leverage(10.0).unwrap()))
            .liquidation_policy(LiquidationPolicy::Partial)
    })
    .unwrap();
    assert!(!trimmed.liquidations.is_empty());
    let after_partial = h5i_db_backtest::position::Portfolio::replay(&trimmed.fills).unwrap();
    let left = after_partial
        .position(&perp_id(), OutcomeId::FIRST)
        .map(|position| position.quantity)
        .unwrap_or(Qty::ZERO);
    assert!(
        left.is_positive() && left < qty(10.0),
        "the account should be trimmed back to health, not emptied; left {left}"
    );
}

// -- isolated margin ------------------------------------------------------

#[test]
fn an_isolated_position_posts_its_own_collateral_out_of_cash() {
    let mut strategy = BuyOnce::new(qty(1.0));
    let result = run(
        &mut strategy,
        vec![book(1, 100.0, 100.0, 10.0), book(2, 100.0, 100.0, 10.0)],
        1_000.0,
        |builder| {
            builder
                .margin_model(Box::new(LinearMargin::from_leverage(10.0).unwrap()))
                .isolate(perp_id())
        },
    )
    .unwrap();

    // Ten of notional at ten times leverage: one dollar moves into the
    // position's own bucket and out of spendable cash.
    assert_eq!(result.final_cash, money(990.0));
    assert!(result.liquidations.is_empty());
}

#[test]
fn an_isolated_position_cannot_be_rescued_by_the_cross_account() {
    // The bargain a strategy makes when it isolates: the position can lose
    // exactly its bucket, and a healthy account elsewhere will not save it.
    let records = vec![
        book(1, 100.0, 100.0, 100.0),
        book(2, 80.0, 80.0, 100.0),
        book(3, 80.0, 80.0, 100.0),
    ];

    let mut cross = BuyOnce::new(qty(1.0));
    let shared = run(&mut cross, records.clone(), 10_000.0, |builder| {
        builder.margin_model(Box::new(LinearMargin::from_leverage(10.0).unwrap()))
    })
    .unwrap();
    assert!(
        shared.liquidations.is_empty(),
        "ten thousand of cross collateral covers a twenty dollar loss easily"
    );

    let mut alone = BuyOnce::new(qty(1.0));
    let isolated = run(&mut alone, records, 10_000.0, |builder| {
        builder
            .margin_model(Box::new(LinearMargin::from_leverage(10.0).unwrap()))
            .isolate(perp_id())
    })
    .unwrap();
    assert!(
        !isolated.liquidations.is_empty(),
        "the same trade, isolated, loses more than its ten dollar bucket"
    );
    assert_eq!(
        isolated.liquidations[0].instrument,
        perp_id(),
        "and it is closed on its own account"
    );
}

#[test]
fn a_reference_with_only_an_oracle_leaves_the_mark_on_the_book() {
    let mut strategy = BuyOnce::new(qty(1.0));
    let result = run(
        &mut strategy,
        vec![book(1, 100.0, 100.0, 10.0), reference(2, None, Some(90.0))],
        1_000.0,
        |builder| builder,
    )
    .unwrap();
    assert_eq!(
        result.marks[&(perp_id(), OutcomeId::FIRST)],
        price(100.0),
        "an absent mark is not a zero mark, and must not become one"
    );
}
