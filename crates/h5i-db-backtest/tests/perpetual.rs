//! Perpetual mechanics: the prices a venue margins and funds against.
//!
//! A derivatives venue does not value your position at the mid. It
//! publishes a mark, derived from an oracle and the book, and margins
//! against that; it charges funding on the oracle. Substituting the mid for
//! either is not a small approximation, and these tests are about the
//! direction of the error rather than only the mechanism.

use h5i_db_backtest::Result;
use h5i_db_backtest::account::LinearMargin;
use h5i_db_backtest::engine::{
    Context, Engine, EngineBuilder, MarkSource, OrderRequest, RunResult, Strategy,
};
use h5i_db_backtest::event::{MarketEvent, Record};
use h5i_db_backtest::instrument::{Instrument, InstrumentId, InstrumentSet, OutcomeId};
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
