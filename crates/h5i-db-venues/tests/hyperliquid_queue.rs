//! The maker path, driven by real Hyperliquid prints.
//!
//! `tests/fixtures/hyperliquid/live_btc_capture.lz4` is thirty consecutive
//! websocket messages for BTC -- six book snapshots and seventy-seven trade
//! prints carrying both aggressor sides -- recorded off
//! `wss://api.hyperliquid.xyz/ws` and written in the archive's own line
//! format so one reader handles a recording and a download alike.
//!
//! Recording rather than downloading is not a convenience. The archive has
//! **no trades**: `market_data/<date>/<hour>/` contains `l2Book/` and
//! nothing else. Prints are the only thing that moves a queue, so without a
//! capture the queue-position model cannot be exercised against real data at
//! all.
//!
//! What this window shows is a negative, and it is the point of the model:
//! over thirty seconds of real BTC, **no passive quote fills**. The size
//! displayed ahead at every price exceeds the volume that actually traded
//! there, so an order joining the back of a real queue waits. A model that
//! fills every touched limit would have reported several fills from the same
//! data.

use h5i_db_backtest::Result;
use h5i_db_backtest::engine::{Context, Engine, OrderRequest, RunResult, Strategy};
use h5i_db_backtest::event::{MarketEvent, Record};
use h5i_db_backtest::instrument::{InstrumentId, InstrumentSet, OutcomeId};
use h5i_db_backtest::models::{BookFills, FillModel, QueuePositionFills};
use h5i_db_backtest::order::TimeInForce;
use h5i_db_backtest::replay::{Replay, priority};
use h5i_db_backtest::types::{Money, Price, Qty, Side};
use h5i_db_venues::hyperliquid;

fn capture() -> Vec<Record> {
    let raw = std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/hyperliquid/live_btc_capture.lz4"),
    )
    .unwrap();
    let read = hyperliquid::read_archive_lz4(&raw).unwrap();
    assert_eq!(read.malformed, 0);
    read.records
}

fn instruments() -> InstrumentSet {
    let meta = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/hyperliquid/meta.json"),
    )
    .unwrap();
    let universe = hyperliquid::parse_meta(&meta).unwrap();
    let mut set = InstrumentSet::new();
    set.insert(
        universe
            .iter()
            .find(|asset| asset.name == "BTC")
            .unwrap()
            .instrument()
            .unwrap(),
    )
    .unwrap();
    set
}

/// Rests one passive quote at a fixed price and leaves it there.
struct QuoteAt {
    at: Price,
    side: Side,
    done: bool,
}

impl Strategy for QuoteAt {
    fn on_event(&mut self, ctx: &mut Context<'_>, _record: &Record) -> Result<()> {
        let id = InstrumentId::new("BTC-PERP").unwrap();
        if self.done || ctx.best_bid(&id, OutcomeId::FIRST).is_none() {
            return Ok(());
        }
        self.done = true;
        ctx.submit(
            OrderRequest::limit(
                id,
                OutcomeId::FIRST,
                self.side,
                self.at,
                Qty::from_f64(0.00001).unwrap(),
            )
            .with_time_in_force(TimeInForce::GoodTilCancel),
        );
        Ok(())
    }
}

fn replay_capture(model: Box<dyn FillModel>, at: f64, side: Side) -> RunResult {
    // Prints carry their own priority so a book update at the same instant
    // lands first, exactly as the replay orders them in a real run.
    let (books, trades): (Vec<Record>, Vec<Record>) = capture()
        .into_iter()
        .partition(|record| !matches!(record.event, MarketEvent::Trade { .. }));
    let mut replay = Replay::builder()
        .stream("book", priority::SNAPSHOT, books)
        .stream("trades", priority::TRADE, trades)
        .build()
        .unwrap();
    let mut engine = Engine::builder(instruments())
        .starting_cash(Money::from_f64(1_000_000.0).unwrap())
        .fill_model(model)
        .build();
    let mut strategy = QuoteAt {
        at: Price::from_f64(at).unwrap(),
        side,
        done: false,
    };
    engine.run(&mut replay, &mut strategy).unwrap()
}

#[test]
fn the_capture_carries_the_prints_the_archive_does_not() {
    let records = capture();
    let trades = records
        .iter()
        .filter(|record| matches!(record.event, MarketEvent::Trade { .. }))
        .count();
    let books = records.len() - trades;
    assert!(books >= 5, "book snapshots to rest against");
    assert!(trades >= 50, "prints to move the queue with");

    // Both aggressor sides are present, so the queue's side logic is
    // genuinely exercised rather than half of it.
    let mut buys = 0;
    let mut sells = 0;
    let mut unknown = 0;
    for record in &records {
        if let MarketEvent::Trade { aggressor, .. } = record.event {
            match aggressor {
                Some(Side::Buy) => buys += 1,
                Some(Side::Sell) => sells += 1,
                None => unknown += 1,
            }
        }
    }
    assert!(buys > 0 && sells > 0, "{buys} buys, {sells} sells");
    assert_eq!(unknown, 0, "Hyperliquid always names the taker");

    // Replay order is by ts_init, which the recorder stamped on receipt.
    assert!(records.windows(2).all(|pair| pair[0].ts() <= pair[1].ts()));
}

#[test]
fn a_passive_quote_behind_real_depth_does_not_fill() {
    // The headline. 64899 is below every ask in the window, so the book
    // never comes to this order -- the only thing that could fill it is
    // sell-aggressor volume at that price clearing the size displayed ahead.
    // In reality 0.00016 traded there against a deeper resting queue, so it
    // waits, and a backtest that filled it would be inventing a fill.
    let queued = replay_capture(Box::new(QueuePositionFills::new()), 64_899.0, Side::Buy);
    assert_eq!(queued.metrics.queue_joins, 1, "it joined a real queue");
    assert!(
        queued.fills.is_empty(),
        "real traded volume at this price never cleared the real size ahead"
    );

    // The same is true a few ticks lower, and for the other side above the
    // book: this is a property of the depth, not of one lucky price.
    for (price, side) in [
        (64_898.0, Side::Buy),
        (64_897.0, Side::Buy),
        (64_920.0, Side::Sell),
        (64_925.0, Side::Sell),
    ] {
        let result = replay_capture(Box::new(QueuePositionFills::new()), price, side);
        assert_eq!(result.metrics.queue_joins, 1, "{price} {side:?}");
        assert!(result.fills.is_empty(), "{price} {side:?} must not fill");
    }
}

#[test]
fn the_queue_model_never_fills_more_than_the_book_model_on_the_same_data() {
    // The conservatism claim, checked across the window rather than
    // asserted. Where a quote is crossed by the book both models fill it;
    // where it is not, only an optimistic model would.
    for (price, side) in [
        (64_897.0, Side::Buy),
        (64_899.0, Side::Buy),
        (64_900.0, Side::Buy),
        (64_912.0, Side::Sell),
        (64_920.0, Side::Sell),
    ] {
        let book = replay_capture(Box::new(BookFills), price, side);
        let queue = replay_capture(Box::new(QueuePositionFills::new()), price, side);
        assert!(
            queue.fills.len() <= book.fills.len(),
            "{price} {side:?}: queue {} > book {}",
            queue.fills.len(),
            book.fills.len()
        );
    }
}

#[test]
fn replaying_the_same_real_bytes_twice_gives_the_same_answer() {
    // Determinism, on real data rather than generated data.
    let first = replay_capture(Box::new(QueuePositionFills::new()), 64_900.0, Side::Buy);
    let second = replay_capture(Box::new(QueuePositionFills::new()), 64_900.0, Side::Buy);
    assert_eq!(first.fills, second.fills);
    assert_eq!(first.final_cash, second.final_cash);
    assert_eq!(first.metrics, second.metrics);
}

#[test]
fn a_recorded_line_is_byte_identical_to_the_archives_own_format() {
    // Why it matters: a capture written this way replays through
    // `read_archive`, so trades and books come from one reader whether they
    // were recorded live or downloaded.
    let at = h5i_db_backtest::types::UnixNanos::new(1_735_689_602_238_437_296);
    let line = hyperliquid::archive_line(at, r#"{"channel":"trades","data":[]}"#).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(parsed["time"], "2025-01-01T00:00:02.238437296");
    assert_eq!(parsed["ver_num"], 1);
    assert_eq!(parsed["raw"]["channel"], "trades");
    assert_eq!(
        hyperliquid::format_archive_time(at),
        "2025-01-01T00:00:02.238437296"
    );

    // And it round-trips back out through the reader with the stamp intact.
    let book = hyperliquid::archive_line(
        at,
        r#"{"channel":"l2Book","data":{"coin":"BTC","time":1735689600000,
            "levels":[[{"px":"93619.0","sz":"1.0","n":1}],[{"px":"93620.0","sz":"1.0","n":1}]]}}"#,
    )
    .unwrap();
    let read = hyperliquid::read_archive(std::io::Cursor::new(book)).unwrap();
    assert_eq!(read.records.len(), 1);
    assert_eq!(read.records[0].stamps.ts_init.get(), at.get());
    assert_eq!(
        read.records[0].stamps.ts_event.get(),
        1_735_689_600_000_000_000
    );

    assert!(hyperliquid::archive_line(at, "not json").is_err());
}
