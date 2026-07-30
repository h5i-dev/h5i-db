//! Parsers against payloads the live venue actually sent.
//!
//! The fixtures in `tests/fixtures/hyperliquid` are real responses from
//! `api.hyperliquid.xyz/info`, trimmed to a representative slice of the
//! universe and otherwise untouched. Hand-written fixtures test that a
//! parser matches what its author believed; these test that it matches what
//! the venue sends, which is a different and stricter question -- the
//! `fundingHistory` request shape was wrong for as long as only the first
//! kind of test existed.
//!
//! Refresh them with the recipe in `docs-src/manual/backtest.md`.

use h5i_db_backtest::event::MarketEvent;
use h5i_db_backtest::instrument::PriceRule;
use h5i_db_backtest::types::{Price, UnixNanos};
use h5i_db_venues::hyperliquid;

fn fixture(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/hyperliquid")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

#[test]
fn the_real_universe_parses_into_instruments_the_venue_would_accept() {
    let universe = hyperliquid::parse_meta(&fixture("meta.json")).unwrap();
    assert!(universe.len() >= 4);

    let btc = universe.iter().find(|a| a.name == "BTC").unwrap();
    assert_eq!(btc.sz_decimals, 5);
    assert_eq!(btc.max_leverage, 40);

    // The long tail really is margined differently from the majors, which
    // is the whole reason leverage is per coin.
    let atom = universe.iter().find(|a| a.name == "ATOM").unwrap();
    assert_eq!(atom.max_leverage, 5);
    assert!(btc.max_leverage > atom.max_leverage * 4);

    // And the price grid really does vary with szDecimals.
    assert_eq!(btc.price_decimals(), 1);
    assert_eq!(atom.price_decimals(), 4);

    let instrument = btc.instrument().unwrap();
    assert_eq!(instrument.id.as_str(), "BTC-PERP");
    assert!(matches!(
        instrument.price_rule,
        PriceRule::SignificantFigures {
            significant_figures: 5,
            max_decimals: 1
        }
    ));
}

#[test]
fn every_price_in_a_real_book_passes_the_rule_derived_from_the_real_metadata() {
    // The end-to-end claim of the price rule: quotes the venue is showing
    // right now must all be prices the rule accepts. A rule that rejects
    // any of them would reject orders the venue would have taken.
    let universe = hyperliquid::parse_meta(&fixture("meta.json")).unwrap();
    let btc = universe.iter().find(|a| a.name == "BTC").unwrap();
    let instrument = btc.instrument().unwrap();

    let record = hyperliquid::parse_l2_book(&fixture("l2_book.json"), "BTC-PERP").unwrap();
    let MarketEvent::BookSnapshot { bids, asks } = &record.event else {
        panic!("expected a snapshot");
    };
    assert!(!bids.is_empty() && !asks.is_empty());
    for (price, size) in bids.iter().chain(asks) {
        instrument
            .check_price(*price)
            .unwrap_or_else(|error| panic!("the venue is quoting {price}: {error}"));
        assert!(size.is_positive());
    }
    // Sanity on the shape: bids descend, asks ascend, and they do not cross.
    assert!(bids.windows(2).all(|pair| pair[0].0 > pair[1].0));
    assert!(asks.windows(2).all(|pair| pair[0].0 < pair[1].0));
    assert!(bids[0].0 < asks[0].0);
}

#[test]
fn the_real_asset_contexts_pair_with_the_universe_and_carry_both_prices() {
    let at = UnixNanos::new(1_785_451_400_000_000_000);
    let (universe, references) =
        hyperliquid::parse_meta_and_asset_ctxs(&fixture("meta_and_asset_ctxs.json"), at).unwrap();
    assert_eq!(universe.len(), references.len());

    let btc = references
        .iter()
        .find(|record| record.instrument.as_str() == "BTC-PERP")
        .expect("BTC has a context");
    let MarketEvent::Reference { mark, oracle } = btc.event else {
        panic!("expected a reference");
    };
    let mark = mark.expect("the venue publishes a mark");
    let oracle = oracle.expect("the venue publishes an oracle");
    assert!(mark > Price::ZERO && oracle > Price::ZERO);
    // They are close but not equal, which is the premium funding is
    // computed from -- and is why substituting one for the other, or for
    // the mid, is not a rounding decision.
    let gap = (mark.raw() - oracle.raw()).abs();
    assert!(gap > 0, "a real mark and oracle differ");
    assert!(
        gap * 100 < mark.raw(),
        "but only by a fraction of a percent"
    );
}

#[test]
fn real_candles_become_knowable_at_their_close() {
    let records = hyperliquid::parse_candles(&fixture("candles.json"), "BTC-PERP").unwrap();
    assert!(!records.is_empty());
    for record in &records {
        assert!(
            record.stamps.ts_init > record.stamps.ts_event,
            "a bar is knowable at its close, not its open"
        );
        // Hourly bars, in nanoseconds, minus the venue's inclusive end.
        assert_eq!(
            record.stamps.ts_init.get() - record.stamps.ts_event.get(),
            3_599_999_000_000
        );
    }
    assert!(records.windows(2).all(|pair| pair[0].ts() <= pair[1].ts()));
}

#[test]
fn real_funding_rates_are_hourly_and_keep_their_sign() {
    let records = hyperliquid::parse_funding(&fixture("funding.json"), "BTC-PERP").unwrap();
    assert!(records.len() >= 2);

    // Hourly, but not to the nanosecond: the venue stamps a settlement when
    // it processes it, so a live capture drifts a few tens of milliseconds
    // either side of the hour. A model that snapped these onto exact hour
    // boundaries would be inventing precision the feed does not have.
    let hour = 3_600_000_000_000_i64;
    let tolerance = 1_000_000_000_i64;
    for pair in records.windows(2) {
        let gap = pair[1].ts().get() - pair[0].ts().get();
        assert!(
            (gap - hour).abs() < tolerance,
            "Hyperliquid settles funding every hour, not every eight; got {gap}"
        );
    }
    // A live capture contains both signs; carrying the rate unscaled is
    // what makes a carry backtest come out right.
    let rates: Vec<Price> = records
        .iter()
        .map(|record| match record.event {
            MarketEvent::Funding { rate } => rate,
            _ => panic!("expected funding"),
        })
        .collect();
    assert!(rates.iter().any(|rate| !rate.is_zero()));
    assert!(
        rates.iter().all(|rate| rate.raw().abs() < 40_000_000),
        "an hourly rate is basis points, not percent; anything larger means \
         a scaling mistake"
    );
}
