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
fn a_real_archive_slice_reads_and_keeps_both_timestamps_apart() {
    // Six consecutive lines from
    // s3://hyperliquid-archive/market_data/20250101/0/l2Book/BTC.lz4,
    // byte for byte. The archive wraps the live envelope in
    // `{"time": <archiver receive>, "ver_num": 1, "raw": {...}}`, and the
    // venue's own stamp lives inside `raw.data.time`.
    //
    // That pair is the whole reason to read the outer object. Over the full
    // hour this slice came from, the archiver trails the venue by 57ms at
    // the median and 3.2 seconds at the worst; stamping both from the
    // venue's clock would hand a strategy up to three seconds of look-ahead
    // on every book update.
    let raw = std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/hyperliquid/archive_l2book_btc.lz4"),
    )
    .unwrap();
    let read = hyperliquid::read_archive_lz4(&raw).unwrap();

    assert_eq!(read.lines, 6);
    assert_eq!(read.records.len(), 6);
    assert_eq!(read.malformed, 0);
    assert_eq!(read.skipped, 0);
    assert_eq!(
        read.barren_ratio(),
        0.0,
        "every line of a real archive file must produce a record"
    );
    read.require_yield(0.99).unwrap();

    for record in &read.records {
        assert_eq!(record.instrument.as_str(), "BTC-PERP");
        assert!(matches!(record.event, MarketEvent::BookSnapshot { .. }));
        assert!(
            record.stamps.ts_init > record.stamps.ts_event,
            "the archiver cannot have received a message before the venue \
             sent it, and in practice it is measurably later"
        );
        // Sub-second in the common case; the tail is what makes it matter.
        assert!(record.stamps.delay_nanos() < 5_000_000_000);
    }
    // Replay orders by ts_init, so the reader must too.
    assert!(
        read.records
            .windows(2)
            .all(|pair| pair[0].ts() <= pair[1].ts())
    );

    // The uncompressed path reads identically.
    let plain = hyperliquid::read_archive(std::io::BufReader::new(
        std::fs::File::open(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/hyperliquid/archive_l2book_btc.jsonl"),
        )
        .unwrap(),
    ))
    .unwrap();
    assert_eq!(plain.records, read.records);
}

#[test]
fn a_real_archive_book_is_deep_and_uncrossed() {
    // The reason to pay for the archive at all: the REST book is a single
    // snapshot of now, and this is depth over time.
    let raw = std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/hyperliquid/archive_l2book_btc.lz4"),
    )
    .unwrap();
    let read = hyperliquid::read_archive_lz4(&raw).unwrap();
    let universe = hyperliquid::parse_meta(&fixture("meta.json")).unwrap();
    let instrument = universe
        .iter()
        .find(|a| a.name == "BTC")
        .unwrap()
        .instrument()
        .unwrap();

    for record in &read.records {
        let MarketEvent::BookSnapshot { bids, asks } = &record.event else {
            panic!("expected a snapshot");
        };
        assert!(
            bids.len() > 5 && asks.len() > 5,
            "the archive carries depth"
        );
        assert!(bids[0].0 < asks[0].0, "an uncrossed book");
        for (price, _) in bids.iter().chain(asks) {
            instrument
                .check_price(*price)
                .unwrap_or_else(|error| panic!("archived quote {price}: {error}"));
        }
    }
}

#[test]
fn the_asset_ctxs_archive_is_the_historical_mark_and_oracle() {
    // Real rows from s3://hyperliquid-archive/asset_ctxs/20250101.csv.lz4.
    // This file is the only *historical* source of marks and oracles the
    // venue publishes: metaAndAssetCtxs answers for right now, and the
    // hourly market_data files carry books alone.
    let contexts = hyperliquid::parse_asset_ctxs_csv(&fixture("asset_ctxs.csv")).unwrap();
    assert!(contexts.len() >= 8);
    assert!(contexts.iter().any(|c| c.coin == "BTC"));
    assert!(contexts.iter().any(|c| c.coin == "ETH"));

    let btc = contexts.iter().find(|c| c.coin == "BTC").unwrap();
    assert_eq!(btc.mark, Price::from_f64(93_620.0).unwrap());
    assert_eq!(btc.oracle, Price::from_f64(93_576.0).unwrap());
    assert!(
        btc.mark != btc.mid,
        "the mark is not the mid, which is the entire reason to carry it"
    );
    assert!(btc.premium.is_positive());

    // One minute apart, sorted.
    let times: Vec<i64> = contexts.iter().map(|c| c.at.get()).collect();
    assert!(times.windows(2).all(|pair| pair[0] <= pair[1]));

    // Compressed and plain agree.
    let raw = std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/hyperliquid/asset_ctxs.csv.lz4"),
    )
    .unwrap();
    assert_eq!(hyperliquid::read_asset_ctxs_lz4(&raw).unwrap(), contexts);
}

#[test]
fn asset_ctxs_produce_reference_prices_and_deliberately_no_funding() {
    // The trap this guards: the file samples the standing funding rate once
    // a minute. Emitting each sample as a payment would charge a carry
    // sixty times an hour. Settlements come from fundingHistory.
    let contexts = hyperliquid::parse_asset_ctxs_csv(&fixture("asset_ctxs.csv")).unwrap();
    assert!(
        contexts.iter().all(|c| !c.funding_rate.is_zero()),
        "the rate is present in the row"
    );

    let records = hyperliquid::asset_context_records(&contexts, None).unwrap();
    assert_eq!(records.len(), contexts.len());
    assert!(
        records
            .iter()
            .all(|record| matches!(record.event, MarketEvent::Reference { .. })),
        "prices only: a sampled rate is not a payment due"
    );

    // Filtering to a subset is how a run picks its handful out of 168 coins.
    let only_btc =
        hyperliquid::asset_context_records(&contexts, Some(&["BTC".to_string()])).unwrap();
    assert!(!only_btc.is_empty());
    assert!(
        only_btc
            .iter()
            .all(|record| record.instrument.as_str() == "BTC-PERP")
    );
}

#[test]
fn an_asset_ctxs_file_whose_columns_moved_is_refused() {
    // Read by position after a silent schema change, a mark lands in the
    // open-interest column and the numbers stay plausible for a long time.
    let shuffled = "time,coin,funding,open_interest,prev_day_px,day_ntl_vlm,\
                    premium,mark_px,oracle_px,mid_px,impact_bid_px,impact_ask_px\n";
    let error = hyperliquid::parse_asset_ctxs_csv(shuffled).unwrap_err();
    assert!(error.to_string().contains("expected"), "{error}");
    assert!(hyperliquid::parse_asset_ctxs_csv("").is_err());
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
