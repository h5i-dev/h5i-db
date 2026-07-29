//! Vendor payloads all the way to a finished backtest.
//!
//! This is the test that makes the crate boundary mean something: two
//! unrelated venues, parsed by unrelated code, land in the same canonical
//! tables and run through the same kernel with nothing venue-specific
//! downstream of the loader.

use h5i_db_backtest::engine::{OrderRequest, SignalReplay};
use h5i_db_backtest::instrument::{InstrumentId, OutcomeId};
use h5i_db_backtest::run::{run_in_fork, RunSpec};
use h5i_db_backtest::store;
use h5i_db_backtest::types::{Money, Qty, Side, UnixNanos};
use h5i_db_core::database::ReadAt;
use h5i_db_core::Database;
use h5i_db_venues::{hyperliquid, polymarket, write_plan, IngestPlan};

const HOUR_MS: i64 = 3_600_000;

/// Four hourly BTC candles and the funding that accrued alongside them.
fn hyperliquid_candles() -> String {
    let mut rows = Vec::new();
    for hour in 0..4i64 {
        let open = 46_000.0 + hour as f64 * 100.0;
        rows.push(format!(
            r#"{{"t":{},"T":{},"s":"BTC","i":"1h","o":"{}","c":"{}","h":"{}","l":"{}","v":"10.5","n":100}}"#,
            hour * HOUR_MS,
            (hour + 1) * HOUR_MS - 1,
            open,
            open + 50.0,
            open + 120.0,
            open - 80.0,
        ));
    }
    format!("[{}]", rows.join(","))
}

fn hyperliquid_funding() -> String {
    r#"[{"coin":"BTC","fundingRate":"0.0001","premium":"0.0","time":3600000},
        {"coin":"BTC","fundingRate":"0.0001","premium":"0.0","time":7200000}]"#
        .to_string()
}

fn hyperliquid_book(hour: i64, mid: f64) -> String {
    format!(
        r#"{{"coin":"BTC","time":{},"levels":[
            [{{"px":"{}","sz":"5.0","n":3}}],
            [{{"px":"{}","sz":"5.0","n":3}}]
        ]}}"#,
        hour * HOUR_MS,
        mid - 0.5,
        mid + 0.5
    )
}

const POLY_MARKET: &str = r#"{
    "condition_id": "0xelection",
    "minimum_tick_size": "0.001",
    "closed": false,
    "tokens": [
        {"token_id": "yes", "outcome": "Yes", "winner": false},
        {"token_id": "no",  "outcome": "No",  "winner": false}
    ]
}"#;

const POLY_RESOLVED: &str = r#"{
    "condition_id": "0xelection",
    "closed": true,
    "tokens": [
        {"token_id": "yes", "outcome": "Yes", "winner": true},
        {"token_id": "no",  "outcome": "No",  "winner": false}
    ]
}"#;

fn poly_book(at_ms: i64, best_bid: f64, best_ask: f64) -> String {
    format!(
        r#"{{"market":"0xelection","asset_id":"yes","timestamp":{at_ms},
            "bids":[{{"price":"{best_bid}","size":"500"}}],
            "asks":[{{"price":"{best_ask}","size":"500"}}]}}"#
    )
}

/// Build the Hyperliquid half of a mixed-venue database.
fn hyperliquid_plan() -> IngestPlan {
    let mut books = Vec::new();
    for hour in 0..4i64 {
        books.push(
            hyperliquid::parse_l2_book(
                &hyperliquid_book(hour, 46_000.0 + hour as f64 * 100.0),
                "BTC-PERP",
            )
            .unwrap(),
        );
    }
    IngestPlan::new("hyperliquid")
        .with_instruments(vec![hyperliquid::instrument("BTC", 0.5, 0.0001).unwrap()])
        .with_book_events(books)
        .with_funding(hyperliquid::parse_funding(&hyperliquid_funding(), "BTC-PERP").unwrap())
}

fn polymarket_plan() -> IngestPlan {
    let (instrument, tokens) = polymarket::instrument_from_market(POLY_MARKET).unwrap();
    let books: Vec<_> = (0..4i64)
        .map(|hour| {
            polymarket::parse_book(
                &poly_book(hour * HOUR_MS, 0.40 + hour as f64 * 0.01, 0.42 + hour as f64 * 0.01),
                &tokens,
            )
            .unwrap()
        })
        .collect();
    let resolution =
        polymarket::resolution_from_market(POLY_RESOLVED, UnixNanos::new(10 * HOUR_MS * 1_000_000))
            .unwrap()
            .unwrap();
    IngestPlan::new("polymarket")
        .with_instruments(vec![instrument])
        .with_book_events(books)
        .with_resolutions(vec![resolution])
}

#[tokio::test]
async fn a_hyperliquid_plan_ingests_and_runs() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("hl.db")).await.unwrap();
    let plan = hyperliquid_plan();
    assert_eq!(plan.vendor, "hyperliquid");
    assert_eq!(plan.record_count(), 6, "four books and two funding stamps");
    write_plan(&db, &plan, UnixNanos::new(0)).await.unwrap();

    // Everything landed in the canonical tables.
    let instruments = store::read_instruments(&db, ReadAt::Latest).await.unwrap();
    assert_eq!(instruments.len(), 1);
    let funding = store::read_funding(&db, ReadAt::Latest, None).await.unwrap();
    assert_eq!(funding.len(), 2);

    let id = InstrumentId::new("BTC-PERP").unwrap();
    let mut strategy = SignalReplay::new(vec![(
        UnixNanos::new(0),
        OrderRequest::market(id, OutcomeId::FIRST, Side::Buy, Qty::from_f64(1.0).unwrap()),
    )])
    .unwrap();
    let report = run_in_fork(
        &db,
        RunSpec::new("hl-run", Money::from_units(1_000_000).unwrap()),
        &mut strategy,
        |b| b,
    )
    .await
    .unwrap();

    assert_eq!(report.result.fills.len(), 1);
    assert!(
        report.result.funding_paid.is_positive(),
        "a long pays funding at a positive rate"
    );
}

#[tokio::test]
async fn a_polymarket_plan_ingests_and_settles() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("pm.db")).await.unwrap();
    write_plan(&db, &polymarket_plan(), UnixNanos::new(0))
        .await
        .unwrap();

    let id = InstrumentId::new("0xelection").unwrap();
    let mut strategy = SignalReplay::new(vec![(
        UnixNanos::new(0),
        OrderRequest::market(id, OutcomeId::FIRST, Side::Buy, Qty::from_f64(100.0).unwrap()),
    )])
    .unwrap();
    let report = run_in_fork(
        &db,
        RunSpec::new("pm-run", Money::from_units(1_000).unwrap()),
        &mut strategy,
        |b| b,
    )
    .await
    .unwrap();

    assert_eq!(report.result.fills.len(), 1);
    // The market resolves long after this replay ends, so nothing settles.
    assert!(!report.settlement.was_applied());
    assert!(report.warnings().iter().any(|w| w.contains("unsettled")));
}

#[tokio::test]
async fn two_venues_share_one_database_and_one_kernel() {
    // The point of the boundary: nothing downstream of the loaders knows
    // which venue a row came from.
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("both.db")).await.unwrap();
    let plan = hyperliquid_plan().merge(polymarket_plan());
    assert_eq!(plan.instruments.len(), 2);
    write_plan(&db, &plan, UnixNanos::new(0)).await.unwrap();

    let instruments = store::read_instruments(&db, ReadAt::Latest).await.unwrap();
    assert_eq!(instruments.len(), 2);
    let names: Vec<String> = instruments.ids().iter().map(|i| i.to_string()).collect();
    assert!(names.contains(&"BTC-PERP".to_string()));
    assert!(names.contains(&"0xelection".to_string()));

    // One run trades both a perpetual and a prediction market.
    // Act on the second bar, by which point both venues' books exist. An
    // order placed before its instrument's first book has no liquidity to
    // meet and cancels, which the run report now warns about.
    let act_at = UnixNanos::new(HOUR_MS * 1_000_000);
    let mut strategy = SignalReplay::new(vec![
        (
            act_at,
            OrderRequest::market(
                InstrumentId::new("BTC-PERP").unwrap(),
                OutcomeId::FIRST,
                Side::Buy,
                Qty::from_f64(1.0).unwrap(),
            ),
        ),
        (
            act_at,
            OrderRequest::market(
                InstrumentId::new("0xelection").unwrap(),
                OutcomeId::FIRST,
                Side::Buy,
                Qty::from_f64(10.0).unwrap(),
            ),
        ),
    ])
    .unwrap();
    let report = run_in_fork(
        &db,
        RunSpec::new("mixed", Money::from_units(1_000_000).unwrap()),
        &mut strategy,
        |b| b,
    )
    .await
    .unwrap();

    assert_eq!(report.result.fills.len(), 2, "one fill per venue");
    let venues: Vec<String> = report
        .result
        .fills
        .iter()
        .map(|fill| fill.instrument.to_string())
        .collect();
    assert!(venues.contains(&"BTC-PERP".to_string()));
    assert!(venues.contains(&"0xelection".to_string()));
}

#[tokio::test]
async fn an_invalid_plan_is_refused_before_anything_is_written() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("bad.db")).await.unwrap();

    // Book events for an instrument the plan never declares.
    let (_, tokens) = polymarket::instrument_from_market(POLY_MARKET).unwrap();
    let plan = IngestPlan::new("polymarket")
        .with_instruments(vec![hyperliquid::instrument("BTC", 0.5, 0.0001).unwrap()])
        .with_book_events(vec![polymarket::parse_book(&poly_book(0, 0.4, 0.42), &tokens).unwrap()]);

    assert!(write_plan(&db, &plan, UnixNanos::new(0)).await.is_err());
    // Nothing was committed, so a later correct load starts clean.
    let instruments = store::read_instruments(&db, ReadAt::Latest)
        .await
        .unwrap_or_default();
    assert!(
        instruments.is_empty(),
        "a refused plan must not leave half its rows behind"
    );
}

#[tokio::test]
async fn candles_ingest_as_bars_and_keep_their_close_stamp() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("bars.db")).await.unwrap();
    let bars = hyperliquid::parse_candles(&hyperliquid_candles(), "BTC-PERP").unwrap();
    assert_eq!(bars.len(), 4);
    // A bar arrives when it closes, not when it opens.
    assert_eq!(bars[0].stamps.ts_event.get(), 0);
    assert_eq!(bars[0].stamps.ts_init.get(), (HOUR_MS - 1) * 1_000_000);

    let plan = IngestPlan::new("hyperliquid")
        .with_instruments(vec![hyperliquid::instrument("BTC", 0.5, 0.0001).unwrap()]);
    write_plan(&db, &plan, UnixNanos::new(0)).await.unwrap();
    assert_eq!(
        store::read_instruments(&db, ReadAt::Latest).await.unwrap().len(),
        1
    );
}
