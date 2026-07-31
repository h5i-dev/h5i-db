//! Vendor payloads all the way to a finished backtest.
//!
//! This is the test that makes the crate boundary mean something: two
//! unrelated venues, parsed by unrelated code, land in the same canonical
//! tables and run through the same kernel with nothing venue-specific
//! downstream of the loader.

use h5i_db_backtest::engine::{OrderRequest, SignalReplay};
use h5i_db_backtest::instrument::{InstrumentId, OutcomeId};
use h5i_db_backtest::run::{RunSpec, run_in_fork};
use h5i_db_backtest::store;
use h5i_db_backtest::types::{Money, Qty, Side, UnixNanos};
use h5i_db_core::Database;
use h5i_db_core::database::ReadAt;
use h5i_db_venues::{IngestPlan, hyperliquid, polymarket, write_plan};

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
                &poly_book(
                    hour * HOUR_MS,
                    0.40 + hour as f64 * 0.01,
                    0.42 + hour as f64 * 0.01,
                ),
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
    let funding = store::read_funding(&db, ReadAt::Latest, None)
        .await
        .unwrap();
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
        OrderRequest::market(
            id,
            OutcomeId::FIRST,
            Side::Buy,
            Qty::from_f64(100.0).unwrap(),
        ),
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
        .with_book_events(vec![
            polymarket::parse_book(&poly_book(0, 0.4, 0.42), &tokens).unwrap(),
        ]);

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
async fn reference_prices_reach_the_database_and_come_back_apart() {
    // A venue's mark and oracle are no use parsed if there is no supported
    // way to store them: a caller reaching past `write_plan` to
    // `store::write_references` skips the idempotency the ingest log gives
    // everything else, so a reload would double the rows.
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("refs.db")).await.unwrap();

    let ctxs = r#"[
        {"universe":[{"name":"BTC","szDecimals":5,"maxLeverage":40}]},
        [{"markPx":"46000.0","oraclePx":"45995.0","funding":"0.0000125"}]
    ]"#;
    let (universe, references) =
        hyperliquid::parse_meta_and_asset_ctxs(ctxs, UnixNanos::new(HOUR_MS * 1_000_000)).unwrap();
    assert_eq!(references.len(), 1);

    let plan = IngestPlan::new("hyperliquid")
        .with_instruments(vec![universe[0].instrument().unwrap()])
        .with_references(references.clone());
    assert_eq!(plan.record_count(), 1, "references are records too");
    assert!(plan.loaded_window().is_some());

    assert!(
        write_plan(&db, &plan, UnixNanos::new(0))
            .await
            .unwrap()
            .was_written()
    );
    // The same plan again is recognised rather than re-written.
    assert!(
        !write_plan(&db, &plan, UnixNanos::new(1))
            .await
            .unwrap()
            .was_written()
    );

    let source = store::reference_source(&db, ReadAt::Latest, None)
        .await
        .unwrap();
    let read: Vec<_> = source.map(|record| record.unwrap()).collect();
    assert_eq!(read, references, "both prices survive the round trip");

    // A reference for an instrument the plan declares nothing about is
    // refused like any other stream. (A plan that declares no instruments
    // at all is a legal top-up onto existing ones, so this one names a
    // different coin.)
    let stray = IngestPlan::new("hyperliquid")
        .with_instruments(vec![hyperliquid::instrument("ETH", 0.01, 0.001).unwrap()])
        .with_references(references);
    assert!(write_plan(&db, &stray, UnixNanos::new(2)).await.is_err());
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
        store::read_instruments(&db, ReadAt::Latest)
            .await
            .unwrap()
            .len(),
        1
    );
}

// ---------------------------------------------------------------------------
// idempotent ingestion (Tier 2, item 6)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn reloading_the_same_plan_writes_nothing_twice() {
    // The failure this prevents: the natural response to a partial failure
    // is to run the loader again, which without a digest doubles the book.
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("idem.db")).await.unwrap();
    let plan = hyperliquid_plan();

    let first = write_plan(&db, &plan, UnixNanos::new(0)).await.unwrap();
    assert!(first.was_written());
    let books_after_first = store::read_book_events(&db, ReadAt::Latest, None)
        .await
        .unwrap()
        .len();

    let second = write_plan(&db, &plan, UnixNanos::new(0)).await.unwrap();
    assert!(!second.was_written(), "a reload must be recognised");
    assert_eq!(first.digest(), second.digest());
    assert_eq!(
        store::read_book_events(&db, ReadAt::Latest, None)
            .await
            .unwrap()
            .len(),
        books_after_first,
        "no rows were duplicated"
    );
}

#[tokio::test]
async fn two_plans_over_the_same_window_are_merged_before_writing() {
    // Two venues covering the same hours share a book_deltas table, so
    // they are one load, not two: merging keeps the stream in time order,
    // which is what an append requires.
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("two.db")).await.unwrap();

    let combined = hyperliquid_plan().merge(polymarket_plan());
    let outcome = write_plan(&db, &combined, UnixNanos::new(0)).await.unwrap();
    assert!(outcome.was_written());
    assert_ne!(combined.digest(), hyperliquid_plan().digest());

    let log = store::read_ingest_log(&db).await.unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].instruments, 2);
}

#[tokio::test]
async fn a_later_load_covering_an_earlier_window_is_merged_in() {
    // Backfill: the affected span is read back, merged with the new rows,
    // sorted and rewritten atomically, so history stays in time order
    // without the caller having to know the load order in advance.
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("backfill.db"))
        .await
        .unwrap();

    write_plan(&db, &hyperliquid_plan(), UnixNanos::new(0))
        .await
        .unwrap();
    let after_first = store::read_book_events(&db, ReadAt::Latest, None)
        .await
        .unwrap();

    // Polymarket's window starts at the same instant but is a separate
    // load, so appending it would go backwards.
    let outcome = write_plan(&db, &polymarket_plan(), UnixNanos::new(0))
        .await
        .unwrap();
    assert!(outcome.was_written());

    let after_both = store::read_book_events(&db, ReadAt::Latest, None)
        .await
        .unwrap();
    assert!(after_both.len() > after_first.len(), "the backfill landed");

    // And the stored stream is still ordered, which is what replay needs.
    let stamps: Vec<i64> = after_both.iter().map(|r| r.ts().get()).collect();
    let mut sorted = stamps.clone();
    sorted.sort_unstable();
    assert_eq!(stamps, sorted, "a backfilled table must stay time-ordered");

    // Nothing from the first load was lost.
    for record in &after_first {
        assert!(
            after_both.contains(record),
            "backfill dropped a record from the earlier load"
        );
    }
}

#[tokio::test]
async fn a_backfilled_table_still_replays() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("bf-run.db"))
        .await
        .unwrap();
    write_plan(&db, &hyperliquid_plan(), UnixNanos::new(0))
        .await
        .unwrap();
    write_plan(&db, &polymarket_plan(), UnixNanos::new(0))
        .await
        .unwrap();

    let mut strategy = SignalReplay::new(vec![(
        UnixNanos::new(HOUR_MS * 1_000_000),
        OrderRequest::market(
            InstrumentId::new("0xelection").unwrap(),
            OutcomeId::FIRST,
            Side::Buy,
            Qty::from_f64(10.0).unwrap(),
        ),
    )])
    .unwrap();
    let report = run_in_fork(
        &db,
        RunSpec::new("after-backfill", Money::from_units(1_000).unwrap()),
        &mut strategy,
        |b| b,
    )
    .await
    .unwrap();
    assert_eq!(report.result.fills.len(), 1);
}

#[tokio::test]
async fn the_digest_depends_on_content_not_on_assembly_order() {
    // Two plans built differently but covering the same data must collide,
    // or a reload assembled another way would write again.
    let one = hyperliquid_plan();
    let mut records = one.book_events.clone();
    records.reverse();
    records.sort_by_key(|record| record.ts().get());
    let two = IngestPlan::new("hyperliquid")
        .with_instruments(one.instruments.clone())
        .with_book_events(records)
        .with_funding(one.funding.clone());
    assert_eq!(one.digest(), two.digest());
}

#[tokio::test]
async fn changing_a_single_price_changes_the_digest() {
    let plan = hyperliquid_plan();
    let mut altered = plan.clone();
    altered.funding = h5i_db_venues::hyperliquid::parse_funding(
        r#"[{"coin":"BTC","fundingRate":"0.0002","premium":"0.0","time":3600000},
            {"coin":"BTC","fundingRate":"0.0001","premium":"0.0","time":7200000}]"#,
        "BTC-PERP",
    )
    .unwrap();
    assert_ne!(plan.digest(), altered.digest());
}

#[tokio::test]
async fn the_ingest_log_records_what_was_loaded() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("log.db")).await.unwrap();
    let plan = hyperliquid_plan();
    write_plan(&db, &plan, UnixNanos::new(42)).await.unwrap();

    let log = store::read_ingest_log(&db).await.unwrap();
    assert_eq!(log.len(), 1);
    let entry = &log[0];
    assert_eq!(entry.vendor, "hyperliquid");
    assert_eq!(entry.records, plan.record_count() as i64);
    assert_eq!(entry.instruments, 1);
    assert_eq!(entry.ts, UnixNanos::new(42));
    // The window recorded is the span the data actually covers.
    assert_eq!(entry.window, plan.loaded_window());
}
