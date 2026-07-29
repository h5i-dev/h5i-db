//! The database seam: reading market data out and writing runs back.
//!
//! These are the tests that make "a backtester on versioned data" a claim
//! rather than a description. They cover the round trip through storage,
//! runs living on forks, and the ordering that keeps resolutions out of a
//! strategy's reach.

use std::collections::BTreeMap;

use h5i_db_backtest::book::BookDelta;
use h5i_db_backtest::engine::{OrderRequest, SignalReplay};
use h5i_db_backtest::event::{MarketEvent, Record};
use h5i_db_backtest::instrument::{Instrument, InstrumentId, OutcomeId};
use h5i_db_backtest::models::PredictionMarketFees;
use h5i_db_backtest::position::Portfolio;
use h5i_db_backtest::run::{run_in_fork, RunSpec};
use h5i_db_backtest::settlement::Resolution;
use h5i_db_backtest::store;
use h5i_db_backtest::types::{Money, Price, Qty, Side, Stamps, UnixNanos};
use h5i_db_backtest::window::TimeWindow;
use h5i_db_core::database::ReadAt;
use h5i_db_core::Database;

const MARKET: &str = "will-x-happen";
const SECOND: i64 = 1_000_000_000;

fn id() -> InstrumentId {
    InstrumentId::new(MARKET).unwrap()
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

fn market() -> Instrument {
    Instrument::binary(MARKET, "polymarket")
        .unwrap()
        .with_settlement_observable(ts(100 * SECOND))
}

fn snapshot_at(at: i64, best_ask: f64) -> Record {
    Record::new(
        Stamps::immediate(ts(at)),
        id(),
        OutcomeId::FIRST,
        MarketEvent::BookSnapshot {
            bids: vec![(price(best_ask - 0.02), qty(500.0))],
            asks: vec![(price(best_ask), qty(500.0))],
        },
    )
}

/// A database with a few seconds of book history and a resolution.
async fn seeded_db(dir: &tempfile::TempDir) -> Database {
    let db = Database::create(&dir.path().join("bt.db")).await.unwrap();
    store::create_market_data_tables(&db).await.unwrap();
    store::write_instruments(&db, &[market()], ts(0)).await.unwrap();

    let mut records = Vec::new();
    for step in 1..=10i64 {
        records.push(snapshot_at(step * SECOND, 0.40 + step as f64 * 0.01));
    }
    store::write_book_events(&db, &records).await.unwrap();

    store::write_trades(
        &db,
        &[Record::new(
            Stamps::immediate(ts(2 * SECOND)),
            id(),
            OutcomeId::FIRST,
            MarketEvent::Trade {
                price: price(0.41),
                size: qty(25.0),
                aggressor: Some(Side::Buy),
            },
        )],
    )
    .await
    .unwrap();

    store::write_resolutions(
        &db,
        &[Resolution::new(id(), OutcomeId::FIRST, ts(100 * SECOND))],
    )
    .await
    .unwrap();
    db
}

#[tokio::test]
async fn instruments_survive_the_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("i.db")).await.unwrap();
    store::create_market_data_tables(&db).await.unwrap();

    let categorical = Instrument::prediction_market(
        "three-way",
        "polymarket",
        vec!["A".into(), "B".into(), "C".into()],
    )
    .unwrap()
    .with_expiration(ts(42))
    .with_settlement_observable(ts(99));
    store::write_instruments(&db, &[market(), categorical.clone()], ts(0))
        .await
        .unwrap();

    let read = store::read_instruments(&db, ReadAt::Latest).await.unwrap();
    assert_eq!(read.len(), 2);
    let restored = read.get(&InstrumentId::new("three-way").unwrap()).unwrap();
    assert_eq!(restored, &categorical, "an instrument must round trip whole");
    assert_eq!(restored.outcomes, vec!["A", "B", "C"]);
    assert_eq!(restored.expiration, Some(ts(42)));
}

#[tokio::test]
async fn book_events_survive_the_round_trip_including_snapshot_grouping() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("b.db")).await.unwrap();
    store::create_market_data_tables(&db).await.unwrap();

    let written = vec![
        Record::new(
            Stamps::immediate(ts(1)),
            id(),
            OutcomeId::FIRST,
            MarketEvent::BookSnapshot {
                bids: vec![(price(0.40), qty(10.0)), (price(0.39), qty(20.0))],
                asks: vec![(price(0.42), qty(30.0))],
            },
        ),
        Record::new(
            Stamps::new(ts(2), ts(3)).unwrap(),
            id(),
            OutcomeId::FIRST,
            MarketEvent::BookDelta(BookDelta::set(Side::Buy, price(0.41), qty(5.0))),
        ),
        Record::new(
            Stamps::immediate(ts(4)),
            id(),
            OutcomeId::FIRST,
            MarketEvent::BookDelta(BookDelta::delete(Side::Sell, price(0.42))),
        ),
        Record::new(
            Stamps::immediate(ts(5)),
            id(),
            OutcomeId::FIRST,
            MarketEvent::BookDelta(BookDelta::clear(Side::Buy)),
        ),
        Record::new(
            Stamps::immediate(ts(6)),
            id(),
            OutcomeId::FIRST,
            MarketEvent::Gap,
        ),
    ];
    store::write_book_events(&db, &written).await.unwrap();

    let read = store::read_book_events(&db, ReadAt::Latest, None).await.unwrap();
    assert_eq!(read, written, "book events must round trip exactly");
    // The late-arriving delta kept both of its stamps.
    assert_eq!(read[1].stamps.ts_event, ts(2));
    assert_eq!(read[1].stamps.ts_init, ts(3));
}

#[tokio::test]
async fn an_empty_snapshot_is_a_state_not_an_absence() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("e.db")).await.unwrap();
    store::create_market_data_tables(&db).await.unwrap();
    let written = vec![Record::new(
        Stamps::immediate(ts(1)),
        id(),
        OutcomeId::FIRST,
        MarketEvent::BookSnapshot {
            bids: vec![],
            asks: vec![],
        },
    )];
    store::write_book_events(&db, &written).await.unwrap();
    assert_eq!(
        store::read_book_events(&db, ReadAt::Latest, None).await.unwrap(),
        written
    );
}

#[tokio::test]
async fn a_time_window_reads_a_half_open_slice() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(&dir).await;
    let window = TimeWindow::new(ts(3 * SECOND), ts(6 * SECOND)).unwrap();
    let read = store::read_book_events(&db, ReadAt::Latest, Some(window))
        .await
        .unwrap();
    let stamps: Vec<i64> = read.iter().map(|r| r.ts().get() / SECOND).collect();
    assert_eq!(stamps, vec![3, 4, 5], "the end of the window is excluded");
}

#[tokio::test]
async fn trades_round_trip_including_an_unknown_aggressor() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("t.db")).await.unwrap();
    store::create_market_data_tables(&db).await.unwrap();
    let written = vec![
        Record::new(
            Stamps::immediate(ts(1)),
            id(),
            OutcomeId::FIRST,
            MarketEvent::Trade {
                price: price(0.5),
                size: qty(3.0),
                aggressor: Some(Side::Sell),
            },
        ),
        Record::new(
            Stamps::immediate(ts(2)),
            id(),
            OutcomeId::FIRST,
            MarketEvent::Trade {
                price: price(0.5),
                size: qty(3.0),
                // The vendor did not say, and it must stay unsaid.
                aggressor: None,
            },
        ),
    ];
    store::write_trades(&db, &written).await.unwrap();
    assert_eq!(
        store::read_trades(&db, ReadAt::Latest, None).await.unwrap(),
        written
    );
}

#[tokio::test]
async fn a_run_lives_on_a_fork_and_leaves_the_base_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(&dir).await;

    let mut strategy = SignalReplay::new(vec![(
        ts(3 * SECOND),
        OrderRequest::market(id(), OutcomeId::FIRST, Side::Buy, qty(100.0)),
    )])
    .unwrap();

    let report = run_in_fork(
        &db,
        RunSpec::new("demo", money(1_000.0)),
        &mut strategy,
        |builder| builder.fee_model(Box::new(PredictionMarketFees::new(0.07).unwrap())),
    )
    .await
    .unwrap();

    assert_eq!(report.fork, "bt-demo");
    assert_eq!(report.result.fills.len(), 1);

    // The base has no run tables at all.
    let base_tables: Vec<String> = db
        .list_tables()
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.name)
        .collect();
    assert!(
        !base_tables.iter().any(|name| name.starts_with("bt_")),
        "a run must not write into the base: {base_tables:?}"
    );

    // The fork has all of them.
    let fork = db.open_fork("bt-demo").await.unwrap();
    let fork_tables: Vec<String> = fork
        .list_tables()
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.name)
        .collect();
    for expected in ["bt_run", "bt_orders", "bt_fills", "bt_positions", "bt_equity"] {
        assert!(fork_tables.contains(&expected.to_string()), "missing {expected}");
    }
}

#[tokio::test]
async fn positions_rebuild_from_the_stored_fill_log() {
    // The audit claim: bt_fills alone reconstructs the run's positions.
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(&dir).await;

    let mut strategy = SignalReplay::new(vec![
        (
            ts(2 * SECOND),
            OrderRequest::market(id(), OutcomeId::FIRST, Side::Buy, qty(100.0)),
        ),
        (
            ts(5 * SECOND),
            OrderRequest::market(id(), OutcomeId::FIRST, Side::Sell, qty(40.0)),
        ),
    ])
    .unwrap();

    let report = run_in_fork(&db, RunSpec::new("audit", money(1_000.0)), &mut strategy, |b| b)
        .await
        .unwrap();

    let fork = db.open_fork("bt-audit").await.unwrap();
    let stored = store::read_fills(&fork, ReadAt::Latest).await.unwrap();
    assert_eq!(stored.len(), report.result.fills.len());

    let from_storage = Portfolio::replay(&stored).unwrap();
    let from_run = Portfolio::replay(&report.result.fills).unwrap();
    assert_eq!(
        from_storage.position(&id(), OutcomeId::FIRST),
        from_run.position(&id(), OutcomeId::FIRST),
        "a stored run must rebuild to the same position it reported"
    );
    assert_eq!(
        from_storage.realized_pnl().unwrap(),
        from_run.realized_pnl().unwrap()
    );
}

#[tokio::test]
async fn a_run_is_reproducible_from_its_spec() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(&dir).await;

    let make_strategy = || {
        SignalReplay::new(vec![(
            ts(3 * SECOND),
            OrderRequest::market(id(), OutcomeId::FIRST, Side::Buy, qty(75.0)),
        )])
        .unwrap()
    };

    let mut first_strategy = make_strategy();
    let first = run_in_fork(
        &db,
        RunSpec::new("rep-1", money(500.0)).read_at(ReadAt::Version(1)),
        &mut first_strategy,
        |b| b,
    )
    .await
    .unwrap();

    let mut second_strategy = make_strategy();
    let second = run_in_fork(
        &db,
        RunSpec::new("rep-2", money(500.0)).read_at(ReadAt::Version(1)),
        &mut second_strategy,
        |b| b,
    )
    .await
    .unwrap();

    assert_eq!(first.result.fills, second.result.fills);
    assert_eq!(first.result.equity, second.result.equity);
    assert_eq!(first.result.final_cash, second.result.final_cash);
}

#[tokio::test]
async fn the_equity_curve_is_written_and_ends_where_the_run_did() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(&dir).await;

    let mut strategy = SignalReplay::new(vec![(
        ts(2 * SECOND),
        OrderRequest::market(id(), OutcomeId::FIRST, Side::Buy, qty(100.0)),
    )])
    .unwrap();
    let report = run_in_fork(&db, RunSpec::new("curve", money(1_000.0)), &mut strategy, |b| b)
        .await
        .unwrap();

    let curve = &report.result.equity;
    assert!(curve.len() >= 2, "a ten second run must produce a curve");
    assert_eq!(curve.last().unwrap().ts, report.result.simulated_through.unwrap());
    // Equity starts at the starting cash, because nothing has traded yet.
    assert_eq!(curve[0].equity, money(1_000.0));
    // Buying converts cash into position value, and the only thing it costs
    // is the half-spread: the fill lifts the offer at 0.42 while the mark is
    // the mid at 0.41, so 100 contracts give up 0.01 each. That is a real
    // cost of taking liquidity, not drift, and it should appear immediately
    // rather than at exit.
    let after_trade = curve
        .iter()
        .find(|point| point.ts.get() >= 2 * SECOND)
        .unwrap();
    assert!(after_trade.position_value.is_positive());
    assert_eq!(
        after_trade.equity,
        money(999.0),
        "equity should fall by exactly the half-spread paid"
    );
    assert_eq!(after_trade.unrealized_pnl, money(-1.0));
}

#[tokio::test]
async fn settlement_is_gated_on_what_the_run_actually_reached() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(&dir).await;

    // The market resolves at 100s; this run stops at 10s.
    let mut strategy = SignalReplay::new(vec![(
        ts(2 * SECOND),
        OrderRequest::market(id(), OutcomeId::FIRST, Side::Buy, qty(100.0)),
    )])
    .unwrap();
    let short = run_in_fork(&db, RunSpec::new("short", money(1_000.0)), &mut strategy, |b| b)
        .await
        .unwrap();

    assert!(
        !short.settlement.was_applied(),
        "a run that ended before resolution must not book settlement"
    );
    assert!(short
        .warnings()
        .iter()
        .any(|warning| warning.contains("unsettled")));

    // Extend the data past resolution and the same strategy settles.
    store::write_book_events(&db, &[snapshot_at(200 * SECOND, 0.99)])
        .await
        .unwrap();
    let mut strategy = SignalReplay::new(vec![(
        ts(2 * SECOND),
        OrderRequest::market(id(), OutcomeId::FIRST, Side::Buy, qty(100.0)),
    )])
    .unwrap();
    let full = run_in_fork(&db, RunSpec::new("full", money(1_000.0)), &mut strategy, |b| b)
        .await
        .unwrap();
    assert!(full.settlement.was_applied());
    assert_eq!(full.settlement.settled.len(), 1);
}

#[tokio::test]
async fn coverage_below_the_floor_refuses_to_run() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(&dir).await;

    // Ask for a window four times longer than the data.
    let window = TimeWindow::new(ts(0), ts(40 * SECOND)).unwrap();
    let mut strategy = SignalReplay::new(vec![]).unwrap();
    let error = run_in_fork(
        &db,
        RunSpec::new("thin", money(100.0))
            .window(window)
            .minimum_coverage(0.9),
        &mut strategy,
        |b| b,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("coverage"), "{error}");
}

#[tokio::test]
async fn a_run_over_a_window_records_its_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(&dir).await;
    let window = TimeWindow::new(ts(SECOND), ts(11 * SECOND)).unwrap();
    let mut strategy = SignalReplay::new(vec![]).unwrap();
    let report = run_in_fork(
        &db,
        RunSpec::new("covered", money(100.0)).window(window),
        &mut strategy,
        |b| b,
    )
    .await
    .unwrap();
    let coverage = report.coverage.expect("a windowed run reports coverage");
    assert!(coverage.ratio() > 0.89, "got {}", coverage.ratio());
}

#[tokio::test]
async fn the_digest_changes_with_anything_that_changes_the_result() {
    let base = RunSpec::new("d", money(100.0));
    assert_eq!(base.digest(), RunSpec::new("d", money(100.0)).digest());
    assert_ne!(base.digest(), RunSpec::new("d", money(101.0)).digest());
    assert_ne!(
        base.digest(),
        RunSpec::new("d", money(100.0))
            .read_at(ReadAt::Version(3))
            .digest()
    );
    assert_ne!(
        base.digest(),
        RunSpec::new("d", money(100.0))
            .window(TimeWindow::new(ts(0), ts(10)).unwrap())
            .digest()
    );
}

#[tokio::test]
async fn a_truncated_snapshot_is_refused_rather_than_half_applied() {
    // Simulate a writer that died mid-snapshot by writing the levels of one
    // event with no is_last row.
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("trunc.db")).await.unwrap();
    store::create_market_data_tables(&db).await.unwrap();

    use arrow::array::{
        BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
        UInt16Array,
    };
    use std::sync::Arc;
    let batch = arrow::record_batch::RecordBatch::try_new(
        h5i_db_backtest::schema::book_deltas(),
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![1i64, 2])),
            Arc::new(TimestampNanosecondArray::from(vec![1i64, 2])),
            Arc::new(StringArray::from(vec![MARKET, MARKET])),
            Arc::new(UInt16Array::from(vec![0u16, 0])),
            Arc::new(StringArray::from(vec!["snapshot", "snapshot"])),
            Arc::new(StringArray::from(vec![Some("buy"), Some("sell")])),
            Arc::new(Float64Array::from(vec![Some(0.4), Some(0.42)])),
            Arc::new(Float64Array::from(vec![Some(1.0), Some(1.0)])),
            Arc::new(Int64Array::from(vec![0i64, 0])),
            // Neither row closes the event.
            Arc::new(BooleanArray::from(vec![false, false])),
            Arc::new(StringArray::from(vec![None::<&str>, None])),
        ],
    )
    .unwrap();
    db.append(
        "book_deltas",
        vec![batch],
        h5i_db_core::database::WriteOptions::default(),
    )
    .await
    .unwrap();

    let error = store::read_book_events(&db, ReadAt::Latest, None)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("truncated"), "{error}");
}

#[tokio::test]
async fn running_without_instruments_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("bare.db")).await.unwrap();
    store::create_market_data_tables(&db).await.unwrap();
    let mut strategy = SignalReplay::new(vec![]).unwrap();
    let error = run_in_fork(&db, RunSpec::new("bare", money(1.0)), &mut strategy, |b| b)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("no instruments"), "{error}");
}

#[tokio::test]
async fn marks_and_settlement_agree_on_the_stored_positions() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded_db(&dir).await;
    store::write_book_events(&db, &[snapshot_at(200 * SECOND, 0.99)])
        .await
        .unwrap();

    let mut strategy = SignalReplay::new(vec![(
        ts(2 * SECOND),
        OrderRequest::market(id(), OutcomeId::FIRST, Side::Buy, qty(10.0)),
    )])
    .unwrap();
    let report = run_in_fork(&db, RunSpec::new("marks", money(100.0)), &mut strategy, |b| b)
        .await
        .unwrap();

    let settled = &report.settlement.settled[0];
    // A YES position that resolved YES is worth 1.00 per contract.
    let portfolio = Portfolio::replay(&report.result.fills).unwrap();
    let position = portfolio.position(&id(), OutcomeId::FIRST).unwrap();
    let expected = position
        .unrealized_pnl(price(1.0))
        .unwrap();
    assert_eq!(settled.settled_pnl, expected);
    // And the market-exit figure came from the last mark, not from nothing.
    let marks: BTreeMap<_, _> = report.result.marks.clone();
    assert!(marks.contains_key(&(id(), OutcomeId::FIRST)));
    assert!(settled.market_exit_pnl.is_some());
}
