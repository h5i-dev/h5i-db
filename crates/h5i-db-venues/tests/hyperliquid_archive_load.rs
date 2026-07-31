//! Loading a downloaded archive directory into a database.
//!
//! Built from the same real fixtures the parser tests use, laid out the way
//! `aws s3 sync` leaves the bucket, so this exercises the walk a caller
//! would otherwise have to write themselves.

use h5i_db_backtest::store;
use h5i_db_backtest::types::UnixNanos;
use h5i_db_core::Database;
use h5i_db_core::database::ReadAt;
use h5i_db_venues::hyperliquid;
use h5i_db_venues::hyperliquid_archive::{ArchiveSpec, load_archive, plan_from_archive};

const DATE: &str = "20250101";

fn fixtures() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/hyperliquid")
}

/// Lay the real fixtures out exactly as the bucket does.
fn archive_root(dir: &tempfile::TempDir) -> std::path::PathBuf {
    let root = dir.path().join("archive");
    let books = root.join("market_data").join(DATE).join("0").join("l2Book");
    std::fs::create_dir_all(&books).unwrap();
    std::fs::copy(
        fixtures().join("archive_l2book_btc.lz4"),
        books.join("BTC.lz4"),
    )
    .unwrap();

    let ctxs = root.join("asset_ctxs");
    std::fs::create_dir_all(&ctxs).unwrap();
    std::fs::copy(
        fixtures().join("asset_ctxs.csv.lz4"),
        ctxs.join(format!("{DATE}.csv.lz4")),
    )
    .unwrap();
    root
}

fn universe() -> Vec<hyperliquid::AssetMeta> {
    hyperliquid::parse_meta(&std::fs::read_to_string(fixtures().join("meta.json")).unwrap())
        .unwrap()
}

#[test]
fn a_synced_directory_becomes_a_plan_without_touching_a_database() {
    let dir = tempfile::tempdir().unwrap();
    let spec = ArchiveSpec::new(archive_root(&dir)).date(DATE).coin("BTC");
    let load = plan_from_archive(&spec, &universe()).unwrap();

    load.require_files(2).unwrap();
    assert_eq!(load.book_records, 6, "the l2Book hour");
    assert!(load.reference_records >= 4, "the asset_ctxs day");
    assert_eq!(load.malformed, 0);

    let plan = load.plan.as_ref().unwrap();
    assert_eq!(plan.vendor, "hyperliquid");
    assert_eq!(
        plan.instruments.len(),
        1,
        "only the coin that was asked for"
    );
    assert_eq!(plan.instruments[0].id.as_str(), "BTC-PERP");
    assert!(
        plan.references
            .iter()
            .all(|record| record.instrument.as_str() == "BTC-PERP")
    );
    assert!(load.window.is_some());
}

#[test]
fn a_date_that_is_not_on_disk_is_named_rather_than_silently_empty() {
    // An incomplete sync that reads as "no data" is much worse found at
    // replay time, where it looks like a thin book.
    let dir = tempfile::tempdir().unwrap();
    let spec = ArchiveSpec::new(archive_root(&dir))
        .date(DATE)
        .date("20250102")
        .coin("BTC");
    let load = plan_from_archive(&spec, &universe()).unwrap();
    assert!(
        load.missing.iter().any(|entry| entry.contains("20250102")),
        "{:?}",
        load.missing
    );

    // A coin the sync never fetched is named too.
    let spec = ArchiveSpec::new(archive_root(&dir))
        .date(DATE)
        .coin("BTC")
        .coin("SOL");
    let load = plan_from_archive(&spec, &universe()).unwrap();
    assert!(
        load.missing.iter().any(|entry| entry.contains("SOL")),
        "{:?}",
        load.missing
    );
    assert!(load.require_files(9).is_err(), "and can be refused");
}

#[test]
fn a_load_needs_to_be_told_which_dates_it_wants() {
    // Otherwise a run depends on the shape of whatever sync last ran
    // rather than on what was asked for.
    let dir = tempfile::tempdir().unwrap();
    let spec = ArchiveSpec::new(archive_root(&dir));
    assert!(plan_from_archive(&spec, &universe()).is_err());
}

#[test]
fn archive_data_without_matching_metadata_is_refused() {
    // Loading a book with no tick size would accept prices the venue never
    // quoted, which is the whole reason the universe is supplied.
    let dir = tempfile::tempdir().unwrap();
    let spec = ArchiveSpec::new(archive_root(&dir)).date(DATE).coin("BTC");
    let without_btc: Vec<_> = universe()
        .into_iter()
        .filter(|asset| asset.name != "BTC")
        .collect();
    let error = plan_from_archive(&spec, &without_btc).unwrap_err();
    assert!(error.to_string().contains("tick size"), "{error}");
}

#[tokio::test]
async fn loading_the_same_directory_twice_does_not_double_the_book() {
    let dir = tempfile::tempdir().unwrap();
    let root = archive_root(&dir);
    let db = Database::create(&dir.path().join("hl.db")).await.unwrap();
    let spec = ArchiveSpec::new(&root).date(DATE).coin("BTC");
    let universe = universe();

    let first = load_archive(&db, &spec, &universe, UnixNanos::new(0))
        .await
        .unwrap();
    assert!(first.outcome.as_ref().unwrap().was_written());

    // Re-running an interrupted sync is the natural thing to do, so it must
    // be a no-op rather than a way to double a book.
    let second = load_archive(&db, &spec, &universe, UnixNanos::new(1))
        .await
        .unwrap();
    assert!(!second.outcome.as_ref().unwrap().was_written());

    // Everything landed in the canonical tables, ready for a run.
    let instruments = store::read_instruments(&db, ReadAt::Latest).await.unwrap();
    assert_eq!(instruments.len(), 1);
    let books = store::read_book_events(&db, ReadAt::Latest, None)
        .await
        .unwrap();
    assert_eq!(books.len(), first.book_records);

    let references: Vec<_> = store::reference_source(&db, ReadAt::Latest, None)
        .await
        .unwrap()
        .map(|record| record.unwrap())
        .collect();
    assert_eq!(references.len(), first.reference_records);
    assert!(references.iter().all(|record| matches!(
        record.event,
        h5i_db_backtest::event::MarketEvent::Reference { .. }
    )));
}

#[tokio::test]
async fn a_capture_and_a_download_load_through_the_same_path() {
    // The live capture carries the prints the archive does not, and it is
    // written in the archive's own envelope precisely so it drops into the
    // same directory layout and the same loader.
    let dir = tempfile::tempdir().unwrap();
    let root = archive_root(&dir);
    // Its own hour, named for its coin: the loader filters on file name so
    // it can skip a hundred and sixty files without decompressing them.
    let hour = root.join("market_data").join(DATE).join("1").join("l2Book");
    std::fs::create_dir_all(&hour).unwrap();
    std::fs::copy(
        fixtures().join("live_btc_capture.lz4"),
        hour.join("BTC.lz4"),
    )
    .unwrap();

    let db = Database::create(&dir.path().join("mixed.db"))
        .await
        .unwrap();
    // The capture's coin is BTC inside the payload; the file name is only a
    // label, and the loader keys instruments off the payload.
    let spec = ArchiveSpec::new(&root).date(DATE).coin("BTC");
    let load = load_archive(&db, &spec, &universe(), UnixNanos::new(0))
        .await
        .unwrap();

    assert!(
        load.trade_records >= 50,
        "prints reached the database: {}",
        load.trade_records
    );
    let trades: Vec<_> = store::trade_source(&db, ReadAt::Latest, None)
        .await
        .unwrap()
        .map(|record| record.unwrap())
        .collect();
    assert_eq!(trades.len(), load.trade_records);
}
