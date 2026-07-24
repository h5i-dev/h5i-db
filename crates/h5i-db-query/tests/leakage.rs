//! V-A1 leakage-delta integration tests: run a query against head vs an as-of
//! read point and assert the diff surfaces availability leakage (restatement,
//! withheld rows) while a correctly time-bounded query shows none.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use h5i_db_core::{Database, ReadAt, TableOptions, WriteOptions};
use h5i_db_query::{check_leakage, DEFAULT_TOLERANCE};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]))
}

fn batch(ts: &[i64], symbol: &[&str], price: &[f64]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ts.to_vec())),
            Arc::new(StringArray::from(symbol.to_vec())),
            Arc::new(Float64Array::from(price.to_vec())),
        ],
    )
    .unwrap()
}

async fn setup() -> (tempfile::TempDir, Arc<Database>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::create(&dir.path().join("db")).await.unwrap());
    db.create_table(
        "prices",
        schema(),
        TableOptions {
            time_column: Some("ts".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    (dir, db)
}

#[tokio::test]
async fn restatement_produces_nonzero_delta() {
    let (_dir, db) = setup().await;
    // v1: two rows, avg price = 15.
    let v1 = db
        .write(
            "prices",
            vec![batch(&[1, 2], &["A", "A"], &[10.0, 20.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap()
        .sequence;
    // Restate ts=1's price 10 -> 100. head avg = (100+20)/2 = 60.
    db.replace_range(
        "prices",
        1,
        2,
        vec![batch(&[1], &["A"], &[100.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    let report = check_leakage(
        db.clone(),
        "SELECT avg(price) AS p FROM prices",
        ReadAt::Version(v1),
        DEFAULT_TOLERANCE,
    )
    .await
    .unwrap();

    assert!(report.comparable);
    assert_eq!(report.head_rows, 1);
    assert_eq!(report.asof_rows, 1);
    assert!(report.leakage_detected);
    let col = &report.columns[0];
    assert_eq!(col.name, "p");
    assert!(
        (col.head.unwrap() - 60.0).abs() < 1e-9,
        "head {:?}",
        col.head
    );
    assert!(
        (col.asof.unwrap() - 15.0).abs() < 1e-9,
        "asof {:?}",
        col.asof
    );
    assert!(
        (col.delta.unwrap() - 45.0).abs() < 1e-9,
        "delta {:?}",
        col.delta
    );
    // The report attributes the leak to a withheld version of `prices`.
    let w = &report.withheld_versions;
    assert_eq!(w.len(), 1);
    assert_eq!(w[0].table, "prices");
    assert!(w[0].asof_version < w[0].head_version);
}

#[tokio::test]
async fn time_bounded_query_shows_no_leakage() {
    let (_dir, db) = setup().await;
    let v1 = db
        .write(
            "prices",
            vec![batch(&[1, 2], &["A", "A"], &[10.0, 20.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap()
        .sequence;
    // Append strictly-later rows (available only after the decision instant).
    db.append(
        "prices",
        vec![batch(&[3, 4], &["A", "A"], &[30.0, 40.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    // A query bounded to ts<=2 does not depend on the withheld rows.
    let report = check_leakage(
        db.clone(),
        "SELECT count(*) AS c FROM prices WHERE ts <= 2",
        ReadAt::Version(v1),
        DEFAULT_TOLERANCE,
    )
    .await
    .unwrap();

    assert!(report.comparable);
    assert!(!report.row_count_differs);
    assert!(
        !report.leakage_detected,
        "a correctly-bounded query must show no leakage: {report:?}"
    );
    assert!((report.columns[0].delta.unwrap()).abs() < 1e-9);
    // Data *was* withheld even though this query didn't depend on it; the
    // report still records the version gap honestly.
    assert_eq!(report.withheld_versions.len(), 1);
}

#[tokio::test]
async fn as_of_timestamp_matches_version_pin() {
    let (_dir, db) = setup().await;
    let v1 = db
        .write(
            "prices",
            vec![batch(&[1, 2], &["A", "A"], &[10.0, 20.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap()
        .sequence;
    db.replace_range(
        "prices",
        1,
        2,
        vec![batch(&[1], &["A"], &[100.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    // Resolve v1's commit wall-clock time and use it as the as-of instant.
    let versions = db.list_versions("prices").await.unwrap();
    let v1_ts = versions
        .iter()
        .find(|s| s.sequence == v1)
        .expect("v1 summary")
        .committed_at_ns;

    let report = check_leakage(
        db.clone(),
        "SELECT avg(price) AS p FROM prices",
        ReadAt::AsOf(v1_ts),
        DEFAULT_TOLERANCE,
    )
    .await
    .unwrap();

    assert!(report.leakage_detected);
    // Same delta the explicit version pin produced (as-of resolves to v1).
    assert!((report.columns[0].delta.unwrap() - 45.0).abs() < 1e-9);
}

#[tokio::test]
async fn row_count_change_is_detected() {
    let (_dir, db) = setup().await;
    let v1 = db
        .write(
            "prices",
            vec![batch(&[1], &["A"], &[10.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap()
        .sequence;
    // A new symbol appears only after the decision instant.
    db.append(
        "prices",
        vec![batch(&[2], &["B"], &[20.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    let report = check_leakage(
        db.clone(),
        "SELECT symbol, count(*) AS c FROM prices GROUP BY symbol ORDER BY symbol",
        ReadAt::Version(v1),
        DEFAULT_TOLERANCE,
    )
    .await
    .unwrap();

    assert!(report.comparable);
    assert_eq!(report.head_rows, 2, "head sees A and B");
    assert_eq!(report.asof_rows, 1, "as-of sees only A");
    assert!(report.row_count_differs);
    assert!(report.leakage_detected);
}
