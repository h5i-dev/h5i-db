use arrow::array::{Float64Array, RecordBatch, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use h5i_db_core::{Database, ReadAt, ScanOptions, StorageOptions, TableOptions, WriteOptions};
use std::sync::Arc;
use std::time::Instant;

fn sch() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("px", DataType::Float64, false),
    ]))
}
fn opts() -> TableOptions {
    TableOptions {
        time_column: Some("ts".into()),
        sort_key: vec![],
        storage: StorageOptions::default(),
        max_segments_per_manifest: None,
    }
}
fn batch(s: i64, n: i64) -> RecordBatch {
    let ts: Vec<i64> = (s..s + n).collect();
    let px: Vec<f64> = ts.iter().map(|v| *v as f64).collect();
    RecordBatch::try_new(
        sch(),
        vec![
            Arc::new(TimestampNanosecondArray::from(ts).with_timezone("UTC".to_string())),
            Arc::new(Float64Array::from(px)),
        ],
    )
    .unwrap()
}

/// How expensive is the *sequential metadata* path on a LOCAL filesystem?
#[tokio::test]
async fn metadata_cost_on_local_fs() {
    let d = tempfile::tempdir().unwrap();
    let db = Database::create(&d.path().join("db")).await.unwrap();
    for i in 0..200 {
        db.create_table(&format!("t{i:04}"), sch(), opts())
            .await
            .unwrap();
        db.append(
            &format!("t{i:04}"),
            vec![batch(0, 20)],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    let t = Instant::now();
    for _ in 0..20 {
        db.list_tables().await.unwrap();
    }
    let list = t.elapsed() / 20;

    // What a query pays before planning: list_tables + resolve every table.
    let t = Instant::now();
    for _ in 0..20 {
        let entries = db.list_tables().await.unwrap();
        for e in &entries {
            db.resolve(&e.name, ReadAt::Latest).await.unwrap();
        }
    }
    let session_setup = t.elapsed() / 20;

    let t = Instant::now();
    for _ in 0..20 {
        db.scan("t0000", ReadAt::Latest, ScanOptions::default())
            .await
            .unwrap();
    }
    let scan = t.elapsed() / 20;

    println!(
        "LOCAL list_tables(200)={list:?}  session_setup(200 tables)={session_setup:?}  scan_one_table={scan:?}"
    );
    println!("LOCAL per-table list cost = {:?}", list / 200);
}
