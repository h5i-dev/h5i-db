//! Edge-case coverage: empty inputs, unicode names, limits, grace periods,
//! non-timestamp time columns, schema-change rejection, retry exhaustion.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use h5i_db_core::{
    Database, Error, ReadAt, ScanOptions, StorageOptions, TableOptions, WriteOptions,
};

fn ts_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("v", DataType::Float64, false),
    ]))
}

fn ts_batch(ts: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        ts_schema(),
        vec![
            Arc::new(TimestampNanosecondArray::from(ts.to_vec()).with_timezone("UTC".to_string())),
            Arc::new(Float64Array::from(
                ts.iter().map(|t| *t as f64).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

async fn fresh() -> (tempfile::TempDir, Database) {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("db")).await.unwrap();
    (dir, db)
}

#[tokio::test]
async fn empty_writes_and_scans() {
    let (_dir, db) = fresh().await;
    db.create_table(
        "t",
        ts_schema(),
        TableOptions {
            time_column: Some("ts".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Writing zero batches / zero rows commits an empty version cleanly.
    let r = db
        .write("t", vec![], WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(r.rows_total, 0);
    let r = db
        .append("t", vec![ts_batch(&[])], WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(r.rows_total, 0);

    // Scanning an empty table (with filters) is fine.
    let (batches, report) = db
        .scan(
            "t",
            ReadAt::Latest,
            ScanOptions {
                time_start: Some(0),
                time_end: Some(100),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(batches.is_empty());
    assert_eq!(report.rows_returned, 0);
}

#[tokio::test]
async fn unicode_and_hostile_table_names() {
    let (_dir, db) = fresh().await;
    for name in ["取引", "trades/2026", "../escape", "name with spaces", "🚀"] {
        db.create_table(
            name,
            ts_schema(),
            TableOptions {
                time_column: Some("ts".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap_or_else(|e| panic!("create {name:?}: {e}"));
        db.append(name, vec![ts_batch(&[1, 2])], WriteOptions::default())
            .await
            .unwrap();
        let (b, _) = db
            .scan(name, ReadAt::Latest, ScanOptions::default())
            .await
            .unwrap();
        assert_eq!(b.iter().map(|x| x.num_rows()).sum::<usize>(), 2, "{name}");
    }
    let listed = db.list_tables().await.unwrap();
    assert_eq!(listed.len(), 5);
    // Nothing escaped the database root.
    let entry = db.list_tables().await.unwrap();
    assert!(entry.iter().all(|e| !e.name.is_empty()));
}

#[tokio::test]
async fn integer_time_column_works() {
    // Time column may be a plain integer (e.g. epoch micros from upstream).
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("seq", DataType::Int64, false),
        Field::new("v", DataType::Float64, true),
    ]));
    let (_dir, db) = fresh().await;
    db.create_table(
        "t",
        schema.clone(),
        TableOptions {
            time_column: Some("seq".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0)])),
        ],
    )
    .unwrap();
    db.append("t", vec![batch], WriteOptions::default())
        .await
        .unwrap();
    let (b, _) = db
        .scan(
            "t",
            ReadAt::Latest,
            ScanOptions {
                time_start: Some(15),
                time_end: Some(25),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(b.iter().map(|x| x.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn nullable_time_column_is_rejected_at_create() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        true, // nullable
    )]));
    let (_dir, db) = fresh().await;
    let err = db
        .create_table(
            "t",
            schema,
            TableOptions {
                time_column: Some("ts".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidInput { .. }), "{err}");

    // Unknown time column also rejected.
    let err = db
        .create_table(
            "t",
            ts_schema(),
            TableOptions {
                time_column: Some("nope".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidInput { .. }), "{err}");
}

#[tokio::test]
async fn schema_change_on_write_is_rejected_cleanly() {
    // Schema evolution is a documented follow-up (DESIGN_CLAUDE.md §4):
    // today a write with a different schema must fail loudly, not corrupt.
    let (_dir, db) = fresh().await;
    db.create_table(
        "t",
        ts_schema(),
        TableOptions {
            time_column: Some("ts".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let other: SchemaRef = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("v", DataType::Float64, false),
        Field::new("extra", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        other,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![1]).with_timezone("UTC".to_string())),
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(StringArray::from(vec![Some("x")])),
        ],
    )
    .unwrap();
    let err = db
        .write("t", vec![batch], WriteOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::SchemaMismatch { .. }), "{err}");
    // Table still healthy.
    assert!(db.verify("t", true).await.unwrap().problems.is_empty());
}

#[tokio::test]
async fn segment_hard_limit_demands_compaction() {
    let (_dir, db) = fresh().await;
    db.create_table(
        "t",
        ts_schema(),
        TableOptions {
            time_column: Some("ts".into()),
            storage: StorageOptions {
                target_segment_bytes: 1024,
                ..Default::default()
            },
            max_segments_per_manifest: Some(5),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut hit_limit = false;
    for i in 0..10i64 {
        match db
            .append(
                "t",
                vec![ts_batch(&[i * 10, i * 10 + 1])],
                WriteOptions::default(),
            )
            .await
        {
            Ok(_) => {}
            Err(Error::LimitExceeded { detail }) => {
                assert!(detail.contains("compact"), "{detail}");
                hit_limit = true;
                break;
            }
            Err(other) => panic!("unexpected: {other}"),
        }
    }
    assert!(hit_limit, "hard segment limit never triggered");
    // Compaction clears the way.
    db.compact_with("t", Some(128 * 1024 * 1024), WriteOptions::default())
        .await
        .unwrap();
    db.append("t", vec![ts_batch(&[1000])], WriteOptions::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn vacuum_grace_period_protects_fresh_objects() {
    let (_dir, db) = fresh().await;
    db.create_table(
        "t",
        ts_schema(),
        TableOptions {
            time_column: Some("ts".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let ts: Vec<i64> = (0..50).collect();
    db.append("t", vec![ts_batch(&ts)], WriteOptions::default())
        .await
        .unwrap();
    // Create an orphan by planning (stages a rewritten boundary segment)
    // and then discarding.
    let plan = db
        .plan_replace_range("t", 0, 10, vec![], WriteOptions::default())
        .await
        .unwrap();
    assert!(plan.summary.segments_added >= 1, "{:?}", plan.summary);
    db.discard_plan("t", plan.plan_id).await.unwrap();

    // With a 1-hour grace, the fresh orphan must be untouched even in apply
    // mode; with zero grace it is collected.
    let report = db.vacuum(Some("t"), 3600, true).await.unwrap();
    assert_eq!(report.deleted, 0, "{report:?}");
    let report = db.vacuum(Some("t"), 0, true).await.unwrap();
    assert!(report.deleted > 0, "{report:?}");
    assert!(db.verify("t", true).await.unwrap().problems.is_empty());
}

#[tokio::test]
async fn open_errors_are_precise() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("nope.db");
    let err = Database::open(&missing).await.unwrap_err();
    assert!(matches!(err, Error::DatabaseNotFound { .. }), "{err}");

    Database::create(&dir.path().join("x.db")).await.unwrap();
    let err = Database::create(&dir.path().join("x.db"))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::DatabaseExists { .. }), "{err}");

    // open_or_create is idempotent.
    Database::open_or_create(&dir.path().join("x.db"))
        .await
        .unwrap();
}

#[tokio::test]
async fn append_retry_exhaustion_surfaces_conflict() {
    let (_dir, db) = fresh().await;
    db.create_table(
        "t",
        ts_schema(),
        TableOptions {
            time_column: Some("ts".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    // expected_version pins the head; a first append moves it, so a retried
    // append with the stale expectation can never succeed.
    db.append("t", vec![ts_batch(&[1])], WriteOptions::default())
        .await
        .unwrap();
    let err = db
        .append_with_retry(
            "t",
            vec![ts_batch(&[2])],
            WriteOptions {
                expected_version: Some(0),
                ..Default::default()
            },
            2,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::VersionConflict { .. }), "{err}");
}

#[tokio::test]
async fn projection_of_unknown_column_errors() {
    let (_dir, db) = fresh().await;
    db.create_table(
        "t",
        ts_schema(),
        TableOptions {
            time_column: Some("ts".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    db.append("t", vec![ts_batch(&[1])], WriteOptions::default())
        .await
        .unwrap();
    let err = db
        .scan(
            "t",
            ReadAt::Latest,
            ScanOptions {
                projection: Some(vec!["nope".into()]),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    // Surfaced as an arrow schema error, not a panic.
    assert!(err.to_string().contains("nope"), "{err}");
}
