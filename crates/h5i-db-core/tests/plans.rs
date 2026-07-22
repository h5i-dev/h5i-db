//! Previewable-mutation (plan/apply) tests: preview accuracy, conflict on
//! moved head, discard + vacuum, and tamper detection.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use h5i_db_core::{
    Database, Error, MutationPlan, ReadAt, ScanOptions, StorageOptions, TableOptions, WriteOptions,
};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("size", DataType::Int64, false),
    ]))
}

fn batch(ts: &[i64], price: f64) -> RecordBatch {
    let syms: Vec<&str> = ts.iter().map(|_| "A").collect();
    let prices: Vec<f64> = ts.iter().map(|_| price).collect();
    let sizes: Vec<i64> = ts.iter().map(|_| 1).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(TimestampNanosecondArray::from(ts.to_vec()).with_timezone("UTC".to_string())),
            Arc::new(StringArray::from(syms)),
            Arc::new(Float64Array::from(prices)),
            Arc::new(Int64Array::from(sizes)),
        ],
    )
    .unwrap()
}

async fn setup() -> (tempfile::TempDir, Database) {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("db")).await.unwrap();
    db.create_table(
        "t",
        schema(),
        TableOptions {
            time_column: Some("ts".into()),
            sort_key: vec![],
            storage: StorageOptions {
                target_segment_bytes: 8 * 1024,
                target_row_group_bytes: 4 * 1024,
                ..Default::default()
            },
            max_segments_per_manifest: None,
        },
    )
    .await
    .unwrap();
    for base in [0i64, 1000, 2000] {
        let ts: Vec<i64> = (base..base + 300).collect();
        db.append("t", vec![batch(&ts, base as f64)], WriteOptions::default())
            .await
            .unwrap();
    }
    (dir, db)
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test]
async fn plan_previews_exactly_then_apply_publishes() {
    let (_dir, db) = setup().await;
    let head_before = db.resolve("t", ReadAt::Latest).await.unwrap();

    let ts: Vec<i64> = (1000..1100).collect();
    let plan = db
        .plan_replace_range(
            "t",
            1000,
            1100,
            vec![batch(&ts, 777.0)],
            WriteOptions::default(),
        )
        .await
        .unwrap();

    // Preview numbers are exact.
    assert_eq!(plan.base_version, head_before.manifest.sequence);
    assert_eq!(plan.summary.rows_before, 900);
    assert_eq!(plan.summary.rows_affected, 100); // 100 rows replaced
    assert_eq!(plan.summary.rows_after, 900);
    assert!(plan.summary.segments_added >= 1);
    assert!(plan.summary.added_bytes > 0);
    assert_eq!(plan.summary.affected_time_range, Some((1000, 1100)));

    // Samples decode and show old vs new prices.
    let before = MutationPlan::decode_sample(plan.before_sample_ipc_b64.as_ref().unwrap()).unwrap();
    let after = MutationPlan::decode_sample(plan.after_sample_ipc_b64.as_ref().unwrap()).unwrap();
    let first_price = |bs: &[RecordBatch]| -> f64 {
        let b = &bs[0];
        b.column(b.schema().index_of("price").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
    };
    assert_eq!(first_price(&before), 1000.0);
    assert_eq!(first_price(&after), 777.0);

    // Nothing is visible until apply.
    let visible = db.resolve("t", ReadAt::Latest).await.unwrap();
    assert_eq!(visible.manifest.sequence, head_before.manifest.sequence);
    let (b, _) = db
        .scan(
            "t",
            ReadAt::Latest,
            ScanOptions {
                time_start: Some(1000),
                time_end: Some(1100),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let p = b[0]
        .column(b[0].schema().index_of("price").unwrap())
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert_eq!(p, 1000.0, "pre-apply reads must see the old data");

    // The plan is listed and reloadable.
    let listed = db.list_plans("t").await.unwrap();
    assert_eq!(listed.len(), 1);
    let reloaded = db.load_plan("t", plan.plan_id).await.unwrap();
    assert_eq!(reloaded.plan_id, plan.plan_id);

    // Apply publishes exactly the previewed state.
    let res = db.apply_plan(&plan).await.unwrap();
    assert_eq!(res.sequence, plan.base_version + 1);
    assert_eq!(res.rows_total, 900);
    let (b, _) = db
        .scan(
            "t",
            ReadAt::Latest,
            ScanOptions {
                time_start: Some(1000),
                time_end: Some(1100),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(rows(&b), 100);
    for bb in &b {
        let p = bb
            .column(bb.schema().index_of("price").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(p.iter().all(|v| v == Some(777.0)));
    }
    // The consumed plan is gone.
    assert!(db.list_plans("t").await.unwrap().is_empty());
    let verify = db.verify("t", true).await.unwrap();
    assert!(verify.problems.is_empty(), "{:?}", verify.problems);
}

#[tokio::test]
async fn apply_conflicts_if_head_moved() {
    let (_dir, db) = setup().await;
    let plan = db
        .plan_replace_range("t", 0, 100, vec![], WriteOptions::default())
        .await
        .unwrap();

    // Someone else commits in between.
    db.append("t", vec![batch(&[5000], 1.0)], WriteOptions::default())
        .await
        .unwrap();

    let err = db.apply_plan(&plan).await.unwrap_err();
    assert!(matches!(err, Error::VersionConflict { .. }), "{err}");
    assert!(err.retryable());

    // Discard; segments become vacuumable orphans once the plan is gone.
    db.discard_plan("t", plan.plan_id).await.unwrap();
    let report = db.vacuum(Some("t"), 0, true).await.unwrap();
    assert!(
        report.deleted > 0,
        "discarded plan segments must be vacuumable: {report:?}"
    );
    // Table remains intact.
    let verify = db.verify("t", true).await.unwrap();
    assert!(verify.problems.is_empty(), "{:?}", verify.problems);
}

#[tokio::test]
async fn live_plan_segments_survive_vacuum() {
    let (_dir, db) = setup().await;
    let plan = db
        .plan_replace_range("t", 0, 100, vec![], WriteOptions::default())
        .await
        .unwrap();

    // Vacuum with zero grace must NOT collect the plan's staged segments.
    let _ = db.vacuum(Some("t"), 0, true).await.unwrap();
    let res = db.apply_plan(&plan).await.unwrap();
    assert_eq!(res.sequence, plan.base_version + 1);
    let (b, _) = db
        .scan("t", ReadAt::Latest, ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(rows(&b), 800);
}

#[tokio::test]
async fn tampered_plan_is_rejected() {
    let (_dir, db) = setup().await;
    let mut plan = db
        .plan_replace_range("t", 0, 100, vec![], WriteOptions::default())
        .await
        .unwrap();
    plan.summary.rows_affected = 1; // tamper after sealing
    let err = db.apply_plan(&plan).await.unwrap_err();
    assert!(matches!(err, Error::Corruption { .. }), "{err}");
}

#[tokio::test]
async fn plan_write_previews_full_replacement() {
    let (_dir, db) = setup().await;
    let plan = db
        .plan_write("t", vec![batch(&[1, 2, 3], 9.0)], WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(plan.summary.rows_before, 900);
    assert_eq!(plan.summary.rows_after, 3);
    let res = db.apply_plan(&plan).await.unwrap();
    assert_eq!(res.rows_total, 3);
}
