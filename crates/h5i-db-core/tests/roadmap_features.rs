use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use h5i_db_core::{
    Backend, Database, Error, ReadAt, RetentionCut, ScanOptions, TableOptions, TailEvent,
    WriteOptions,
};
use object_store::memory::InMemory;
use tempfile::TempDir;
use url::Url;

fn schema_v1() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]))
}

fn schema_v2() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new("quality", DataType::Float64, true),
    ]))
}

fn options() -> TableOptions {
    TableOptions {
        time_column: Some("ts".into()),
        ..Default::default()
    }
}

fn batch_v1(ts: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema_v1(),
        vec![
            Arc::new(Int64Array::from(ts.to_vec())),
            Arc::new(StringArray::from(vec!["A"; ts.len()])),
            Arc::new(Int32Array::from(vec![10; ts.len()])),
        ],
    )
    .unwrap()
}

fn batch_v2(ts: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema_v2(),
        vec![
            Arc::new(Int64Array::from(ts.to_vec())),
            Arc::new(StringArray::from(vec!["A"; ts.len()])),
            Arc::new(Int64Array::from(vec![20; ts.len()])),
            Arc::new(Float64Array::from(vec![Some(0.9); ts.len()])),
        ],
    )
    .unwrap()
}

async fn local_db() -> (TempDir, Database) {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path()).await.unwrap();
    (dir, db)
}

#[tokio::test]
async fn schema_evolution_adapts_old_segments_and_accepts_new_rows() {
    let (_dir, db) = local_db().await;
    db.create_table("ticks", schema_v1(), options())
        .await
        .unwrap();
    db.append("ticks", vec![batch_v1(&[1, 2])], WriteOptions::default())
        .await
        .unwrap();
    db.evolve_schema("ticks", schema_v2(), WriteOptions::default())
        .await
        .unwrap();
    db.append("ticks", vec![batch_v2(&[3])], WriteOptions::default())
        .await
        .unwrap();

    let (batches, _) = db
        .scan("ticks", ReadAt::Latest, ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    assert!(batches[0].column(3).null_count() > 0);
    assert_eq!(batches[0].schema(), schema_v2());
    assert_eq!(
        db.resolve("ticks", ReadAt::Version(1))
            .await
            .unwrap()
            .schema,
        schema_v1()
    );
}

#[tokio::test]
async fn incremental_tail_and_retention_are_enforced() {
    let (_dir, db) = local_db().await;
    db.create_table("ticks", schema_v1(), options())
        .await
        .unwrap();
    db.append("ticks", vec![batch_v1(&[1])], WriteOptions::default())
        .await
        .unwrap();
    db.append("ticks", vec![batch_v1(&[2])], WriteOptions::default())
        .await
        .unwrap();
    let diff = db.diff("ticks", 1, 2).await.unwrap();
    assert_eq!(diff.added_rows, 1);
    let (rows, _) = db
        .diff_scan("ticks", 1, 2, ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(
        db.wait_for_version(
            "ticks",
            1,
            Duration::from_millis(1),
            Duration::from_millis(10)
        )
        .await
        .unwrap(),
        TailEvent::Advanced(2)
    );

    let floor = db
        .set_retention("ticks", RetentionCut::KeepLast(1), Some("test".into()))
        .await
        .unwrap();
    assert_eq!(floor.min_retained_sequence, 2);
    assert!(matches!(
        db.resolve("ticks", ReadAt::Version(1)).await,
        Err(Error::VersionNotFound { .. })
    ));
    assert_eq!(db.list_versions("ticks").await.unwrap().len(), 1);
    let vacuum = db.vacuum(Some("ticks"), 0, false).await.unwrap();
    let expired: Vec<u64> = vacuum
        .candidates
        .iter()
        .filter_map(|p| {
            h5i_db_core::layout::manifest_sequence_from_path(&object_store::path::Path::from(
                p.as_str(),
            ))
        })
        .collect();
    assert!(expired.contains(&0), "candidates: {:?}", vacuum.candidates);
    assert!(expired.contains(&1), "candidates: {:?}", vacuum.candidates);
}

#[tokio::test]
async fn multi_table_transaction_advances_both_heads() {
    let (_dir, db) = local_db().await;
    db.create_table("a", schema_v1(), options()).await.unwrap();
    db.create_table("b", schema_v1(), options()).await.unwrap();
    let mut txn = db.transaction();
    txn.append("a", vec![batch_v1(&[1])]).unwrap();
    txn.append("b", vec![batch_v1(&[2])]).unwrap();
    let committed = txn.commit().await.unwrap();
    assert_eq!(committed.len(), 2);
    assert_eq!(
        db.resolve("a", ReadAt::Latest).await.unwrap().head_sequence,
        1
    );
    assert_eq!(
        db.resolve("b", ReadAt::Latest).await.unwrap().head_sequence,
        1
    );
}

#[tokio::test]
async fn in_memory_object_backend_is_constructible_and_commits() {
    let store = Arc::new(InMemory::new());
    let backend = Backend::from_store(store.clone(), Url::parse("memory:///").unwrap());
    let db = Database::create_with_backend(backend).await.unwrap();
    db.create_table("ticks", schema_v1(), options())
        .await
        .unwrap();
    db.append("ticks", vec![batch_v1(&[1])], WriteOptions::default())
        .await
        .unwrap();
    let reopened = Database::open_backend(
        Backend::from_store(store, Url::parse("memory:///").unwrap()),
        true,
    )
    .await
    .unwrap();
    assert_eq!(
        reopened
            .resolve("ticks", ReadAt::Latest)
            .await
            .unwrap()
            .manifest
            .rows,
        1
    );
}
