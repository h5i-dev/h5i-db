//! Phase 1 exit-gate tests: versioning semantics, crash safety at every
//! commit step, racing writers, compaction equivalence, vacuum reachability,
//! and verify.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use h5i_db_core::{
    Database, Error, ReadAt, ScanOptions, StorageOptions, TableOptions, WriteOptions,
};

fn trades_schema() -> SchemaRef {
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

fn trades_batch(ts: &[i64], symbols: &[&str], prices: &[f64]) -> RecordBatch {
    let sizes: Vec<i64> = ts.iter().map(|t| t % 100 + 1).collect();
    RecordBatch::try_new(
        trades_schema(),
        vec![
            Arc::new(TimestampNanosecondArray::from(ts.to_vec()).with_timezone("UTC".to_string())),
            Arc::new(StringArray::from(symbols.to_vec())),
            Arc::new(Float64Array::from(prices.to_vec())),
            Arc::new(Int64Array::from(sizes)),
        ],
    )
    .unwrap()
}

fn default_options() -> TableOptions {
    TableOptions {
        time_column: Some("ts".into()),
        sort_key: vec![],
        storage: StorageOptions::default(),
        max_segments_per_manifest: None,
    }
}

fn small_segment_options() -> TableOptions {
    TableOptions {
        time_column: Some("ts".into()),
        sort_key: vec![],
        storage: StorageOptions {
            target_segment_bytes: 4 * 1024, // force many small segments
            target_row_group_bytes: 1024,
            ..Default::default()
        },
        max_segments_per_manifest: None,
    }
}

async fn fresh_db() -> (tempfile::TempDir, Database) {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("db")).await.unwrap();
    (dir, db)
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

// ---------------------------------------------------------------------------
// lifecycle & basic reads
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_write_append_read_roundtrip() {
    let (_dir, db) = fresh_db().await;
    db.create_table("trades", trades_schema(), default_options())
        .await
        .unwrap();

    let r1 = db
        .write(
            "trades",
            vec![trades_batch(
                &[100, 200, 300],
                &["A", "B", "A"],
                &[1.0, 2.0, 3.0],
            )],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(r1.sequence, 1);
    assert_eq!(r1.rows_total, 3);

    let r2 = db
        .append(
            "trades",
            vec![trades_batch(&[400, 500], &["B", "A"], &[4.0, 5.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(r2.sequence, 2);
    assert_eq!(r2.rows_total, 5);

    let (batches, report) = db
        .scan("trades", ReadAt::Latest, ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(total_rows(&batches), 5);
    assert_eq!(report.rows_returned, 5);

    // Time-range scan: [200, 400) → rows at 200, 300.
    let (batches, _) = db
        .scan(
            "trades",
            ReadAt::Latest,
            ScanOptions {
                time_start: Some(200),
                time_end: Some(400),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(total_rows(&batches), 2);

    // Projection.
    let (batches, _) = db
        .scan(
            "trades",
            ReadAt::Latest,
            ScanOptions {
                projection: Some(vec!["symbol".into(), "price".into()]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(batches[0].num_columns(), 2);

    // Projection excluding the time column still filters by time.
    let (batches, _) = db
        .scan(
            "trades",
            ReadAt::Latest,
            ScanOptions {
                projection: Some(vec!["price".into()]),
                time_start: Some(400),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(total_rows(&batches), 2);
    assert_eq!(batches[0].num_columns(), 1);
}

#[tokio::test]
async fn time_travel_and_restore() {
    let (_dir, db) = fresh_db().await;
    db.create_table("t", trades_schema(), default_options())
        .await
        .unwrap();
    db.write(
        "t",
        vec![trades_batch(&[1, 2], &["A", "A"], &[1.0, 1.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    db.append(
        "t",
        vec![trades_batch(&[3, 4], &["A", "A"], &[1.0, 1.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    // Version reads.
    let (v1, _) = db
        .scan("t", ReadAt::Version(1), ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(total_rows(&v1), 2);
    let (v2, _) = db
        .scan("t", ReadAt::Version(2), ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(total_rows(&v2), 4);
    let (v0, _) = db
        .scan("t", ReadAt::Version(0), ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(total_rows(&v0), 0);

    // Nonexistent version has an actionable error.
    let err = db
        .scan("t", ReadAt::Version(99), ScanOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::VersionNotFound { .. }));
    assert!(err.hint().unwrap().contains("versions"));

    // as_of resolution.
    let versions = db.list_versions("t").await.unwrap();
    assert_eq!(versions.len(), 3);
    let v1_ts = versions[1].committed_at_ns;
    let resolved = db.resolve("t", ReadAt::AsOf(v1_ts)).await.unwrap();
    assert_eq!(resolved.manifest.sequence, 1);
    // Between v1 and v2 commits → still v1.
    let v2_ts = versions[2].committed_at_ns;
    let resolved = db.resolve("t", ReadAt::AsOf(v2_ts - 1)).await.unwrap();
    assert_eq!(resolved.manifest.sequence, 1);
    // Before the first commit → error.
    let err = db
        .resolve("t", ReadAt::AsOf(versions[0].committed_at_ns - 1_000))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::VersionNotFound { .. }));

    // Restore: history moves forward, contents rewind.
    let r = db.restore("t", 1, WriteOptions::default()).await.unwrap();
    assert_eq!(r.sequence, 3);
    let (now, _) = db
        .scan("t", ReadAt::Latest, ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(total_rows(&now), 2);
    // The overwritten version 2 is still readable.
    let (v2_again, _) = db
        .scan("t", ReadAt::Version(2), ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(total_rows(&v2_again), 4);
}

#[tokio::test]
async fn append_strictness() {
    let (_dir, db) = fresh_db().await;
    db.create_table("t", trades_schema(), default_options())
        .await
        .unwrap();
    db.write(
        "t",
        vec![trades_batch(&[100, 200], &["A", "A"], &[1.0, 1.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    // Unsorted input batch.
    let err = db
        .append(
            "t",
            vec![trades_batch(&[300, 250], &["A", "A"], &[1.0, 1.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::SortOrderViolation { .. }), "{err}");

    // Input starting before the table max.
    let err = db
        .append(
            "t",
            vec![trades_batch(&[150, 300], &["A", "A"], &[1.0, 1.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::SortOrderViolation { .. }), "{err}");

    // Wrong schema.
    let wrong = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let err = db
        .append("t", vec![wrong], WriteOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::SchemaMismatch { .. }), "{err}");

    // expected_version mismatch is a conflict before any work happens.
    let err = db
        .append(
            "t",
            vec![trades_batch(&[300], &["A"], &[1.0])],
            WriteOptions {
                expected_version: Some(0),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::VersionConflict { .. }), "{err}");
}

#[tokio::test]
async fn replace_and_delete_range_share_untouched_segments() {
    let (_dir, db) = fresh_db().await;
    db.create_table("t", trades_schema(), small_segment_options())
        .await
        .unwrap();

    // Three appends → at least three segments with disjoint time ranges.
    for base in [0i64, 1000, 2000] {
        let ts: Vec<i64> = (base..base + 500).collect();
        let syms: Vec<&str> = ts.iter().map(|_| "A").collect();
        let prices: Vec<f64> = ts.iter().map(|t| *t as f64).collect();
        db.append(
            "t",
            vec![trades_batch(&ts, &syms, &prices)],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    let before = db.resolve("t", ReadAt::Latest).await.unwrap();
    assert!(before.manifest.segments.len() >= 3);

    // Replace [1100, 1200) with corrected rows.
    let ts: Vec<i64> = (1100..1200).collect();
    let syms: Vec<&str> = ts.iter().map(|_| "A").collect();
    let prices: Vec<f64> = ts.iter().map(|_| 999.0).collect();
    let res = db
        .replace_range(
            "t",
            1100,
            1200,
            vec![trades_batch(&ts, &syms, &prices)],
            WriteOptions::default(),
        )
        .await
        .unwrap();

    let after = db.resolve("t", ReadAt::Latest).await.unwrap();
    assert_eq!(after.manifest.rows, before.manifest.rows);
    // Untouched segments are shared (same segment ids present).
    let before_ids: std::collections::BTreeSet<_> =
        before.manifest.segments.iter().map(|s| s.id).collect();
    let shared = after
        .manifest
        .segments
        .iter()
        .filter(|s| before_ids.contains(&s.id))
        .count();
    assert!(shared >= 2, "expected untouched segments to be shared");
    assert!(res.segments_added >= 1);

    // Corrected values visible; total count unchanged.
    let (batches, _) = db
        .scan(
            "t",
            ReadAt::Latest,
            ScanOptions {
                time_start: Some(1100),
                time_end: Some(1200),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(total_rows(&batches), 100);
    for b in &batches {
        let prices = b
            .column(b.schema().index_of("price").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .clone();
        assert!(prices.iter().all(|p| p == Some(999.0)));
    }

    // Replacement rows outside the range are rejected.
    let err = db
        .replace_range(
            "t",
            100,
            200,
            vec![trades_batch(&[250], &["A"], &[1.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidInput { .. }));

    // delete_range removes rows and shares the rest.
    db.delete_range("t", 0, 500, WriteOptions::default())
        .await
        .unwrap();
    let (batches, _) = db
        .scan("t", ReadAt::Latest, ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(total_rows(&batches), 1000);
}

// ---------------------------------------------------------------------------
// concurrency
// ---------------------------------------------------------------------------

#[tokio::test]
async fn racing_writers_one_wins_one_conflicts() {
    let (dir, db) = fresh_db().await;
    db.create_table("t", trades_schema(), default_options())
        .await
        .unwrap();
    db.write(
        "t",
        vec![trades_batch(&[1], &["A"], &[1.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    // Two independent handles to the same database.
    let db_a = Database::open(&dir.path().join("db")).await.unwrap();
    let db_b = Database::open(&dir.path().join("db")).await.unwrap();
    let a = tokio::spawn(async move {
        db_a.append(
            "t",
            vec![trades_batch(&[10], &["A"], &[1.0])],
            WriteOptions::default(),
        )
        .await
    });
    let b = tokio::spawn(async move {
        db_b.append(
            "t",
            vec![trades_batch(&[10], &["B"], &[1.0])],
            WriteOptions::default(),
        )
        .await
    });
    let (ra, rb) = (a.await.unwrap(), b.await.unwrap());
    let oks = [ra.is_ok(), rb.is_ok()].iter().filter(|x| **x).count();
    let conflicts = [&ra, &rb]
        .iter()
        .filter(|r| matches!(r, Err(Error::VersionConflict { .. })))
        .count();
    assert_eq!(oks, 1, "exactly one writer must win: {ra:?} {rb:?}");
    assert_eq!(conflicts, 1, "the loser must see VersionConflict");

    // The committed state is consistent.
    let (batches, _) = db
        .scan("t", ReadAt::Latest, ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(total_rows(&batches), 2);
    db.verify("t", true)
        .await
        .map(|r| assert!(r.problems.is_empty(), "{:?}", r.problems))
        .unwrap();
}

#[tokio::test]
async fn append_with_retry_rebases_over_conflicts() {
    let (dir, db) = fresh_db().await;
    db.create_table("t", trades_schema(), default_options())
        .await
        .unwrap();
    db.write(
        "t",
        vec![trades_batch(&[1], &["A"], &[1.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    let mut handles = Vec::new();
    for _ in 0..8i64 {
        let db_i = Database::open(&dir.path().join("db")).await.unwrap();
        handles.push(tokio::spawn(async move {
            // Equal timestamps: strict append allows min == current max, and
            // commit order between racing writers is arbitrary.
            db_i.append_with_retry(
                "t",
                vec![trades_batch(&[100], &["X"], &[1.0])],
                WriteOptions::default(),
                20,
            )
            .await
        }));
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }
    let (batches, _) = db
        .scan("t", ReadAt::Latest, ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(total_rows(&batches), 9);
    let resolved = db.resolve("t", ReadAt::Latest).await.unwrap();
    assert_eq!(resolved.manifest.sequence, 9);
}

// ---------------------------------------------------------------------------
// crash safety: kill the writer at every commit step
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crash_at_every_commit_step_never_corrupts() {
    for step in ["pre_publish", "post_manifest_put", "pre_head_swap"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = Database::create(&path).await.unwrap();
        db.create_table("t", trades_schema(), default_options())
            .await
            .unwrap();
        db.write(
            "t",
            vec![trades_batch(&[1, 2], &["A", "A"], &[1.0, 1.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();

        // Inject a crash at `step` of the next commit.
        let mut crashing = Database::open(&path).await.unwrap();
        let step_owned = step.to_string();
        crashing.set_commit_hook(Arc::new(move |s: &str| {
            if s == step_owned {
                Err(Error::internal(format!("injected crash at {s}")))
            } else {
                Ok(())
            }
        }));
        let err = crashing
            .append(
                "t",
                vec![trades_batch(&[3], &["A"], &[1.0])],
                WriteOptions::default(),
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("injected crash"),
            "step={step}: {err}"
        );

        // Reopen: the old head must be intact and fully readable.
        let reopened = Database::open(&path).await.unwrap();
        let (batches, _) = reopened
            .scan("t", ReadAt::Latest, ScanOptions::default())
            .await
            .unwrap();
        assert_eq!(
            total_rows(&batches),
            2,
            "step={step}: old head must survive"
        );
        let resolved = reopened.resolve("t", ReadAt::Latest).await.unwrap();
        assert_eq!(resolved.manifest.sequence, 1, "step={step}");
        let verify = reopened.verify("t", true).await.unwrap();
        assert!(
            verify.problems.is_empty(),
            "step={step}: {:?}",
            verify.problems
        );

        // Orphans from the failed commit are vacuumable, and a later commit
        // succeeds normally.
        let report = reopened.vacuum(Some("t"), 0, true).await.unwrap();
        assert_eq!(report.dry_run, false);
        reopened
            .append(
                "t",
                vec![trades_batch(&[3], &["A"], &[1.0])],
                WriteOptions::default(),
            )
            .await
            .unwrap();
        let (batches, _) = reopened
            .scan("t", ReadAt::Latest, ScanOptions::default())
            .await
            .unwrap();
        assert_eq!(total_rows(&batches), 3, "step={step}");
        let verify = reopened.verify("t", true).await.unwrap();
        assert!(
            verify.problems.is_empty(),
            "step={step}: {:?}",
            verify.problems
        );
    }
}

// ---------------------------------------------------------------------------
// compaction, dedup, vacuum, verify
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tiny_appends_then_compaction_preserves_rows_and_bounds_segments() {
    let (_dir, db) = fresh_db().await;
    db.create_table("t", trades_schema(), small_segment_options())
        .await
        .unwrap();
    // 50 tiny appends.
    for i in 0..50i64 {
        let ts: Vec<i64> = (i * 10..i * 10 + 10).collect();
        let syms: Vec<&str> = ts.iter().map(|_| "A").collect();
        let prices: Vec<f64> = ts.iter().map(|t| *t as f64).collect();
        db.append(
            "t",
            vec![trades_batch(&ts, &syms, &prices)],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    let before = db.resolve("t", ReadAt::Latest).await.unwrap();
    assert_eq!(before.manifest.rows, 500);
    assert!(before.manifest.segments.len() >= 50);

    let res = db
        .compact_with("t", Some(128 * 1024 * 1024), WriteOptions::default())
        .await
        .unwrap();
    assert!(res.segments_total < before.manifest.segments.len());
    let after = db.resolve("t", ReadAt::Latest).await.unwrap();
    assert_eq!(after.manifest.rows, 500);

    // Logical contents identical (sorted by ts on both sides).
    let (b1, _) = db
        .scan(
            "t",
            ReadAt::Version(before.manifest.sequence),
            ScanOptions::default(),
        )
        .await
        .unwrap();
    let (b2, _) = db
        .scan("t", ReadAt::Latest, ScanOptions::default())
        .await
        .unwrap();
    let sum = |bs: &[RecordBatch]| -> f64 {
        bs.iter()
            .map(|b| {
                let p = b
                    .column(b.schema().index_of("price").unwrap())
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                p.iter().flatten().sum::<f64>()
            })
            .sum()
    };
    assert_eq!(total_rows(&b1), total_rows(&b2));
    assert_eq!(sum(&b1), sum(&b2));

    // Old versions still readable after compaction + vacuum (nothing
    // referenced may be deleted).
    let vac = db.vacuum(Some("t"), 0, true).await.unwrap();
    let (b_old, _) = db
        .scan("t", ReadAt::Version(25), ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(total_rows(&b_old), 250);
    let _ = vac;
    let verify = db.verify("t", true).await.unwrap();
    assert!(verify.problems.is_empty(), "{:?}", verify.problems);
}

#[tokio::test]
async fn identical_write_dedups_segments() {
    let (_dir, db) = fresh_db().await;
    db.create_table("t", trades_schema(), default_options())
        .await
        .unwrap();
    let batch = trades_batch(&[1, 2, 3], &["A", "B", "C"], &[1.0, 2.0, 3.0]);
    db.write("t", vec![batch.clone()], WriteOptions::default())
        .await
        .unwrap();
    let r = db
        .write("t", vec![batch], WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(r.segments_deduped, 1, "identical rewrite must dedup");
    assert_eq!(r.segments_added, 0);
}

#[tokio::test]
async fn snapshots_pin_versions_and_block_drop() {
    let (_dir, db) = fresh_db().await;
    db.create_table("t", trades_schema(), default_options())
        .await
        .unwrap();
    db.write(
        "t",
        vec![trades_batch(&[1], &["A"], &[1.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    db.create_snapshot("eod", &["t".into()], None)
        .await
        .unwrap();
    db.append(
        "t",
        vec![trades_batch(&[2], &["A"], &[1.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    // Snapshot read resolves the pinned version.
    let (batches, _) = db
        .scan("t", ReadAt::Snapshot("eod".into()), ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(total_rows(&batches), 1);

    // Snapshots are immutable and unique by name.
    let err = db.create_snapshot("eod", &[], None).await.unwrap_err();
    assert!(matches!(err, Error::InvalidInput { .. }));

    // A pinned table cannot be dropped.
    let err = db.drop_table("t").await.unwrap_err();
    assert!(err.to_string().contains("pinned by snapshot"));

    db.delete_snapshot("eod").await.unwrap();
    db.drop_table("t").await.unwrap();
    assert!(db.list_tables().await.unwrap().is_empty());
}

#[tokio::test]
async fn read_only_blocks_writes() {
    let (dir, db) = fresh_db().await;
    db.create_table("t", trades_schema(), default_options())
        .await
        .unwrap();
    let ro = Database::open_read_only(&dir.path().join("db"))
        .await
        .unwrap();
    let err = ro
        .write(
            "t",
            vec![trades_batch(&[1], &["A"], &[1.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::ReadOnly { .. }));
    // Reads still work.
    ro.scan("t", ReadAt::Latest, ScanOptions::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn corruption_is_detected_and_named() {
    let (dir, db) = fresh_db().await;
    let path = dir.path().join("db");
    db.create_table("t", trades_schema(), default_options())
        .await
        .unwrap();
    db.write(
        "t",
        vec![trades_batch(&[1, 2], &["A", "A"], &[1.0, 1.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    // Corrupt the head manifest on disk.
    let resolved = db.resolve("t", ReadAt::Latest).await.unwrap();
    let manifest_rel = format!(
        "tables/{}/manifests/{:012}.json",
        resolved.entry.table_id, resolved.manifest.sequence
    );
    let manifest_path = path.join(&manifest_rel);
    let mut bytes = std::fs::read(&manifest_path).unwrap();
    let len = bytes.len();
    bytes[len / 2] ^= 0xff;
    std::fs::write(&manifest_path, bytes).unwrap();

    let err = db.resolve("t", ReadAt::Latest).await.unwrap_err();
    match err {
        Error::Corruption { object, .. } => {
            assert!(object.contains("manifests"), "object was {object}")
        }
        other => panic!("expected corruption, got {other}"),
    }
}

#[tokio::test]
async fn many_versions_stay_fast_to_resolve() {
    let (_dir, db) = fresh_db().await;
    db.create_table("t", trades_schema(), default_options())
        .await
        .unwrap();
    for i in 0..200i64 {
        db.append(
            "t",
            vec![trades_batch(&[i], &["A"], &[1.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    // Direct version read is O(1): resolve an old version quickly.
    let t0 = std::time::Instant::now();
    let resolved = db.resolve("t", ReadAt::Version(3)).await.unwrap();
    assert_eq!(resolved.manifest.sequence, 3);
    assert!(t0.elapsed().as_millis() < 200, "version read took too long");

    // as_of uses binary search: count manifest loads indirectly by timing.
    let versions = db.list_versions("t").await.unwrap();
    let mid_ts = versions[100].committed_at_ns;
    let resolved = db.resolve("t", ReadAt::AsOf(mid_ts)).await.unwrap();
    assert_eq!(resolved.manifest.sequence, 100);
}

#[tokio::test]
async fn rename_is_a_catalog_edit() {
    let (_dir, db) = fresh_db().await;
    db.create_table("a", trades_schema(), default_options())
        .await
        .unwrap();
    db.write(
        "a",
        vec![trades_batch(&[1], &["A"], &[1.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    db.rename_table("a", "b").await.unwrap();
    let (batches, _) = db
        .scan("b", ReadAt::Latest, ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(total_rows(&batches), 1);
    assert!(matches!(
        db.scan("a", ReadAt::Latest, ScanOptions::default()).await,
        Err(Error::TableNotFound { .. })
    ));
}

#[tokio::test]
async fn concurrent_readers_see_consistent_snapshots_during_writes() {
    let (dir, db) = fresh_db().await;
    db.create_table("t", trades_schema(), default_options())
        .await
        .unwrap();
    db.write(
        "t",
        vec![trades_batch(&[0], &["A"], &[0.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    let writes_done = Arc::new(AtomicUsize::new(0));
    let wd = writes_done.clone();
    let path = dir.path().join("db");
    let writer = tokio::spawn({
        let path = path.clone();
        async move {
            let db = Database::open(&path).await.unwrap();
            for i in 1..=20i64 {
                db.append(
                    "t",
                    vec![trades_batch(&[i], &["A"], &[i as f64])],
                    WriteOptions::default(),
                )
                .await
                .unwrap();
                wd.fetch_add(1, Ordering::SeqCst);
            }
        }
    });
    // Readers run concurrently; every read must see a complete prefix
    // (rows == resolved version's row count, no partial commits).
    let reader = tokio::spawn({
        let path = path.clone();
        async move {
            let db = Database::open(&path).await.unwrap();
            for _ in 0..40 {
                let resolved = db.resolve("t", ReadAt::Latest).await.unwrap();
                let (batches, _) = db
                    .scan_resolved(&resolved, ScanOptions::default())
                    .await
                    .unwrap();
                assert_eq!(
                    batches.iter().map(|b| b.num_rows() as u64).sum::<u64>(),
                    resolved.manifest.rows,
                    "read must match its resolved manifest exactly"
                );
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }
    });
    writer.await.unwrap();
    reader.await.unwrap();
    assert_eq!(writes_done.load(Ordering::SeqCst), 20);
}
