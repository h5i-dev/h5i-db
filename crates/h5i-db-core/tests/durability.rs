//! Tests for the durability / locking / CAS / vacuum hardening:
//! segment fsync before head swap (1.1), flock-based writer lock (1.3),
//! catalog CAS (3.5), staging leases + vacuum edges (3.4), read-path
//! verification (3.6), early segment-budget enforcement (3.13), and misc
//! correctness debt (3.14).

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use h5i_db_core::{Database, Error, ReadAt, ScanOptions, TableOptions, WriteOptions};
use tempfile::TempDir;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]))
}

fn batch(ts: &[i64], px: f64) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ts.to_vec())),
            Arc::new(StringArray::from(vec!["A"; ts.len()])),
            Arc::new(Float64Array::from(vec![px; ts.len()])),
        ],
    )
    .unwrap()
}

async fn new_db(dir: &TempDir) -> Database {
    let db = Database::create(dir.path()).await.unwrap();
    db.create_table(
        "t",
        schema(),
        TableOptions {
            time_column: Some("ts".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    db
}

// ---------------------------------------------------------------------------
// 1.3 — flock-based writer lock
// ---------------------------------------------------------------------------

/// The lock file persists after a commit (flock semantics: existence carries
/// no meaning) and must NOT be treated as vacuum debris.
#[tokio::test]
async fn lock_file_survives_and_vacuum_leaves_it() {
    let dir = TempDir::new().unwrap();
    let db = new_db(&dir).await;
    db.append("t", vec![batch(&[1, 2, 3], 1.0)], WriteOptions::default())
        .await
        .unwrap();

    let entry = &db.list_tables().await.unwrap()[0];
    let lock_path = dir
        .path()
        .join(format!("tables/{}/HEAD.lock", entry.table_id));
    assert!(lock_path.exists(), "flock lock file should persist");

    let report = db.vacuum(None, 0, true).await.unwrap();
    assert!(
        report.candidates.iter().all(|c| !c.ends_with(".lock")),
        "vacuum must never collect lock files: {:?}",
        report.candidates
    );
    assert!(lock_path.exists());
}

/// Two writers racing on the same table: exactly one commit per sequence —
/// the loser gets VersionConflict (or LockTimeout), never a silent double
/// commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_appends_never_double_commit() {
    let dir = TempDir::new().unwrap();
    let db = new_db(&dir).await;
    let db2 = Database::open(dir.path()).await.unwrap();

    let a = tokio::spawn({
        let db = db.clone();
        async move {
            db.append("t", vec![batch(&[1, 2], 1.0)], WriteOptions::default())
                .await
        }
    });
    let b = tokio::spawn(async move {
        db2.append("t", vec![batch(&[1, 2], 2.0)], WriteOptions::default())
            .await
    });
    let (ra, rb) = (a.await.unwrap(), b.await.unwrap());
    let ok = [&ra, &rb].iter().filter(|r| r.is_ok()).count();
    // Both may succeed (serialized by the lock, second rebases is not
    // automatic here so it conflicts) — but sequences must be distinct.
    match (ra, rb) {
        (Ok(x), Ok(y)) => assert_ne!(x.sequence, y.sequence),
        (Ok(_), Err(e)) | (Err(e), Ok(_)) => {
            assert!(
                matches!(e, Error::VersionConflict { .. } | Error::LockTimeout { .. }),
                "loser must fail with conflict/timeout, got {e}"
            );
        }
        (Err(a), Err(b)) => panic!("both writers failed: {a} / {b}"),
    }
    assert!(ok >= 1);
}

// ---------------------------------------------------------------------------
// 3.5 — catalog CAS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_rename_drop_are_race_safe_and_precise() {
    let dir = TempDir::new().unwrap();
    let db = new_db(&dir).await;

    // Duplicate create fails cleanly.
    let err = db
        .create_table("t", schema(), TableOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::TableExists { .. }));

    // Rename to an existing name fails; original stays reachable.
    db.create_table("u", schema(), TableOptions::default())
        .await
        .unwrap();
    let err = db.rename_table("u", "t").await.unwrap_err();
    assert!(matches!(err, Error::TableExists { .. }));
    assert!(db
        .list_tables()
        .await
        .unwrap()
        .iter()
        .any(|e| e.name == "u"));

    // Rename then drop.
    db.rename_table("u", "v").await.unwrap();
    db.drop_table("v").await.unwrap();
    let names: Vec<String> = db
        .list_tables()
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(names, vec!["t".to_string()]);
}

#[tokio::test]
async fn update_policy_round_trips() {
    let dir = TempDir::new().unwrap();
    let db = new_db(&dir).await;
    let updated = db
        .update_policy(|p| {
            p.set("direct_delete", false)?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(!updated.direct_delete);
    assert!(!db.policy().await.unwrap().direct_delete);
}

// ---------------------------------------------------------------------------
// 3.4 — staging leases and vacuum edges
// ---------------------------------------------------------------------------

/// A committed write leaves no staging lease behind; vacuum right after a
/// commit (grace 0) must not find lease files or collect live segments.
#[tokio::test]
async fn staging_lease_released_after_commit() {
    let dir = TempDir::new().unwrap();
    let db = new_db(&dir).await;
    db.append("t", vec![batch(&[1, 2, 3], 1.0)], WriteOptions::default())
        .await
        .unwrap();

    let entry = &db.list_tables().await.unwrap()[0];
    let staging_dir = dir
        .path()
        .join(format!("tables/{}/staging", entry.table_id));
    let leases: Vec<_> = match std::fs::read_dir(&staging_dir) {
        Ok(rd) => rd.collect(),
        Err(_) => vec![], // dir may not exist once empty — equally fine
    };
    assert!(leases.is_empty(), "lease should be deleted after commit");

    let report = db.vacuum(None, 0, true).await.unwrap();
    assert_eq!(
        report.deleted, 0,
        "nothing to collect: {:?}",
        report.candidates
    );
    let (batches, _) = db
        .scan("t", ReadAt::Latest, ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
}

/// An orphaned table directory (no catalog entry) is collected by a full
/// vacuum once past the grace period, and never before.
#[tokio::test]
async fn orphan_table_dir_is_collected() {
    let dir = TempDir::new().unwrap();
    let db = new_db(&dir).await;

    // Fabricate an orphan dir the way a crashed create_table would: objects
    // under tables/<uuid>/ with no catalog entry.
    let orphan = dir
        .path()
        .join("tables/00000000-dead-beef-0000-000000000000");
    std::fs::create_dir_all(orphan.join("spec")).unwrap();
    std::fs::write(orphan.join("spec/00000001.json"), b"{}").unwrap();

    // Young objects survive a graceful vacuum…
    let report = db.vacuum(None, 3600, true).await.unwrap();
    assert!(report.candidates.iter().all(|c| !c.contains("dead-beef")));
    assert!(orphan.join("spec/00000001.json").exists());

    // …and are collected once grace is zero.
    let report = db.vacuum(None, 0, true).await.unwrap();
    assert!(report.candidates.iter().any(|c| c.contains("dead-beef")));
    assert!(!orphan.join("spec/00000001.json").exists());
}

// ---------------------------------------------------------------------------
// 3.6 — read-path verification
// ---------------------------------------------------------------------------

/// Version/AsOf reads verify the manifest against the child's
/// parent_checksum: tampering with a historical manifest is detected.
#[tokio::test]
async fn tampered_historical_manifest_is_detected() {
    let dir = TempDir::new().unwrap();
    let db = new_db(&dir).await;
    db.append("t", vec![batch(&[1, 2], 1.0)], WriteOptions::default())
        .await
        .unwrap();
    db.append("t", vec![batch(&[3, 4], 2.0)], WriteOptions::default())
        .await
        .unwrap();

    // Reading version 1 works before tampering.
    db.scan("t", ReadAt::Version(1), ScanOptions::default())
        .await
        .unwrap();

    // Tamper with manifest 1 (keep it valid JSON so only the checksum trips).
    let entry = &db.list_tables().await.unwrap()[0];
    let m1 = dir.path().join(format!(
        "tables/{}/manifests/{:012}.json",
        entry.table_id, 1
    ));
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&m1).unwrap()).unwrap();
    manifest["note"] = serde_json::json!("tampered");
    std::fs::write(&m1, serde_json::to_vec(&manifest).unwrap()).unwrap();

    let err = db
        .scan("t", ReadAt::Version(1), ScanOptions::default())
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::Corruption { .. }),
        "expected corruption, got {err}"
    );
}

/// verify_checksums scans detect a corrupted segment object.
#[tokio::test]
async fn verified_scan_detects_corrupt_segment() {
    let dir = TempDir::new().unwrap();
    let db = new_db(&dir).await;
    db.append("t", vec![batch(&[1, 2, 3], 1.0)], WriteOptions::default())
        .await
        .unwrap();

    // Flip one byte in the (only) segment.
    let entry = &db.list_tables().await.unwrap()[0];
    let seg_dir = dir
        .path()
        .join(format!("tables/{}/segments", entry.table_id));
    let seg = std::fs::read_dir(&seg_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let mut bytes = std::fs::read(seg.path()).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xff;
    std::fs::write(seg.path(), &bytes).unwrap();

    let err = db
        .scan(
            "t",
            ReadAt::Latest,
            ScanOptions {
                verify_checksums: true,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::Corruption { .. } | Error::Parquet(_)),
        "expected corruption/parquet error, got {err}"
    );
}

// ---------------------------------------------------------------------------
// 3.13 / 3.14 — segment budget, dedup deletion
// ---------------------------------------------------------------------------

/// Appending into a full table with compaction forbidden fails BEFORE
/// uploading anything (no new orphan segments appear).
#[tokio::test]
async fn full_table_append_fails_before_staging() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).await.unwrap();
    db.create_table(
        "t",
        schema(),
        TableOptions {
            time_column: Some("ts".into()),
            max_segments_per_manifest: Some(2),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    db.append("t", vec![batch(&[1], 1.0)], WriteOptions::default())
        .await
        .unwrap();
    db.append("t", vec![batch(&[2], 1.0)], WriteOptions::default())
        .await
        .unwrap();

    // Forbid compaction so the budget cannot be auto-recovered.
    db.update_policy(|p| {
        p.set("direct_compact", false)?;
        Ok(())
    })
    .await
    .unwrap();

    let entry = &db.list_tables().await.unwrap()[0];
    let seg_dir = dir
        .path()
        .join(format!("tables/{}/segments", entry.table_id));
    let before = std::fs::read_dir(&seg_dir).unwrap().count();

    let err = db
        .append("t", vec![batch(&[3], 1.0)], WriteOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::LimitExceeded { .. }), "got {err}");

    let after = std::fs::read_dir(&seg_dir).unwrap().count();
    assert_eq!(before, after, "failed append must not stage segments");
}

/// With compaction allowed, hitting the budget compacts opportunistically
/// and the append then succeeds.
#[tokio::test]
async fn full_table_append_auto_compacts() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).await.unwrap();
    db.create_table(
        "t",
        schema(),
        TableOptions {
            time_column: Some("ts".into()),
            max_segments_per_manifest: Some(2),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    db.append("t", vec![batch(&[1], 1.0)], WriteOptions::default())
        .await
        .unwrap();
    db.append("t", vec![batch(&[2], 1.0)], WriteOptions::default())
        .await
        .unwrap();

    let res = db
        .append("t", vec![batch(&[3], 1.0)], WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(res.op, "append");
    let (batches, _) = db
        .scan("t", ReadAt::Latest, ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
}

/// Content-hash dedup removes the redundant duplicate object it replaced.
#[tokio::test]
async fn dedup_deletes_redundant_upload() {
    let dir = TempDir::new().unwrap();
    let db = new_db(&dir).await;
    let data = vec![batch(&[1, 2, 3], 1.0)];
    db.write("t", data.clone(), WriteOptions::default())
        .await
        .unwrap();
    // Identical rewrite: the new segment dedups against the parent.
    let res = db.write("t", data, WriteOptions::default()).await.unwrap();
    assert_eq!(res.segments_deduped, 1);

    let entry = &db.list_tables().await.unwrap()[0];
    let seg_dir = dir
        .path()
        .join(format!("tables/{}/segments", entry.table_id));
    assert_eq!(
        std::fs::read_dir(&seg_dir).unwrap().count(),
        1,
        "redundant duplicate object should have been deleted eagerly"
    );
}

// ---------------------------------------------------------------------------
// 2.4 — streaming scan parity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn scan_stream_matches_scan() {
    use futures::TryStreamExt;
    let dir = TempDir::new().unwrap();
    let db = new_db(&dir).await;
    db.append(
        "t",
        vec![batch(&[1, 2, 3, 4, 5], 1.0)],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    let opts = ScanOptions {
        time_start: Some(2),
        time_end: Some(5),
        limit: Some(2),
        ..Default::default()
    };
    let (collected, report) = db.scan("t", ReadAt::Latest, opts.clone()).await.unwrap();
    let (stream, _) = db.scan_stream("t", ReadAt::Latest, opts).await.unwrap();
    let streamed: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let rows = |bs: &[RecordBatch]| bs.iter().map(|b| b.num_rows()).sum::<usize>();
    assert_eq!(rows(&collected), 2);
    assert_eq!(rows(&streamed), rows(&collected));
    assert_eq!(report.rows_returned, 2);
}
