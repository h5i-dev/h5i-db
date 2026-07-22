//! On-disk format-compatibility gate.
//!
//! Opens the committed golden fixture (tests/fixtures/golden-v1, generated
//! by tests/fixtures/generate-golden.sh with an earlier build) strictly
//! read-only and asserts that current code can still read everything in it:
//! catalog, version history, time travel, snapshots, stored plans, and the
//! full checksum chain. If this test fails, the change broke the format for
//! existing databases — either fix the incompatibility or bump
//! `FORMAT_VERSION` with a migration story and a new golden-v<N> fixture.

use std::path::PathBuf;

use h5i_db_core::database::{Database, ReadAt, ScanOptions};

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/golden-v1")
}

async fn open_fixture() -> Database {
    let root = fixture_root();
    assert!(
        root.join("FORMAT").is_file(),
        "golden fixture missing at {} — run tests/fixtures/generate-golden.sh",
        root.display()
    );
    Database::open_read_only(&root)
        .await
        .expect("current code must open the golden fixture")
}

#[tokio::test]
async fn golden_catalog_and_history_are_readable() {
    let db = open_fixture().await;

    let mut names: Vec<String> = db
        .list_tables()
        .await
        .expect("list_tables")
        .into_iter()
        .map(|e| e.name)
        .collect();
    names.sort();
    assert_eq!(names, ["quotes", "trades"]);

    // trades: create, append, append, delete_range — exactly as generated.
    let versions = db.list_versions("trades").await.expect("list_versions");
    let ops: Vec<&str> = versions.iter().map(|v| v.op.as_str()).collect();
    assert_eq!(ops, ["create", "append", "append", "delete_range"]);
    assert_eq!(versions.last().unwrap().sequence, 3);
    assert_eq!(versions.last().unwrap().rows, 6);

    let quotes = db.list_versions("quotes").await.expect("list_versions");
    assert_eq!(quotes.last().unwrap().sequence, 1);
    assert_eq!(quotes.last().unwrap().rows, 3);
}

#[tokio::test]
async fn golden_data_reads_at_head_version_and_snapshot() {
    let db = open_fixture().await;

    let rows = |batches: &Vec<arrow::record_batch::RecordBatch>| -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    };

    // Head: 8 appended rows minus the 2 deleted by the range.
    let (head, _) = db
        .scan("trades", ReadAt::Latest, ScanOptions::default())
        .await
        .expect("scan head");
    assert_eq!(rows(&head), 6);

    // Time travel to the first append.
    let (v1, _) = db
        .scan("trades", ReadAt::Version(1), ScanOptions::default())
        .await
        .expect("scan v1");
    assert_eq!(rows(&v1), 4);

    // Snapshot pin resolves and reads.
    let (snap, _) = db
        .scan("trades", ReadAt::Snapshot("golden".into()), ScanOptions::default())
        .await
        .expect("scan snapshot");
    assert_eq!(rows(&snap), 6);

    let snapshots = db.list_snapshots().await.expect("list_snapshots");
    assert!(snapshots.iter().any(|s| s.name == "golden"));
}

#[tokio::test]
async fn golden_checksum_chain_verifies_deep() {
    let db = open_fixture().await;
    for table in ["trades", "quotes"] {
        let report = db.verify(table, true).await.expect("verify");
        assert!(
            report.problems.is_empty(),
            "verify({table}) found problems: {:?}",
            report.problems
        );
        assert!(report.segments_checked > 0);
    }
}

#[tokio::test]
async fn golden_stored_plan_still_parses() {
    let db = open_fixture().await;
    // The fixture contains one never-applied delete-range plan. It is long
    // expired by the time this test runs — listing must still parse and
    // checksum-verify it rather than erroring.
    let plans = db.list_plans("trades").await.expect("list_plans");
    assert_eq!(plans.len(), 1, "expected exactly one stored plan");
    assert_eq!(plans[0].table, "trades");
}
