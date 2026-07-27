//! Fork exit-gate tests (ROADMAP Part IX): isolation between parallel agents,
//! zero-copy sharing of the base, GC safety across the fork boundary, and
//! first-commit-wins promotion.
//!
//! The assertions to read first are the GC ones. A fork's shadow tables
//! reference the base table's segments by path, which is the only place in the
//! system where one table's manifest points into another's storage. Everything
//! else here is convenience; those tests are the ones standing between this
//! feature and silent data loss.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use h5i_db_core::{
    Database, Error, ForkTableKind, ReadAt, RetentionCut, ScanOptions, StorageOptions,
    TableOptions, WriteOptions,
};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

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
            target_segment_bytes: 4 * 1024,
            target_row_group_bytes: 1024,
            ..Default::default()
        },
        max_segments_per_manifest: None,
    }
}

/// A database with `trades` holding three rows at v1.
async fn db_with_trades() -> (tempfile::TempDir, Database, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("db");
    let db = Database::create(&root).await.unwrap();
    db.create_table("trades", trades_schema(), default_options())
        .await
        .unwrap();
    db.write(
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
    (dir, db, root)
}

fn walk(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            walk(&p, out);
        } else {
            out.push(p);
        }
    }
}

/// Every Parquet segment on disk, as (path, bytes).
fn parquet_files(root: &Path) -> Vec<(std::path::PathBuf, u64)> {
    let mut all = Vec::new();
    walk(root, &mut all);
    all.into_iter()
        .filter(|p| p.extension().map(|e| e == "parquet").unwrap_or(false))
        .map(|p| {
            let len = std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
            (p, len)
        })
        .collect()
}

fn parquet_bytes(root: &Path) -> u64 {
    parquet_files(root).iter().map(|(_, b)| b).sum()
}

/// Distinct inodes backing the Parquet files, so a hardlink is not counted as
/// a second physical copy.
#[cfg(unix)]
fn parquet_inodes(root: &Path) -> BTreeSet<u64> {
    use std::os::unix::fs::MetadataExt;
    parquet_files(root)
        .iter()
        .filter_map(|(p, _)| std::fs::metadata(p).ok().map(|m| m.ino()))
        .collect()
}

async fn rows(db: &Database, table: &str) -> usize {
    let (batches, _) = db
        .scan(table, ReadAt::Latest, ScanOptions::default())
        .await
        .unwrap();
    batches.iter().map(|b| b.num_rows()).sum()
}

async fn prices(db: &Database, table: &str) -> Vec<f64> {
    let (batches, _) = db
        .scan(table, ReadAt::Latest, ScanOptions::default())
        .await
        .unwrap();
    let mut out = Vec::new();
    for b in batches {
        let col = b
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        out.extend(col.iter().flatten());
    }
    out.sort_by(|a, b| a.partial_cmp(b).unwrap());
    out
}

// ---------------------------------------------------------------------------
// creation is O(1) and copies nothing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn creating_a_fork_copies_no_data() {
    let (_dir, db, root) = db_with_trades().await;
    let before = parquet_files(&root);
    let fork = db
        .create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let after = parquet_files(&root);
    assert_eq!(
        before.len(),
        after.len(),
        "fork creation must not write a single segment"
    );
    assert_eq!(
        parquet_bytes(&root),
        before.iter().map(|(_, b)| b).sum::<u64>()
    );
    assert_eq!(fork.pins.len(), 1, "the one base table must be pinned");
    assert_eq!(fork.pins.values().next().unwrap().sequence, 1);
}

#[tokio::test]
async fn writing_in_a_fork_copies_no_base_parquet() {
    let (_dir, db, root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let base_bytes = parquet_bytes(&root);

    let fork_db = db.open_fork("agent-01").await.unwrap();
    fork_db
        .append(
            "trades",
            vec![trades_batch(&[400], &["C"], &[4.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();

    // The shadow's manifest references the base's segments; only the newly
    // appended rows are new bytes. A `cp -r` implementation would have doubled
    // the base here, which is the entire point of the design.
    let grown = parquet_bytes(&root) - base_bytes;
    assert!(
        grown < base_bytes,
        "fork write added {grown} bytes over a {base_bytes}-byte base; the base was copied"
    );
    assert_eq!(rows(&fork_db, "trades").await, 4);
    assert_eq!(rows(&db, "trades").await, 3, "base must be untouched");
}

#[tokio::test]
async fn twenty_forks_share_one_base() {
    // The headline claim: N agents over one dataset cost one dataset.
    let (_dir, db, root) = db_with_trades().await;
    let base_bytes = parquet_bytes(&root);
    for i in 0..20 {
        db.create_fork(&format!("agent-{i:02}"), None, None, Default::default())
            .await
            .unwrap();
    }
    assert_eq!(
        parquet_bytes(&root),
        base_bytes,
        "20 forks must not add a byte of Parquet"
    );
    assert_eq!(db.list_forks().await.unwrap().len(), 20);
}

// ---------------------------------------------------------------------------
// isolation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn forks_writing_the_same_table_name_do_not_collide() {
    let (_dir, db, _root) = db_with_trades().await;
    for i in 0..3 {
        db.create_fork(&format!("agent-{i}"), None, None, Default::default())
            .await
            .unwrap();
    }
    // Each agent appends a different row to "the same" table.
    for i in 0..3 {
        let f = db.open_fork(&format!("agent-{i}")).await.unwrap();
        f.append(
            "trades",
            vec![trades_batch(&[400 + i as i64], &["C"], &[10.0 + i as f64])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    for i in 0..3 {
        let f = db.open_fork(&format!("agent-{i}")).await.unwrap();
        let p = prices(&f, "trades").await;
        assert_eq!(p.len(), 4, "fork {i} must see exactly its own extra row");
        assert_eq!(*p.last().unwrap(), 10.0 + i as f64);
    }
    assert_eq!(rows(&db, "trades").await, 3, "base untouched by all three");
}

#[tokio::test]
async fn a_fork_never_sees_the_future_of_its_base() {
    let (_dir, db, _root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();

    // Main races ahead.
    db.append(
        "trades",
        vec![trades_batch(&[900, 950], &["Z", "Z"], &[9.0, 9.5])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    assert_eq!(rows(&db, "trades").await, 5);
    assert_eq!(
        rows(&fork_db, "trades").await,
        3,
        "a fork read resolves through its pin, not through HEAD"
    );
    // …and the version list stops at the pin rather than leaking main's commits.
    let versions = fork_db.list_versions("trades").await.unwrap();
    assert_eq!(versions.last().unwrap().sequence, 1);
    assert_eq!(
        db.list_versions("trades")
            .await
            .unwrap()
            .last()
            .unwrap()
            .sequence,
        2
    );
}

#[tokio::test]
async fn a_fork_can_read_its_pinned_tables_history_but_not_past_it() {
    let (_dir, db, _root) = db_with_trades().await;
    db.append(
        "trades",
        vec![trades_batch(&[400], &["C"], &[4.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    // Pin at v2, then let main move to v3.
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    db.append(
        "trades",
        vec![trades_batch(&[500], &["D"], &[5.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();

    // Looking back is fine: history below the pin is still history.
    let (b, _) = fork_db
        .scan("trades", ReadAt::Version(1), ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(b.iter().map(|x| x.num_rows()).sum::<usize>(), 3);
    // Looking forward is not.
    let err = fork_db
        .scan("trades", ReadAt::Version(3), ScanOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::VersionNotFound { .. }), "got {err:?}");
}

#[tokio::test]
async fn a_fork_does_not_see_tables_created_on_main_after_it() {
    let (_dir, db, _root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    db.create_table("quotes", trades_schema(), default_options())
        .await
        .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();

    assert!(fork_db.resolve("quotes", ReadAt::Latest).await.is_err());
    let names: Vec<String> = fork_db
        .list_tables()
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(names, vec!["trades".to_string()]);
}

#[tokio::test]
async fn tables_created_inside_a_fork_are_invisible_to_main() {
    let (_dir, db, _root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();
    fork_db
        .create_table("features", trades_schema(), default_options())
        .await
        .unwrap();
    fork_db
        .write(
            "features",
            vec![trades_batch(&[1], &["F"], &[42.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();

    assert_eq!(rows(&fork_db, "features").await, 1);
    assert!(db.resolve("features", ReadAt::Latest).await.is_err());
    let base_names: Vec<String> = db
        .list_tables()
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(base_names, vec!["trades".to_string()]);
}

#[tokio::test]
async fn two_forks_may_each_hold_a_table_of_the_same_new_name() {
    let (_dir, db, _root) = db_with_trades().await;
    for n in ["a", "b"] {
        db.create_fork(n, None, None, Default::default())
            .await
            .unwrap();
        let f = db.open_fork(n).await.unwrap();
        f.create_table("features", trades_schema(), default_options())
            .await
            .unwrap();
        f.write(
            "features",
            vec![trades_batch(
                &[1],
                &["F"],
                &[if n == "a" { 1.0 } else { 2.0 }],
            )],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    assert_eq!(
        prices(&db.open_fork("a").await.unwrap(), "features").await,
        vec![1.0]
    );
    assert_eq!(
        prices(&db.open_fork("b").await.unwrap(), "features").await,
        vec![2.0]
    );
}

// ---------------------------------------------------------------------------
// GC safety — the tests that matter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn vacuum_never_collects_segments_a_fork_still_reads() {
    let (_dir, db, _root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();
    let before = prices(&fork_db, "trades").await;

    // Main rewrites the table completely, so the fork's pinned segments are no
    // longer referenced by main's head at all.
    db.write(
        "trades",
        vec![trades_batch(&[900], &["Z"], &[9.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    db.compact("trades", WriteOptions::default()).await.unwrap();
    let report = db.vacuum(None, 0, true).await.unwrap();

    // The fork must still read byte-identical data after a full vacuum.
    let after = prices(&fork_db, "trades").await;
    assert_eq!(
        before, after,
        "vacuum deleted segments a fork was reading (deleted {} objects)",
        report.deleted
    );
    let v = fork_db.verify("trades", true).await.unwrap();
    assert!(
        v.problems.is_empty(),
        "verify after vacuum: {:?}",
        v.problems
    );
}

#[tokio::test]
async fn vacuum_never_collects_a_forks_own_tables() {
    // Fork-owned tables are deliberately absent from the global catalog, which
    // is exactly what makes them look like orphaned directories to the sweep
    // that reclaims crashed `create_table`s.
    let (_dir, db, _root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();
    fork_db
        .create_table("features", trades_schema(), default_options())
        .await
        .unwrap();
    fork_db
        .write(
            "features",
            vec![trades_batch(&[1, 2], &["F", "G"], &[1.0, 2.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    fork_db
        .append(
            "trades",
            vec![trades_batch(&[400], &["C"], &[4.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();

    db.vacuum(None, 0, true).await.unwrap();

    assert_eq!(
        rows(&fork_db, "features").await,
        2,
        "fork-created table was vacuumed"
    );
    assert_eq!(
        rows(&fork_db, "trades").await,
        4,
        "fork shadow was vacuumed"
    );
    for t in ["features", "trades"] {
        let v = fork_db.verify(t, true).await.unwrap();
        assert!(v.problems.is_empty(), "{t}: {:?}", v.problems);
    }
}

#[tokio::test]
async fn retention_refuses_to_expire_a_version_a_fork_pins() {
    let (_dir, db, _root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    for i in 0..3 {
        db.append(
            "trades",
            vec![trades_batch(&[900 + i], &["Z"], &[9.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    let err = db
        .set_retention("trades", RetentionCut::KeepLast(1), None)
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("agent-01"), "{msg}");
    assert!(msg.contains("drop the fork first"), "{msg}");

    // Dropping the fork releases the pin and the same cut then succeeds.
    db.drop_fork("agent-01").await.unwrap();
    db.set_retention("trades", RetentionCut::KeepLast(1), None)
        .await
        .unwrap();
}

#[tokio::test]
async fn dropping_a_base_table_is_refused_while_a_fork_pins_it() {
    let (_dir, db, _root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let err = db.drop_table("trades").await.unwrap_err();
    assert!(format!("{err}").contains("pinned by fork"), "{err}");
    db.drop_fork("agent-01").await.unwrap();
    db.drop_table("trades").await.unwrap();
}

#[tokio::test]
async fn a_fork_pinning_an_old_version_holds_the_floor_there() {
    let (_dir, db, _root) = db_with_trades().await;
    for i in 0..4 {
        db.append(
            "trades",
            vec![trades_batch(&[400 + i], &["C"], &[4.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    // Fork at v5 (head), then let main advance further.
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    db.append(
        "trades",
        vec![trades_batch(&[800], &["Y"], &[8.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    // Keeping the last 1 version would expire the pinned v5.
    assert!(
        db.set_retention("trades", RetentionCut::KeepLast(1), None)
            .await
            .is_err()
    );
    // Keeping enough to cover the pin is allowed.
    db.set_retention("trades", RetentionCut::KeepLast(2), None)
        .await
        .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();
    assert_eq!(rows(&fork_db, "trades").await, 7);
}

// ---------------------------------------------------------------------------
// format fence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_first_fork_raises_the_min_reader_version_and_the_fence_is_sticky() {
    let (_dir, db, root) = db_with_trades().await;
    let read_fence = || -> u32 {
        let bytes = std::fs::read(root.join("FORMAT")).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        v["min_reader_version"].as_u64().unwrap() as u32
    };
    assert_eq!(read_fence(), 1, "a fork-free database stays readable by v1");

    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    assert_eq!(
        read_fence(),
        2,
        "a reader blind to forks must be locked out before one exists"
    );

    // Dropping every fork does not lower it: the hazard is the old reader's
    // blindness to `forks/`, not the presence of forks right now.
    db.drop_fork("agent-01").await.unwrap();
    assert_eq!(read_fence(), 2);
    // This binary understands v2, so it keeps working.
    Database::open(&root).await.unwrap();
}

#[tokio::test]
async fn a_database_demanding_a_newer_reader_is_refused() {
    let (_dir, _db, root) = db_with_trades().await;
    let path = root.join("FORMAT");
    let mut v: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    v["min_reader_version"] = serde_json::json!(99);
    std::fs::write(&path, serde_json::to_vec_pretty(&v).unwrap()).unwrap();

    let err = Database::open(&root).await.unwrap_err();
    assert!(
        matches!(err, Error::FormatTooNew { found: 99, .. }),
        "{err:?}"
    );
}

// ---------------------------------------------------------------------------
// compaction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn compacting_in_a_fork_never_rewrites_inherited_segments() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("db");
    let db = Database::create(&root).await.unwrap();
    db.create_table("trades", trades_schema(), small_segment_options())
        .await
        .unwrap();
    // Many small segments in the base.
    for i in 0..8 {
        db.append(
            "trades",
            vec![trades_batch(&[100 + i], &["A"], &[1.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let base_bytes = parquet_bytes(&root);

    let fork_db = db.open_fork("agent-01").await.unwrap();
    for i in 0..8 {
        fork_db
            .append(
                "trades",
                vec![trades_batch(&[500 + i], &["B"], &[2.0])],
                WriteOptions::default(),
            )
            .await
            .unwrap();
    }
    let before_compact = parquet_bytes(&root);
    let rows_before = rows(&fork_db, "trades").await;
    fork_db
        .compact("trades", WriteOptions::default())
        .await
        .unwrap();

    // Compaction rewrote only what the fork wrote. If inherited segments had
    // been merged, the fork would have duplicated the base's bytes into its
    // own directory and this would exceed the base's whole footprint.
    let added_by_compaction = parquet_bytes(&root) - before_compact;
    assert!(
        added_by_compaction < base_bytes,
        "compaction added {added_by_compaction} bytes; inherited segments were rewritten"
    );
    assert_eq!(rows(&fork_db, "trades").await, rows_before);
    let v = fork_db.verify("trades", true).await.unwrap();
    assert!(v.problems.is_empty(), "{:?}", v.problems);
}

#[tokio::test]
async fn compacting_main_leaves_forks_reading_the_old_layout() {
    let (_dir, db, _root) = db_with_trades().await;
    for i in 0..4 {
        db.append(
            "trades",
            vec![trades_batch(&[400 + i], &["C"], &[4.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();
    let before = prices(&fork_db, "trades").await;

    db.compact("trades", WriteOptions::default()).await.unwrap();

    assert_eq!(prices(&fork_db, "trades").await, before);
    // …and the fork can still fork-write on top of the pre-compaction layout.
    fork_db
        .append(
            "trades",
            vec![trades_batch(&[999], &["Z"], &[9.9])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(rows(&fork_db, "trades").await, before.len() + 1);
}

// ---------------------------------------------------------------------------
// diff
// ---------------------------------------------------------------------------

#[tokio::test]
async fn diff_reports_what_the_fork_added_and_what_it_shares() {
    let (_dir, db, _root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();
    fork_db
        .append(
            "trades",
            vec![trades_batch(&[400], &["C"], &[4.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    fork_db
        .create_table("features", trades_schema(), default_options())
        .await
        .unwrap();

    let diff = db.fork_diff("agent-01", None).await.unwrap();
    assert_eq!(diff.tables.len(), 2);

    let shadow = diff.tables.iter().find(|t| t.table == "trades").unwrap();
    assert_eq!(shadow.kind, ForkTableKind::Shadowed);
    assert_eq!(shadow.base_sequence, Some(1));
    assert_eq!(shadow.rows_base, 3);
    assert_eq!(shadow.rows_fork, 4);
    assert_eq!(shadow.segments_added, 1, "one appended segment");
    assert_eq!(
        shadow.segments_shared, 1,
        "the base segment, shared not copied"
    );
    assert_eq!(shadow.segments_removed, 0);
    assert!(!shadow.base_moved);

    let created = diff.tables.iter().find(|t| t.table == "features").unwrap();
    assert_eq!(created.kind, ForkTableKind::Created);
    assert_eq!(created.rows_base, 0);
    assert_eq!(created.base_sequence, None);
}

#[tokio::test]
async fn diff_flags_a_base_that_moved_and_names_compaction_when_that_is_all_it_was() {
    let (_dir, db, _root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();
    fork_db
        .append(
            "trades",
            vec![trades_batch(&[400], &["C"], &[4.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();

    let d = db.fork_diff("agent-01", Some("trades")).await.unwrap();
    assert!(!d.tables[0].base_moved);

    // A real content change on main.
    db.append(
        "trades",
        vec![trades_batch(&[700], &["Y"], &[7.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    let d = db.fork_diff("agent-01", Some("trades")).await.unwrap();
    assert!(d.tables[0].base_moved);
    assert!(!d.tables[0].base_moved_by_compaction_only);
}

#[tokio::test]
async fn diff_of_an_untouched_table_is_empty_rather_than_an_error() {
    let (_dir, db, _root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let d = db.fork_diff("agent-01", Some("trades")).await.unwrap();
    assert!(
        d.tables.is_empty(),
        "a fork that wrote nothing changed nothing"
    );
}

// ---------------------------------------------------------------------------
// promote
// ---------------------------------------------------------------------------

#[tokio::test]
async fn promote_lands_the_forks_rows_on_main() {
    let (_dir, db, _root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();
    fork_db
        .append(
            "trades",
            vec![trades_batch(&[400], &["C"], &[4.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();

    let result = db.promote("agent-01", "trades").await.unwrap();
    assert_eq!(result.kind, ForkTableKind::Shadowed);
    assert_eq!(result.base_sequence, 2);
    assert_eq!(result.rows, 4);
    assert_eq!(result.segments_linked, 1);

    assert_eq!(rows(&db, "trades").await, 4);
    assert_eq!(prices(&db, "trades").await, vec![1.0, 2.0, 3.0, 4.0]);
    let v = db.verify("trades", true).await.unwrap();
    assert!(v.problems.is_empty(), "{:?}", v.problems);
}

#[cfg(unix)]
#[tokio::test]
async fn promote_links_rather_than_copying_on_one_filesystem() {
    let (_dir, db, root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();
    fork_db
        .append(
            "trades",
            vec![trades_batch(&[400], &["C"], &[4.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    let inodes_before = parquet_inodes(&root);

    let result = db.promote("agent-01", "trades").await.unwrap();

    assert_eq!(
        result.bytes_copied, 0,
        "promote copied bytes instead of linking"
    );
    assert_eq!(
        parquet_inodes(&root),
        inodes_before,
        "promote created a second physical copy of a segment"
    );
}

#[tokio::test]
async fn promote_is_first_commit_wins() {
    let (_dir, db, _root) = db_with_trades().await;
    for n in ["agent-a", "agent-b"] {
        db.create_fork(n, None, None, Default::default())
            .await
            .unwrap();
        let f = db.open_fork(n).await.unwrap();
        f.append(
            "trades",
            vec![trades_batch(
                &[400],
                &["C"],
                &[if n == "agent-a" { 4.0 } else { 5.0 }],
            )],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }

    db.promote("agent-a", "trades").await.unwrap();
    let err = db.promote("agent-b", "trades").await.unwrap_err();
    match &err {
        Error::PromoteConflict { base, actual, .. } => {
            assert_eq!((*base, *actual), (1, 2));
        }
        other => panic!("expected PromoteConflict, got {other:?}"),
    }
    // The loser wrote nothing: main holds exactly the winner's result.
    assert_eq!(prices(&db, "trades").await, vec![1.0, 2.0, 3.0, 4.0]);
    // …and it is not advertised as retryable, because retrying cannot help.
    assert!(!err.retryable());
    assert!(!err.next_actions().is_empty());
}

/// Part IX detected this case and refused it, telling the caller to re-fork.
/// Part X (X-B3) replays the fork's work onto the new layout instead, because
/// compaction changes where rows live and not which rows there are. The diff
/// still flags the movement — it is now a "this will rebase" signal rather
/// than a "this will fail" one.
#[tokio::test]
async fn a_promote_blocked_only_by_compaction_is_rebased_not_refused() {
    let (_dir, db, _root) = db_with_trades().await;
    for i in 0..6 {
        db.append(
            "trades",
            vec![trades_batch(&[400 + i], &["C"], &[4.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();
    fork_db
        .append(
            "trades",
            vec![trades_batch(&[999], &["Z"], &[9.9])],
            WriteOptions::default(),
        )
        .await
        .unwrap();

    // Main only reorganises storage; its contents do not change.
    let compacted = db
        .compact_with("trades", Some(64 * 1024 * 1024), WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(compacted.op, "compact");

    // The diff sees the movement and classifies it as layout-only.
    let d = db.fork_diff("agent-01", Some("trades")).await.unwrap();
    assert!(d.tables[0].base_moved);
    assert!(d.tables[0].base_moved_by_compaction_only);

    // And the promote goes through, rebased onto the compacted layout.
    let result = db.promote("agent-01", "trades").await.unwrap();
    assert!(
        result.rebased_from.is_some(),
        "a compaction-only conflict should be rebased, not lost"
    );
    // Main holds its own rows plus the fork's one addition.
    let after = prices(&db, "trades").await;
    assert_eq!(after.len(), 10, "3 seeded + 6 appended + 1 from the fork");
    assert!(after.contains(&9.9), "the fork's row should have landed");
    assert!(
        db.verify("trades", true).await.unwrap().problems.is_empty(),
        "a rebased promote must leave the base structurally sound"
    );
}

#[tokio::test]
async fn promoting_a_fork_created_table_is_a_catalog_move() {
    let (_dir, db, root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();
    fork_db
        .create_table("features", trades_schema(), default_options())
        .await
        .unwrap();
    fork_db
        .write(
            "features",
            vec![trades_batch(&[1, 2], &["F", "G"], &[1.0, 2.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    let bytes_before = parquet_bytes(&root);

    let result = db.promote("agent-01", "features").await.unwrap();
    assert_eq!(result.kind, ForkTableKind::Created);
    assert_eq!(result.segments_linked, 0);
    assert_eq!(result.bytes_copied, 0);
    assert_eq!(
        parquet_bytes(&root),
        bytes_before,
        "a catalog move moved data"
    );

    assert_eq!(rows(&db, "features").await, 2);
    // It left the fork, so the fork no longer offers it.
    let fork_db = db.open_fork("agent-01").await.unwrap();
    assert!(fork_db.resolve("features", ReadAt::Latest).await.is_err());
    // Dropping the fork must not take the promoted table with it.
    db.drop_fork("agent-01").await.unwrap();
    assert_eq!(rows(&db, "features").await, 2);
}

#[tokio::test]
async fn promoting_onto_an_occupied_name_is_refused() {
    let (_dir, db, _root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();
    fork_db
        .create_table("features", trades_schema(), default_options())
        .await
        .unwrap();
    // Main creates the same name in the meantime.
    db.create_table("features", trades_schema(), default_options())
        .await
        .unwrap();

    let err = db.promote("agent-01", "features").await.unwrap_err();
    assert!(matches!(err, Error::TableExists { .. }), "{err:?}");
}

#[tokio::test]
async fn promoting_a_table_the_fork_never_touched_is_refused() {
    let (_dir, db, _root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let err = db.promote("agent-01", "trades").await.unwrap_err();
    assert!(format!("{err}").contains("does not own"), "{err}");
}

#[tokio::test]
async fn main_holds_no_reference_into_fork_storage_after_promote_and_drop() {
    // The fsck-grade assertion: once the fork is gone, nothing main reads may
    // live under a directory that went with it.
    let (_dir, db, _root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();
    fork_db
        .append(
            "trades",
            vec![trades_batch(&[400], &["C"], &[4.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    db.promote("agent-01", "trades").await.unwrap();
    db.drop_fork("agent-01").await.unwrap();

    let resolved = db.resolve("trades", ReadAt::Latest).await.unwrap();
    let base_prefix = format!("tables/{}/", resolved.entry.table_id);
    for seg in &resolved.manifest.segments {
        assert!(
            seg.path.starts_with(&base_prefix),
            "main still references {} outside its own table directory",
            seg.path
        );
    }
    assert_eq!(rows(&db, "trades").await, 4);
    let v = db.verify("trades", true).await.unwrap();
    assert!(v.problems.is_empty(), "{:?}", v.problems);
    db.vacuum(None, 0, true).await.unwrap();
    assert_eq!(
        rows(&db, "trades").await,
        4,
        "vacuum broke main after promote"
    );
}

// ---------------------------------------------------------------------------
// drop
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dropping_a_fork_reclaims_its_storage_and_releases_the_pin() {
    let (_dir, db, root) = db_with_trades().await;
    let base_bytes = parquet_bytes(&root);
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();
    fork_db
        .append(
            "trades",
            vec![trades_batch(&[400], &["C"], &[4.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    fork_db
        .create_table("features", trades_schema(), default_options())
        .await
        .unwrap();
    assert!(parquet_bytes(&root) > base_bytes);

    let dropped = db.drop_fork("agent-01").await.unwrap();
    assert_eq!(dropped, 2, "shadow + created table");
    assert_eq!(
        parquet_bytes(&root),
        base_bytes,
        "the fork's own segments were not reclaimed"
    );
    assert!(db.fork_info("agent-01").await.is_err());
    assert_eq!(rows(&db, "trades").await, 3, "the base is intact");
}

#[tokio::test]
async fn dropping_a_shadow_inside_a_fork_reverts_to_the_base_view() {
    let (_dir, db, _root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();
    fork_db
        .append(
            "trades",
            vec![trades_batch(&[400], &["C"], &[4.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(rows(&fork_db, "trades").await, 4);

    // "Undo my edits to this table" — the name falls back to the pinned base.
    fork_db.drop_table("trades").await.unwrap();
    assert_eq!(rows(&fork_db, "trades").await, 3);
    assert_eq!(rows(&db, "trades").await, 3);
}

#[tokio::test]
async fn a_fork_cannot_drop_a_base_table() {
    let (_dir, db, _root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();
    let err = fork_db.drop_table("trades").await.unwrap_err();
    assert!(matches!(err, Error::Unsupported { .. }), "{err:?}");
    assert_eq!(rows(&db, "trades").await, 3);
}

// ---------------------------------------------------------------------------
// as-of forks: a writable past
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_as_of_fork_is_a_writable_past() {
    let (_dir, db, _root) = db_with_trades().await;
    let v1 = db.resolve("trades", ReadAt::Latest).await.unwrap();
    let cutoff = v1.manifest.committed_at_ns;
    // Main moves on after the cutoff.
    db.append(
        "trades",
        vec![trades_batch(&[400, 500], &["C", "D"], &[4.0, 5.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(rows(&db, "trades").await, 5);

    db.create_fork("backtest", None, Some(cutoff), Default::default())
        .await
        .unwrap();
    let fork_db = db.open_fork("backtest").await.unwrap();
    assert_eq!(
        rows(&fork_db, "trades").await,
        3,
        "the fork must see the world as of the cutoff"
    );

    // …and it is writable, which is the part a read-only pin cannot do.
    fork_db
        .create_table("signals", trades_schema(), default_options())
        .await
        .unwrap();
    fork_db
        .write(
            "signals",
            vec![trades_batch(&[1], &["S"], &[0.5])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(rows(&fork_db, "signals").await, 1);
    assert_eq!(rows(&fork_db, "trades").await, 3, "still no look-ahead");
}

#[tokio::test]
async fn an_as_of_fork_skips_tables_that_did_not_exist_yet() {
    let (_dir, db, _root) = db_with_trades().await;
    let cutoff = db
        .resolve("trades", ReadAt::Latest)
        .await
        .unwrap()
        .manifest
        .committed_at_ns;
    db.create_table("quotes", trades_schema(), default_options())
        .await
        .unwrap();

    let fork = db
        .create_fork("backtest", None, Some(cutoff), Default::default())
        .await
        .unwrap();
    assert_eq!(fork.pins.len(), 1);
    let fork_db = db.open_fork("backtest").await.unwrap();
    assert!(fork_db.resolve("quotes", ReadAt::Latest).await.is_err());
}

#[tokio::test]
async fn an_as_of_before_all_history_is_refused_rather_than_silently_empty() {
    // Left to itself this produces a fork that pins nothing, and the mistake
    // only surfaces much later as a bare "table not found" from the query
    // engine. Fail where the timestamp was typed.
    let (_dir, db, _root) = db_with_trades().await;
    let err = db
        .create_fork("backtest", None, Some(1), Default::default())
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("--as-of pins no version"), "{msg}");
    assert!(
        db.fork_info("backtest").await.is_err(),
        "no fork was left behind"
    );
}

#[tokio::test]
async fn an_as_of_fork_of_an_empty_database_is_allowed() {
    // Empty because the database is empty is a different thing from empty
    // because the timestamp was wrong, and only the latter is a mistake.
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("db")).await.unwrap();
    let fork = db
        .create_fork("scratch", None, Some(1), Default::default())
        .await
        .unwrap();
    assert!(fork.pins.is_empty());
    let fork_db = db.open_fork("scratch").await.unwrap();
    fork_db
        .create_table("features", trades_schema(), default_options())
        .await
        .unwrap();
    assert_eq!(rows(&fork_db, "features").await, 0);
}

// ---------------------------------------------------------------------------
// guards
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fork_names_are_unique() {
    let (_dir, db, _root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let err = db
        .create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::ForkExists { .. }), "{err:?}");
}

#[tokio::test]
async fn a_missing_fork_suggests_the_closest_name() {
    let (_dir, db, _root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let err = db.fork_info("agent-02").await.unwrap_err();
    match &err {
        Error::ForkNotFound { did_you_mean, .. } => {
            assert_eq!(did_you_mean.as_deref(), Some("agent-01"));
        }
        other => panic!("expected ForkNotFound, got {other:?}"),
    }
    assert_eq!(err.code(), "fork_not_found");
}

#[tokio::test]
async fn database_wide_operations_are_refused_inside_a_fork() {
    let (_dir, db, _root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();

    // No reaching for the global roots from inside a fork. (Forking *is* now
    // allowed from here — see the nested-fork tests — because a child pins its
    // parent's tables rather than touching anything database-wide.)
    for err in [
        fork_db
            .create_snapshot("snap", &[], None)
            .await
            .unwrap_err(),
        fork_db.vacuum(None, 0, true).await.unwrap_err(),
        fork_db
            .set_retention("trades", RetentionCut::KeepLast(1), None)
            .await
            .unwrap_err(),
        fork_db.drop_fork("agent-01").await.unwrap_err(),
        fork_db.promote("agent-01", "trades").await.unwrap_err(),
    ] {
        let msg = format!("{err}");
        assert!(
            msg.contains("agent-01") && msg.contains("base database"),
            "expected a base-only refusal, got: {msg}"
        );
    }
}

#[tokio::test]
async fn a_read_only_handle_stays_read_only_inside_a_fork() {
    let (_dir, db, root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let ro = Database::open_read_only(&root).await.unwrap();
    let ro_fork = ro.open_fork("agent-01").await.unwrap();

    assert_eq!(rows(&ro_fork, "trades").await, 3);
    let err = ro_fork
        .append(
            "trades",
            vec![trades_batch(&[400], &["C"], &[4.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::ReadOnly { .. }), "{err:?}");
}

#[tokio::test]
async fn a_shadow_is_materialized_once_no_matter_how_many_writes() {
    let (_dir, db, _root) = db_with_trades().await;
    db.create_fork("agent-01", None, None, Default::default())
        .await
        .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();
    for i in 0..5 {
        fork_db
            .append(
                "trades",
                vec![trades_batch(&[400 + i], &["C"], &[4.0])],
                WriteOptions::default(),
            )
            .await
            .unwrap();
    }
    // Sequence 0 is the shadow's copy of the base; five appends follow it.
    let resolved = fork_db.resolve("trades", ReadAt::Latest).await.unwrap();
    assert_eq!(resolved.manifest.sequence, 5);
    assert_eq!(rows(&fork_db, "trades").await, 8);
    let diff = db.fork_diff("agent-01", None).await.unwrap();
    assert_eq!(diff.tables.len(), 1, "one shadow, not five");
}

#[tokio::test]
async fn fork_list_reports_what_each_fork_owns_and_pins() {
    let (_dir, db, _root) = db_with_trades().await;
    db.create_fork("idle", None, None, Default::default())
        .await
        .unwrap();
    db.create_fork("busy", Some("has work".into()), None, Default::default())
        .await
        .unwrap();
    let busy = db.open_fork("busy").await.unwrap();
    busy.append(
        "trades",
        vec![trades_batch(&[400], &["C"], &[4.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    busy.create_table("features", trades_schema(), default_options())
        .await
        .unwrap();

    let forks = db.list_forks().await.unwrap();
    let idle = forks.iter().find(|f| f.name == "idle").unwrap();
    let busy = forks.iter().find(|f| f.name == "busy").unwrap();

    assert_eq!(idle.tables_created, 0);
    assert_eq!(idle.tables_shadowed, 0);
    assert_eq!(
        idle.bytes_own, 0,
        "an idle fork costs no storage of its own"
    );
    assert!(idle.bytes_pinned > 0, "but it does hold the base back");

    assert_eq!(busy.tables_shadowed, 1);
    assert_eq!(busy.tables_created, 1);
    assert!(busy.bytes_own > 0);
    assert_eq!(busy.note.as_deref(), Some("has work"));
}

#[tokio::test]
async fn fork_metadata_survives_a_reopen() {
    let (_dir, db, root) = db_with_trades().await;
    let mut meta = serde_json::Map::new();
    meta.insert("run_id".into(), serde_json::json!("r-42"));
    db.create_fork("agent-01", Some("hypothesis 3".into()), None, meta)
        .await
        .unwrap();
    let fork_db = db.open_fork("agent-01").await.unwrap();
    fork_db
        .append(
            "trades",
            vec![trades_batch(&[400], &["C"], &[4.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    drop(fork_db);
    drop(db);

    let db = Database::open(&root).await.unwrap();
    let fork = db.fork_info("agent-01").await.unwrap();
    assert_eq!(fork.note.as_deref(), Some("hypothesis 3"));
    assert_eq!(fork.user_meta["run_id"], serde_json::json!("r-42"));
    let fork_db = db.open_fork("agent-01").await.unwrap();
    assert_eq!(rows(&fork_db, "trades").await, 4);
}
