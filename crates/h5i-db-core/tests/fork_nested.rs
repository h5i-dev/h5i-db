//! Nested forks (ROADMAP Part X, X-C1).
//!
//! Part IX refused fork-of-fork because nesting multiplies the promote and GC
//! paths. BranchBench supplies the reason to pay that: MCTS — the workload that
//! lifts agent task success most — explores a *deep narrow tree*, parameterised
//! at depth 25, and cannot be expressed as a flat star of forks at all.
//!
//! Two properties are load-bearing.
//!
//! **Reads must not care how deep they are.** A shadow manifest names its
//! segments by path, so a fork at depth 20 resolves in the same number of reads
//! as one at depth 1 — no chain replay, no tree walk. That is the property
//! BranchBench measured every existing system failing (Dolt's reads degrade
//! 5-4000x with branch depth), so it is asserted rather than assumed.
//!
//! **GC must not lose a level.** A child's shadows reference its parent's
//! segments by path, exactly as a top-level fork's reference the database's.
//! Deleting a parent's tables under a live child is the same data loss the
//! retention floor guards against one level up.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use h5i_db_core::{
    Backend, Database, Error, ReadAt, RetentionCut, ScanOptions, TableOptions, WriteOptions,
};

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

fn options() -> TableOptions {
    TableOptions {
        time_column: Some("ts".into()),
        ..Default::default()
    }
}

async fn seeded() -> (tempfile::TempDir, Database) {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(&dir.path().join("db")).await.unwrap();
    db.create_table("t", schema(), options()).await.unwrap();
    db.write(
        "t",
        vec![batch(&[1, 2], &["A", "B"], &[10.0, 20.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    (dir, db)
}

async fn prices(db: &Database, table: &str) -> Vec<f64> {
    let (batches, _) = db
        .scan(table, ReadAt::Latest, ScanOptions::default())
        .await
        .unwrap();
    let mut out: Vec<f64> = batches
        .iter()
        .flat_map(|b| {
            b.column_by_name("price")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    out.sort_by(|a, b| a.partial_cmp(b).unwrap());
    out
}

/// Fork `child` inside `parent` and append one row to `t` in it.
async fn nest(db: &Database, parent: &str, child: &str, ts: i64, price: f64) {
    let parent_db = db.open_fork(parent).await.unwrap();
    parent_db
        .create_fork(child, None, None, serde_json::Map::new())
        .await
        .unwrap();
    let child_db = db.open_fork(child).await.unwrap();
    child_db
        .append(
            "t",
            vec![batch(&[ts], &["Z"], &[price])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// the model
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_child_sees_its_parents_work_and_its_own() {
    let (_dir, db) = seeded().await;
    db.create_fork("p", None, None, serde_json::Map::new())
        .await
        .unwrap();
    let p = db.open_fork("p").await.unwrap();
    p.append(
        "t",
        vec![batch(&[100], &["P"], &[100.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    nest(&db, "p", "c", 200, 200.0).await;

    let c = db.open_fork("c").await.unwrap();
    assert_eq!(
        prices(&c, "t").await,
        vec![10.0, 20.0, 100.0, 200.0],
        "the child should see base rows, its parent's row, and its own"
    );
    // The parent is unaffected by the child.
    assert_eq!(prices(&p, "t").await, vec![10.0, 20.0, 100.0]);
    // …and so is the base.
    assert_eq!(prices(&db, "t").await, vec![10.0, 20.0]);
}

/// A child forked before the parent wrote sees the parent's *frozen* view, and
/// stays frozen when the parent moves on afterwards.
#[tokio::test]
async fn a_child_is_frozen_against_later_parent_commits() {
    let (_dir, db) = seeded().await;
    db.create_fork("p", None, None, serde_json::Map::new())
        .await
        .unwrap();
    let p = db.open_fork("p").await.unwrap();
    p.append(
        "t",
        vec![batch(&[100], &["P"], &[100.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    db.open_fork("p")
        .await
        .unwrap()
        .create_fork("c", None, None, serde_json::Map::new())
        .await
        .unwrap();

    // The parent keeps working after the child was taken.
    p.append(
        "t",
        vec![batch(&[300], &["P"], &[300.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    let c = db.open_fork("c").await.unwrap();
    assert_eq!(
        prices(&c, "t").await,
        vec![10.0, 20.0, 100.0],
        "the child must not see a parent commit made after it forked"
    );
}

#[tokio::test]
async fn siblings_at_the_same_level_are_isolated() {
    let (_dir, db) = seeded().await;
    db.create_fork("p", None, None, serde_json::Map::new())
        .await
        .unwrap();
    nest(&db, "p", "a", 100, 100.0).await;
    nest(&db, "p", "b", 200, 200.0).await;

    let a = db.open_fork("a").await.unwrap();
    let b = db.open_fork("b").await.unwrap();
    assert_eq!(prices(&a, "t").await, vec![10.0, 20.0, 100.0]);
    assert_eq!(prices(&b, "t").await, vec![10.0, 20.0, 200.0]);
}

#[tokio::test]
async fn lineage_is_recorded_on_each_fork() {
    let (_dir, db) = seeded().await;
    db.create_fork("p", None, None, serde_json::Map::new())
        .await
        .unwrap();
    nest(&db, "p", "c", 100, 100.0).await;
    nest(&db, "c", "g", 200, 200.0).await;

    assert_eq!(db.fork_info("p").await.unwrap().parent, None);
    assert_eq!(db.fork_info("p").await.unwrap().depth(), 0);
    assert_eq!(
        db.fork_info("c").await.unwrap().parent.as_deref(),
        Some("p")
    );
    assert_eq!(db.fork_info("c").await.unwrap().depth(), 1);
    assert_eq!(
        db.fork_info("g").await.unwrap().parent.as_deref(),
        Some("c")
    );
    assert_eq!(db.fork_info("g").await.unwrap().depth(), 2);
}

/// A table created inside a fork is visible to that fork's children, since a
/// child pins whatever its parent could see.
#[tokio::test]
async fn a_child_inherits_a_table_its_parent_created() {
    let (_dir, db) = seeded().await;
    db.create_fork("p", None, None, serde_json::Map::new())
        .await
        .unwrap();
    let p = db.open_fork("p").await.unwrap();
    p.create_table("scratch", schema(), options())
        .await
        .unwrap();
    p.write(
        "scratch",
        vec![batch(&[5], &["S"], &[50.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    p.create_fork("c", None, None, serde_json::Map::new())
        .await
        .unwrap();
    let c = db.open_fork("c").await.unwrap();
    assert_eq!(prices(&c, "scratch").await, vec![50.0]);
    // And it is still invisible to the base.
    assert!(db.resolve("scratch", ReadAt::Latest).await.is_err());
}

// ---------------------------------------------------------------------------
// depth is free
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn object_store::ObjectStore>,
    gets: AtomicUsize,
}

impl CountingStore {
    fn take(&self) -> usize {
        self.gets.swap(0, Ordering::SeqCst)
    }
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// The differentiating claim: **resolving** a table costs the same at any
/// depth, because a shadow manifest names its segments by path and there is no
/// per-level indirection to walk.
///
/// Measured on resolution rather than on a full scan, deliberately. A chain 24
/// deep really does hold more data than one 2 deep — each level wrote a row —
/// so its scan reads more segments, and folding that into the number would
/// measure the fixture instead of the property. What must not grow is the
/// metadata lookup, which is where Dolt's 5-4000x depth penalty lives.
#[tokio::test]
async fn resolution_cost_does_not_grow_with_fork_depth() {
    async fn reads_at_depth(depth: usize) -> (usize, usize) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("db");
        std::fs::create_dir_all(&root).unwrap();
        let plain = Backend::local(&root).unwrap();
        let counter = Arc::new(CountingStore {
            inner: plain.store.clone(),
            gets: AtomicUsize::new(0),
        });
        let db = Database::create_with_backend(Backend {
            store: counter.clone(),
            heads: plain.heads,
            base_url: plain.base_url,
            local_root: plain.local_root,
        })
        .await
        .unwrap();
        db.create_table("t", schema(), options()).await.unwrap();
        db.write(
            "t",
            vec![batch(&[1, 2], &["A", "B"], &[10.0, 20.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();

        // A chain, each level writing one row of its own.
        let mut parent = String::new();
        for level in 0..depth {
            let name = format!("f{level:02}");
            let handle = if level == 0 {
                db.base()
            } else {
                db.open_fork(&parent).await.unwrap()
            };
            handle
                .create_fork(&name, None, None, serde_json::Map::new())
                .await
                .unwrap();
            db.open_fork(&name)
                .await
                .unwrap()
                .append(
                    "t",
                    vec![batch(&[100 + level as i64], &["Z"], &[level as f64])],
                    WriteOptions::default(),
                )
                .await
                .unwrap();
            parent = name;
        }

        let leaf = db.open_fork(&parent).await.unwrap();
        counter.take();
        let resolved = leaf.resolve("t", ReadAt::Latest).await.unwrap();
        let reads = counter.take();
        // Read the rows too, but outside the measurement.
        let (batches, _) = leaf
            .scan("t", ReadAt::Latest, ScanOptions::default())
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            resolved.manifest.segments.len(),
            depth + 1,
            "fixture should accumulate one segment per level"
        );
        (reads, rows)
    }

    let (shallow_reads, shallow_rows) = reads_at_depth(2).await;
    let (deep_reads, deep_rows) = reads_at_depth(24).await;

    // Each level really did contribute a row, so the deep case holds twelve
    // times the data — the point is that finding it costs no more lookups.
    assert_eq!(shallow_rows, 4, "2 base rows + one per level");
    assert_eq!(deep_rows, 26);
    assert_eq!(
        deep_reads, shallow_reads,
        "resolving at depth 24 took {deep_reads} reads where depth 2 took \
         {shallow_reads}: resolution is walking the chain"
    );
}

#[tokio::test]
async fn nesting_past_the_depth_cap_is_refused_with_a_way_out() {
    let (_dir, db) = seeded().await;
    let mut parent = String::new();
    // The cap is on the child's depth, so depth 0..=MAX is creatable: that is
    // MAX+1 forks in the chain.
    for level in 0..=32u32 {
        let name = format!("f{level:02}");
        let handle = if level == 0 {
            db.base()
        } else {
            db.open_fork(&parent).await.unwrap()
        };
        handle
            .create_fork(&name, None, None, serde_json::Map::new())
            .await
            .unwrap();
        parent = name;
    }
    let err = db
        .open_fork(&parent)
        .await
        .unwrap()
        .create_fork("too-deep", None, None, serde_json::Map::new())
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("MAX_FORK_DEPTH"), "{msg}");
    assert!(
        msg.contains("promote or drop"),
        "the refusal should say what to do: {msg}"
    );
}

// ---------------------------------------------------------------------------
// GC across levels
// ---------------------------------------------------------------------------

/// The data-loss guard. A child's shadow references its parent's segments by
/// path, so dropping the parent would strand the child exactly as vacuuming a
/// pinned base version would.
#[tokio::test]
async fn a_parent_with_a_live_child_cannot_be_dropped() {
    let (_dir, db) = seeded().await;
    db.create_fork("p", None, None, serde_json::Map::new())
        .await
        .unwrap();
    let p = db.open_fork("p").await.unwrap();
    p.append(
        "t",
        vec![batch(&[100], &["P"], &[100.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    nest(&db, "p", "c", 200, 200.0).await;

    let err = db.drop_fork("p").await.unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("nested below it"), "{msg}");
    assert!(msg.contains('c'), "the child should be named: {msg}");

    // Dropping the child first frees the parent.
    db.drop_fork("c").await.unwrap();
    db.drop_fork("p").await.unwrap();
    assert!(db.fork_names().await.unwrap().is_empty());
}

/// The same guard, one table at a time. `drop_fork` refuses while a child
/// lives, but `drop_table` inside the parent deletes everything under the
/// shadow's prefix directly, and a child that pins that shadow reads its
/// segments by path. Dropping one table is the same data loss as dropping the
/// fork, so it is refused for the same reason.
#[tokio::test]
async fn a_parent_cannot_drop_a_shadow_its_child_pins() {
    let (_dir, db) = seeded().await;
    db.create_fork("p", None, None, serde_json::Map::new())
        .await
        .unwrap();
    let p = db.open_fork("p").await.unwrap();
    // Materializes p's shadow of `t` and gives it a segment of its own.
    p.append(
        "t",
        vec![batch(&[100], &["P"], &[100.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    // The child pins the shadow, not the base table.
    p.create_fork("c", None, None, serde_json::Map::new())
        .await
        .unwrap();

    let err = p.drop_table("t").await.unwrap_err();
    let msg = format!("{err}");
    assert!(matches!(err, Error::InvalidInput { .. }), "{err:?}");
    assert!(msg.contains("pinned by fork"), "{msg}");

    // The child still reads every row, which is what the guard is protecting.
    let c = db.open_fork("c").await.unwrap();
    assert_eq!(prices(&c, "t").await, vec![10.0, 20.0, 100.0]);

    // Dropping the child releases the pin and the parent may then undo its
    // edits, exactly as `drop_fork` behaves one level up.
    db.drop_fork("c").await.unwrap();
    p.drop_table("t").await.unwrap();
    assert_eq!(prices(&p, "t").await, vec![10.0, 20.0]);
}

/// A child that forked before its parent wrote anything holds nothing of the
/// parent's, so the parent is free to go. The guard is about dependency, not
/// about lineage.
#[tokio::test]
async fn a_parent_whose_child_pins_nothing_of_its_own_can_be_dropped() {
    let (_dir, db) = seeded().await;
    db.create_fork("p", None, None, serde_json::Map::new())
        .await
        .unwrap();
    db.open_fork("p")
        .await
        .unwrap()
        .create_fork("c", None, None, serde_json::Map::new())
        .await
        .unwrap();

    db.drop_fork("p").await.expect("nothing of p's is pinned");
    // The child still reads, through its own pins on the base.
    let c = db.open_fork("c").await.unwrap();
    assert_eq!(prices(&c, "t").await, vec![10.0, 20.0]);
}

#[tokio::test]
async fn a_subtree_drop_removes_the_whole_chain_deepest_first() {
    let (_dir, db) = seeded().await;
    db.create_fork("p", None, None, serde_json::Map::new())
        .await
        .unwrap();
    db.open_fork("p")
        .await
        .unwrap()
        .append(
            "t",
            vec![batch(&[50], &["P"], &[50.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    nest(&db, "p", "c", 100, 100.0).await;
    nest(&db, "c", "g", 200, 200.0).await;
    nest(&db, "c", "g2", 300, 300.0).await;

    db.drop_fork_tree("p").await.unwrap();
    assert!(db.fork_names().await.unwrap().is_empty());

    // The base is intact and nothing was left for vacuum to trip over.
    assert_eq!(prices(&db, "t").await, vec![10.0, 20.0]);
    assert!(
        db.verify("t", true).await.unwrap().problems.is_empty(),
        "the base should still verify after the subtree went"
    );
    let vac = db.vacuum(None, 0, true).await.unwrap();
    assert!(
        vac.candidates.is_empty(),
        "subtree drop left debris: {:?}",
        vac.candidates
    );
}

/// Vacuum must treat every level as a root. A three-level tree, vacuumed from
/// a cold index, must lose nothing.
#[tokio::test]
async fn vacuum_over_a_three_level_tree_collects_nothing_live() {
    let (dir, db) = seeded().await;
    db.create_fork("p", None, None, serde_json::Map::new())
        .await
        .unwrap();
    db.open_fork("p")
        .await
        .unwrap()
        .append(
            "t",
            vec![batch(&[50], &["P"], &[50.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    nest(&db, "p", "c", 100, 100.0).await;
    nest(&db, "c", "g", 200, 200.0).await;

    // Cold index: the state a restored or crashed database is in.
    let _ = std::fs::remove_file(dir.path().join("db").join("FORK_INDEX.json"));
    db.vacuum(None, 0, true).await.unwrap();

    for (fork, expected) in [
        ("p", vec![10.0, 20.0, 50.0]),
        ("c", vec![10.0, 20.0, 50.0, 100.0]),
        ("g", vec![10.0, 20.0, 50.0, 100.0, 200.0]),
    ] {
        let handle = db.open_fork(fork).await.unwrap();
        assert_eq!(
            prices(&handle, "t").await,
            expected,
            "vacuum damaged fork {fork}"
        );
    }
}

/// The retention floor must still refuse to rise past what a *nested* fork
/// transitively holds: the child pins the base too, so the guard sees it.
#[tokio::test]
async fn a_nested_fork_still_holds_the_base_retention_floor_down() {
    let (_dir, db) = seeded().await;
    for i in 0..3i64 {
        db.append(
            "t",
            vec![batch(&[10 + i], &["A"], &[1.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    db.create_fork("p", None, None, serde_json::Map::new())
        .await
        .unwrap();
    nest(&db, "p", "c", 500, 500.0).await;
    db.append(
        "t",
        vec![batch(&[100], &["A"], &[1.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    let err = db
        .set_retention("t", RetentionCut::KeepLast(1), None)
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("pins version"), "{err}");
}

// ---------------------------------------------------------------------------
// promote goes up one level
// ---------------------------------------------------------------------------

#[tokio::test]
async fn promoting_a_child_lands_on_its_parent_not_on_main() {
    let (_dir, db) = seeded().await;
    db.create_fork("p", None, None, serde_json::Map::new())
        .await
        .unwrap();
    let p = db.open_fork("p").await.unwrap();
    p.append(
        "t",
        vec![batch(&[100], &["P"], &[100.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    nest(&db, "p", "c", 200, 200.0).await;

    let result = db.promote("c", "t").await.unwrap();
    assert_eq!(result.fork, "c");

    // The parent gained the child's row...
    assert_eq!(prices(&p, "t").await, vec![10.0, 20.0, 100.0, 200.0]);
    // ...and main gained nothing.
    assert_eq!(prices(&db, "t").await, vec![10.0, 20.0]);
    assert!(db.verify("t", true).await.unwrap().problems.is_empty());
}

#[tokio::test]
async fn a_table_created_in_a_child_promotes_into_the_parents_catalog() {
    let (_dir, db) = seeded().await;
    db.create_fork("p", None, None, serde_json::Map::new())
        .await
        .unwrap();
    db.open_fork("p")
        .await
        .unwrap()
        .create_fork("c", None, None, serde_json::Map::new())
        .await
        .unwrap();
    let c = db.open_fork("c").await.unwrap();
    c.create_table("finding", schema(), options())
        .await
        .unwrap();
    c.write(
        "finding",
        vec![batch(&[9], &["F"], &[90.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    db.promote("c", "finding").await.unwrap();

    // The parent now owns it; the database still does not see it.
    let p = db.open_fork("p").await.unwrap();
    assert_eq!(prices(&p, "finding").await, vec![90.0]);
    assert!(db.resolve("finding", ReadAt::Latest).await.is_err());
}

/// Promotion is still first-commit-wins at every level.
#[tokio::test]
async fn two_children_racing_onto_one_parent_still_conflict() {
    let (_dir, db) = seeded().await;
    db.create_fork("p", None, None, serde_json::Map::new())
        .await
        .unwrap();
    db.open_fork("p")
        .await
        .unwrap()
        .append(
            "t",
            vec![batch(&[50], &["P"], &[50.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    nest(&db, "p", "a", 100, 100.0).await;
    nest(&db, "p", "b", 200, 200.0).await;

    db.promote("a", "t").await.unwrap();
    let err = db.promote("b", "t").await.unwrap_err();
    assert!(matches!(err, Error::PromoteConflict { .. }), "{err}");
    assert!(!err.retryable());

    // The winner's row is on the parent; the loser wrote nothing.
    let p = db.open_fork("p").await.unwrap();
    assert_eq!(prices(&p, "t").await, vec![10.0, 20.0, 50.0, 100.0]);
}
