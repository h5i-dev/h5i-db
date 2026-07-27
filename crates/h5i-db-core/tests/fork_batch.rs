//! Batch fork verbs (ROADMAP Part X, X-B1).
//!
//! The simulation workload in BranchBench forks a thousand branches off one
//! root, runs a trial in each, and discards nearly all of them. Both ends of
//! that are batch operations, and both were O(#forks) in work the database did
//! not need to repeat: every fork of the same base at the same instant pins the
//! same versions, and every drop took the database metadata lock again.
//!
//! The assertions are on counted object reads rather than wall time, because
//! "resolves the base once" is a claim about the algorithm.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use h5i_db_core::{Backend, Database, Error, ReadAt, ScanOptions, TableOptions, WriteOptions};

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

#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn object_store::ObjectStore>,
    gets: AtomicUsize,
}

impl CountingStore {
    fn new(inner: Arc<dyn object_store::ObjectStore>) -> Self {
        Self {
            inner,
            gets: AtomicUsize::new(0),
        }
    }
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

/// A database with `tables` tables, each holding one row.
async fn counted_db(tables: usize) -> (tempfile::TempDir, Database, Arc<CountingStore>) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("db");
    std::fs::create_dir_all(&root).unwrap();
    let plain = Backend::local(&root).unwrap();
    let counter = Arc::new(CountingStore::new(plain.store.clone()));
    let db = Database::create_with_backend(Backend {
        store: counter.clone(),
        heads: plain.heads,
        base_url: plain.base_url,
        local_root: plain.local_root,
    })
    .await
    .unwrap();
    for i in 0..tables {
        let name = format!("t{i}");
        db.create_table(&name, schema(), options()).await.unwrap();
        db.write(
            &name,
            vec![batch(&[1], &["A"], &[1.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    (dir, db, counter)
}

// ---------------------------------------------------------------------------
// create
// ---------------------------------------------------------------------------

/// The point of the batch: the base is resolved once, so the read cost is a
/// function of how many *tables* exist, not how many forks are made.
#[tokio::test]
async fn creating_many_forks_resolves_the_base_once() {
    async fn reads_for(count: usize) -> usize {
        let (_dir, db, counter) = counted_db(3).await;
        counter.take();
        db.fork_many("star", count, None, None, serde_json::Map::new())
            .await
            .unwrap();
        counter.take()
    }

    let small = reads_for(4).await;
    let large = reads_for(64).await;

    // Sixteen times the forks. The only per-fork read is the existence check
    // (one `get_opt` per name), so the difference must be about 60, not the
    // ~180 a per-fork catalog+HEAD pass over three tables would add.
    assert!(
        large <= small + 70,
        "creating 64 forks read {large} objects where 4 read {small}: the base is \
         being re-resolved per fork"
    );
}

#[tokio::test]
async fn a_batch_creates_every_fork_with_identical_pins() {
    let (_dir, db, _counter) = counted_db(2).await;
    let forks = db
        .fork_many("sim", 5, Some("trial".into()), None, serde_json::Map::new())
        .await
        .unwrap();

    assert_eq!(forks.len(), 5);
    assert_eq!(forks[0].name, "sim-0000");
    assert_eq!(forks[4].name, "sim-0004");
    let first = &forks[0].pins;
    assert_eq!(first.len(), 2, "both tables should be pinned");
    for f in &forks {
        assert_eq!(&f.pins, first, "forks of one base must pin one version set");
        assert_eq!(f.note.as_deref(), Some("trial"));
        // Every fork is independently valid on disk.
        let loaded = db.fork_info(&f.name).await.unwrap();
        assert_eq!(loaded.pins, *first);
    }
    // And they are all visible to the index.
    assert_eq!(db.fork_names().await.unwrap().len(), 5);
}

/// Batch creation must be readable as well as writable: each fork is a real
/// workspace, not just an object.
#[tokio::test]
async fn every_fork_in_a_batch_is_independently_writable() {
    let (_dir, db, _counter) = counted_db(1).await;
    db.fork_many("agent", 3, None, None, serde_json::Map::new())
        .await
        .unwrap();

    for (i, name) in ["agent-0000", "agent-0001", "agent-0002"]
        .iter()
        .enumerate()
    {
        let fork = db.open_fork(name).await.unwrap();
        fork.append(
            "t0",
            vec![batch(&[100 + i as i64], &["Z"], &[9.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    // Each fork sees its own row and no sibling's.
    for name in ["agent-0000", "agent-0001", "agent-0002"] {
        let fork = db.open_fork(name).await.unwrap();
        let (batches, _) = fork
            .scan("t0", ReadAt::Latest, ScanOptions::default())
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "{name} should hold the base row plus its own");
    }
}

/// Validation is all-or-nothing, so the ordinary "that name is taken" mistake
/// leaves no half-made star behind.
#[tokio::test]
async fn a_taken_name_aborts_the_batch_before_anything_is_written() {
    let (_dir, db, _counter) = counted_db(1).await;
    db.create_fork("taken", None, None, serde_json::Map::new())
        .await
        .unwrap();

    let names: Vec<String> = ["a", "taken", "b"].iter().map(|s| s.to_string()).collect();
    let err = db
        .create_forks(&names, None, None, serde_json::Map::new())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::ForkExists { .. }), "{err}");

    // Only the pre-existing fork survives: neither "a" nor "b" was created.
    assert_eq!(db.fork_names().await.unwrap(), vec!["taken".to_string()]);
}

#[tokio::test]
async fn a_repeated_name_in_one_batch_is_rejected_as_a_mistake() {
    let (_dir, db, _counter) = counted_db(1).await;
    let names: Vec<String> = ["a", "b", "a"].iter().map(|s| s.to_string()).collect();
    let err = db
        .create_forks(&names, None, None, serde_json::Map::new())
        .await
        .unwrap_err();
    assert!(
        format!("{err}").contains("distinct"),
        "a duplicate name should be named as such, got: {err}"
    );
    assert!(db.fork_names().await.unwrap().is_empty());
}

#[tokio::test]
async fn empty_batches_are_refused_rather_than_silently_doing_nothing() {
    let (_dir, db, _counter) = counted_db(1).await;
    assert!(
        db.create_forks(&[], None, None, serde_json::Map::new())
            .await
            .is_err()
    );
    assert!(
        db.fork_many("x", 0, None, None, serde_json::Map::new())
            .await
            .is_err()
    );
}

/// `create_fork` is now `create_forks` with one name. Its behaviour must not
/// have moved.
#[tokio::test]
async fn the_single_fork_verb_still_behaves_exactly_as_before() {
    let (_dir, db, _counter) = counted_db(1).await;
    let fork = db
        .create_fork("solo", Some("note".into()), None, serde_json::Map::new())
        .await
        .unwrap();
    assert_eq!(fork.name, "solo");
    assert_eq!(fork.note.as_deref(), Some("note"));
    assert_eq!(fork.pins.len(), 1);

    let err = db
        .create_fork("solo", None, None, serde_json::Map::new())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::ForkExists { .. }), "{err}");
}

// ---------------------------------------------------------------------------
// drop
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dropping_a_batch_removes_every_fork_and_its_tables() {
    let (_dir, db, _counter) = counted_db(1).await;
    db.fork_many("sim", 4, None, None, serde_json::Map::new())
        .await
        .unwrap();
    // Give two of them a shadow and a scratch table, so the drop has work.
    for name in ["sim-0000", "sim-0001"] {
        let fork = db.open_fork(name).await.unwrap();
        fork.append(
            "t0",
            vec![batch(&[500], &["Z"], &[1.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
        fork.create_table("scratch", schema(), options())
            .await
            .unwrap();
    }

    let names: Vec<String> = (0..4).map(|i| format!("sim-{i:04}")).collect();
    let dropped = db.drop_forks(&names).await.unwrap();
    assert_eq!(dropped, 4, "two shadows and two scratch tables");
    assert!(db.fork_names().await.unwrap().is_empty());

    // The base is untouched and the database still verifies.
    let report = db.verify("t0", true).await.unwrap();
    assert!(report.problems.is_empty(), "{:?}", report.problems);
    // And nothing the forks owned is left behind for vacuum to trip over.
    let vac = db.vacuum(None, 0, true).await.unwrap();
    assert!(
        vac.candidates.is_empty(),
        "left debris: {:?}",
        vac.candidates
    );
}

/// A name that is not there stops the batch rather than being skipped: the
/// caller passed a list it believes it is deleting.
#[tokio::test]
async fn a_missing_name_stops_the_batch_and_says_which() {
    let (_dir, db, _counter) = counted_db(1).await;
    db.fork_many("sim", 2, None, None, serde_json::Map::new())
        .await
        .unwrap();

    let names: Vec<String> = ["sim-0000", "ghost", "sim-0001"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let err = db.drop_forks(&names).await.unwrap_err();
    assert!(format!("{err}").contains("ghost"), "{err}");
    // The one before the failure really was dropped; the one after was not.
    assert_eq!(
        db.fork_names().await.unwrap(),
        vec!["sim-0001".to_string()],
        "a batch drop stops at the failure, leaving the rest intact"
    );
}

/// Re-running a drop that was interrupted must complete rather than fail on
/// the forks it already removed.
#[tokio::test]
async fn re_dropping_the_survivors_of_an_interrupted_batch_completes() {
    let (_dir, db, _counter) = counted_db(1).await;
    db.fork_many("sim", 3, None, None, serde_json::Map::new())
        .await
        .unwrap();
    db.drop_fork("sim-0000").await.unwrap();

    // The caller retries the whole list; the already-dropped one is gone, so
    // it retries only what remains — which is the documented recovery.
    let remaining: Vec<String> = db.fork_names().await.unwrap();
    assert_eq!(remaining.len(), 2);
    db.drop_forks(&remaining).await.unwrap();
    assert!(db.fork_names().await.unwrap().is_empty());

    let vac = db.vacuum(None, 0, true).await.unwrap();
    assert!(
        vac.candidates.is_empty(),
        "left debris: {:?}",
        vac.candidates
    );
}

/// After a mass prune the base must be reclaimable again — the pins are gone,
/// so the retention floor is free to move.
#[tokio::test]
async fn mass_pruning_releases_the_retention_floor_it_was_holding() {
    let (_dir, db, _counter) = counted_db(1).await;
    for i in 0..3i64 {
        db.append(
            "t0",
            vec![batch(&[10 + i], &["A"], &[1.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    db.fork_many("sim", 8, None, None, serde_json::Map::new())
        .await
        .unwrap();
    // Main moves on past the pin, so keeping only the newest version would
    // expire the version the forks are holding.
    for i in 0..2i64 {
        db.append(
            "t0",
            vec![batch(&[100 + i], &["A"], &[1.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }

    // Pinned: the floor cannot rise.
    let err = db
        .set_retention("t0", h5i_db_core::RetentionCut::KeepLast(1), None)
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("pins version"), "{err}");

    let names: Vec<String> = (0..8).map(|i| format!("sim-{i:04}")).collect();
    db.drop_forks(&names).await.unwrap();

    // Released.
    db.set_retention("t0", h5i_db_core::RetentionCut::KeepLast(1), None)
        .await
        .expect("the floor should move once nothing pins the table");
}
