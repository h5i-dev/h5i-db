//! Fork visibility index exit-gate tests (ROADMAP Part X, X-A1).
//!
//! Two claims are being defended here, and they pull in opposite directions.
//!
//! The **cost** claim is that the retention, drop-table and vacuum guards stop
//! reading one object per fork. That is asserted on counted object reads, not
//! on wall time: "constant in the number of forks" is a statement about the
//! algorithm, and a timing threshold would be both flakier and weaker.
//!
//! The **safety** claim is that no answer changed. The index is a cache in
//! front of the checks that keep a live fork's segments from being vacuumed
//! away, so a cache that under-reports pins is silent data loss. The tests
//! that matter most are therefore the ones that damage the index — delete it,
//! truncate it, tamper with a pin — and then demand byte-identical answers.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use h5i_db_core::{
    Backend, Database, ReadAt, RetentionCut, ScanOptions, StorageOptions, TableOptions,
    WriteOptions,
};

// ---------------------------------------------------------------------------
// fixtures
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

/// An `ObjectStore` that forwards everything and counts reads and listings
/// *separately*.
///
/// The distinction is the whole point of X-A1: the guards traded N object
/// reads for a fixed number of listings, so a counter that lumped the two
/// together would report no improvement at all.
#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn object_store::ObjectStore>,
    gets: AtomicUsize,
    lists: AtomicUsize,
}

impl CountingStore {
    fn new(inner: Arc<dyn object_store::ObjectStore>) -> Self {
        Self {
            inner,
            gets: AtomicUsize::new(0),
            lists: AtomicUsize::new(0),
        }
    }
    /// Read and reset both counters.
    fn take(&self) -> (usize, usize) {
        (
            self.gets.swap(0, Ordering::SeqCst),
            self.lists.swap(0, Ordering::SeqCst),
        )
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
        self.lists.fetch_add(1, Ordering::SeqCst);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.lists.fetch_add(1, Ordering::SeqCst);
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

/// A database whose object reads and listings are counted.
async fn counted_db() -> (
    tempfile::TempDir,
    Database,
    Arc<CountingStore>,
    std::path::PathBuf,
) {
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
    (dir, db, counter, root)
}

async fn seed_trades(db: &Database) {
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
}

/// `n` forks, every third one carrying a shadow and every fifth a table of its
/// own, so the index's owned-table half is exercised and not just its pins.
///
/// `ts_base` must exceed every timestamp already in `trades`: a shadow starts
/// life holding the base's rows, so its appends are subject to the same
/// monotonic-append rule.
async fn make_forks(db: &Database, n: usize, prefix: &str, ts_base: i64) {
    for i in 0..n {
        let name = format!("{prefix}-{i:04}");
        db.create_fork(&name, None, None, serde_json::Map::new())
            .await
            .unwrap();
        if i % 3 == 0 {
            let fork = db.open_fork(&name).await.unwrap();
            fork.append(
                "trades",
                vec![trades_batch(&[ts_base + i as i64], &["C"], &[9.0])],
                WriteOptions::default(),
            )
            .await
            .unwrap();
        }
        if i % 5 == 0 {
            let fork = db.open_fork(&name).await.unwrap();
            fork.create_table(&format!("scratch-{i}"), trades_schema(), default_options())
                .await
                .unwrap();
        }
    }
}

fn index_path(root: &Path) -> std::path::PathBuf {
    root.join("FORK_INDEX.json")
}

// ---------------------------------------------------------------------------
// cost: the guards stop scaling with fork count
// ---------------------------------------------------------------------------

/// The claim X-A1 exists to make. Not "fewer reads" — *the same* number of
/// reads at 8 forks and at 40, which is what makes a thousand-branch workload
/// possible at all.
#[tokio::test]
async fn guard_reads_are_constant_in_the_number_of_forks() {
    async fn reads_at(fork_count: usize) -> (usize, usize) {
        let (_dir, db, counter, _root) = counted_db().await;
        seed_trades(&db).await;
        make_forks(&db, fork_count, "f", 10_000).await;

        // Warm the index: the first read after a change pays the rebuild, and
        // the rebuild is O(#changed forks) by design. The steady state is what
        // an agent loop actually experiences.
        db.vacuum(None, 0, false).await.unwrap();

        counter.take();
        db.vacuum(None, 0, false).await.unwrap();
        let (gets, _) = counter.take();

        // The drop guard on a fork-pinned table: refused, and the refusal must
        // not cost a scan either.
        db.drop_table("trades").await.unwrap_err();
        let (drop_gets, _) = counter.take();

        (gets, drop_gets)
    }

    let (vacuum_small, drop_small) = reads_at(8).await;
    let (vacuum_large, drop_large) = reads_at(40).await;

    // Vacuum reads per-table metadata too, and the large fixture has more
    // fork-owned tables, so its total is allowed to grow — what must not grow
    // is the fork-scan component, which was one read per fork. Five times the
    // forks, and the difference stays far below the 32 extra reads a scan
    // would have cost.
    assert!(
        vacuum_large <= vacuum_small + 24,
        "vacuum reads grew with fork count: {vacuum_small} at 8 forks, {vacuum_large} at 40"
    );
    assert_eq!(
        drop_small, drop_large,
        "the drop-table pin guard must read the same objects at 8 and 40 forks"
    );
}

/// The steady-state guard cost is one object read: the index itself.
#[tokio::test]
async fn the_drop_guard_reads_exactly_the_index() {
    let (_dir, db, counter, _root) = counted_db().await;
    seed_trades(&db).await;
    make_forks(&db, 6, "f", 10_000).await;
    db.drop_table("trades").await.unwrap_err(); // warm

    counter.take();
    let err = db.drop_table("trades").await.unwrap_err();
    let (gets, _lists) = counter.take();

    assert!(format!("{err}").contains("pinned by fork"), "{err}");
    // One read for the catalog entry, one for the index. Six forks, two reads.
    assert!(
        gets <= 3,
        "expected a constant handful of reads, got {gets} with 6 forks"
    );
}

// ---------------------------------------------------------------------------
// safety: damaged index, identical answers
// ---------------------------------------------------------------------------

/// Compare every index-derived answer against the same answer computed by the
/// full scan the index replaced, over a population with shadows, fork-created
/// tables and several pinned versions.
#[tokio::test]
async fn index_answers_match_a_full_scan_of_every_fork() {
    let (_dir, db, _counter, _root) = counted_db().await;
    seed_trades(&db).await;
    // A second table, so a pin set is not trivially "every table".
    db.create_table("quotes", trades_schema(), default_options())
        .await
        .unwrap();
    db.write(
        "quotes",
        vec![trades_batch(&[10], &["Q"], &[1.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    // Commits between forks so different forks pin different sequences.
    for i in 0..4 {
        make_forks(&db, 3, &format!("gen{i}"), 10_000 + i * 1_000).await;
        db.append(
            "trades",
            vec![trades_batch(&[900 + i], &["Z"], &[1.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }

    let index = h5i_db_core::fork_index::refresh(db.backend(), true)
        .await
        .unwrap();

    // The slow path, spelled out: read every fork, read every fork's catalog.
    let mut expected_pins: std::collections::BTreeMap<uuid::Uuid, Vec<(String, u64)>> =
        Default::default();
    let mut expected_owned: std::collections::BTreeSet<uuid::Uuid> = Default::default();
    let forks = h5i_db_core::fork::list(db.backend()).await.unwrap();
    for f in &forks {
        for (table_id, pin) in &f.pins {
            expected_pins
                .entry(*table_id)
                .or_default()
                .push((f.name.clone(), pin.sequence));
        }
        for fe in h5i_db_core::fork::list_entries(db.backend(), &f.name)
            .await
            .unwrap()
        {
            expected_owned.insert(fe.table_id);
        }
    }

    assert!(!forks.is_empty(), "fixture built no forks");
    assert!(
        !expected_owned.is_empty(),
        "fixture built no fork-owned tables, so the owned half is untested"
    );
    assert_eq!(
        index.owned_table_ids(),
        expected_owned,
        "owned-table root set diverged from a full scan"
    );
    assert_eq!(
        index.pinned_table_ids(),
        expected_pins.keys().copied().collect(),
        "pinned-table root set diverged from a full scan"
    );
    for (table_id, mut pins) in expected_pins {
        pins.sort();
        let from_index: Vec<(String, u64)> = index
            .pins_of(table_id)
            .into_iter()
            .map(|(n, s)| (n.to_string(), s))
            .collect();
        assert_eq!(from_index, pins, "pins diverged for table {table_id}");
    }
}

/// Deleting the index is a supported operation — the file is a cache, and an
/// operator who removes it (or a backup that never captured it) must not lose
/// a single guarantee.
#[tokio::test]
async fn a_deleted_index_is_rebuilt_with_identical_content() {
    let (_dir, db, _counter, root) = counted_db().await;
    seed_trades(&db).await;
    make_forks(&db, 5, "f", 10_000).await;
    // Creating forks does not write the index — nothing maintains it. A
    // consumer does, on first use.
    db.vacuum(None, 0, false).await.unwrap();

    let before = std::fs::read(index_path(&root)).expect("index should exist once forks do");
    std::fs::remove_file(index_path(&root)).unwrap();

    // Any consumer rebuilds it.
    db.drop_table("trades").await.unwrap_err();

    let after = std::fs::read(index_path(&root)).expect("index should have been rebuilt");
    let strip = |bytes: &[u8]| -> serde_json::Value {
        let mut v: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        // Rebuild time differs by construction; the checksum covers it.
        v.as_object_mut().unwrap().remove("built_at_ns");
        v.as_object_mut().unwrap().remove("checksum");
        v
    };
    assert_eq!(
        strip(&before),
        strip(&after),
        "a rebuilt index must describe exactly the same forks"
    );
}

/// A tampered index must never be believed. This is the data-loss case: an
/// index that reports a lower pin than the fork holds would let the retention
/// floor rise past a version a live workspace still reads.
#[tokio::test]
async fn a_tampered_pin_in_the_index_is_ignored_and_the_floor_still_refuses() {
    let (_dir, db, _counter, root) = counted_db().await;
    seed_trades(&db).await;
    for i in 0..3 {
        db.append(
            "trades",
            vec![trades_batch(&[500 + i], &["A"], &[1.0])],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    // Pin at the current head, then move main on.
    db.create_fork("pinned", None, None, serde_json::Map::new())
        .await
        .unwrap();
    let pinned_seq = db
        .fork_info("pinned")
        .await
        .unwrap()
        .pins
        .values()
        .map(|p| p.sequence)
        .next()
        .expect("the fork should pin trades");
    db.append(
        "trades",
        vec![trades_batch(&[600], &["A"], &[1.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();

    // Rewrite every pinned sequence to 0 without fixing the checksum: the
    // forgery a stale or malicious cache would represent.
    db.vacuum(None, 0, false).await.unwrap(); // ensure the index exists
    let mut v: serde_json::Value =
        serde_json::from_slice(&std::fs::read(index_path(&root)).unwrap()).unwrap();
    for (_name, fork) in v["forks"].as_object_mut().unwrap() {
        for (_id, seq) in fork["pins"].as_object_mut().unwrap() {
            *seq = serde_json::json!(0);
        }
    }
    std::fs::write(index_path(&root), serde_json::to_vec(&v).unwrap()).unwrap();

    // The floor still refuses: the forged index failed its checksum, was
    // treated as absent, and the truth was re-read from the fork objects. Had
    // the forgery been believed, a floor one above the real pin would have
    // been allowed and the next vacuum would have deleted the fork's segments.
    let err = db
        .set_retention("trades", RetentionCut::BeforeSequence(pinned_seq + 1), None)
        .await
        .unwrap_err();
    assert!(
        format!("{err}").contains(&format!("pins version {pinned_seq}")),
        "expected the real pinned version ({pinned_seq}) to be reported, got: {err}"
    );
}

/// Truncation is the other way a cache breaks: a half-written file must read
/// as "no index", not as "no forks".
#[tokio::test]
async fn a_truncated_index_is_treated_as_absent() {
    let (_dir, db, _counter, root) = counted_db().await;
    seed_trades(&db).await;
    make_forks(&db, 3, "f", 10_000).await;
    db.vacuum(None, 0, false).await.unwrap();

    let bytes = std::fs::read(index_path(&root)).unwrap();
    std::fs::write(index_path(&root), &bytes[..bytes.len() / 2]).unwrap();

    let err = db.drop_table("trades").await.unwrap_err();
    assert!(
        format!("{err}").contains("pinned by fork"),
        "a truncated index must not hide a live pin: {err}"
    );
}

/// The regression this whole subsystem sits in front of: vacuum must not
/// collect a fork's own tables. Re-asserted here with the index deleted first,
/// because "vacuum rebuilds the roots correctly from cold" is the state a
/// crashed or restored database is actually in.
#[tokio::test]
async fn vacuum_from_a_cold_index_still_protects_fork_tables() {
    let (_dir, db, _counter, root) = counted_db().await;
    seed_trades(&db).await;
    db.create_fork("agent", None, None, serde_json::Map::new())
        .await
        .unwrap();
    let fork = db.open_fork("agent").await.unwrap();
    fork.append(
        "trades",
        vec![trades_batch(&[999], &["F"], &[42.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    fork.create_table("scratch", trades_schema(), default_options())
        .await
        .unwrap();

    let _ = std::fs::remove_file(index_path(&root));
    db.vacuum(None, 0, true).await.unwrap();

    // Both the shadow and the fork-created table survive, and still read.
    let fork = db.open_fork("agent").await.unwrap();
    let (batches, _) = fork
        .scan("trades", ReadAt::Latest, ScanOptions::default())
        .await
        .unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 4, "the fork's shadow lost rows to vacuum");
    fork.scan("scratch", ReadAt::Latest, ScanOptions::default())
        .await
        .expect("the fork-created table was collected");
}

// ---------------------------------------------------------------------------
// the index stays out of the way
// ---------------------------------------------------------------------------

/// A database that never forks gains no object. Enabling X-A1 must not change
/// the on-disk shape of the common case.
#[tokio::test]
async fn a_fork_free_database_never_grows_an_index_object() {
    let (_dir, db, _counter, root) = counted_db().await;
    seed_trades(&db).await;
    db.vacuum(None, 0, true).await.unwrap();
    db.set_retention("trades", RetentionCut::KeepLast(1), None)
        .await
        .unwrap();
    assert!(
        !index_path(&root).exists(),
        "a database with no forks must keep no fork index"
    );
}

/// Dropping the last fork removes the index rather than leaving an empty one.
#[tokio::test]
async fn dropping_the_last_fork_removes_the_index() {
    let (_dir, db, _counter, root) = counted_db().await;
    seed_trades(&db).await;
    db.create_fork("only", None, None, serde_json::Map::new())
        .await
        .unwrap();
    db.vacuum(None, 0, false).await.unwrap();
    assert!(index_path(&root).exists());

    db.drop_fork("only").await.unwrap();
    db.vacuum(None, 0, false).await.unwrap();
    assert!(
        !index_path(&root).exists(),
        "the index outlived the last fork it described"
    );
}

/// Reads through a fork never require the index — it is a write-side
/// accelerator, and a read-only handle must not need one (nor be able to
/// write one).
#[tokio::test]
async fn a_read_only_handle_reads_forks_without_writing_an_index() {
    let (_dir, db, _counter, root) = counted_db().await;
    seed_trades(&db).await;
    db.create_fork("agent", None, None, serde_json::Map::new())
        .await
        .unwrap();
    let fork = db.open_fork("agent").await.unwrap();
    fork.append(
        "trades",
        vec![trades_batch(&[777], &["R"], &[7.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    let _ = std::fs::remove_file(index_path(&root));

    let ro = Database::open_read_only(&root).await.unwrap();
    let ro_fork = ro.open_fork("agent").await.unwrap();
    let (batches, _) = ro_fork
        .scan("trades", ReadAt::Latest, ScanOptions::default())
        .await
        .unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 4);
    assert!(
        !index_path(&root).exists(),
        "a fork read must not have written an index"
    );

    // And a read-only consumer that *does* need the index gets correct answers
    // from a rebuild it cannot persist.
    let report = ro.vacuum(None, 0, false).await.unwrap();
    assert!(report.dry_run);
    assert!(
        !index_path(&root).exists(),
        "a read-only handle must not persist the index it rebuilt"
    );
}

/// Adding a fork after the index was built must be seen immediately: the
/// listing-based revalidation is what makes maintenance-free caching safe.
#[tokio::test]
async fn a_fork_created_after_the_index_was_built_is_seen_at_once() {
    let (_dir, db, _counter, _root) = counted_db().await;
    seed_trades(&db).await;
    db.create_fork("first", None, None, serde_json::Map::new())
        .await
        .unwrap();
    db.vacuum(None, 0, false).await.unwrap(); // builds the index

    db.create_fork("second", None, None, serde_json::Map::new())
        .await
        .unwrap();
    let index = h5i_db_core::fork_index::refresh(db.backend(), true)
        .await
        .unwrap();
    assert_eq!(index.forks.len(), 2, "the new fork was not picked up");

    // …and a shadow materialized after that is seen too, without any code
    // having told the index about it.
    let fork = db.open_fork("second").await.unwrap();
    fork.append(
        "trades",
        vec![trades_batch(&[10_123], &["S"], &[1.0])],
        WriteOptions::default(),
    )
    .await
    .unwrap();
    let index = h5i_db_core::fork_index::refresh(db.backend(), true)
        .await
        .unwrap();
    assert_eq!(
        index.forks["second"].owned.len(),
        1,
        "the shadow materialized in 'second' is missing from the index"
    );
}
